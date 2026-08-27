package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// resetFaultConfig 清理全局故障配置，测试隔离用
func resetFaultConfig() {
	faultMu.Lock()
	defer faultMu.Unlock()
	faultConfig["营销舱"] = &CabinFaultConfig{Enable: false, FaultPercent: 0}
	faultConfig["报表辅助舱"] = &CabinFaultConfig{Enable: false, FaultPercent: 0}
}

// resetTimelineEvents 重置时序事件，测试隔离
func resetTimelineEvents() {
	timelineMu.Lock()
	defer timelineMu.Unlock()
	globalTimelineEvents = []TimelineEvent{}
}

func TestLogTimelineEvent(t *testing.T) {
	resetTimelineEvents()
	// 写入一条事件
	logTimelineEvent("测试舱", "normal", "测试消息1")

	timelineMu.Lock()
	l := len(globalTimelineEvents)
	evt := globalTimelineEvents[0]
	timelineMu.Unlock()

	if l != 1 {
		t.Fatalf("期望事件数量1，实际 %d", l)
	}
	if evt.Cabin != "测试舱" || evt.EvType != "normal" || evt.Message != "测试消息1" {
		t.Error("logTimelineEvent 字段写入不正确")
	}

	// 测试超过600条自动截断
	resetTimelineEvents()
	for i := 0; i < 650; i++ {
		logTimelineEvent("A", "normal", "msg")
	}
	timelineMu.Lock()
	afterLen := len(globalTimelineEvents)
	timelineMu.Unlock()
	if afterLen != 600 {
		t.Errorf("超过600应截断为600，实际=%d", afterLen)
	}
}

// 简单：Reset重置
func TestBusinessCabin_Reset(t *testing.T) {
	cabin := NewCabin("reset‑test", 5, NewCircuitBreaker(50, 10, 200*time.Millisecond, 3), 0, 0)
	_ = cabin.Run(context.Background(), func(ctx context.Context) error { return errors.New("err") })
	_ = cabin.Run(context.Background(), func(ctx context.Context) error { return nil })
	cabin.Reset()
	if cabin.metrics.TotalTask != 0 || cabin.metrics.Success != 0 || cabin.metrics.BusinessError != 0 {
		t.Error("Reset后metrics没有全部清零")
	}
	if cabin.cb.GetState() != StateClosed {
		t.Error("Reset后熔断器状态不是Closed")
	}
}

// 熔断器 Closed -> Open
func TestCircuitBreaker_ClosedToOpen(t *testing.T) {
	cb := NewCircuitBreaker(50, 10, 200*time.Millisecond, 3)
	for i := 0; i < 3; i++ {
		cb.OnResult(true)
	}
	cb.OnResult(false)
	if cb.GetState() != StateOpen {
		t.Errorf("expect StateOpen, got %s", cb.GetState())
	}
}

// 熔断器 Open 冷却后切换 HalfOpen
// 熔断器 Open 冷却后切换 HalfOpen
func TestCircuitBreaker_OpenToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(50, 10, 100*time.Millisecond, 3)
	cb.state.Store(int32(StateOpen))
	cb.lastOpenTime.Store(time.Now().UnixMilli())

	// 冷却期内调用Allow，应当返回false
	if cb.Allow() {
		t.Error("冷却期内Open状态应该不允许访问")
	}

	// 等待超过冷却时间
	time.Sleep(110 * time.Millisecond)

	// ✅第一次调用Allow：内部把Open→HalfOpen，但是返回false
	first := cb.Allow()
	if first != false {
		t.Error("时间到后的第一次Allow调用，应当返回false（本次拒绝，仅切换状态）")
	}
	if cb.GetState() != StateHalfOpen {
		t.Error("第一次Allow调用后，状态必须切换为HalfOpen")
	}

	// ✅第二次调用Allow，才返回true，允许试探
	second := cb.Allow()
	if !second {
		t.Error("进入HalfOpen之后，第二次Allow应当返回true，允许试探请求")
	}
}

// HalfOpen状态测试
func TestCircuitBreaker_HalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(50, 10, 100*time.Millisecond, 3)
	cb.state.Store(int32(StateHalfOpen))
	cb.OnResult(true)
	if cb.GetState() != StateOpen {
		t.Error("HalfOpen失败应切回Open")
	}
	cb.state.Store(int32(StateHalfOpen))
	cb.OnResult(false)
	if cb.GetState() != StateClosed {
		t.Error("HalfOpen成功应恢复Closed")
	}
}

// 简单ErrGroup
func TestSimpleErrGroup(t *testing.T) {
	ctx := context.Background()
	g := NewSimpleErrGroup(ctx)
	g.Go(func(ctx context.Context) error { return nil })
	err := g.Wait()
	if err != nil {
		t.Errorf("no error expect nil got %v", err)
	}
	g2 := NewSimpleErrGroup(ctx)
	expErr := errors.New("group error")
	g2.Go(func(ctx context.Context) error { return expErr })
	err = g2.Wait()
	if err == nil || !errors.Is(err, expErr) {
		t.Errorf("expect err %v", expErr)
	}
}

// ===========新增：故障注入函数单元测试 shouldProduceFault ==========
func TestShouldProduceFault(t *testing.T) {
	resetFaultConfig()
	// case1: 开关关闭，无论百分比，一定返回false
	faultMu.Lock()
	faultConfig["营销舱"].Enable = false
	faultConfig["营销舱"].FaultPercent = 100
	faultMu.Unlock()
	if shouldProduceFault("营销舱") == true {
		t.Error("开关关闭，不应该产生故障")
	}
	// case2: enable=true，FaultPercent=0 永不故障
	faultMu.Lock()
	faultConfig["营销舱"].Enable = true
	faultConfig["营销舱"].FaultPercent = 0
	faultMu.Unlock()
	if shouldProduceFault("营销舱") == true {
		t.Error("faultPercent=0，不应该产生故障")
	}
	// case3: enable=true，FaultPercent=100 每次必故障
	faultMu.Lock()
	faultConfig["营销舱"].Enable = true
	faultConfig["营销舱"].FaultPercent = 100
	faultMu.Unlock()
	if shouldProduceFault("营销舱") == false {
		t.Error("faultPercent=100，必须返回故障true")
	}
	// case4: 不存在的舱名，返回false
	if shouldProduceFault("不存在舱") == true {
		t.Error("不存在舱名返回false")
	}
	resetFaultConfig()
}

// 业务函数测试，现在依靠全局故障配置控制报错
func TestBizFuncs(t *testing.T) {
	resetFaultConfig()
	cabinOrder.Reset()
	cabinMarketing.Reset()
	cabinReport.Reset()
	ctx := context.Background()
	// 关闭全部故障，bizOrder永远成功
	err := bizOrder(ctx, 1)
	if err != nil {
		t.Fatalf("bizOrder should return nil, err=%v", err)
	}
	// 故障全部关闭，营销舱业务正常返回nil
	err = bizMarketing(ctx, 1)
	if err != nil {
		t.Fatalf("bizMarketing fault off should return nil, err=%v", err)
	}
	// 报表舱故障关闭，业务正常
	err = bizReport(ctx, 1)
	if err != nil {
		t.Fatalf("bizReport fault off should return nil, err=%v", err)
	}
	// 打开报表舱故障100%，调用bizReport必然报错
	faultMu.Lock()
	faultConfig["报表辅助舱"].Enable = true
	faultConfig["报表辅助舱"].FaultPercent = 100
	faultMu.Unlock()
	err = bizReport(ctx, 999)
	if err == nil {
		t.Error("报表舱开启100%故障，bizReport应当返回error")
	}
	resetFaultConfig()
}

// 测试：熔断器打开时，CbReject熔断拒绝计数
func TestBusinessCabin_CbReject(t *testing.T) {
	// 窗口10，失败阈值50%，冷却1秒
	cabin := NewCabin("cb‑reject‑test", 10, NewCircuitBreaker(50, 10, 1*time.Second, 3), 0, 0)
	// 手动置为Open，必须同时设置lastOpenTime！
	nowMs := time.Now().UnixMilli()
	cabin.cb.state.Store(int32(StateOpen))
	cabin.cb.lastOpenTime.Store(nowMs)
	err := cabin.Run(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Fatal("期望返回熔断拒绝错误")
	}
	if cabin.metrics.TotalTask != 1 {
		t.Errorf("TotalTask want=1 got=%d", cabin.metrics.TotalTask)
	}
	if cabin.metrics.CbReject != 1 {
		t.Errorf("CbReject want=1 got=%d", cabin.metrics.CbReject)
	}
}

// 测试：信号量满触发LimitReject限流拒绝
// 技巧：maxConcurrency=0，TryAcquire(1)直接返回false，不需要goroutine占坑！
func TestBusinessCabin_LimitReject(t *testing.T) {
	// maxConcurrency设置为0，信号量直接拿不到
	cabin := NewCabin("limit‑reject‑test", 0, NewCircuitBreaker(50, 10, 1*time.Second, 3), 0, 0)
	err := cabin.Run(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Fatal("期望返回信号量满限流错误")
	}
	if cabin.metrics.TotalTask != 1 {
		t.Errorf("TotalTask want=1 got=%d", cabin.metrics.TotalTask)
	}
	if cabin.metrics.LimitReject != 1 {
		t.Errorf("LimitReject want=1 got=%d", cabin.metrics.LimitReject)
	}
}

// 模拟营销降级兜底函数，和业务代码保持一致
func testMarketingFallback(ctx context.Context) error {
	onEventLog("[营销舱‑降级兜底] 返回缓存活动数据")
	return nil
}

// TestMarketingCabinFallbackOpen 单元测试：熔断器Open状态，请求命中降级
func TestMarketingCabinFallbackOpen(t *testing.T) {
	// 1. 新建营销舱，绑定fallback（模拟main初始化）
	cb := NewCircuitBreaker(50, 40, 1*time.Second, 3)
	marketingCabin := NewCabin("营销舱", 20, cb, 0, 0)
	marketingCabin.fallback = testMarketingFallback // 关键：挂载降级函数
	// 2. 强制把熔断器置为 Open
	marketingCabin.cb.state.Store(int32(StateOpen))
	nowMs := time.Now().UnixMilli()
	marketingCabin.cb.lastOpenTime.Store(nowMs)
	// 3. 发起新请求（Open之后的新请求，模拟网页点按钮）
	err := marketingCabin.Run(context.Background(), func(ctx context.Context) error {
		// 如果走到这里代表没有走降级，直接失败测试
		t.Fatal("错误：熔断器Open，不应该执行真实biz业务函数！")
		return nil
	})
	if err != nil {
		t.Fatalf("降级应该返回nil成功，实际err=%v", err)
	}
	// 校验指标：降级次数 +1
	marketingCabin.mu.Lock()
	degradeCnt := marketingCabin.metrics.DegradeCount
	marketingCabin.mu.Unlock()
	if degradeCnt != 1 {
		t.Fatalf("期望降级次数=1，实际=%d", degradeCnt)
	}
	t.Log("✅测试通过：熔断器Open，成功命中营销舱降级兜底，没有执行业务函数")
}

// TestMarketingCabinClosed 对照测试：熔断器Closed正常执行业务，不走降级
func TestMarketingCabinClosed(t *testing.T) {
	cb := NewCircuitBreaker(50, 40, 1*time.Second, 3)
	marketingCabin := NewCabin("营销舱", 20, cb, 0, 0)
	marketingCabin.fallback = testMarketingFallback
	bizCalled := false
	err := marketingCabin.Run(context.Background(), func(ctx context.Context) error {
		bizCalled = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bizCalled {
		t.Fatal("Closed状态下，应该执行真实业务函数")
	}
	marketingCabin.mu.Lock()
	degradeCnt := marketingCabin.metrics.DegradeCount
	marketingCabin.mu.Unlock()
	if degradeCnt != 0 {
		t.Fatalf("Closed状态不应该触发降级，降级次数=%d", degradeCnt)
	}
	t.Log("✅测试通过：Closed状态正常执行业务，未触发降级")
}

// TestCabinReCreateLoseFallback 重点复现Web【应用配置】的bug：重建Cabin忘记赋值fallback
func TestCabinReCreateLoseFallback(t *testing.T) {
	cb := NewCircuitBreaker(20, 10, 1*time.Second, 3)
	oldCabin := NewCabin("营销舱", 20, cb, 0, 0)
	oldCabin.fallback = testMarketingFallback
	// 模拟apiApplyConfig重建舱（BUG版本：忘记赋值 newCabin.fallback）
	newCb := NewCircuitBreaker(20, 10, 1*time.Second, 3)
	newCabin := NewCabin("营销舱", 100, newCb, 0, 0)
	// newCabin.fallback = testMarketingFallback  // 故意注释掉，模拟网页bug
	nowMs := time.Now().UnixMilli()
	newCabin.cb.state.Store(int32(StateOpen))
	newCabin.cb.lastOpenTime.Store(nowMs)
	// Open状态发起请求，此时fallback是nil，不会降级，直接熔断拒绝
	err := newCabin.Run(context.Background(), func(ctx context.Context) error {
		t.Fatal("不应该执行业务")
		return nil
	})
	if err == nil {
		t.Fatal("fallback为nil时Open请求应该返回拒绝错误，不是nil")
	}
	t.Logf("✅复现网页BUG成功，err=%v，fallback丢失，降级完全失效", err)
}

// =========接口测试==========
// TestApiStatus 接口测试：单独测试 /api/status，校验新增events字段
func TestApiStatus(t *testing.T) {
	resetFaultConfig()
	resetTimelineEvents()
	// 重置全局舱，清除历史状态
	cabinOrder.Reset()
	cabinMarketing.Reset()
	cabinReport.Reset()

	// 写入一条测试时序事件
	logTimelineEvent("测试舱", "normal", "接口测试事件")

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	apiStatus(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", resp.StatusCode)
	}

	var payload struct {
		Cabins []CabinView     `json:"cabins"`
		Events []TimelineEvent `json:"events"`
	}
	err := json.NewDecoder(resp.Body).Decode(&payload)
	if err != nil {
		t.Fatalf("json decode failed: %v", err)
	}
	if len(payload.Cabins) != 3 {
		t.Fatalf("expect 3 cabins, got %d", len(payload.Cabins))
	}
	if len(payload.Events) != 1 {
		t.Errorf("apiStatus应返回events数组，期望1条，实际=%d", len(payload.Events))
	}
	// 校验FaultConfig字段存在
	for _, v := range payload.Cabins {
		if v.Name == "营销舱" && v.FaultConfig == nil {
			t.Error("营销舱FaultConfig不应该为nil")
		}
	}
}

// TestApiSetFault 新增接口测试：/api/setFault 设置故障注入
func TestApiSetFault(t *testing.T) {
	resetFaultConfig()
	// 正常设置：营销舱 enable=true faultPercent=100
	bodyJson, _ := json.Marshal(map[string]any{
		"cabin_name":    "营销舱",
		"enable":        true,
		"fault_percent": 100,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/setFault", bytes.NewBuffer(bodyJson))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	apiSetFault(rec, req)
	if rec.Result().StatusCode != 200 {
		t.Fatalf("正常设置故障，期望200，got=%d", rec.Result().StatusCode)
	}
	faultMu.Lock()
	cfg := faultConfig["营销舱"]
	faultMu.Unlock()
	if !cfg.Enable || cfg.FaultPercent != 100 {
		t.Error("api setFault 配置没有生效")
	}
	// 测试非法百分比 101
	badBody, _ := json.Marshal(map[string]any{
		"cabin_name":    "营销舱",
		"enable":        true,
		"fault_percent": 101,
	})
	req2 := httptest.NewRequest(http.MethodPost, "/api/setFault", bytes.NewBuffer(badBody))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	apiSetFault(rec2, req2)
	if rec2.Result().StatusCode != 400 {
		t.Error("fault_percent=101应该返回400")
	}
	// 无效舱名
	badBody2, _ := json.Marshal(map[string]any{
		"cabin_name":    "不存在舱",
		"enable":        true,
		"fault_percent": 50,
	})
	req3 := httptest.NewRequest(http.MethodPost, "/api/setFault", bytes.NewBuffer(badBody2))
	req3.Header.Set("Content-Type", "application/json")
	rec3 := httptest.NewRecorder()
	apiSetFault(rec3, req3)
	if rec3.Result().StatusCode != 400 {
		t.Error("无效舱名应该返回400")
	}
	resetFaultConfig()
}

// TestApiReset 接口测试：测试 /api/reset 重置接口，校验时序事件被清空
func TestApiReset(t *testing.T) {
	resetTimelineEvents()
	// 先写入事件
	logTimelineEvent("A", "normal", "重置前事件")

	req := httptest.NewRequest(http.MethodPost, "/api/reset", nil)
	rec := httptest.NewRecorder()
	apiReset(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reset api expect 200, got %d", resp.StatusCode)
	}
	// 校验时序事件数组已清空
	timelineMu.Lock()
	evLen := len(globalTimelineEvents)
	timelineMu.Unlock()
	if evLen != 0 {
		t.Errorf("apiReset应当清空globalTimelineEvents，实际长度=%d", evLen)
	}

	// reset接口调用后，校验熔断器回到Closed
	if cabinOrder.cb.GetState() != StateClosed {
		t.Error("after apiReset, cabinOrder should be Closed")
	}
}

// TestApiClearLogs 测试新增接口 /api/clearLogs 仅清空日志，不改动舱状态指标
// 同时校验时序事件数组被清空
func TestApiClearLogs(t *testing.T) {
	resetTimelineEvents()
	//先写一点日志与时序事件
	onEventLog("测试日志1")
	logTimelineEvent("测试舱", "normal", "clearLogs测试事件")

	req := httptest.NewRequest(http.MethodPost, "/api/clearLogs", nil)
	rec := httptest.NewRecorder()
	apiClearLogs(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("apiClearLogs expect 200 got %d", resp.StatusCode)
	}

	logMu.Lock()
	l := len(globalLogs)
	logMu.Unlock()
	// clearLogs内部会生成一条事件日志："日志已手动清空"，所以长度应为1
	if l != 1 {
		t.Errorf("globalLogs 清空后预期剩余1条提示日志，当前长度=%d", l)
	}

	//校验时序事件数组清空
	timelineMu.Lock()
	evLen := len(globalTimelineEvents)
	timelineMu.Unlock()
	if evLen != 0 {
		t.Errorf("apiClearLogs应当清空globalTimelineEvents，实际长度=%d", evLen)
	}

	//校验舱状态不受影响：熔断器状态、metrics完全不变
	if cabinOrder.cb.GetState() != StateClosed {
		t.Error("执行clearLogs不应该改变熔断器状态")
	}
}

// TestFullFlow_Integration 集成测试：完整模拟网页整套操作流程
// reset → runFirst触发熔断 → waitHalfOpen冷却 → runSecond半开试探 → status校验结果
func TestFullFlow_Integration(t *testing.T) {
	resetFaultConfig()
	resetTimelineEvents()
	// 1.重置全部舱
	reqReset := httptest.NewRequest(http.MethodPost, "/api/reset", nil)
	recReset := httptest.NewRecorder()
	apiReset(recReset, reqReset)
	if recReset.Result().StatusCode != http.StatusOK {
		t.Fatal("api reset failed")
	}
	// 2.第一轮批量请求，触发熔断
	reqRun1 := httptest.NewRequest(http.MethodPost, "/api/runFirst", nil)
	recRun1 := httptest.NewRecorder()
	apiRunFirst(recRun1, reqRun1)
	if recRun1.Result().StatusCode != http.StatusOK {
		t.Fatal("api runFirst failed")
	}
	// 3.等待熔断恢复窗口期
	reqWait := httptest.NewRequest(http.MethodPost, "/api/waitHalfOpen", nil)
	recWait := httptest.NewRecorder()
	apiWaitHalfOpen(recWait, reqWait)
	if recWait.Result().StatusCode != http.StatusOK {
		t.Fatal("api waitHalfOpen failed")
	}
	// 4.第二轮半开试探请求
	reqRun2 := httptest.NewRequest(http.MethodPost, "/api/runSecond", nil)
	recRun2 := httptest.NewRecorder()
	apiRunSecond(recRun2, reqRun2)
	if recRun2.Result().StatusCode != http.StatusOK {
		t.Fatal("api runSecond failed")
	}
	// 5.拉取状态接口，校验返回events不为空（产生了时序事件）
	reqStatus := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	recStatus := httptest.NewRecorder()
	apiStatus(recStatus, reqStatus)

	var payload struct {
		Cabins []CabinView     `json:"cabins"`
		Events []TimelineEvent `json:"events"`
	}
	err := json.NewDecoder(recStatus.Result().Body).Decode(&payload)
	if err != nil {
		t.Fatalf("decode status resp err: %v", err)
	}
	// 简单校验：拿到三个舱，不为空
	if len(payload.Cabins) != 3 {
		t.Fatalf("expect 3 cabins, got %d", len(payload.Cabins))
	}
	if len(payload.Events) == 0 {
		t.Error("集成测试：/api/status events数组不应该为空，业务执行需要产生时序事件")
	}

	var marketingCabinView CabinView
	for _, v := range payload.Cabins {
		if v.Name == "营销舱" {
			marketingCabinView = v
		}
	}
	t.Logf("集成测试结束，营销舱状态=%s，降级次数=%d，时序事件数量=%d",
		marketingCabinView.State, marketingCabinView.Metrics.DegradeCount, len(payload.Events))
	resetFaultConfig()
}

// =========== 新增：覆盖 P0-2 / 报错池 / 熔断切换日志 / 报错详情 的针对性测试 ===========
// TestApiStatusOpenRemainMs 校验 /api/status 新增 cb_open_remain_ms 字段
func TestApiStatusOpenRemainMs(t *testing.T) {
	resetFaultConfig()
	resetTimelineEvents()
	cabinOrder.Reset()
	cabinMarketing.Reset()
	cabinReport.Reset()
	// 非Open状态：所有舱 remain 应为 0
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	apiStatus(rec, req)
	var payload struct {
		Cabins []CabinView `json:"cabins"`
	}
	if err := json.NewDecoder(rec.Result().Body).Decode(&payload); err != nil {
		t.Fatalf("decode err: %v", err)
	}
	for _, v := range payload.Cabins {
		if v.CbOpenRemainMs != 0 {
			t.Errorf("非Open状态 cb_open_remain_ms 应为0，%s got %d", v.Name, v.CbOpenRemainMs)
		}
	}
	// 手动置报表舱为Open：cb_open_remain_ms 应 > 0
	cabinReport.cb.state.Store(int32(StateOpen))
	cabinReport.cb.lastOpenTime.Store(time.Now().UnixMilli())
	req2 := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec2 := httptest.NewRecorder()
	apiStatus(rec2, req2)
	var payload2 struct {
		Cabins []CabinView `json:"cabins"`
	}
	if err := json.NewDecoder(rec2.Result().Body).Decode(&payload2); err != nil {
		t.Fatalf("decode err: %v", err)
	}
	for _, v := range payload2.Cabins {
		if v.Name == "报表辅助舱" && v.CbOpenRemainMs <= 0 {
			t.Errorf("Open状态 cb_open_remain_ms 应>0，got %d", v.CbOpenRemainMs)
		}
	}
	cabinReport.cb.state.Store(int32(StateClosed))
}
// TestRandomBizErrorPool 校验真实世界风格报错池：命中模板、带请求ID
func TestRandomBizErrorPool(t *testing.T) {
	for _, cabin := range []string{"下单核心舱", "营销舱", "报表辅助舱"} {
		for i := 0; i < 5; i++ {
			err := randomBizError(cabin, 888)
			if err == nil {
				t.Fatalf("%s randomBizError 不应返回nil", cabin)
			}
			if !strings.Contains(err.Error(), "888") {
				t.Errorf("%s 报错应包含请求ID 888，got %q", cabin, err.Error())
			}
		}
	}
	if err := randomBizError("不存在舱", 1); err == nil {
		t.Error("未知舱名应返回兜底内部错误")
	}
}
// TestCircuitBreaker_TransitionLogOnce 校验熔断切换点日志 + CAS防重
func TestCircuitBreaker_TransitionLogOnce(t *testing.T) {
	// Closed→Open：日志应带窗口失败率且只记录一次
	resetTimelineEvents()
	cb := NewCircuitBreaker(30, 20, 100*time.Millisecond, 3)
	for i := 0; i < 25; i++ {
		cb.OnResult(true)
	}
	timelineMu.Lock()
	openCnt := 0
	for _, e := range globalTimelineEvents {
		if e.EvType == "open" && strings.Contains(e.Message, "熔断器切换 Open（熔断打开）") {
			openCnt++
			if !strings.Contains(e.Message, "窗口失败率") {
				t.Errorf("Closed→Open日志应带失败率，got %q", e.Message)
			}
		}
	}
	timelineMu.Unlock()
	if openCnt != 1 {
		t.Errorf("Closed→Open切换日志应恰好1条，got %d", openCnt)
	}
	// Open→HalfOpen：并发调用Allow，切换日志应只记录一次
	resetTimelineEvents()
	cb2 := NewCircuitBreaker(30, 20, 50*time.Millisecond, 3)
	cb2.state.Store(int32(StateOpen))
	cb2.lastOpenTime.Store(time.Now().UnixMilli())
	time.Sleep(60 * time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cb2.Allow() }()
	}
	wg.Wait()
	timelineMu.Lock()
	halfOpenCnt := 0
	for _, e := range globalTimelineEvents {
		if e.EvType == "halfOpen" && strings.Contains(e.Message, "熔断冷却期结束，进入 HalfOpen 半开试探") {
			halfOpenCnt++
		}
	}
	timelineMu.Unlock()
	if halfOpenCnt != 1 {
		t.Errorf("Open→HalfOpen切换日志应恰好1条（CAS防重），got %d", halfOpenCnt)
	}
}
// TestRunErrorLogDetail 校验Run报错日志带上真实错误详情（报错池改造）
func TestRunErrorLogDetail(t *testing.T) {
	resetTimelineEvents()
	cabin := NewCabin("日志测试舱", 10, NewCircuitBreaker(50, 10, 1*time.Second, 3), 0, 0)
	realErr := errors.New("HBase region 不可用：行键查询失败 (report=9, table=rpt_daily, region=rs-02)")
	_ = cabin.Run(context.Background(), func(ctx context.Context) error { return realErr })
	timelineMu.Lock()
	found := false
	for _, e := range globalTimelineEvents {
		if e.EvType == "error" && strings.Contains(e.Message, realErr.Error()) {
			found = true
		}
	}
	timelineMu.Unlock()
	if !found {
		t.Error("Run报错日志应包含真实错误详情")
	}
}

// =========== 新增：可切换统计窗口（fixed/sliding）针对性测试 ===========
// TestSlidingWindow_Basic 校验滑动窗口记录与统计
func TestSlidingWindow_Basic(t *testing.T) {
	sw := NewSlidingWindow(10)
	sw.Record(false)
	sw.Record(false)
	sw.Record(true)
	succ, fail := sw.Stats()
	if succ != 2 || fail != 1 {
		t.Errorf("期望 succ=2 fail=1，got succ=%d fail=%d", succ, fail)
	}
}

// TestSlidingWindow_Expire 校验超出窗口时长的旧桶被忽略（时间衰减）
func TestSlidingWindow_Expire(t *testing.T) {
	sw := NewSlidingWindow(10) // 10s 窗口，10 桶，每桶 1s
	sw.Record(false)           // 记录一次失败
	// 把该桶起始时间戳拨到超过整个窗口时长 → 统计时应被忽略
	sw.mu.Lock()
	now := time.Now().UnixMilli()
	for i := range sw.buckets {
		if sw.buckets[i].startMs != 0 {
			sw.buckets[i].startMs = now - 11*1000 // 超过10s窗口
		}
	}
	sw.mu.Unlock()
	succ, fail := sw.Stats()
	if succ != 0 || fail != 0 {
		t.Errorf("过期桶应被忽略，got succ=%d fail=%d", succ, fail)
	}
}

// TestCircuitBreaker_SlidingTrip 校验滑动窗口达到最少样本后按失败率熔断
func TestCircuitBreaker_SlidingTrip(t *testing.T) {
	cb := NewCircuitBreakerWindowType("sliding", 50, 5, 100*time.Millisecond, 3, 10)
	// 4失败 + 1成功：总样本5>=5，失败率80%>=50% → Open
	for i := 0; i < 4; i++ {
		cb.OnResult(true)
	}
	cb.OnResult(false)
	if cb.GetState() != StateOpen {
		t.Errorf("期望 Open，got %s", cb.GetState())
	}
}

// TestCircuitBreaker_SlidingNoTripFewSamples 校验样本不足不熔断（防单点误判）
func TestCircuitBreaker_SlidingNoTripFewSamples(t *testing.T) {
	cb := NewCircuitBreakerWindowType("sliding", 50, 10, 100*time.Millisecond, 3, 10)
	// 5失败，总样本5<10，未达最少样本门槛 → 即使100%失败率也不熔断
	for i := 0; i < 5; i++ {
		cb.OnResult(true)
	}
	if cb.GetState() != StateClosed {
		t.Errorf("样本不足不应熔断，got %s", cb.GetState())
	}
}

// TestCircuitBreaker_SlidingHalfOpen 校验滑动窗口下半开状态切换仍正常
func TestCircuitBreaker_SlidingHalfOpen(t *testing.T) {
	cb := NewCircuitBreakerWindowType("sliding", 50, 5, 100*time.Millisecond, 3, 10)
	cb.state.Store(int32(StateHalfOpen))
	cb.OnResult(false) // 半开试探成功 → Closed
	if cb.GetState() != StateClosed {
		t.Error("sliding模式HalfOpen成功应恢复Closed")
	}
	cb.state.Store(int32(StateHalfOpen))
	cb.OnResult(true) // 半开试探失败 → Open
	if cb.GetState() != StateOpen {
		t.Error("sliding模式HalfOpen失败应切回Open")
	}
}

// TestApiApplyConfigWindowType 校验 applyConfig 切换窗口类型 + status 回显 + 滑动窗口统计来源
func TestApiApplyConfigWindowType(t *testing.T) {
	payload := `{"index":2,"cfg":{"max_concurrency":8,"fail_threshold":50,"window_size":5,"open_wait_sec":1,"half_open_max_probe":2,"normal_delay_ms":0,"fault_delay_ms":0,"window_type":"sliding","sliding_window_sec":10}}`
	req := httptest.NewRequest(http.MethodPost, "/api/applyConfig", bytes.NewBufferString(payload))
	rec := httptest.NewRecorder()
	apiApplyConfig(rec, req)
	if rec.Code != 200 {
		t.Fatalf("applyConfig 失败: %d", rec.Code)
	}
	// status 回显窗口类型
	req2 := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec2 := httptest.NewRecorder()
	apiStatus(rec2, req2)
	var payload2 struct {
		Cabins []CabinView `json:"cabins"`
	}
	if err := json.NewDecoder(rec2.Result().Body).Decode(&payload2); err != nil {
		t.Fatalf("decode err: %v", err)
	}
	var report *CabinView
	for i := range payload2.Cabins {
		if payload2.Cabins[i].Name == "报表辅助舱" {
			report = &payload2.Cabins[i]
		}
	}
	if report == nil {
		t.Fatal("未找到报表辅助舱")
	}
	if report.Config.WindowType != "sliding" {
		t.Errorf("window_type 应为 sliding，got %q", report.Config.WindowType)
	}
	if report.Config.SlidingWindowSec != 10 {
		t.Errorf("sliding_window_sec 应为10，got %d", report.Config.SlidingWindowSec)
	}
	// 滑动窗口模式下写入结果，cb_window 统计应来自滑动窗口
	cabinReport.cb.OnResult(false)
	cabinReport.cb.OnResult(false)
	cabinReport.cb.OnResult(true)
	req3 := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec3 := httptest.NewRecorder()
	apiStatus(rec3, req3)
	var payload3 struct {
		Cabins []CabinView `json:"cabins"`
	}
	if err := json.NewDecoder(rec3.Result().Body).Decode(&payload3); err != nil {
		t.Fatalf("decode err: %v", err)
	}
	for _, v := range payload3.Cabins {
		if v.Name == "报表辅助舱" {
			if v.CbWindowSuccess != 2 || v.CbWindowFail != 1 {
				t.Errorf("滑动窗口统计应 succ=2 fail=1，got succ=%d fail=%d", v.CbWindowSuccess, v.CbWindowFail)
			}
		}
	}
	// 复位报表舱回默认（fixed），避免影响后续测试
	cabinReport = NewCabin("报表辅助舱", 8, NewCircuitBreaker(40, 30, 1*time.Second, 3), 0, 0)
}

// TestFullFlow_SlidingWindow 滑动窗口模式完整流程：故障注入→滑动窗口统计→熔断
func TestFullFlow_SlidingWindow(t *testing.T) {
	resetFaultConfig()
	resetTimelineEvents()
	cabinOrder.Reset()
	cabinMarketing.Reset()
	cabinReport.Reset()
	// 报表舱：滑动窗口 window_size=5 阈值50，100%故障，并发放足让25个请求全部执行
	faultMu.Lock()
	faultConfig["报表辅助舱"] = &CabinFaultConfig{Enable: true, FaultPercent: 100}
	faultMu.Unlock()
	cabinReport = NewCabin("报表辅助舱", 30, NewCircuitBreakerWindowType("sliding", 50, 5, 2*time.Second, 3, 10), 0, 50)
	// 跑第一轮：25个请求全部失败 → 滑动窗口样本25>=5 失败率100% → 熔断Open
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runFirst", nil)
	apiRunFirst(rec, req)
	if cabinReport.cb.GetState() != StateOpen {
		t.Errorf("滑动窗口下高失败率应熔断Open，got %s", cabinReport.cb.GetState())
	}
	succ, fail := cabinReport.cb.WindowStats()
	if succ != 0 || fail < 20 {
		t.Errorf("滑动窗口统计应 成功=0 失败>=20，got succ=%d fail=%d", succ, fail)
	}
	// 复位报表舱与故障
	cabinReport = NewCabin("报表辅助舱", 8, NewCircuitBreaker(40, 30, 1*time.Second, 3), 0, 0)
	resetFaultConfig()
}

// TestApiApplyConfigEmptyWindowType 校验 window_type 缺省按 fixed 处理（兼容场景按钮旧配置）
func TestApiApplyConfigEmptyWindowType(t *testing.T) {
	payload := `{"index":0,"cfg":{"max_concurrency":20,"fail_threshold":50,"window_size":40,"open_wait_sec":1,"half_open_max_probe":2,"normal_delay_ms":100,"fault_delay_ms":800}}`
	req := httptest.NewRequest(http.MethodPost, "/api/applyConfig", bytes.NewBufferString(payload))
	rec := httptest.NewRecorder()
	apiApplyConfig(rec, req)
	if rec.Code != 200 {
		t.Fatalf("无window_type的配置应200，got %d: %s", rec.Code, rec.Body.String())
	}
	req2 := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec2 := httptest.NewRecorder()
	apiStatus(rec2, req2)
	var payload2 struct {
		Cabins []CabinView `json:"cabins"`
	}
	if err := json.NewDecoder(rec2.Result().Body).Decode(&payload2); err != nil {
		t.Fatalf("decode err: %v", err)
	}
	for _, c := range payload2.Cabins {
		if c.Name == "下单核心舱" && c.Config.WindowType != "fixed" {
			t.Errorf("缺省 window_type 应为 fixed，got %q", c.Config.WindowType)
		}
	}
}

// TestApiApplyConfigInvalidWindowType 校验非法 window_type 返回400
func TestApiApplyConfigInvalidWindowType(t *testing.T) {
	payload := `{"index":0,"cfg":{"max_concurrency":20,"fail_threshold":50,"window_size":40,"open_wait_sec":1,"half_open_max_probe":2,"normal_delay_ms":0,"fault_delay_ms":0,"window_type":"xxx","sliding_window_sec":10}}`
	req := httptest.NewRequest(http.MethodPost, "/api/applyConfig", bytes.NewBufferString(payload))
	rec := httptest.NewRecorder()
	apiApplyConfig(rec, req)
	if rec.Code != 400 {
		t.Errorf("非法 window_type 应400，got %d", rec.Code)
	}
}
