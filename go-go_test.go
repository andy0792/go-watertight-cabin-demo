package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

// 简单：Reset重置
func TestBusinessCabin_Reset(t *testing.T) {
	cabin := NewCabin("reset‑test", 5, NewCircuitBreaker(50, 10, 1*time.Second))
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
	cb := NewCircuitBreaker(50, 10, 200*time.Millisecond)
	for i := 0; i < 3; i++ {
		cb.OnResult(true)
	}
	cb.OnResult(false)
	if cb.GetState() != StateOpen {
		t.Errorf("expect StateOpen, got %s", cb.GetState())
	}
}

// 熔断器 Open 冷却后切换 HalfOpen
func TestCircuitBreaker_OpenToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(50, 10, 100*time.Millisecond)
	cb.state.Store(int32(StateOpen))
	cb.lastOpenTime.Store(time.Now().UnixMilli())
	if cb.Allow() {
		t.Error("冷却期内Open状态应该不允许访问")
	}
	time.Sleep(110 * time.Millisecond)
	ok := cb.Allow()
	if !ok {
		t.Error("冷却时间过后应该切换HalfOpen并允许访问")
	}
	if cb.GetState() != StateHalfOpen {
		t.Errorf("expect HalfOpen got %s", cb.GetState())
	}
}

// HalfOpen状态测试
func TestCircuitBreaker_HalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(50, 10, 100*time.Millisecond)
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
	cabin := NewCabin("cb‑reject‑test", 10, NewCircuitBreaker(50, 10, 1*time.Second))
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
	cabin := NewCabin("limit‑reject‑test", 0, NewCircuitBreaker(50, 10, 1*time.Second))
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
	cb := NewCircuitBreaker(50, 40, 1*time.Second)
	marketingCabin := NewCabin("营销舱", 20, cb)
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
	cb := NewCircuitBreaker(50, 40, 1*time.Second)
	marketingCabin := NewCabin("营销舱", 20, cb)
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
	cb := NewCircuitBreaker(20, 10, 1*time.Second)
	oldCabin := NewCabin("营销舱", 20, cb)
	oldCabin.fallback = testMarketingFallback
	// 模拟apiApplyConfig重建舱（BUG版本：忘记赋值 newCabin.fallback）
	newCb := NewCircuitBreaker(20, 10, 1*time.Second)
	newCabin := NewCabin("营销舱", 100, newCb)
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

// TestApiStatus 接口测试：单独测试 /api/status
func TestApiStatus(t *testing.T) {
	resetFaultConfig()
	// 重置全局舱，清除历史状态
	cabinOrder.Reset()
	cabinMarketing.Reset()
	cabinReport.Reset()
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	apiStatus(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", resp.StatusCode)
	}
	var payload struct {
		Cabins []CabinView `json:"cabins"`
	}
	err := json.NewDecoder(resp.Body).Decode(&payload)
	if err != nil {
		t.Fatalf("json decode failed: %v", err)
	}
	if len(payload.Cabins) != 3 {
		t.Fatalf("expect 3 cabins, got %d", len(payload.Cabins))
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

// TestApiReset 接口测试：测试 /api/reset 重置接口
func TestApiReset(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/reset", nil)
	rec := httptest.NewRecorder()
	apiReset(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reset api expect 200, got %d", resp.StatusCode)
	}
	// reset接口调用后，校验熔断器回到Closed
	if cabinOrder.cb.GetState() != StateClosed {
		t.Error("after apiReset, cabinOrder should be Closed")
	}
}

// TestApiClearLogs 测试新增接口 /api/clearLogs 仅清空日志，不改动舱状态指标
func TestApiClearLogs(t *testing.T) {
	//先写一点日志
	onEventLog("测试日志1")
	onEventLog("测试日志2")

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

	//校验舱状态不受影响：熔断器状态、metrics完全不变
	if cabinOrder.cb.GetState() != StateClosed {
		t.Error("执行clearLogs不应该改变熔断器状态")
	}
}

// TestFullFlow_Integration 集成测试：完整模拟网页整套操作流程
// reset → runFirst触发熔断 → waitHalfOpen冷却 → runSecond半开试探 → status校验结果
func TestFullFlow_Integration(t *testing.T) {
	resetFaultConfig()
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
	// 5.拉取状态接口，校验最终数据
	reqStatus := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	recStatus := httptest.NewRecorder()
	apiStatus(recStatus, reqStatus)
	var payload struct {
		Cabins []CabinView `json:"cabins"`
	}
	err := json.NewDecoder(recStatus.Result().Body).Decode(&payload)
	if err != nil {
		t.Fatalf("decode status resp err: %v", err)
	}
	// 简单校验：拿到三个舱，不为空
	if len(payload.Cabins) != 3 {
		t.Fatalf("expect 3 cabins, got %d", len(payload.Cabins))
	}
	var marketingCabinView CabinView
	for _, v := range payload.Cabins {
		if v.Name == "营销舱" {
			marketingCabinView = v
		}
	}
	t.Logf("集成测试结束，营销舱状态=%s，降级次数=%d",
		marketingCabinView.State, marketingCabinView.Metrics.DegradeCount)
	resetFaultConfig()
}
