package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	colorRed   = "\033[31m"
	colorGreen = "\033[32m"
	colorCyan  = "\033[36m"
	colorReset = "\033[0m"
)

type CabinConfig struct {
	MaxConcurrency   int64  `json:"max_concurrency"`
	FailThreshold    uint64 `json:"fail_threshold"`
	WindowSize       uint64 `json:"window_size"`
	OpenWaitSec      int    `json:"open_wait_sec"`
	HalfOpenMaxProbe uint64 `json:"half_open_max_probe"` //新增：半开最大试探请求数
	NormalDelayMs    int    `json:"normal_delay_ms"`     //✅新增：正常情况延时ms
	FaultDelayMs     int    `json:"fault_delay_ms"`      //✅新增：故障情况延时ms
}

type CabinMetrics struct {
	TotalTask     int64 `json:"total_task"`
	Success       int64 `json:"success"`
	LimitReject   int64 `json:"limit_reject"`
	CbReject      int64 `json:"cb_reject"`
	BusinessError int64 `json:"business_error"`
	DegradeCount  int64 `json:"degrade_count"`
}

type State int32

const (
	StateClosed   State = 0
	StateOpen     State = 1
	StateHalfOpen State = 2
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "Closed"
	case StateOpen:
		return "Open"
	case StateHalfOpen:
		return "HalfOpen"
	default:
		return "Unknown"
	}
}

type CircuitBreaker struct {
	state               atomic.Int32
	successCount        atomic.Uint64
	failureCount        atomic.Uint64
	thresholdFailurePct uint64
	windowSize          uint64
	openWaitDuration    time.Duration
	lastOpenTime        atomic.Int64
	//====新增：半开试探配额控制====
	halfOpenMaxProbe  uint64        // 半开最大允许试探请求数
	halfOpenProbeUsed atomic.Uint64 // 当前半开已经放行试探请求计数
}

func NewCircuitBreaker(failurePct uint64, window uint64, openWait time.Duration, halfOpenMaxProbe uint64) *CircuitBreaker {
	cb := &CircuitBreaker{
		thresholdFailurePct: failurePct,
		windowSize:          window,
		openWaitDuration:    openWait,
		halfOpenMaxProbe:    halfOpenMaxProbe, //半开最大试探配额
	}
	cb.state.Store(int32(StateClosed))
	cb.halfOpenProbeUsed.Store(0)
	return cb
}

func (cb *CircuitBreaker) GetState() State {
	return State(cb.state.Load())
}

func (cb *CircuitBreaker) Allow() bool {
	st := State(cb.state.Load())
	switch st {
	case StateClosed:
		return true
	case StateOpen:
		elapsed := time.Since(time.UnixMilli(cb.lastOpenTime.Load()))
		if elapsed >= cb.openWaitDuration {
			// 从Open切换进入HalfOpen：重置试探已使用计数器
			cb.halfOpenProbeUsed.Store(0)
			cb.state.Store(int32(StateHalfOpen))
			// 切换后的第一个请求走下面HalfOpen分支做配额判断
		}
		return false

	case StateHalfOpen:
		used := cb.halfOpenProbeUsed.Load()
		if used < cb.halfOpenMaxProbe {
			// 还有剩余试探配额，放行，计数+1
			cb.halfOpenProbeUsed.Add(1)
			return true
		}
		// 试探配额耗尽，直接拒绝
		return false
	default:
		return false
	}
}

func (cb *CircuitBreaker) OnResult(isFail bool) {
	succ := cb.successCount.Load()
	fail := cb.failureCount.Load()
	total := succ + fail
	if total >= cb.windowSize {
		cb.successCount.Store(0)
		cb.failureCount.Store(0)
		succ, fail, total = 0, 0, 0
	}
	if isFail {
		cb.failureCount.Add(1)
		fail++
	} else {
		cb.successCount.Add(1)
		succ++
	}
	total = succ + fail
	st := State(cb.state.Load())
	switch st {
	case StateClosed:
		if total > 0 {
			failRate := fail * 100 / total
			if failRate >= cb.thresholdFailurePct {
				cb.state.Store(int32(StateOpen))
				cb.lastOpenTime.Store(time.Now().UnixMilli())
				// onEventLog("熔断器切换 Open（熔断打开）")
				logTimelineEvent("熔断器全局", "open", "熔断器切换 Open（熔断打开）") //canvas时序事件图功能
			}
		}
	case StateHalfOpen:
		if isFail {
			cb.state.Store(int32(StateOpen))
			cb.lastOpenTime.Store(time.Now().UnixMilli())
			// onEventLog("半开试探失败，重新切回 Open 熔断")
			logTimelineEvent("熔断器全局", "open", "半开试探失败，重新切回 Open 熔断") //canvas时序事件图功能
		} else {
			cb.state.Store(int32(StateClosed))
			// onEventLog("半开试探成功，恢复 Closed 正常状态")
			logTimelineEvent("熔断器全局", "halfOpen", "半开试探成功，恢复 Closed 正常状态") //canvas时序事件图功能
		}
	case StateOpen:
	}
}

type BusinessCabin struct {
	name           string
	maxConcurrency int64
	sem            *semaphore.Weighted
	cb             *CircuitBreaker
	fallback       func(ctx context.Context) error
	metrics        CabinMetrics
	mu             sync.Mutex
	//✅新增 舱级延时配置
	normalDelayMs int
	faultDelayMs  int
}

func NewCabin(name string, maxConcurrency int64, cb *CircuitBreaker, normalDelayMs, faultDelayMs int) *BusinessCabin {
	return &BusinessCabin{
		name:           name,
		maxConcurrency: maxConcurrency,
		sem:            semaphore.NewWeighted(maxConcurrency),
		cb:             cb,
		normalDelayMs:  normalDelayMs,
		faultDelayMs:   faultDelayMs,
	}
}

func (c *BusinessCabin) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = CabinMetrics{}
	c.metrics.DegradeCount = 0
	c.cb.state.Store(int32(StateClosed))
	c.cb.successCount.Store(0)
	c.cb.failureCount.Store(0)
}

// ResetMetricsOnly 仅清空业务指标metrics，保留熔断器全部状态与窗口计数不变
func (c *BusinessCabin) ResetMetricsOnly() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 只重置业务统计指标，不触碰 c.cb
	c.metrics = CabinMetrics{}
}

func (c *BusinessCabin) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	c.mu.Lock()
	c.metrics.TotalTask += 1
	c.mu.Unlock()

	if !c.cb.Allow() {
		if c.fallback != nil {
			c.mu.Lock()
			c.metrics.DegradeCount += 1
			c.mu.Unlock()
			// onEventLog(fmt.Sprintf("[%s] 触发降级兜底，返回缓存活动数据", c.name))
			logTimelineEvent(c.name, "fallback", fmt.Sprintf("[%s] 触发降级兜底，返回缓存活动数据", c.name)) //canvas时序事件图功能
			return c.fallback(ctx)
		}
		c.mu.Lock()
		c.metrics.CbReject += 1
		c.mu.Unlock()
		return errors.New("cabin circuit breaker open: service reject")
	}

	// 信号量满，限流拒绝
	if !c.sem.TryAcquire(1) {
		c.mu.Lock()
		c.metrics.LimitReject += 1
		c.mu.Unlock()
		logTimelineEvent(c.name, "reject", fmt.Sprintf("[%s]信号量满，并发限流拒绝", c.name)) //canvas时序事件图功能
		return errors.New("cabin semaphore full: concurrency limit reject")
	}
	defer c.sem.Release(1)

	var execErr error
	func() {
		ctxT, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		execErr = fn(ctxT)
	}()

	// ✅新增：舱配置驱动延时
	var delay time.Duration
	if execErr != nil {
		delay = time.Duration(c.faultDelayMs) * time.Millisecond
	} else {
		delay = time.Duration(c.normalDelayMs) * time.Millisecond
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done(): // 上下文取消，立刻跳出sleep
		}
	}

	if execErr != nil {
		c.mu.Lock()
		c.metrics.BusinessError += 1
		c.mu.Unlock()
		logTimelineEvent(c.name, "error", fmt.Sprintf("[%s]业务执行报错", c.name)) //canvas时序事件图功能
	} else {
		c.mu.Lock()
		c.metrics.Success += 1
		c.mu.Unlock()
		logTimelineEvent(c.name, "normal", fmt.Sprintf("[%s]业务执行成功", c.name)) //canvas时序事件图功能
	}
	c.cb.OnResult(execErr != nil)
	return execErr
}

type SimpleErrGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	err    error
}

func NewSimpleErrGroup(parent context.Context) *SimpleErrGroup {
	ctx, cancel := context.WithCancel(parent)
	return &SimpleErrGroup{ctx: ctx, cancel: cancel}
}

func (g *SimpleErrGroup) Go(f func(ctx context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		select {
		case <-g.ctx.Done():
			return
		default:
		}
		err := f(g.ctx)
		if err != nil {
			g.mu.Lock()
			if g.err == nil {
				g.err = err
				g.cancel()
			}
			g.mu.Unlock()
		}
	}()
}

func (g *SimpleErrGroup) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

type CabinFaultConfig struct {
	Enable       bool `json:"enable"`
	FaultPercent int  `json:"fault_percent"`
}

// SVG时序事件时序图
type TimelineEvent struct {
	Ts      int64  `json:"ts"`      // 毫秒时间戳
	Cabin   string `json:"cabin"`   // 舱名称：下单核心舱 / 营销舱 / 报表辅助舱
	EvType  string `json:"ev_type"` // 事件类型 normal / reject / open / halfOpen / fallback / error
	Message string `json:"msg"`     // 显示文本
}

var (
	cabinOrder               *BusinessCabin
	cabinMarketing           *BusinessCabin
	cabinReport              *BusinessCabin
	runLock                  sync.Mutex
	globalLogs               []string
	logMu                    sync.Mutex
	firstBatchOrderCount     = 10
	firstBatchMarketingCount = 20
	firstBatchReportCount    = 25
	secondBatchReportCount   = 10
	faultConfig              map[string]*CabinFaultConfig
	faultMu                  sync.Mutex
	// SVG时序事件时序图功能
	globalTimelineEvents []TimelineEvent
	timelineMu           sync.Mutex
)

// logTimelineEvent 同时输出文本日志 + 存入结构化时序事件
func logTimelineEvent(cabinName string, evType string, msg string) {
	onEventLog(msg) // 保留原有文本日志输出

	timelineMu.Lock()
	defer timelineMu.Unlock()
	evt := TimelineEvent{
		Ts:      time.Now().UnixMilli(),
		Cabin:   cabinName,
		EvType:  evType,
		Message: msg,
	}
	globalTimelineEvents = append(globalTimelineEvents, evt)
	// 限制最大条数，防止内存暴涨
	if len(globalTimelineEvents) > 200 {
		globalTimelineEvents = globalTimelineEvents[len(globalTimelineEvents)-200:]
	}
}

func shouldProduceFault(cabinName string) bool {
	faultMu.Lock()
	defer faultMu.Unlock()
	cfg, ok := faultConfig[cabinName]
	if !ok || !cfg.Enable {
		return false
	}
	p := cfg.FaultPercent
	if p <= 0 {
		return false
	}
	if p >= 100 {
		return true
	}
	return rand.Intn(100) < p
}

func bizOrder(ctx context.Context, orderID int) error {
	// time.Sleep(100 * time.Millisecond) //删掉！延时交给舱配置
	fmt.Printf(colorGreen+"[下单核心舱] order=%d SUCCESS"+colorReset+"\n", orderID)
	return nil
}

func bizMarketing(ctx context.Context, actID int) error {
	if shouldProduceFault("营销舱") {
		return errors.New("marketing db query timeout")
	}
	fmt.Printf(colorGreen+"[营销舱-查询] act=%d SUCCESS"+colorReset+"\n", actID)
	return nil
}

func bizReport(ctx context.Context, reportID int) error {
	if shouldProduceFault("报表辅助舱") {
		return errors.New("report slow sql fail")
	}
	fmt.Printf(colorGreen+"[报表辅助舱] report=%d ok"+colorReset+"\n", reportID)
	return nil
}

var marketingFallback = func(ctx context.Context) error {
	fmt.Printf(colorGreen + "[营销舱-降级兜底] 返回缓存活动数据" + colorReset + "\n")
	return nil
}

func onEventLog(msg string) {
	t := time.Now().Format("15:04:05")
	line := fmt.Sprintf("[%s] %s", t, msg)
	logMu.Lock()
	globalLogs = append(globalLogs, line)
	if len(globalLogs) > 80 {
		globalLogs = globalLogs[len(globalLogs)-80:]
	}
	logMu.Unlock()
	fmt.Println(line)
}

type CabinView struct {
	Name        string            `json:"name"`
	State       string            `json:"state"`
	Metrics     CabinMetrics      `json:"metrics"`
	Config      CabinConfig       `json:"config"`
	FaultConfig *CabinFaultConfig `json:"fault_config,omitempty"`
	//====新增熔断器窗口内部统计====
	CbWindowSuccess uint64 `json:"cb_window_success"`
	CbWindowFail    uint64 `json:"cb_window_fail"`
}

func getCabinConfig(c *BusinessCabin) CabinConfig {
	return CabinConfig{
		MaxConcurrency:   c.maxConcurrency,
		FailThreshold:    c.cb.thresholdFailurePct,
		WindowSize:       c.cb.windowSize,
		OpenWaitSec:      int(c.cb.openWaitDuration / time.Second),
		HalfOpenMaxProbe: c.cb.halfOpenMaxProbe, //新增
		NormalDelayMs:    c.normalDelayMs,       //新增,正常情况延时ms
		FaultDelayMs:     c.faultDelayMs,        //新增,故障情况延时ms
	}
}

func apiStatus(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()

	getFault := func(name string) *CabinFaultConfig {
		faultMu.Lock()
		defer faultMu.Unlock()
		cfg := faultConfig[name]
		return cfg
	}

	list := []CabinView{
		{
			Name:            cabinOrder.name,
			State:           cabinOrder.cb.GetState().String(),
			Metrics:         cabinOrder.metrics,
			Config:          getCabinConfig(cabinOrder),
			CbWindowSuccess: cabinOrder.cb.successCount.Load(),
			CbWindowFail:    cabinOrder.cb.failureCount.Load(),
		},
		{
			Name:            cabinMarketing.name,
			State:           cabinMarketing.cb.GetState().String(),
			Metrics:         cabinMarketing.metrics,
			Config:          getCabinConfig(cabinMarketing),
			FaultConfig:     getFault("营销舱"),
			CbWindowSuccess: cabinMarketing.cb.successCount.Load(),
			CbWindowFail:    cabinMarketing.cb.failureCount.Load(),
		},
		{
			Name:            cabinReport.name,
			State:           cabinReport.cb.GetState().String(),
			Metrics:         cabinReport.metrics,
			Config:          getCabinConfig(cabinReport),
			FaultConfig:     getFault("报表辅助舱"),
			CbWindowSuccess: cabinReport.cb.successCount.Load(),
			CbWindowFail:    cabinReport.cb.failureCount.Load(),
		},
	}

	logMu.Lock()
	logsCopy := make([]string, len(globalLogs))
	copy(logsCopy, globalLogs)
	logMu.Unlock()

	//====canvas时序事件图功能，复制canvas时序事件副本给前端====
	timelineMu.Lock()
	eventsCopy := make([]TimelineEvent, len(globalTimelineEvents))
	copy(eventsCopy, globalTimelineEvents)
	timelineMu.Unlock()

	payload := map[string]any{
		"cabins": list,
		"logs":   logsCopy,
		"events": eventsCopy, // <====canvas时序事件图功能，新增结构化事件数组
		"first_batch": map[string]int{
			"order":     firstBatchOrderCount,
			"marketing": firstBatchMarketingCount,
			"report":    firstBatchReportCount,
		},
		"second_batch": map[string]int{
			"report": secondBatchReportCount,
		},
	}
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_ = json.NewEncoder(w).Encode(payload)
}

func apiSetFault(w http.ResponseWriter, r *http.Request) {
	type Req struct {
		CabinName    string `json:"cabin_name"`
		Enable       bool   `json:"enable"`
		FaultPercent int    `json:"fault_percent"`
	}
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	if req.FaultPercent < 0 || req.FaultPercent > 100 {
		http.Error(w, "fault_percent must 0-100", 400)
		return
	}

	faultMu.Lock()
	cfg, ok := faultConfig[req.CabinName]
	if !ok {
		faultMu.Unlock()
		http.Error(w, "invalid cabin name", 400)
		return
	}
	cfg.Enable = req.Enable
	cfg.FaultPercent = req.FaultPercent
	faultMu.Unlock()

	// onEventLog(fmt.Sprintf("[故障设置] %s enable=%v faultPercent=%d%%", req.CabinName, req.Enable, req.FaultPercent))
	// canvas时序事件图功能
	logTimelineEvent("系统", "config", fmt.Sprintf("[故障设置] %s enable=%v faultPercent=%d%%", req.CabinName, req.Enable, req.FaultPercent))
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func apiApplyConfig(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()
	type Req struct {
		Index int         `json:"index"`
		Cfg   CabinConfig `json:"cfg"`
	}
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	idx := req.Index
	cfg := req.Cfg
	if cfg.MaxConcurrency < 1 || cfg.FailThreshold < 1 || cfg.FailThreshold > 100 || cfg.WindowSize < 1 || cfg.OpenWaitSec < 1 || cfg.HalfOpenMaxProbe < 1 || cfg.NormalDelayMs < 0 || cfg.FaultDelayMs < 0 {
		http.Error(w, "参数非法：并发>=1，错误率1‑100，窗口>=1，等待秒>=1，半开试探数>=1，延时>=0", 400)
		return
	}

	newCb := NewCircuitBreaker(cfg.FailThreshold, cfg.WindowSize, time.Duration(cfg.OpenWaitSec)*time.Second, cfg.HalfOpenMaxProbe)

	var newCabin *BusinessCabin
	switch idx {
	case 0:
		newCabin = NewCabin(cabinOrder.name, cfg.MaxConcurrency, newCb, cfg.NormalDelayMs, cfg.FaultDelayMs)
		cabinOrder = newCabin
	case 1:
		newCabin = NewCabin(cabinMarketing.name, cfg.MaxConcurrency, newCb, cfg.NormalDelayMs, cfg.FaultDelayMs)
		newCabin.fallback = marketingFallback
		cabinMarketing = newCabin
	case 2:
		newCabin = NewCabin(cabinReport.name, cfg.MaxConcurrency, newCb, cfg.NormalDelayMs, cfg.FaultDelayMs)
		cabinReport = newCabin
	default:
		http.Error(w, "invalid index", 400)
		return
	}

	// onEventLog(fmt.Sprintf("已更新配置：%s 最大并发=%d 错误阈值=%d%% 窗口=%d 熔断等待=%ds 半开试探=%d 正常耗时=%dms 故障耗时=%dms",
	//	newCabin.name, cfg.MaxConcurrency, cfg.FailThreshold, cfg.WindowSize, cfg.OpenWaitSec,
	//	cfg.HalfOpenMaxProbe, cfg.NormalDelayMs, cfg.FaultDelayMs))
	// canvas时序事件图功能
	logTimelineEvent("系统", "config", fmt.Sprintf("已更新配置：%s 最大并发=%d 错误阈值=%d%% 窗口=%d 熔断等待=%ds 半开试探=%d 正常耗时=%dms 故障耗时=%dms",
		newCabin.name, cfg.MaxConcurrency, cfg.FailThreshold, cfg.WindowSize, cfg.OpenWaitSec,
		cfg.HalfOpenMaxProbe, cfg.NormalDelayMs, cfg.FaultDelayMs))
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func apiReset(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()
	resetAllCabins() // 完整重建舱与熔断器，全部配置恢复出厂默认
	logMu.Lock()
	globalLogs = []string{}
	logMu.Unlock()

	//====canvas时序事件图功能，新增清空canvas时序事件====
	timelineMu.Lock()
	globalTimelineEvents = []TimelineEvent{}
	timelineMu.Unlock()

	onEventLog("全部舱已重置，回到初始状态")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// apiResetMetricsOnly 仅重置业务指标、清空日志；保留熔断器状态、熔断器窗口统计不变
func apiResetMetricsOnly(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()

	// 调用新方法，只清业务metrics，熔断器不动
	cabinOrder.ResetMetricsOnly()
	cabinMarketing.ResetMetricsOnly()
	cabinReport.ResetMetricsOnly()

	logMu.Lock()
	globalLogs = []string{}
	logMu.Unlock()

	onEventLog("【仅重置指标】业务指标已清空，熔断器状态保持不变")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// randomJitter 生成 [minMs,maxMs] 之间随机毫秒时间
// 总请求数 > 10 时，每提交一个任务，sleep 随机 50‑150ms，平缓的流量爬坡，而不是瞬间全部爆发
func randomJitter(minMs, maxMs int) time.Duration {
	delta := maxMs - minMs
	ms := minMs + rand.Intn(delta+1)
	return time.Duration(ms) * time.Millisecond
}

func apiRunFirst(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()
	ctx := context.Background()
	g := NewSimpleErrGroup(ctx)

	// 总请求数 > 20 时，每提交一个任务，sleep 随机 2‑10ms，平缓的流量爬坡，而不是瞬间全部爆发
	totalFirst := firstBatchOrderCount + firstBatchMarketingCount + firstBatchReportCount
	needJitter := totalFirst > 20

	//下单核心舱
	for i := 1; i <= firstBatchOrderCount; i++ {
		oid := i
		g.Go(func(ctx context.Context) error {
			err := cabinOrder.Run(ctx, func(ctx context.Context) error { return bizOrder(ctx, oid) })
			if err != nil {
				fmt.Printf(colorRed+"[下单] %d reject: %v"+colorReset+"\n", oid, err)
			}
			return nil
		})
	}
	//营销舱
	for i := 1; i <= firstBatchMarketingCount; i++ {
		aid := i
		g.Go(func(ctx context.Context) error {
			err := cabinMarketing.Run(ctx, func(ctx context.Context) error { return bizMarketing(ctx, aid) })
			if err != nil {
				fmt.Printf(colorRed+"[营销] %d reject: %v"+colorReset+"\n", aid, err)
			}
			return nil
		})
		if needJitter {
			time.Sleep(randomJitter(2, 10))
		}
	}
	//报表辅助舱
	for i := 1; i <= firstBatchReportCount; i++ {
		rid := i
		g.Go(func(ctx context.Context) error {
			err := cabinReport.Run(ctx, func(ctx context.Context) error { return bizReport(ctx, rid) })
			if err != nil {
				fmt.Printf(colorRed+"[报表] %d reject: %v"+colorReset+"\n", rid, err)
			}
			return nil
		})
		if needJitter {
			time.Sleep(randomJitter(2, 10))
		}
	}
	_ = g.Wait()
	// onEventLog("第一轮请求执行完毕")
	// canvas时序事件图功能
	logTimelineEvent("系统", "normal", "第一轮请求执行完毕")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func apiWaitHalfOpen(w http.ResponseWriter, r *http.Request) {
	// onEventLog("等待1.2s，观察熔断器是否切换到HalfOpen半开状态")
	// canvas时序事件图功能
	logTimelineEvent("系统", "normal", "等待1.2s，观察熔断器是否切换到HalfOpen半开状态")
	time.Sleep(1200 * time.Millisecond)
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func apiRunSecond(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()
	ctx := context.Background()
	g2 := NewSimpleErrGroup(ctx)
	startRID := 1000
	// 总请求数 > 10 时，每提交一个任务，sleep 随机 50‑150ms，平缓的流量爬坡，而不是瞬间全部爆发
	needJitter := secondBatchReportCount > 10

	for i := 0; i < secondBatchReportCount; i++ {
		rid := startRID + i
		g2.Go(func(ctx context.Context) error {
			err := cabinReport.Run(ctx, func(ctx context.Context) error { return bizReport(ctx, rid) })
			if err != nil {
				fmt.Printf(colorRed+"[报表] %d reject: %v"+colorReset+"\n", rid, err)
			}
			return nil
		})
		// 总请求数 > 10 时，每提交一个任务，sleep 随机 50‑150ms，平缓的流量爬坡，而不是瞬间全部爆发
		if needJitter {
			time.Sleep(randomJitter(50, 150))
		}
	}
	_ = g2.Wait()
	// onEventLog("第二轮（半开试探）请求执行完毕")
	// canvas时序事件图功能
	logTimelineEvent("系统", "normal", "第二轮（半开试探）请求执行完毕")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// apiClearLogs 仅清空日志，不修改任何舱、熔断器、metrics状态
func apiClearLogs(w http.ResponseWriter, r *http.Request) {
	logMu.Lock()
	globalLogs = []string{}
	logMu.Unlock()
	//====canvas时序事件图功能，新增清空canvas时序事件====
	timelineMu.Lock()
	globalTimelineEvents = []TimelineEvent{}
	timelineMu.Unlock()

	onEventLog("日志已手动清空")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// 【重点】全部用普通减号 -，无隐形软连字符

// resetAllCabins 完整重建全部舱，恢复程序出厂默认配置
func resetAllCabins() {
	// 第4个参数：halfOpenMaxProbe 默认3
	// 第5、6参数 normalDelayMs=0，faultDelayMs=0 默认无延时
	cabinOrder = NewCabin("下单核心舱", 20, NewCircuitBreaker(50, 40, 1*time.Second, 3), 0, 0)
	cabinMarketing = NewCabin("营销舱", 40, NewCircuitBreaker(50, 40, 1*time.Second, 3), 0, 0)
	cabinReport = NewCabin("报表辅助舱", 8, NewCircuitBreaker(40, 30, 1*time.Second, 3), 0, 0)

	cabinMarketing.fallback = marketingFallback
	faultConfig = make(map[string]*CabinFaultConfig)
	faultConfig["营销舱"] = &CabinFaultConfig{Enable: false, FaultPercent: 30}
	faultConfig["报表辅助舱"] = &CabinFaultConfig{Enable: false, FaultPercent: 66}
}

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	resetAllCabins()
}

func main() {
	http.Handle("/", http.FileServer(http.Dir(".")))
	http.HandleFunc("/api/status", apiStatus)
	http.HandleFunc("/api/applyConfig", apiApplyConfig)
	http.HandleFunc("/api/reset", apiReset)
	http.HandleFunc("/api/runFirst", apiRunFirst)
	http.HandleFunc("/api/waitHalfOpen", apiWaitHalfOpen)
	http.HandleFunc("/api/runSecond", apiRunSecond)
	http.HandleFunc("/api/setFault", apiSetFault)
	http.HandleFunc("/api/clearLogs", apiClearLogs)
	http.HandleFunc("/api/resetMetricsOnly", apiResetMetricsOnly)

	onEventLog("演示程序已启动")
	fmt.Println("网页演示已启动，请浏览器打开：http://127.0.0.1:8080")
	_ = http.ListenAndServe(":8080", nil)
}
