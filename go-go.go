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
	MaxConcurrency int64  `json:"max_concurrency"`
	FailThreshold  uint64 `json:"fail_threshold"`
	WindowSize     uint64 `json:"window_size"`
	OpenWaitSec    int    `json:"open_wait_sec"`
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
}

func NewCircuitBreaker(failurePct uint64, window uint64, openWait time.Duration) *CircuitBreaker {
	cb := &CircuitBreaker{
		thresholdFailurePct: failurePct,
		windowSize:          window,
		openWaitDuration:    openWait,
	}
	cb.state.Store(int32(StateClosed))
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
			cb.state.Store(int32(StateHalfOpen))
			return true
		}
		return false
	case StateHalfOpen:
		return true
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
				onEventLog("熔断器切换 Open（熔断打开）")
			}
		}
	case StateHalfOpen:
		if isFail {
			cb.state.Store(int32(StateOpen))
			cb.lastOpenTime.Store(time.Now().UnixMilli())
			onEventLog("半开试探失败，重新切回 Open 熔断")
		} else {
			cb.state.Store(int32(StateClosed))
			onEventLog("半开试探成功，恢复 Closed 正常状态")
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
}

func NewCabin(name string, maxConcurrency int64, cb *CircuitBreaker) *BusinessCabin {
	return &BusinessCabin{
		name:           name,
		maxConcurrency: maxConcurrency,
		sem:            semaphore.NewWeighted(maxConcurrency),
		cb:             cb,
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

func (c *BusinessCabin) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	c.mu.Lock()
	c.metrics.TotalTask += 1
	c.mu.Unlock()
	if !c.cb.Allow() {
		if c.fallback != nil {
			c.mu.Lock()
			c.metrics.DegradeCount += 1
			c.mu.Unlock()
			onEventLog(fmt.Sprintf("[%s] 触发降级兜底，返回缓存活动数据", c.name))
			return c.fallback(ctx)
		}
		c.mu.Lock()
		c.metrics.CbReject += 1
		c.mu.Unlock()
		return errors.New("cabin circuit breaker open: service reject")
	}
	if !c.sem.TryAcquire(1) {
		c.mu.Lock()
		c.metrics.LimitReject += 1
		c.mu.Unlock()
		return errors.New("cabin semaphore full: concurrency limit reject")
	}
	defer c.sem.Release(1)
	var execErr error
	func() {
		ctxT, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		execErr = fn(ctxT)
	}()
	if execErr != nil {
		c.mu.Lock()
		c.metrics.BusinessError += 1
		c.mu.Unlock()
	} else {
		c.mu.Lock()
		c.metrics.Success += 1
		c.mu.Unlock()
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

	faultConfig map[string]*CabinFaultConfig
	faultMu     sync.Mutex
)

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
	time.Sleep(100 * time.Millisecond)
	fmt.Printf(colorGreen+"[下单核心舱] order=%d SUCCESS"+colorReset+"\n", orderID)
	return nil
}

func bizMarketing(ctx context.Context, actID int) error {
	time.Sleep(150 * time.Millisecond)
	if shouldProduceFault("营销舱") {
		return errors.New("marketing db query timeout")
	}
	fmt.Printf(colorGreen+"[营销舱-查询] act=%d SUCCESS"+colorReset+"\n", actID)
	return nil
}

func bizReport(ctx context.Context, reportID int) error {
	time.Sleep(2 * time.Second)
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
		MaxConcurrency: c.maxConcurrency,
		FailThreshold:  c.cb.thresholdFailurePct,
		WindowSize:     c.cb.windowSize,
		OpenWaitSec:    int(c.cb.openWaitDuration / time.Second),
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

	payload := map[string]any{
		"cabins": list,
		"logs":   logsCopy,
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

	onEventLog(fmt.Sprintf("[故障设置] %s enable=%v faultPercent=%d%%", req.CabinName, req.Enable, req.FaultPercent))
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
	if cfg.MaxConcurrency < 1 || cfg.FailThreshold < 1 || cfg.FailThreshold > 100 || cfg.WindowSize < 1 || cfg.OpenWaitSec < 1 {
		http.Error(w, "参数非法：并发>=1，错误率1‑100，窗口>=1，等待秒>=1", 400)
		return
	}
	newCb := NewCircuitBreaker(cfg.FailThreshold, cfg.WindowSize, time.Duration(cfg.OpenWaitSec)*time.Second)
	var newCabin *BusinessCabin
	switch idx {
	case 0:
		newCabin = NewCabin(cabinOrder.name, cfg.MaxConcurrency, newCb)
		cabinOrder = newCabin
	case 1:
		newCabin = NewCabin(cabinMarketing.name, cfg.MaxConcurrency, newCb)
		newCabin.fallback = marketingFallback
		cabinMarketing = newCabin
	case 2:
		newCabin = NewCabin(cabinReport.name, cfg.MaxConcurrency, newCb)
		cabinReport = newCabin
	default:
		http.Error(w, "invalid index", 400)
		return
	}
	onEventLog(fmt.Sprintf("已更新配置：%s 最大并发=%d 错误阈值=%d%% 窗口=%d 熔断等待=%ds",
		newCabin.name, cfg.MaxConcurrency, cfg.FailThreshold, cfg.WindowSize, cfg.OpenWaitSec))
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func apiReset(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()
	cabinOrder.Reset()
	cabinMarketing.Reset()
	cabinReport.Reset()
	logMu.Lock()
	globalLogs = []string{}
	logMu.Unlock()
	onEventLog("全部舱已重置，回到初始状态")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func apiRunFirst(w http.ResponseWriter, r *http.Request) {
	runLock.Lock()
	defer runLock.Unlock()
	ctx := context.Background()
	g := NewSimpleErrGroup(ctx)
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
	for i := 1; i <= firstBatchMarketingCount; i++ {
		aid := i
		g.Go(func(ctx context.Context) error {
			err := cabinMarketing.Run(ctx, func(ctx context.Context) error { return bizMarketing(ctx, aid) })
			if err != nil {
				fmt.Printf(colorRed+"[营销] %d reject: %v"+colorReset+"\n", aid, err)
			}
			return nil
		})
	}
	for i := 1; i <= firstBatchReportCount; i++ {
		rid := i
		g.Go(func(ctx context.Context) error {
			err := cabinReport.Run(ctx, func(ctx context.Context) error { return bizReport(ctx, rid) })
			if err != nil {
				fmt.Printf(colorRed+"[报表] %d reject: %v"+colorReset+"\n", rid, err)
			}
			return nil
		})
	}
	_ = g.Wait()
	onEventLog("第一轮请求执行完毕")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func apiWaitHalfOpen(w http.ResponseWriter, r *http.Request) {
	onEventLog("等待1.2s，观察熔断器是否切换到HalfOpen半开状态")
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
	for i := 0; i < secondBatchReportCount; i++ {
		rid := startRID + i
		g2.Go(func(ctx context.Context) error {
			err := cabinReport.Run(ctx, func(ctx context.Context) error { return bizReport(ctx, rid) })
			if err != nil {
				fmt.Printf(colorRed+"[报表] %d reject: %v"+colorReset+"\n", rid, err)
			}
			return nil
		})
	}
	_ = g2.Wait()
	onEventLog("第二轮（半开试探）请求执行完毕")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// apiClearLogs 仅清空日志，不修改任何舱、熔断器、metrics状态
func apiClearLogs(w http.ResponseWriter, r *http.Request) {
	logMu.Lock()
	globalLogs = []string{}
	logMu.Unlock()
	onEventLog("日志已手动清空")
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// 【重点】全部用普通减号 -，无隐形软连字符
const pageHTML = `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>软件水密隔舱 可视化演示</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;background:#f7f8fa;color:#1f2329;max-width:1300px;margin:0 auto;padding:24px;line-height:1.55;}
.page-header{margin-bottom:20px;}
.page-header h1{font-size:22px;font-weight:600;color:#1f2329;margin-bottom:8px;}
.desc-text{color:#6b7785;font-size:13px;line-height:1.6;}
.action-bar{background:#ffffff;padding:14px 18px;border-radius:10px;box-shadow:0 1px 2px rgba(0,0,0,0.06);margin-bottom:18px;display:flex;gap:10px;flex-wrap:wrap;align-items:center;}
.cabin{background:#ffffff;border-radius:10px;box-shadow:0 1px 3px rgba(0,0,0,0.08);padding:18px 20px;margin-bottom:14px;border:1px solid #e5e6eb;}
.cabin h3{font-size:16px;font-weight:600;display:flex;align-items:center;gap:10px;margin-bottom:12px;}
.state-Closed{background-color:#e8f7ee;color:#00875a;font-size:12px;padding:3px 9px;border-radius:16px;font-weight:500;}
.state-Open{background-color:#ffe9e9;color:#d93025;font-size:12px;padding:3px 9px;border-radius:16px;font-weight:500;}
.state-HalfOpen{background-color:#fff7e6;color:#ff7d00;font-size:12px;padding:3px 9px;border-radius:16px;font-weight:500;}
.metric-grid{display:grid;grid-template-columns:repeat(auto-fit, minmax(120px,1fr));gap:8px;margin-bottom:14px;}
.metric-item{background:#f7f8fa;padding:10px 8px;border-radius:6px;font-size:13px;color:#4e5969;display:flex;align-items:center;min-height:36px;}
.cfg-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:10px;padding-top:12px;border-top:1px solid #f0f0f0;}
.cfg-row label{font-size:13px;color:#4e5969;}
.cfg-row input{width:110px;padding:6px 8px;border:1px solid #dcdfe6;border-radius:5px;font-size:13px;background:#fff;color:#1f2329;transition:border .2s;}
.cfg-row input:focus{outline:none;border-color:#4080ff;box-shadow:0 0 0 2px rgba(64,128,255,0.15);}
.fault-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;padding-top:10px;border-top:1px solid #f0f0f0;}
button{padding:8px 14px;border-radius:5px;border:none;font-size:13px;font-weight:500;cursor:pointer;transition:opacity 0.2s;}
button:hover{opacity:0.88;}
.btn-green{background:#00b42a;color:#fff;}
.btn-yellow{background:#ff7d00;color:#fff;}
.btn-red{background:#f53f3f;color:#fff;}
.btn-gray{background:#86909c;color:#fff;}
.btn-blue{background:#4080ff;color:#fff;}
.btn-preset{background:#722ed1;color:#fff;}
#logPanel{background:#1d2129;color:#c9cdd4;border-radius:8px;padding:12px;height:220px;overflow:auto;font-family:"Menlo","Consolas",monospace;font-size:12px;margin-top:16px;}
.log-line{padding:2px 0;}
#batchTip{margin-top:8px;font-size:12px;color:#86909c;white-space:pre-line;line-height:1.6;}
.footer{margin-top:28px;padding-top:14px;border-top:1px solid #e5e6eb;text-align:center;}
.footer a{display:inline-flex;align-items:center;gap:6px;color:#86909c;text-decoration:none;}
.footer svg{fill:#86909c;}
.action-bar-group{margin-bottom:8px;}
.group-tip{font-size:12px;color:#6b7785;margin-bottom:6px;}
.action-bar{background:#ffffff;padding:14px 18px;border-radius:10px;box-shadow:0 1px 2px rgba(0,0,0,0.06);display:flex;gap:10px;flex-wrap:wrap;align-items:center;}
/* 舱卡片状态大背景色块，浅色不刺眼 */
.cabin.bg-state-closed {
    background-color: #e8f5e9; /* 浅绿 Closed */
}
.cabin.bg-state-halfopen {
    background-color: #fff9c4; /* 浅黄 HalfOpen */
}
.cabin.bg-state-open {
    background-color: #ffebee; /* 浅红 Open */
}
button:disabled {
    opacity:0.45 !important;
    cursor: not-allowed;
}

</style>
</head>
<body>
<div class="page-header">
<h1>软件水密隔舱演示（限流 + 熔断 + 降级 + 故障注入）</h1>
<div class="desc-text">
说明：各业务独立隔离舱，信号量控制最大并发；失败占比超标触发熔断器。
<br>✅熔断状态流转：Closed正常(允许访问) → Open熔断断开(拒绝访问) → HalfOpen试探恢复(小流量测试) → Closed正常(恢复访问)。
<br>📌演示指引：<br/>
👉▶场景A‑限流压力测试【下单核心舱】：观察限流拒绝计数，体验信号量隔离。<br/>
👉▶场景B‑营销舱熔断+降级演示【营销舱】：故障触发熔断器进入Open，观察「降级次数」上涨。<br/>
👉▶场景C‑报表辅助舱故障隔离【报表辅助舱】：故障触发熔断，无降级兜底，观察「熔断拒绝」上涨。<br/>
✅操作顺序：点击场景按钮完成配置 → 点击「发起第一轮批量请求」产生流量，观察面板指标。<br/>
💡提示：需要足够请求量把熔断器打入Open状态，才会触发降级/熔断拒绝。<br/>
⚠️熔断器变为红色Open状态后，请勿点击重置；继续发起请求观察保护效果。
</div>
</div>

<div class="action-bar-group">
    <div class="group-tip">📋 ①选择演示场景：点击自动配置参数与故障注入</div>
    <div class="action-bar">
        <button class="btn-preset" onclick="sceneA()">场景A‑限流压力测试</button>
        <button class="btn-preset" onclick="sceneB()">场景B‑营销舱熔断+降级演示</button>
        <button class="btn-preset" onclick="sceneC()">场景C‑报表辅助舱故障隔离</button>
    </div>
</div>

<div class="action-bar-group">
    <div class="group-tip">⚙️ ②执行流量操作</div>
    <div class="action-bar">
        <button class="btn-green" onclick="runFirst()">① 发起第一轮批量请求</button>
        <button class="btn-yellow" onclick="waitHalf()">② 等待熔断恢复窗口期</button>
        <button class="btn-red" onclick="runSecond()">③ 发起第二轮试探请求</button>
        <button class="btn-gray" onclick="resetAll()">④ 重置所有舱状态</button>
    </div>
    <div class="group-note">💡重置：清空全部指标、恢复所有舱初始状态，熔断器状态一并清零</div>
</div>


<div id="container"></div>
<div style="display:flex;gap:10px;align-items:center;margin-top:16px;margin-bottom:8px;">
    <button class="btn-red" onclick="clearLogBtnClick()">清空日志</button>
    <label style="font-size:13px;color:#4e5969;">日志过滤:</label>
    <select id="logFilterSel" style="padding:6px 8px;border-radius:5px;border:1px solid #dcdfe6;font-size:13px;">
        <option value="all">全部日志</option>
        <option value="circuit">仅熔断器事件</option>
        <option value="fallback">仅降级事件</option>
    </select>
</div>
<div id="logPanel"></div>
<div id="batchTip"></div>
<div class="footer">
<a href="https://github.com/andy0792" target="_blank" rel="noopener noreferrer">
<svg height="20" width="20" viewBox="0 0 16 16">
<path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path>
</svg>
</a>
</div>
<script>
let lastCabinList = [];
let localCabinsConfig = [];
let localFaultConfig = [];
//====新增====
let isRunning = false;
let logFilterMode = "all"; // all / circuit / fallback 

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function clearLogBtnClick(){
    await fetch("/api/clearLogs",{method:"POST"});
    await fetchStatus();
}

function setButtonsDisabled(disabled) {
    // 获取所有需要置灰的按钮：场景按钮 + 流量操作按钮
    const btns = document.querySelectorAll(
        '.action-bar button.btn-preset, .action-bar button.btn-green, .action-bar button.btn-yellow, .action-bar button.btn-red'
    );
    for(const b of btns){
        b.disabled = disabled;
    }
}


/**
 * 根据熔断器状态返回对应的css class名称
 * @param {string} state 后端返回状态字符串 Closed / HalfOpen / Open
 * @returns {string} css类名
 */
function getCabinStateBgClass(state) {
    switch(state) {
        case "Closed":
            return "bg-state-closed";
        case "HalfOpen":
            return "bg-state-halfopen";
        case "Open":
            return "bg-state-open";
        default:
            return "";
    }
}

function getShowState(rawState){
    switch(rawState){
        case "Closed": return "Closed正常（允许访问）";
        case "HalfOpen": return "HalfOpen试探恢复（小流量测试）";
        case "Open": return "Open熔断阻断（拒绝访问）";
        default: return rawState;
    }
}
function initLocalConfig(list){
    localCabinsConfig = [];
    localFaultConfig = [];
    for(let i=0;i<list.length;i++){
        const c = list[i];
        localCabinsConfig.push({
            MaxConcurrency: c.config.max_concurrency,
            FailThreshold: c.config.fail_threshold,
            WindowSize: c.config.window_size,
            OpenWaitSec: c.config.open_wait_sec
        });
        if(c.fault_config){
            localFaultConfig.push({
                Enable: c.fault_config.enable,
                FaultPercent: c.fault_config.fault_percent
            });
        }else{
            localFaultConfig.push(null);
        }
    }
}

function renderAllLogs(logs){
    const panel = document.getElementById("logPanel");
    panel.innerHTML = "";

    let filtered = [];
    for(let line of logs){
        if(logFilterMode === "all"){
            filtered.push(line);
        }else if(logFilterMode === "circuit"){
            //熔断器事件关键词：熔断器切换 / 半开试探
            if(line.includes("熔断器切换") || line.includes("半开试探")){
                filtered.push(line);
            }
        }else if(logFilterMode === "fallback"){
            //降级事件关键词：触发降级兜底
            if(line.includes("触发降级兜底")){
                filtered.push(line);
            }
        }
    }

    for(let i=0;i<filtered.length;i++){
        let div = document.createElement("div");
        div.className="log-line";
        div.innerText = filtered[i];
        panel.appendChild(div);
    }
    panel.scrollTop = panel.scrollHeight;
}

function rebuildCabinsDOM(list){
	let html="";
	for(let idx=0;idx<list.length;idx++){
		const c = list[idx];
		let cls="state-"+c.state;
		let showText = getShowState(c.state);
		html += '<div class="cabin cabin-card" data-cabin-name="' + c.name + '">';
		html += '<h3>' + c.name + ' <span class="' + cls + '">' + showText + '</span></h3>';
        html += '<div class="metric-grid">';
		html += '<div class="metric-item" data-key="total">总任务: ' + c.metrics.total_task + '</div>';
		html += '<div class="metric-item" data-key="success">成功: ' + c.metrics.success + '</div>';
		html += '<div class="metric-item" data-key="limitReject">限流拒绝: ' + c.metrics.limit_reject + '</div>';
		html += '<div class="metric-item" data-key="cbReject">熔断拒绝: ' + c.metrics.cb_reject + '</div>';
		html += '<div class="metric-item" data-key="bizErr">业务报错: ' + c.metrics.business_error + '</div>';
		html += '<div class="metric-item" data-key="degrade">降级次数: ' + c.metrics.degrade_count + '</div>';
		html += '<div class="metric-item" data-key="cbstat">窗口统计：成功:-- 失败:-- 当前失败占比:--</div>';
        html += '</div>';
		html += '<div class="cfg-row">';
		html += '<label>最大并发:</label><input type="number" id="maxc_'+idx+'" value="'+c.config.max_concurrency+'">';
		html += '<label>失败触发熔断%:</label><input type="number" id="failp_'+idx+'" value="'+c.config.fail_threshold+'">';
		html += '<label>统计样本请求数:</label><input type="number" id="win_'+idx+'" value="'+c.config.window_size+'">';
		html += '<label>熔断冷却时间(s):</label><input type="number" id="wait_'+idx+'" value="'+c.config.open_wait_sec+'">';
		html += '<button class="btn-blue" onclick="applyCfg('+idx+')">应用配置</button>';
		html += '</div>';
        if(c.fault_config){
            const f = c.fault_config;
            html += '<div class="fault-row">';
            html += '<label><input type="checkbox" id="fault-enable-'+idx+'" '+(f.enable?"checked":"")+'>模拟业务故障</label>';
            html += '<label>故障%:</label><input type="number" min="0" max="100" id="fault-pct-'+idx+'" value="'+f.fault_percent+'"> ';
            html += '<button  class="btn-red"  onclick="onFaultChange('+idx+')">设置故障</button>';
            html += '</div>';
        }
		html += '</div>';
	}
	document.getElementById("container").innerHTML=html;
}
async function onFaultChange(idx){
    const c = lastCabinList[idx];
    const enableDom = document.getElementById("fault-enable-"+idx);
    const pctDom = document.getElementById("fault-pct-"+idx);
    const payload = {
        cabin_name: c.name,
        enable: enableDom.checked,
        fault_percent: parseInt(pctDom.value||"0")
    };
    await fetch("/api/setFault",{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify(payload)
    });
    fetchStatus();
}
async function fetchStatus(){
	try{
		const res=await fetch("/api/status");
		const payload=await res.json();
		lastCabinList = payload.cabins;
        const list = payload.cabins;
        initLocalConfig(list);
        const domCabins = document.querySelectorAll(".cabin");
        if(domCabins.length !== list.length){
            rebuildCabinsDOM(list);
        }
        for(let idx=0; idx < list.length; idx++){
            const c = list[idx];
            const cabinDom = document.querySelector('.cabin:nth-child('+(idx+1)+')');
            if(!cabinDom) continue;
            // ==========【1. 这里先粘贴你的窗口统计计算代码】==========
            const winSucc = c.cb_window_success;
            const winFail = c.cb_window_fail;
            const winTotal = winSucc + winFail;
            let failRateStr = "--";
            if(winTotal > 0){
                const rate = Math.round(winFail * 100 / winTotal);
                failRateStr = rate + "%";
            }
            // ========================================================
            // =========👉【在这里粘贴新增的 4 行色块代码】=========
            cabinDom.classList.remove("bg-state-closed","bg-state-halfopen","bg-state-open");
            const bgCls = getCabinStateBgClass(c.state);
            if(bgCls){
                cabinDom.classList.add(bgCls);
            }
            // ====================================================
            let cls="state-"+c.state;
            let showText = getShowState(c.state);
            cabinDom.querySelector("h3 span").className = cls;
            cabinDom.querySelector("h3 span").innerText = showText;
            cabinDom.querySelector('[data-key="total"]').innerText = "总任务: "+c.metrics.total_task;
            cabinDom.querySelector('[data-key="success"]').innerText = "成功: "+c.metrics.success;
            cabinDom.querySelector('[data-key="limitReject"]').innerText = "限流拒绝: "+c.metrics.limit_reject;
            cabinDom.querySelector('[data-key="cbReject"]').innerText = "熔断拒绝: "+c.metrics.cb_reject;
            cabinDom.querySelector('[data-key="bizErr"]').innerText = "业务报错: "+c.metrics.business_error;
            cabinDom.querySelector('[data-key="degrade"]').innerText = "降级次数: "+c.metrics.degrade_count;
            // ============【补上缺失的这一行！！】============
            cabinDom.querySelector('[data-key="cbstat"]').innerText = "窗口统计：成功:"+winSucc+" 失败:"+winFail+" 当前失败占比:"+failRateStr;
            // ==============================================
            const maxcInput = document.getElementById("maxc_"+idx);
            const failpInput = document.getElementById("failp_"+idx);
            const winInput = document.getElementById("win_"+idx);
            const waitInput = document.getElementById("wait_"+idx);
            const localCfg = localCabinsConfig[idx];
            if(document.activeElement !== maxcInput) maxcInput.value = localCfg.MaxConcurrency;
            if(document.activeElement !== failpInput) failpInput.value = localCfg.FailThreshold;
            if(document.activeElement !== winInput) winInput.value = localCfg.WindowSize;
            if(document.activeElement !== waitInput) waitInput.value = localCfg.OpenWaitSec;
            const localFault = localFaultConfig[idx];
            if(localFault){
                const chkEnable = document.getElementById("fault-enable-"+idx);
                const inputPct = document.getElementById("fault-pct-"+idx);
                if(document.activeElement !== chkEnable){
                    chkEnable.checked = localFault.Enable;
                }
                if(document.activeElement !== inputPct){
                    inputPct.value = localFault.FaultPercent;
                }
            }
        }
		renderAllLogs(payload.logs);
		const fb = payload.first_batch;
		const sb = payload.second_batch;
		document.getElementById("batchTip").innerText = "【第一轮批量请求】下单核心舱:" + fb.order + "个，营销舱:" + fb.marketing + "个，报表辅助舱:" + fb.report + "个 \n【第二轮试探请求】报表辅助舱:" + sb.report + "个";
	}catch(e){
		console.error("fetchStatus error",e);
	}
}
async function applyCfg(idx){
	const payload = {
		index: idx,
		cfg:{
			max_concurrency: parseInt(document.getElementById("maxc_"+idx).value),
			fail_threshold: parseInt(document.getElementById("failp_"+idx).value),
			window_size: parseInt(document.getElementById("win_"+idx).value),
			open_wait_sec: parseInt(document.getElementById("wait_"+idx).value),
		}
	};
	await fetch("/api/applyConfig",{
		method:"POST",
		headers:{"Content-Type":"application/json"},
		body:JSON.stringify(payload)
	});
	await fetchStatus();
}

// ---------------------- 一键场景 ----------------------
async function sceneA(){
    if(isRunning) return;
    isRunning = true;
    setButtonsDisabled(true);
    try{
        // 场景A‑限流压力测试：核心舱最大并发=2，关闭全部故障
        await fetch("/api/reset");
        // index0 核心舱‑下单
        await fetch("/api/applyConfig",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({index:0,cfg:{max_concurrency:2,fail_threshold:50,window_size:40,open_wait_sec:1}})
        });
        // 关闭营销舱故障
        await fetch("/api/setFault",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({cabin_name:"营销舱",enable:false,fault_percent:0})
        });
        // 关闭报表辅助舱故障
        await fetch("/api/setFault",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({cabin_name:"报表辅助舱",enable:false,fault_percent:0})
        });
        await fetchStatus();
    }finally{
        isRunning = false;
        setButtonsDisabled(false);
    }
}

async function sceneB(){
    if(isRunning) return;
    isRunning = true;
    setButtonsDisabled(true);
    try{
        // 场景B‑营销舱熔断+降级演示：营销舱冷却30s，故障100%
        await fetch("/api/reset");
        // index1 营销舱
        await fetch("/api/applyConfig",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({index:1,cfg:{max_concurrency:40,fail_threshold:50,window_size:40,open_wait_sec:30}})
        });
        // 营销舱故障100%
        await fetch("/api/setFault",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({cabin_name:"营销舱",enable:true,fault_percent:100})
        });
        // 报表辅助舱关闭故障
        await fetch("/api/setFault",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({cabin_name:"报表辅助舱",enable:false,fault_percent:0})
        });
        await fetchStatus();
    }finally{
        isRunning = false;
        setButtonsDisabled(false);
    }
}

async function sceneC(){
    if(isRunning) return;
    isRunning = true;
    setButtonsDisabled(true);
    try{
        // 场景C‑报表辅助舱故障隔离：报表辅助舱并发5，阈值30，窗口20，冷却2s；故障100%
        await fetch("/api/reset");
        // index2 报表辅助舱 更新舱参数
        await fetch("/api/applyConfig",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({index:2,cfg:{max_concurrency:5,fail_threshold:30,window_size:20,open_wait_sec:2}})
        });
        // 报表辅助舱故障100%开启
        await fetch("/api/setFault",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({cabin_name:"报表辅助舱",enable:true,fault_percent:100})
        });
        // 营销舱关闭故障
        await fetch("/api/setFault",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({cabin_name:"营销舱",enable:false,fault_percent:0})
        });
        await fetchStatus();
    }finally{
        isRunning = false;
        setButtonsDisabled(false);
    }
}

async function runFirst(){
    if(isRunning) return;
    isRunning = true;
    setButtonsDisabled(true);
    try{
        await fetch("/api/runFirst");
        await fetchStatus();
    }finally{
        isRunning = false;
        setButtonsDisabled(false);
    }
}

async function waitHalf(){
    if(isRunning) return;
    isRunning = true;
    setButtonsDisabled(true);
    try{
        await fetch("/api/waitHalfOpen");
        await fetchStatus();
    }finally{
        isRunning = false;
        setButtonsDisabled(false);
    }
}

async function runSecond(){
    if(isRunning) return;
    isRunning = true;
    setButtonsDisabled(true);
    try{
        await fetch("/api/runSecond");
        await fetchStatus();
    }finally{
        isRunning = false;
        setButtonsDisabled(false);
    }
}

async function resetAll(){
	await fetch("/api/reset");
	await fetchStatus();
}
window.onload = async function(){
    await fetchStatus();
    setInterval(fetchStatus, 800);
    //绑定过滤下拉事件
    document.getElementById("logFilterSel").onchange = function(){
        logFilterMode = this.value;
        fetchStatus(); //重新拉取并渲染日志
    }
};

</script>
</body>
`

func indexHTML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html;charset=utf-8")
	_, _ = w.Write([]byte(pageHTML))
}

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	cabinOrder = NewCabin("下单核心舱", 20, NewCircuitBreaker(50, 40, 1*time.Second))
	cabinMarketing = NewCabin("营销舱", 40, NewCircuitBreaker(50, 40, 1*time.Second))
	cabinReport = NewCabin("报表辅助舱", 8, NewCircuitBreaker(40, 30, 1*time.Second))
	cabinMarketing.fallback = marketingFallback

	faultConfig = make(map[string]*CabinFaultConfig)
	faultConfig["营销舱"] = &CabinFaultConfig{Enable: false, FaultPercent: 30}
	faultConfig["报表辅助舱"] = &CabinFaultConfig{Enable: false, FaultPercent: 66}
}

func main() {
	http.HandleFunc("/", indexHTML)
	http.HandleFunc("/api/status", apiStatus)
	http.HandleFunc("/api/applyConfig", apiApplyConfig)
	http.HandleFunc("/api/reset", apiReset)
	http.HandleFunc("/api/runFirst", apiRunFirst)
	http.HandleFunc("/api/waitHalfOpen", apiWaitHalfOpen)
	http.HandleFunc("/api/runSecond", apiRunSecond)
	http.HandleFunc("/api/setFault", apiSetFault)
	http.HandleFunc("/api/clearLogs", apiClearLogs)

	onEventLog("演示程序已启动")
	fmt.Println("网页演示已启动，请浏览器打开：http://127.0.0.1:8080")
	_ = http.ListenAndServe(":8080", nil)
}
