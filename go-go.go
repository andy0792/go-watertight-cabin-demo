package main

import (
	"context"
	_ "embed" // <------【新增】这一行，go内置包，用来把文件打进二进制
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

//go:embed index.html
var embedIndexHTML []byte // <------ 编译阶段读取同目录下index.html，内容存到这个字节切片变量里面

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
	//====新增：可切换熔断统计窗口====
	WindowType       string `json:"window_type"`        // fixed=简单计数窗口 / sliding=滑动统计窗口
	SlidingWindowSec int    `json:"sliding_window_sec"` // 滑动窗口时长(秒)，仅 window_type=sliding 生效
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

// SlidingWindow 滑动统计窗口（时间桶环形数组）
// 总时长 windowSec 秒，均分 bucketCount 个时间桶；请求按当前时间落入对应桶，
// 超过窗口时长的旧桶在统计时自动忽略，实现"时间衰减、滚动推进"（无需手动清零）。
type windowBucket struct {
	startMs int64 // 桶起始时间戳(ms)，0 表示尚未使用
	success uint64
	failure uint64
}

type SlidingWindow struct {
	bucketCount int
	bucketMs    int64
	buckets     []windowBucket
	mu          sync.Mutex
}

// NewSlidingWindow 创建滑动统计窗口，windowSec 为窗口总时长(秒)，内部固定拆 10 个时间桶
func NewSlidingWindow(windowSec int) *SlidingWindow {
	const bucketCount = 10
	ms := int64(windowSec) * 1000 / bucketCount
	if ms < 1 {
		ms = 1
	}
	return &SlidingWindow{
		bucketCount: bucketCount,
		bucketMs:    ms,
		buckets:     make([]windowBucket, bucketCount),
	}
}

// Record 记录一次成功/失败到当前时间桶
func (sw *SlidingWindow) Record(isFail bool) {
	now := time.Now().UnixMilli()
	sw.mu.Lock()
	defer sw.mu.Unlock()
	idx := int((now / sw.bucketMs) % int64(sw.bucketCount))
	b := &sw.buckets[idx]
	// 该桶槽位已滚满一圈（内容已超出整个窗口时长）→ 复用前重置
	if b.startMs == 0 || now-b.startMs >= sw.bucketMs*int64(sw.bucketCount) {
		b.startMs = now
		b.success, b.failure = 0, 0
	}
	if isFail {
		b.failure++
	} else {
		b.success++
	}
}

// Stats 返回滑动窗口内（最近 windowSec 秒）的成功/失败总数
func (sw *SlidingWindow) Stats() (success, failure uint64) {
	now := time.Now().UnixMilli()
	sw.mu.Lock()
	defer sw.mu.Unlock()
	windowMs := sw.bucketMs * int64(sw.bucketCount)
	for _, b := range sw.buckets {
		if b.startMs == 0 || now-b.startMs >= windowMs {
			continue // 空桶或过期桶忽略
		}
		success += b.success
		failure += b.failure
	}
	return success, failure
}

type CircuitBreaker struct {
	state               atomic.Int32
	successCount        atomic.Uint64 // fixed简单计数窗口：成功数
	failureCount        atomic.Uint64 // fixed简单计数窗口：失败数
	thresholdFailurePct uint64
	windowSize          uint64
	//====新增：可切换熔断统计窗口====
	windowType       string         // fixed=简单计数窗口 / sliding=滑动统计窗口
	sliding          *SlidingWindow // 滑动统计窗口（window_type=sliding 时非nil）
	slidingWindowSec int            // 滑动窗口时长(秒)
	openWaitDuration time.Duration
	lastOpenTime     atomic.Int64
	//====新增：半开试探配额控制====
	halfOpenMaxProbe  uint64        // 半开最大允许试探请求数
	halfOpenProbeUsed atomic.Uint64 // 当前半开已经放行试探请求计数
}

// NewCircuitBreaker 创建简单计数窗口（fixed）熔断器，保持原有语义
func NewCircuitBreaker(failurePct uint64, window uint64, openWait time.Duration, halfOpenMaxProbe uint64) *CircuitBreaker {
	return NewCircuitBreakerWindowType("fixed", failurePct, window, openWait, halfOpenMaxProbe, 10)
}

// NewCircuitBreakerWindowType 创建指定统计窗口类型的熔断器
// windowType: "fixed"简单计数窗口 / "sliding"滑动统计窗口；slidingSec 为滑动窗口时长(秒，默认10)
func NewCircuitBreakerWindowType(windowType string, failurePct uint64, window uint64, openWait time.Duration, halfOpenMaxProbe uint64, slidingSec int) *CircuitBreaker {
	if windowType != "sliding" {
		windowType = "fixed"
	}
	if slidingSec < 1 {
		slidingSec = 10
	}
	cb := &CircuitBreaker{
		thresholdFailurePct: failurePct,
		windowSize:          window,
		openWaitDuration:    openWait,
		halfOpenMaxProbe:    halfOpenMaxProbe,
		windowType:          windowType,
		slidingWindowSec:    slidingSec,
	}
	if windowType == "sliding" {
		cb.sliding = NewSlidingWindow(slidingSec)
	}
	cb.state.Store(int32(StateClosed))
	cb.halfOpenProbeUsed.Store(0)
	return cb
}

// GetWindowType 返回熔断器当前统计窗口类型
func (cb *CircuitBreaker) GetWindowType() string {
	return cb.windowType
}

// recordWindow 把一次结果记录进当前统计窗口（fixed 简单计数 / sliding 滑动时间桶）
func (cb *CircuitBreaker) recordWindow(isFail bool) {
	if cb.sliding != nil {
		cb.sliding.Record(isFail)
		return
	}
	// fixed简单计数窗口：攒满 windowSize 个样本先清零再记录（保留原语义）
	succ := cb.successCount.Load()
	fail := cb.failureCount.Load()
	if succ+fail >= cb.windowSize {
		cb.successCount.Store(0)
		cb.failureCount.Store(0)
	}
	if isFail {
		cb.failureCount.Add(1)
	} else {
		cb.successCount.Add(1)
	}
}

// windowFailRate 返回当前窗口失败率(%)与样本总数
func (cb *CircuitBreaker) windowFailRate() (failRate, total uint64) {
	if cb.sliding != nil {
		succ, fail := cb.sliding.Stats()
		total = succ + fail
		if total == 0 {
			return 0, 0
		}
		return fail * 100 / total, total
	}
	succ := cb.successCount.Load()
	fail := cb.failureCount.Load()
	total = succ + fail
	if total == 0 {
		return 0, 0
	}
	return fail * 100 / total, total
}

// WindowStats 返回当前窗口成功/失败数（供状态接口展示）
func (cb *CircuitBreaker) WindowStats() (succ, fail uint64) {
	if cb.sliding != nil {
		return cb.sliding.Stats()
	}
	return cb.successCount.Load(), cb.failureCount.Load()
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
			// 从Open切换进入HalfOpen：原子CAS保证只切换/记录一次
			if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
				cb.halfOpenProbeUsed.Store(0)
				logTimelineEvent("熔断器全局", "halfOpen", "熔断冷却期结束，进入 HalfOpen 半开试探")
			}
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
	cb.recordWindow(isFail)
	st := State(cb.state.Load())
	switch st {
	case StateClosed:
		failRate, total := cb.windowFailRate()
		// 判定门槛：滑动窗口要求窗口内样本达到 windowSize 才判定（防单点误判）；
		// 固定窗口维持原语义（只要有样本即按当前累计失败率判定）
		gate := total > 0
		if cb.sliding != nil {
			gate = total >= cb.windowSize
		}
		if gate && failRate >= cb.thresholdFailurePct {
			// 原子CAS保证只切换/记录一次
			if cb.state.CompareAndSwap(int32(StateClosed), int32(StateOpen)) {
				cb.lastOpenTime.Store(time.Now().UnixMilli())
				logTimelineEvent("熔断器全局", "open", fmt.Sprintf("熔断器切换 Open（熔断打开）：窗口失败率 %d%% 达到阈值 %d%%", failRate, cb.thresholdFailurePct)) //canvas时序事件图功能
			}
		}
	case StateHalfOpen:
		if isFail {
			// 原子CAS保证只切换/记录一次
			if cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateOpen)) {
				cb.lastOpenTime.Store(time.Now().UnixMilli())
				logTimelineEvent("熔断器全局", "open", "半开试探失败，熔断器重新切回 Open（继续阻断）") //canvas时序事件图功能
			}
		} else {
			// 原子CAS保证只切换/记录一次
			if cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateClosed)) {
				logTimelineEvent("熔断器全局", "halfOpen", "半开试探成功，熔断器恢复 Closed 正常状态") //canvas时序事件图功能
			}
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

	// ✅新增：舱配置驱动延时 + 完成时序随机抖动
	// 延时 = 配置RTT + 随机0~60ms：请求全部并发提交保持"突发"，让限流/熔断效果可见；
	// 由每个请求的随机耗时把泳道图完成事件自然错开，便于观察实验过程（替代旧的"提交侧爬坡抖动"）
	var delay time.Duration
	if execErr != nil {
		delay = time.Duration(c.faultDelayMs)*time.Millisecond + randomJitter(0, 60)
	} else {
		delay = time.Duration(c.normalDelayMs)*time.Millisecond + randomJitter(0, 60)
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
		logTimelineEvent(c.name, "error", fmt.Sprintf("[%s]业务执行报错：%v", c.name, execErr)) //canvas时序事件图功能
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
	// 限制最大条数，防止内存暴涨（时序事件最多保留 600 条）
	if len(globalTimelineEvents) > 600 {
		globalTimelineEvents = globalTimelineEvents[len(globalTimelineEvents)-600:]
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

// ===== 真实世界风格的后端业务报错池：每个舱一组，随机命中 =====
var bizErrorTemplates = map[string][]string{
	"下单核心舱": {
		"订单创建超时：DB 写入超时 (code=504, wait=3s, order=%d)",
		"库存扣减失败：库存不足 (order=%d, sku=100023, stock=0)",
		"数据库死锁：事务被回滚，请重试 (errno=1213, order=%d)",
		"支付网关调用超时 (order=%d, gw=PAY-01, timeout=5s)",
		"MySQL 连接池耗尽：无可用连接 (order=%d, pool=20, active=20, waiters=7)",
		"下游订单中心 503 Service Unavailable (order=%d, svc=order-center)",
		"消息发送失败：MQ 不可达 (order=%d, mq=rocketmq-prod, topic=order_create)",
		"分布式锁获取失败：Redis 锁竞争超时 (order=%d, lock=sku_100023:stock, ttl=3s)",
	},
	"营销舱": {
		"活动查询超时：select_activity_by_id 执行超时 3s (act=ACT_%d)",
		"Redis 连接失败，缓存击穿回源 DB (act=ACT_%d, addr=10.0.1.8:6379, err=connection refused)",
		"活动状态异常：活动已下架 (act=ACT_%d, status=OFFLINE)",
		"优惠券服务 502 Bad Gateway (act=ACT_%d, svc=coupon-svc, upstream timeout)",
		"重复提交：幂等键冲突 (act=ACT_%d, idem_key=ACT_%d-U-88)",
		"风控校验拦截：命中限频策略 (act=ACT_%d, uid=U_88, rate=50/min)",
		"活动配置解析失败：JSON 字段缺失 (act=ACT_%d, field=discount_rate)",
		"DB 唯一键冲突：coupon_claim 已存在 (act=ACT_%d, uk=act_uid_coupon)",
	},
	"报表辅助舱": {
		"大报表 SQL 超时被 kill：执行超过 30s (report=%d, sql_id=rpt_q3_revenue)",
		"数据仓库连接失败 (report=%d, warehouse=clickhouse-prod, err=network unreachable)",
		"聚合结果集过大：内存溢出风险被终止 (report=%d, rows=2.1M, est_mem=1.8GB)",
		"报表导出失败：磁盘空间不足 (report=%d, disk=/data/report, free=12MB)",
		"分析引擎 503 Service Unavailable (report=%d, engine=flink-job-7)",
		"维度表关联超时：dim_sku 分片加载失败 (report=%d, shard=3)",
		"HBase region 不可用：行键查询失败 (report=%d, table=rpt_daily, region=rs-02)",
		"报表快照生成失败：ETL 任务未完成 (report=%d, job=etl_report_daily, status=RUNNING)",
	},
}

// randomBizError 从指定舱的报错池中随机取一条真实风格错误
func randomBizError(cabinName string, id int) error {
	pool := bizErrorTemplates[cabinName]
	if len(pool) == 0 {
		return errors.New("service internal error")
	}
	tpl := pool[rand.Intn(len(pool))]
	return errors.New(fmt.Sprintf(tpl, id))
}
func bizOrder(ctx context.Context, orderID int) error {
	// time.Sleep(100 * time.Millisecond) //删掉！延时交给舱配置
	if shouldProduceFault("下单核心舱") {
		return randomBizError("下单核心舱", orderID)
	}
	fmt.Printf(colorGreen+"[下单核心舱] order=%d SUCCESS"+colorReset+"\n", orderID)
	return nil
}
func bizMarketing(ctx context.Context, actID int) error {
	if shouldProduceFault("营销舱") {
		return randomBizError("营销舱", actID)
	}
	fmt.Printf(colorGreen+"[营销舱-查询] act=%d SUCCESS"+colorReset+"\n", actID)
	return nil
}
func bizReport(ctx context.Context, reportID int) error {
	if shouldProduceFault("报表辅助舱") {
		return randomBizError("报表辅助舱", reportID)
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
	//====新增：Open 状态下距进入 HalfOpen 的真实剩余毫秒数（非 Open 为 0），供前端倒计时====
	CbOpenRemainMs int64 `json:"cb_open_remain_ms"`
}

// cbOpenRemainMs 返回熔断器 Open 状态下距离进入 HalfOpen 的剩余毫秒数；非 Open 返回 0
func cbOpenRemainMs(cb *CircuitBreaker) int64 {
	if cb.GetState() != StateOpen {
		return 0
	}
	elapsed := time.Since(time.UnixMilli(cb.lastOpenTime.Load()))
	remain := cb.openWaitDuration - elapsed
	if remain < 0 {
		remain = 0
	}
	return int64(remain / time.Millisecond)
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
		WindowType:       c.cb.windowType,       //新增：统计窗口类型
		SlidingWindowSec: c.cb.slidingWindowSec, //新增：滑动窗口时长(秒)
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

	orderSucc, orderFail := cabinOrder.cb.WindowStats()
	marketingSucc, marketingFail := cabinMarketing.cb.WindowStats()
	reportSucc, reportFail := cabinReport.cb.WindowStats()

	list := []CabinView{
		{
			Name:            cabinOrder.name,
			State:           cabinOrder.cb.GetState().String(),
			Metrics:         cabinOrder.metrics,
			Config:          getCabinConfig(cabinOrder),
			CbWindowSuccess: orderSucc,
			CbWindowFail:    orderFail,
			CbOpenRemainMs:  cbOpenRemainMs(cabinOrder.cb),
		},
		{
			Name:            cabinMarketing.name,
			State:           cabinMarketing.cb.GetState().String(),
			Metrics:         cabinMarketing.metrics,
			Config:          getCabinConfig(cabinMarketing),
			FaultConfig:     getFault("营销舱"),
			CbWindowSuccess: marketingSucc,
			CbWindowFail:    marketingFail,
			CbOpenRemainMs:  cbOpenRemainMs(cabinMarketing.cb),
		},
		{
			Name:            cabinReport.name,
			State:           cabinReport.cb.GetState().String(),
			Metrics:         cabinReport.metrics,
			Config:          getCabinConfig(cabinReport),
			FaultConfig:     getFault("报表辅助舱"),
			CbWindowSuccess: reportSucc,
			CbWindowFail:    reportFail,
			CbOpenRemainMs:  cbOpenRemainMs(cabinReport.cb),
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
	// 窗口类型校验：空值按 fixed（简单计数）处理；仅支持 fixed/sliding
	if cfg.WindowType == "" {
		cfg.WindowType = "fixed"
	}
	if cfg.WindowType != "sliding" && cfg.WindowType != "fixed" {
		http.Error(w, "参数非法：统计窗口类型仅支持 fixed/sliding", 400)
		return
	}
	// 滑动窗口时长缺省默认10秒（仅 window_type=sliding 生效；旧场景配置不携带该字段）
	if cfg.SlidingWindowSec < 1 {
		cfg.SlidingWindowSec = 10
	}

	newCb := NewCircuitBreakerWindowType(cfg.WindowType, cfg.FailThreshold, cfg.WindowSize, time.Duration(cfg.OpenWaitSec)*time.Second, cfg.HalfOpenMaxProbe, cfg.SlidingWindowSec)

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
	windowTypeText := "简单计数窗口"
	if cfg.WindowType == "sliding" {
		windowTypeText = fmt.Sprintf("滑动统计窗口(%ds)", cfg.SlidingWindowSec)
	}
	logTimelineEvent("系统", "config", fmt.Sprintf("已更新配置：%s 最大并发=%d 错误阈值=%d%% 窗口=%d %s 熔断等待=%ds 半开试探=%d 正常耗时=%dms 故障耗时=%dms",
		newCabin.name, cfg.MaxConcurrency, cfg.FailThreshold, cfg.WindowSize, windowTypeText, cfg.OpenWaitSec,
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
// 用于业务RTT的"完成时序抖动"：请求全部并发提交（保持突发，让限流可见），
// 由每个请求的随机耗时把泳道图完成事件自然错开，便于观察实验过程
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

	// 第一轮：全部请求并发提交（突发），限流/熔断效果才可见；
	// 泳道图完成事件的错开由 Run() 内的 RTT 随机抖动实现
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

	// 第二轮（半开试探）：全部并发提交，半开试探可瞬时打满试探配额；
	// 泳道图完成事件的错开由 Run() 内的 RTT 随机抖动实现
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

// func main() {
// 	http.Handle("/", http.FileServer(http.Dir(".")))
// 	http.HandleFunc("/api/status", apiStatus)
// 	http.HandleFunc("/api/applyConfig", apiApplyConfig)
// 	http.HandleFunc("/api/reset", apiReset)
// 	http.HandleFunc("/api/runFirst", apiRunFirst)
// 	http.HandleFunc("/api/waitHalfOpen", apiWaitHalfOpen)
// 	http.HandleFunc("/api/runSecond", apiRunSecond)
// 	http.HandleFunc("/api/setFault", apiSetFault)
// 	http.HandleFunc("/api/clearLogs", apiClearLogs)
// 	http.HandleFunc("/api/resetMetricsOnly", apiResetMetricsOnly)

// 	onEventLog("演示程序已启动")
// 	fmt.Println("网页演示已启动，请浏览器打开：http://127.0.0.1:8080")
// 	_ = http.ListenAndServe(":8080", nil)
// }

func main() {
	// 【替换原来 http.FileServer】：手动处理根路径，输出打包在exe里面的html字节
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			w.Header().Set("Content‑Type", "text/html;charset=utf‑8")
			_, _ = w.Write(embedIndexHTML) // 输出打包进程序的html内容
			return
		}
		http.NotFound(w, r) // 别的路径直接返回404，不再读取磁盘任何文件
	})

	// ========= 下面所有的api接口注册【一行都不要动，原样保留】=========
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
