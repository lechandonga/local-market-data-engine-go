// Package market 是一个纯本地的行情数据引擎：接收逐笔行情（tick），
// 去重、拒绝迟到数据、按固定周期聚合成 K 线（bar），并支持快照与增量订阅。
//
// 本包只依赖 Go 标准库，不接入任何外部数据源。
package market

import "time"

// Tick 表示一笔逐笔行情。
//
// 字段口径：
//   - Symbol：标的代码，同一标的内 Time 唯一（见 Engine 的判重规则）。
//   - Time：成交发生时间（交易所时间），bar 归属与判重均以它为准，与到达顺序无关。
//   - Price：成交价，必须 > 0。
//   - Volume：成交量，必须 >= 0（允许零成交量的报价型 tick 参与 OHLC 但不累计量）。
type Tick struct {
	Symbol string
	Time   time.Time
	Price  float64
	Volume float64
}

// RejectReason 是 tick 被拒绝的可区分原因。
type RejectReason string

const (
	// RejectDuplicate 表示同一标的、同一 Time 的 tick 已存在。
	// 已有数据保持不变，重复数据不会计入任何聚合。
	RejectDuplicate RejectReason = "duplicate"
	// RejectBarClosed 表示该 tick 所属的聚合窗口已经封闭（bar 已对外给出）。
	// 本引擎对封闭窗口采取“拒绝”策略，而不是改写历史。
	RejectBarClosed RejectReason = "bar_closed"
	// RejectTooLate 表示 tick 的 Time 早于“已接收时间水位 - 最大迟到容忍”。
	RejectTooLate RejectReason = "too_late"
	// RejectInvalid 表示字段非法（空标的、零时间、价格 <= 0、成交量 < 0）。
	RejectInvalid RejectReason = "invalid"
)

// IngestResult 是一笔 tick 入库的结果。
type IngestResult struct {
	// Accepted 为 true 表示数据已生效（计入内存状态与持久化日志）。
	Accepted bool
	// Reason 仅在 Accepted == false 时给出拒绝原因。
	Reason RejectReason
	// Detail 给出可解释的细节，例如已存在 tick 的价格、封闭窗口的起点等。
	Detail string
}

// Bar 是一个固定周期的 K 线。
//
// OHLCV 口径：
//   - Open：窗口内“成交时间最早”的 tick 的 Price；时间并列时取先入库的一笔。
//   - High：窗口内 Price 的最大值。
//   - Low：窗口内 Price 的最小值。
//   - Close：窗口内“成交时间最晚”的 tick 的 Price；时间并列时取后入库的一笔。
//   - Volume：窗口内所有 tick 的 Volume 之和。
//   - TickCount：窗口内计入的 tick 笔数（可用于审计聚合自洽性）。
type Bar struct {
	Symbol    string
	Period    Period
	Start     time.Time // 窗口起点，左闭右开 [Start, Start+Period)
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	TickCount int64
}

// Period 是固定聚合周期，始终使用 UTC 并与时间原点对齐：
// 窗口起点 = Time.UTC().Truncate(Period)。例如 5m 的窗口为 :00-:05、:05-:10，
// 与 tick 到达顺序无关，不会随接收顺序漂移。
type Period time.Duration

// 常用周期。
const (
	Period1m  = Period(time.Minute)
	Period5m  = Period(5 * time.Minute)
	Period15m = Period(15 * time.Minute)
	Period1h  = Period(time.Hour)
	Period1d  = Period(24 * time.Hour)
)

// Duration 转回 time.Duration。
func (p Period) Duration() time.Duration { return time.Duration(p) }

// WindowStart 返回给定时间所属窗口的起点（UTC 对齐）。
func (p Period) WindowStart(t time.Time) time.Time {
	d := int64(p.Duration())
	u := t.UTC().UnixNano()
	return time.Unix(0, u-u%d).UTC()
}

func (p Period) String() string { return p.Duration().String() }

// Snapshot 是某一标的在某时刻与“已接收并生效数据”一致的行情快照。
//
// 快照在引擎读锁内一次性构建，因此 LastTime/LastPrice 与 Bars 之间
// 不会出现撕裂状态。
type Snapshot struct {
	Symbol      string
	TakenAt     time.Time // 快照构建时间（引擎本地时钟）
	LastTime    time.Time // 已生效 tick 中的最大成交时间
	LastPrice   float64   // 该最大成交时间对应 tick 的 Price
	TotalVolume float64   // 所有已生效 tick 的成交量之和
	TickCount   int64     // 已生效 tick 笔数
	// Bars 是当前各周期“未封闭（进行中）”的 bar，按周期由小到大排序。
	// 已封闭的 bar 通过 Bars() 查询，不放在快照里。
	Bars []Bar
}
