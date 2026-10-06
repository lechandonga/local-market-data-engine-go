package engine

// RejectReason 描述一个 tick 被拒绝的可区分原因。
type RejectReason string

const (
	// RejectInvalidTime tick 时间戳为零值。
	RejectInvalidTime RejectReason = "invalid_timestamp"
	// RejectTooLate tick 迟到超过引擎允许的窗口（对应窗口已定稿）。
	RejectTooLate RejectReason = "too_late"
	// RejectDuplicate 同一标的、同一时间戳的 tick 已经生效。
	RejectDuplicate RejectReason = "duplicate"
	// RejectDuplicateConflict 与已生效 tick 同标的同时间戳，但价格/量内容冲突；保留先到者。
	RejectDuplicateConflict RejectReason = "duplicate_conflict"
)

// Tick 是一笔逐笔行情。
type Tick struct {
	Symbol string
	Time   int64  // Unix 纳秒，UTC 语义
	Price  uint64 // 定点价格：实际价格 * PriceScale
	Volume uint64
}

// PriceScale 是 Price 字段的放大倍数（1e9，即 9 位小数定点数）。
const PriceScale = 1_000_000_000

// 判等：同标的同时间戳且价格、成交量一致视为重复。
func (t Tick) equal(o Tick) bool {
	return t.Symbol == o.Symbol && t.Time == o.Time && t.Price == o.Price && t.Volume == o.Volume
}

// IngestResult 是 Ingest 的处理结果。Accepted 为 true 表示 tick 已生效并计入聚合。
type IngestResult struct {
	Accepted bool
	// Corrected 表示该 tick 是迟到 tick，但落入可修正窗口，触发了对已发布 bar 的修正。
	Corrected bool
	// CorrectedWindowStart 是被修正 bar 的窗口起点（仅 Corrected 时有意义）。
	CorrectedWindowStart int64
	Reason               RejectReason
}

// Bar 是固定周期 K 线。窗口为左闭右开 [WindowStart, WindowEnd)。
// OHLC 口径：
//   - Open  = 窗口内时间戳最小 tick 的价格（同时间戳按到达顺序，先到者为开）
//   - Close = 窗口内时间戳最大 tick 的价格（同时间戳按到达顺序，最后到者为收）
//   - High  = 窗口内全部 tick 的最高价
//   - Low   = 窗口内全部 tick 的最低价
//   - Volume= 窗口内全部 tick 的成交量之和
type Bar struct {
	Symbol      string
	WindowStart int64
	WindowEnd   int64
	Open        uint64
	High        uint64
	Low         uint64
	Close       uint64
	Volume      uint64
	TickCount   int
	// Final 为 true 表示窗口已定稿，迟到超过 MaxLate 的 tick 无法再落入其中。
	Final bool
	// Revision 在 bar 被迟到 tick 修正后递增；首次产出为 0。
	Revision int
}

// Snapshot 是某一时刻某标的已生效数据的一致快照。
type Snapshot struct {
	Symbol    string
	LastTime  int64 // 最近生效 tick 的时间戳；无数据时为 0
	LastPrice uint64
	TotalVol  uint64
	TickCount int64
}

func (b Bar) clone() Bar { return b }
