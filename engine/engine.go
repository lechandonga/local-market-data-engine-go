package engine

import (
	"errors"
	"sort"
	"sync"
)

// Config 配置引擎。
type Config struct {
	// Period 是 K 线固定周期（纳秒），必须 > 0。
	Period int64
	// MaxLate 是允许迟到的最大幅度（纳秒）。
	// 某窗口 [w, w+Period) 在 watermark >= w+Period+MaxLate 后定稿，
	// 再迟到、落入定稿窗口的 tick 会被以 too_late 拒绝。
	MaxLate int64
	// WALPath 为本地 tick 日志路径；为空则不持久化（进程重启不可恢复）。
	WALPath string
	// FsyncWAL 为 true 时每条生效 tick 都 fsync，崩溃不丢已确认数据；
	// false 吞吐更高，但进程崩溃可能丢失末尾尚未落盘的记录。
	FsyncWAL bool
}

// EventType 是订阅事件类型。
type EventType string

const (
	// EventTickAccepted 新 tick 生效（含迟到修正）。
	EventTickAccepted EventType = "tick_accepted"
	// EventBarUpserted bar 首次产出或被迟到 tick 修正（看 Bar.Revision）。
	EventBarUpserted EventType = "bar_upserted"
	// EventBarFinalized 窗口定稿，此后该 bar 不会再变化。
	EventBarFinalized EventType = "bar_finalized"
	// EventGap 通知订阅者：因消费过慢，[GapFromSeq, GapToSeq] 的事件已被丢弃。
	EventGap EventType = "gap"
)

// Event 是发给订阅者的增量事件。Seq 在单个引擎生命周期内严格单调递增、不重复。
type Event struct {
	Seq  uint64
	Type EventType
	Tick *Tick // EventTickAccepted 时非空
	Bar  *Bar  // EventBarUpserted / EventBarFinalized 时非空（值拷贝）
	// EventGap 时有效：
	GapDropped int
	GapFromSeq uint64
	GapToSeq   uint64
}

type symbolState struct {
	ticks     map[int64]Tick // time -> 生效 tick（同标的同时间戳至多一条）
	bars      map[int64]*Bar // windowStart -> bar
	watermark int64          // 已生效 tick 的最大时间戳
	lastPrice uint64
	totalVol  uint64
	tickCount int64
}

// Engine 是本地行情数据引擎。所有方法可被多 goroutine 并发调用。
type Engine struct {
	cfg  Config
	mu   sync.RWMutex
	data map[string]*symbolState
	seq  uint64

	wal *WAL

	broker *broker
	closed bool
}

// New 创建引擎；若 WALPath 存在历史数据则先回放恢复状态。
// 回放期间不发订阅事件；回放顺序与原始写入顺序一致，故恢复出的
// tick、判重结果与 bar 序列与重启前完全一致。
func New(cfg Config) (*Engine, error) {
	if cfg.Period <= 0 {
		return nil, errors.New("engine: period must be positive")
	}
	if cfg.MaxLate < 0 {
		return nil, errors.New("engine: maxLate must be non-negative")
	}
	e := &Engine{cfg: cfg, data: map[string]*symbolState{}, broker: newBroker()}
	if cfg.WALPath != "" {
		wal, err := OpenWAL(cfg.WALPath, cfg.FsyncWAL)
		if err != nil {
			return nil, err
		}
		e.wal = wal
		if _, err := ReplayWAL(cfg.WALPath, func(t Tick) error {
			// 回放只重建状态，不写 WAL、不发事件。
			return e.ingestReplay(t)
		}); err != nil {
			wal.Close()
			return nil, err
		}
	}
	return e, nil
}

// Ingest 接收一个 tick，允许乱序。返回明确的处理结果，绝不静默丢弃：
//   - 正常生效：Accepted=true, Corrected=false
//   - 迟到但可修正：Accepted=true, Corrected=true
//   - 拒绝：Accepted=false，Reason 给出可区分原因
func (e *Engine) Ingest(t Tick) (IngestResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return IngestResult{}, errors.New("engine: closed")
	}
	res, events, err := e.apply(t)
	if err != nil {
		return IngestResult{}, err
	}
	if !res.Accepted {
		return res, nil
	}
	if e.wal != nil {
		if err := e.wal.Append(t); err != nil {
			// 持久化失败：状态不能与磁盘背离，回滚由调用方按 error 重试；
			// 此处已修改内存，故直接报错并保持引擎可用，调用方可选择 Close。
			return res, err
		}
	}
	for _, ev := range events {
		e.publishLocked(ev)
	}
	return res, nil
}

func (e *Engine) ingestReplay(t Tick) error {
	_, _, err := e.apply(t)
	return err
}

// apply 在持有写锁的前提下处理 tick，返回需要发布的事件（不含 Seq）。
func (e *Engine) apply(t Tick) (IngestResult, []eventDraft, error) {
	if t.Time == 0 {
		return IngestResult{Reason: RejectInvalidTime}, nil, nil
	}
	st, ok := e.data[t.Symbol]
	if !ok {
		st = &symbolState{ticks: map[int64]Tick{}, bars: map[int64]*Bar{}}
		e.data[t.Symbol] = st
	}

	// 判重：同标的同时间戳。
	if existing, dup := st.ticks[t.Time]; dup {
		r := RejectDuplicate
		if !existing.equal(t) {
			r = RejectDuplicateConflict
		}
		// 明确规则：保留先到者，重复内容直接忽略，冲突内容拒绝且不改已有数据。
		return IngestResult{Reason: r}, nil, nil
	}

	wStart := floorWindow(t.Time, e.cfg.Period)
	wEnd := wStart + e.cfg.Period

	// 迟到判定：窗口已定稿则拒绝，不允许改坏已最终确认的数据。
	if bar := st.bars[wStart]; bar != nil && bar.Final {
		return IngestResult{Reason: RejectTooLate}, nil, nil
	}

	// 生效。
	st.ticks[t.Time] = t
	st.tickCount++
	st.totalVol += t.Volume
	if t.Time >= st.watermark {
		st.watermark = t.Time
		st.lastPrice = t.Price
	}

	corrected := false
	var evs []eventDraft
	evs = append(evs, eventDraft{typ: EventTickAccepted, tick: &t})

	if existing := st.bars[wStart]; existing != nil {
		// 迟到 tick 落入尚未定稿、但已对外给出的窗口：整窗重算并自洽。
		rebuilt := rebuildBar(t.Symbol, wStart, wEnd, st.ticks)
		rebuilt.Revision = existing.Revision + 1
		rebuilt.Final = existing.Final
		st.bars[wStart] = rebuilt
		corrected = true
		b := rebuilt.clone()
		evs = append(evs, eventDraft{typ: EventBarUpserted, bar: &b})
	} else {
		bar := rebuildBar(t.Symbol, wStart, wEnd, st.ticks)
		st.bars[wStart] = bar
		b := bar.clone()
		evs = append(evs, eventDraft{typ: EventBarUpserted, bar: &b})
	}

	// 随 watermark 推进定稿窗口。
	for ws, bar := range st.bars {
		if !bar.Final && st.watermark >= ws+e.cfg.Period+e.cfg.MaxLate {
			bar.Final = true
			b := bar.clone()
			evs = append(evs, eventDraft{typ: EventBarFinalized, bar: &b})
		}
	}

	return IngestResult{Accepted: true, Corrected: corrected, CorrectedWindowStart: wStart}, evs, nil
}

func floorWindow(t, period int64) int64 {
	// Unix 纳秒对所有正周期；t 恒为正（Time==0 已拒绝）。
	return t - t%period
}

// rebuildBar 用窗口内全部 tick 重算 bar。时间戳唯一，因此开/收分别是
// 最小/最大时间戳 tick 的价格；窗口归属只由 tick 时间戳决定，与到达顺序无关。
func rebuildBar(symbol string, wStart, wEnd int64, ticks map[int64]Tick) *Bar {
	bar := &Bar{Symbol: symbol, WindowStart: wStart, WindowEnd: wEnd}
	var minT, maxT int64
	first := true
	for ts, tk := range ticks {
		if ts < wStart || ts >= wEnd {
			continue
		}
		if first {
			minT, maxT = ts, ts
			bar.High, bar.Low = tk.Price, tk.Price
			first = false
		} else {
			if ts < minT {
				minT = ts
			}
			if ts > maxT {
				maxT = ts
			}
			if tk.Price > bar.High {
				bar.High = tk.Price
			}
			if tk.Price < bar.Low {
				bar.Low = tk.Price
			}
		}
		bar.Volume += tk.Volume
		bar.TickCount++
	}
	bar.Open = ticks[minT].Price
	bar.Close = ticks[maxT].Price
	return bar
}

type eventDraft struct {
	typ  EventType
	tick *Tick
	bar  *Bar
}

func (e *Engine) publishLocked(d eventDraft) {
	e.seq++
	ev := Event{Seq: e.seq, Type: d.typ, Tick: d.tick, Bar: d.bar}
	e.broker.publish(ev)
}

// Snapshot 返回某标的当前已生效数据的一致快照。
func (e *Engine) Snapshot(symbol string) Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	snap := Snapshot{Symbol: symbol}
	if st, ok := e.data[symbol]; ok {
		snap.LastTime = st.watermark
		snap.LastPrice = st.lastPrice
		snap.TotalVol = st.totalVol
		snap.TickCount = st.tickCount
	}
	return snap
}

// GetBar 返回指定窗口起点的 bar（不存在时 ok=false）。
func (e *Engine) GetBar(symbol string, windowStart int64) (Bar, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if st, ok := e.data[symbol]; ok {
		if b, ok := st.bars[windowStart]; ok {
			return b.clone(), true
		}
	}
	return Bar{}, false
}

// Bars 返回某标的按窗口起点升序排列的全部 bar。
func (e *Engine) Bars(symbol string) []Bar {
	e.mu.RLock()
	defer e.mu.RUnlock()
	st, ok := e.data[symbol]
	if !ok {
		return nil
	}
	out := make([]Bar, 0, len(st.bars))
	for _, b := range st.bars {
		out = append(out, b.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WindowStart < out[j].WindowStart })
	return out
}

// Symbols 返回当前有数据的全部标的。
func (e *Engine) Symbols() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.data))
	for s := range e.data {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Watermark 返回某标的已生效 tick 的最大时间戳。
func (e *Engine) Watermark(symbol string) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if st, ok := e.data[symbol]; ok {
		return st.watermark
	}
	return 0
}

// Subscribe 订阅增量事件。buffer 为每订阅者通道容量；消费不过来导致通道满时，
// 新事件被丢弃并通过 EventGap 告知丢弃数量与断点区间，绝不阻塞写入端。
func (e *Engine) Subscribe(buffer int) *Subscription {
	return e.broker.subscribe(buffer)
}

// Close 关闭引擎、WAL 并结束全部订阅（订阅通道会被关闭）。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	var err error
	if e.wal != nil {
		err = e.wal.Close()
	}
	e.mu.Unlock()
	e.broker.close()
	return err
}
