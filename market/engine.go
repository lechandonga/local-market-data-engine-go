package market

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Config 是 Engine 的配置。
type Config struct {
	// DataDir 是 WAL 持久化目录（必须存在或可创建）。空串表示纯内存、不持久化。
	DataDir string
	// Periods 指定需要聚合的周期集合，至少一个。New 后不可更改。
	Periods []Period
	// MaxTardiness 是乱序容忍上限：tick.Time 早于“该标的已接收最大时间 - MaxTardiness”
	// 时以 RejectTooLate 拒绝。0 表示不允许任何乱序（只能按时间递增到达）。
	MaxTardiness time.Duration
	// SyncWrites 为 true 时每笔 tick 落盘后 fsync，崩溃不丢数据；
	// false 时吞吐更高但进程崩溃可能丢失末尾数据。进程“正常重启”两种模式都可恢复。
	SyncWrites bool
	// Now 用于注入时钟，默认 time.Now。
	Now func() time.Time
}

// Engine 是本地行情引擎。所有方法可被多 goroutine 并发调用。
type Engine struct {
	cfg Config
	wal *walWriter
	hub *hub

	mu      sync.RWMutex
	symbols map[string]*symbolState
}

// symbolState 是单标的的全部内存状态，受 Engine.mu 保护。
type symbolState struct {
	// ticks 是已生效 tick，键为成交时间（同一标的内唯一）。
	ticks map[time.Time]Tick
	// maxSeen 是已接收 tick.Time 的最大值，即迟到判定的“时间水位”。
	maxSeen time.Time
	// closedStart[p]：周期 p 下，起点 <= closedStart[p] 的窗口均已封闭。
	closedStart map[Period]time.Time
	// active[p]：周期 p 当前进行中的 bar（可能为 nil）。
	active map[Period]*aggBar
	// finalized[p]：周期 p 已封闭 bar，按 Start 递增。
	finalized map[Period][]Bar

	totalVolume float64
}

// aggBar 在 Bar 之上额外记录窗口内首/末笔时间，用于确定 Open/Close 的口径。
type aggBar struct {
	Bar
	firstTime time.Time
	lastTime  time.Time
}

// New 创建引擎并从 DataDir 重放本地状态。
// 重放按“写入时的提交顺序”回放；由于所有窗口边界只取决于 tick.Time，
// 重放产出的 bar 与重启前完全一致（确定性见 README“重启恢复”一节）。
func New(cfg Config) (*Engine, error) {
	if len(cfg.Periods) == 0 {
		return nil, errors.New("market: at least one period required")
	}
	if cfg.MaxTardiness < 0 {
		return nil, errors.New("market: MaxTardiness must be >= 0")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	e := &Engine{
		cfg:     cfg,
		hub:     newHub(cfg.Now),
		symbols: make(map[string]*symbolState),
	}

	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, fmt.Errorf("market: create data dir: %w", err)
		}
		path := filepath.Join(cfg.DataDir, "ticks.wal")
		ticks, err := readWAL(path)
		if err != nil {
			return nil, err
		}
		w, err := openWALWriter(path, cfg.SyncWrites)
		if err != nil {
			return nil, err
		}
		e.wal = w
		for _, t := range ticks {
			// 重放路径不做拒绝判定：日志中的记录都是写入时已验证并生效的数据。
			if r := e.ingestReplay(t); !r.Accepted {
				return nil, fmt.Errorf("market: replay rejected %s @ %s: %s", t.Symbol, t.Time, r.Reason)
			}
		}
	}
	return e, nil
}

// Close 关闭底层 WAL 文件。关闭后不应再写入。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal != nil {
		return e.wal.Close()
	}
	return nil
}

func validateTick(t Tick) string {
	switch {
	case t.Symbol == "":
		return "empty symbol"
	case t.Time.IsZero():
		return "zero time"
	case t.Price <= 0:
		return fmt.Sprintf("price must be > 0, got %v", t.Price)
	case t.Volume < 0:
		return fmt.Sprintf("volume must be >= 0, got %v", t.Volume)
	}
	return ""
}

func reject(r RejectReason, detail string) IngestResult {
	return IngestResult{Accepted: false, Reason: r, Detail: detail}
}

// Ingest 接收一笔 tick。允许乱序；重复与迟到按 Config 中的规则处理。
// 拒绝不会改动任何已有状态，也不会写入 WAL。
func (e *Engine) Ingest(t Tick) IngestResult {
	if d := validateTick(t); d != "" {
		return reject(RejectInvalid, d)
	}
	t.Time = t.Time.UTC()

	e.mu.Lock()
	defer e.mu.Unlock()

	st := e.symbols[t.Symbol]
	if st != nil {
		if old, ok := st.ticks[t.Time]; ok {
			return reject(RejectDuplicate,
				fmt.Sprintf("tick for %s @ %s already exists with price %v volume %v; incoming price %v ignored",
					t.Symbol, t.Time.Format(time.RFC3339Nano), old.Price, old.Volume, t.Price))
		}
		for _, p := range e.cfg.Periods {
			if cs, ok := st.closedStart[p]; ok && !p.WindowStart(t.Time).After(cs) {
				return reject(RejectBarClosed,
					fmt.Sprintf("window starting %s (%s) is already closed (closed through %s)",
						p.WindowStart(t.Time).Format(time.RFC3339Nano), p, cs.Format(time.RFC3339Nano)))
			}
		}
		if !st.maxSeen.IsZero() && e.cfg.MaxTardiness > 0 {
			if cutoff := st.maxSeen.Add(-e.cfg.MaxTardiness); t.Time.Before(cutoff) {
				return reject(RejectTooLate,
					fmt.Sprintf("tick time %s is before watermark %s minus max tardiness %s (cutoff %s)",
						t.Time.Format(time.RFC3339Nano), st.maxSeen.Format(time.RFC3339Nano), e.cfg.MaxTardiness, cutoff.Format(time.RFC3339Nano)))
			}
		}
		if !st.maxSeen.IsZero() && e.cfg.MaxTardiness == 0 && t.Time.Before(st.maxSeen) {
			return reject(RejectTooLate,
				fmt.Sprintf("out-of-order tick %s but max tardiness is 0 (watermark %s)",
					t.Time.Format(time.RFC3339Nano), st.maxSeen.Format(time.RFC3339Nano)))
		}
	}

	if e.wal != nil {
		if err := e.wal.Append(t); err != nil {
			return reject(RejectInvalid, "wal append failed: "+err.Error())
		}
	}

	events := e.applyTick(t)
	e.hub.publish(events)
	return IngestResult{Accepted: true}
}

// ingestReplay 是重启重放路径：跳过重复/迟到/封闭窗口判定（日志只含已提交数据），
// 但同样跳过 WAL 追加，也不发布订阅事件（重放期间尚不存在订阅者）。
func (e *Engine) ingestReplay(t Tick) IngestResult {
	if d := validateTick(t); d != "" {
		return reject(RejectInvalid, d)
	}
	t.Time = t.Time.UTC()
	e.applyTick(t)
	return IngestResult{Accepted: true}
}

// applyTick 把已确认的 tick 合并进内存状态，返回需要发布的事件。
// 调用方必须持有 e.mu。
func (e *Engine) applyTick(t Tick) []Event {
	st := e.symbols[t.Symbol]
	if st == nil {
		st = &symbolState{
			ticks:       make(map[time.Time]Tick),
			closedStart: make(map[Period]time.Time),
			active:      make(map[Period]*aggBar),
			finalized:   make(map[Period][]Bar),
		}
		e.symbols[t.Symbol] = st
	}

	st.ticks[t.Time] = t
	st.totalVolume += t.Volume
	if t.Time.After(st.maxSeen) {
		st.maxSeen = t.Time
	}

	var events []Event
	for _, p := range e.cfg.Periods {
		w := p.WindowStart(t.Time)
		// 时间水位前进时，封闭当前窗口之前的所有窗口（含没有 tick 的空窗）。
		if cur := st.active[p]; cur != nil && w.After(cur.Start) {
			events = append(events, e.finalizeBar(st, p, w)...)
		}
		mergeTick(st, p, w, t)
	}
	events = append(events, TickAcceptedEvent{Tick: t, Seq: 0})
	return events
}

// finalizeBar 封闭 active bar，并把水位推进到 curWinStart-p 所在窗口。
func (e *Engine) finalizeBar(st *symbolState, p Period, curWinStart time.Time) []Event {
	var events []Event
	if b := st.active[p]; b != nil {
		st.finalized[p] = append(st.finalized[p], b.Bar)
		events = append(events, BarFinalizedEvent{Bar: b.Bar})
		st.active[p] = nil
	}
	// 跳过的空窗口也标记为封闭：迟到 tick 再落到这些窗口时得到 RejectBarClosed。
	st.closedStart[p] = curWinStart.Add(-p.Duration())
	return events
}

func mergeTick(st *symbolState, p Period, w time.Time, t Tick) {
	b := st.active[p]
	if b == nil || !b.Start.Equal(w) {
		b = &aggBar{
			Bar: Bar{
				Symbol: t.Symbol,
				Period: p,
				Start:  w,
				Open:   t.Price,
				High:   t.Price,
				Low:    t.Price,
				Close:  t.Price,
			},
			firstTime: t.Time,
			lastTime:  t.Time,
		}
		st.active[p] = b
	}
	if t.Time.Equal(b.firstTime) || t.Time.Before(b.firstTime) {
		// 乱序早到：同一纳秒并列时后入库者不改变 Open（先入库为 Open）。
		if t.Time.Before(b.firstTime) {
			b.firstTime = t.Time
			b.Open = t.Price
		}
	}
	if t.Time.After(b.lastTime) {
		b.lastTime = t.Time
		b.Close = t.Price
	}
	if t.Price > b.High {
		b.High = t.Price
	}
	if t.Price < b.Low {
		b.Low = t.Price
	}
	b.Volume += t.Volume
	b.TickCount++
}

// Bars 返回某标的某周期已封闭的 bar 副本（按 Start 递增）。
// 进行中的窗口用 Snapshot 查看。
func (e *Engine) Bars(symbol string, p Period) []Bar {
	e.mu.RLock()
	defer e.mu.RUnlock()
	st := e.symbols[symbol]
	if st == nil {
		return nil
	}
	src := st.finalized[p]
	out := make([]Bar, len(src))
	copy(out, src)
	return out
}

// Symbols 返回当前有已生效数据的标的列表（字典序）。
func (e *Engine) Symbols() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.symbols))
	for s := range e.symbols {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
