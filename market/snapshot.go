package market

import (
	"sort"
)

// Subscribe 订阅全标的增量事件。事件在全局单调 Seq 下分发（见 Event）。
// buffer 为通道缓冲；消费过慢时按“丢弃 + gap 断档标记”处理，绝不阻塞写入。
// buffer <= 0 时使用 DefaultSubscriberBuffer。
func (e *Engine) Subscribe(buffer int) *Subscription {
	return e.hub.subscribe(buffer)
}

// Snapshot 返回某标的当前与已生效数据一致的快照。
// 快照在读锁内一次性构建，字段之间不会出现并发撕裂。
func (e *Engine) Snapshot(symbol string) (Snapshot, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	st := e.symbols[symbol]
	if st == nil {
		return Snapshot{}, false
	}

	snap := Snapshot{
		Symbol:      symbol,
		TakenAt:     e.cfg.Now(),
		LastTime:    st.maxSeen,
		TotalVolume: st.totalVolume,
		TickCount:   int64(len(st.ticks)),
		Bars:        make([]Bar, 0, len(e.cfg.Periods)),
	}
	if t, ok := st.ticks[st.maxSeen]; ok {
		snap.LastPrice = t.Price
	}
	for _, p := range e.cfg.Periods {
		if b := st.active[p]; b != nil {
			snap.Bars = append(snap.Bars, b.Bar)
		}
	}
	return snap, true
}

// FlushBars 按当前时间水位封闭所有进行中的 bar（显式收盘场景）。
// 返回新封闭的 bar 事件，便于调用方拿到完整收盘序列。
func (e *Engine) FlushBars(symbol string) []Bar {
	e.mu.Lock()
	defer e.mu.Unlock()

	st := e.symbols[symbol]
	if st == nil {
		return nil
	}
	var out []Bar
	now := e.cfg.Now().UTC()
	for _, p := range e.cfg.Periods {
		if b := st.active[p]; b != nil {
			curWin := p.WindowStart(now)
			if !curWin.After(b.Start) {
				curWin = b.Start.Add(p.Duration())
			}
			evs := e.finalizeBar(st, p, curWin)
			for _, ev := range evs {
				if be, ok := ev.(BarFinalizedEvent); ok {
					out = append(out, be.Bar)
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start.Equal(out[j].Start) {
			return out[i].Period < out[j].Period
		}
		return out[i].Start.Before(out[j].Start)
	})
	evs := make([]Event, 0, len(out))
	for i := range out {
		evs = append(evs, BarFinalizedEvent{Bar: out[i]})
	}
	e.hub.publish(evs)
	return out
}
