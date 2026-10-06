package market

import (
	"sync"
	"time"
)

// DefaultSubscriberBuffer 是订阅邮箱/输出通道的默认缓冲大小。
const DefaultSubscriberBuffer = 256

type hub struct {
	mu   sync.RWMutex
	seq  int64
	subs map[*Subscription]struct{}
	now  func() time.Time
}

func newHub(now func() time.Time) *hub {
	return &hub{subs: make(map[*Subscription]struct{}), now: now}
}

// subscribe 注册一个订阅者。每个订阅者由一个独立 feeder 协程串行化投递：
// 写入端只做一次非阻塞入队，feeder 负责阻塞写出与断档合成，
// 因此慢订阅者在任何时刻（包括写入停止后）都能收到精确的 gap。
func (h *hub) subscribe(buffer int) *Subscription {
	if buffer <= 0 {
		buffer = DefaultSubscriberBuffer
	}
	out := make(chan Event, buffer)
	s := &Subscription{
		C:      out,
		ch:     out,
		in:     make(chan feedItem, buffer),
		buffer: buffer,
		hub:    h,
	}
	s.done.Add(1)
	go s.feed()

	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// publish 给事件分配全局单调 Seq 并投递给所有订阅者。
// 绝不阻塞：订阅邮箱满时事件进入该订阅者的丢弃区间。
func (h *hub) publish(evs []Event) {
	if len(evs) == 0 {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.subs) == 0 {
		h.seq += int64(len(evs))
		return
	}

	for _, ev := range evs {
		h.seq++
		switch e := ev.(type) {
		case TickAcceptedEvent:
			e.Seq = h.seq
			ev = e
		case BarFinalizedEvent:
			e.Seq = h.seq
			ev = e
		}
		for s := range h.subs {
			s.offer(ev)
		}
	}
}
