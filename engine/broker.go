package engine

import "sync"

// Subscription 是一个事件订阅。Events() 返回的通道只接收不发送；
// 慢消费会触发丢弃并收到 EventGap 信号。
//
// 内部结构：写入端把事件非阻塞放入有界环形邮箱；每个订阅一个 pump
// goroutine 把邮箱事件搬到对外通道。因此慢消费只会阻塞 pump，
// 永远不阻塞写入端；消费端腾出空间后，挂起的 gap 信号一定能补发。
type Subscription struct {
	ch chan Event

	mu     sync.Mutex
	closed bool
	cancel func()

	// lastSeq 是已成功进入邮箱的最近一条业务事件序号；用于 gap 定位断点。
	lastSeq uint64

	// 待通知的丢弃区间：[gapFrom, gapTo]，共 gapDropped 条。
	gapDropped int
	gapFrom    uint64
	gapTo      uint64

	ring     []Event
	head     int
	count    int
	capacity int
	wake     chan struct{}
	done     chan struct{}
}

// Events 返回事件通道。业务事件 Seq 在引擎内全局严格单调递增。
func (s *Subscription) Events() <-chan Event { return s.ch }

// Close 取消订阅并关闭事件通道。可重复调用；会等待 pump 退出。
func (s *Subscription) Close() {
	if s.cancel != nil {
		s.cancel() // 先从总线摘除，保证不再有新的 deliver
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	<-s.done
}

// enqueueLocked 入队时优先补发 gap 断点；调用方持锁。
func (s *Subscription) enqueueLocked(ev Event) {
	if s.gapDropped > 0 && s.count < s.capacity {
		s.pushLocked(s.gapEventLocked())
		s.gapDropped = 0
	}
	if s.count < s.capacity {
		s.pushLocked(ev)
		s.lastSeq = ev.Seq
		return
	}
	// 背压策略：丢弃最新事件并累计断点，绝不阻塞写入端。
	if s.gapDropped == 0 {
		s.gapFrom = ev.Seq
	}
	s.gapTo = ev.Seq
	s.gapDropped++
}

// gapEventLocked 构造本地 gap 信号；不占引擎事件序号，
// Seq 取最近成功收到的业务事件序号（首条就丢时为 0）。
func (s *Subscription) gapEventLocked() Event {
	return Event{
		Seq:        s.lastSeq,
		Type:       EventGap,
		GapDropped: s.gapDropped,
		GapFromSeq: s.gapFrom,
		GapToSeq:   s.gapTo,
	}
}

func (s *Subscription) pushLocked(ev Event) {
	s.ring[(s.head+s.count)%s.capacity] = ev
	s.count++
}

func (s *Subscription) popLocked() (Event, bool) {
	if s.count == 0 {
		return Event{}, false
	}
	ev := s.ring[s.head]
	s.ring[s.head] = Event{}
	s.head = (s.head + 1) % s.capacity
	s.count--
	return ev, true
}

// pump 单 goroutine 运行：把邮箱事件搬到对外通道。
func (s *Subscription) pump() {
	defer close(s.done)
	defer close(s.ch)
	for {
		s.mu.Lock()
		ev, ok := s.popLocked()
		closing := !ok && s.closed
		s.mu.Unlock()
		if closing {
			return
		}
		if !ok {
			<-s.wake
			continue
		}
		s.ch <- ev // 慢消费只阻塞 pump，不阻塞写入端
		// 发送后若有挂起 gap 且邮箱有空位，无需新事件即可补入。
		s.mu.Lock()
		if s.gapDropped > 0 && s.count < s.capacity {
			s.pushLocked(s.gapEventLocked())
			s.gapDropped = 0
		}
		s.mu.Unlock()
		s.signal()
	}
}

func (s *Subscription) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// deliver 由写入端在发布事件时调用：非阻塞、有界丢弃。
func (s *Subscription) deliver(ev Event) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.enqueueLocked(ev)
	s.mu.Unlock()
	s.signal()
}

type broker struct {
	mu   sync.RWMutex
	subs map[*Subscription]struct{}
}

func newBroker() *broker {
	return &broker{subs: map[*Subscription]struct{}{}}
}

func (b *broker) subscribe(buffer int) *Subscription {
	if buffer < 1 {
		buffer = 1
	}
	s := &Subscription{
		ch:       make(chan Event, buffer),
		ring:     make([]Event, buffer),
		capacity: buffer,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	s.cancel = func() { b.unsubscribe(s) }
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	go s.pump()
	return s
}

func (b *broker) unsubscribe(s *Subscription) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
}

func (b *broker) publish(ev Event) {
	b.mu.RLock()
	subs := make([]*Subscription, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.RUnlock()
	for _, s := range subs {
		s.deliver(ev)
	}
}

func (b *broker) close() {
	b.mu.Lock()
	subs := b.subs
	b.subs = map[*Subscription]struct{}{}
	b.mu.Unlock()
	for s := range subs {
		s.Close()
	}
}
