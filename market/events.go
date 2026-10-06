package market

import (
	"sync"
	"time"
)

// Event 是订阅方收到的增量事件。事件在引擎内带有单调递增的 Seq，
// 订阅方按 Seq 接收即可保证顺序不倒退。
type Event interface {
	eventSeq() int64
	eventKind() string
}

// TickAcceptedEvent：一笔 tick 通过校验并生效。
type TickAcceptedEvent struct {
	Tick Tick
	Seq  int64
}

func (e TickAcceptedEvent) eventSeq() int64 { return e.Seq }
func (TickAcceptedEvent) eventKind() string { return "tick_accepted" }

// BarFinalizedEvent：一个窗口封闭（下一个窗口的首笔 tick 到达时触发）。
// 该事件即“bar 对外给出”的时点；事件之后，落在封闭窗口的 tick 一律拒绝。
type BarFinalizedEvent struct {
	Bar Bar
	Seq int64
}

func (e BarFinalizedEvent) eventSeq() int64 { return e.Seq }
func (BarFinalizedEvent) eventKind() string { return "bar_finalized" }

// SubscriptionGapEvent：订阅方因消费过慢被丢弃增量后收到的断档标记。
//
// 语义：收到它意味着 (FromSeq, ToSeq] 区间内的业务事件已永久丢失，
// 订阅方应通过 Snapshot()/Bars() 做一次全量对账，再从 ToSeq+1 继续增量。
// 连续丢弃合并为同一个 gap（断档结束、恢复事件可入队时原子绑定发出），
// 不会用断档标记二次压垮慢订阅者；即使写入已停止，feeder 也会把挂起的
// gap 在通道腾出空间后补发。
type SubscriptionGapEvent struct {
	FromSeq int64     // 丢弃前最后一条成功投递事件的 Seq
	ToSeq   int64     // 丢弃区间末尾事件的 Seq；恢复后下一条事件为 ToSeq+1
	Dropped int64     // 丢弃的业务事件条数（= ToSeq-FromSeq）
	At      time.Time // 断档标记发出时间
	Seq     int64     // 标记自身 Seq（= ToSeq）
}

func (e SubscriptionGapEvent) eventSeq() int64 { return e.Seq }
func (SubscriptionGapEvent) eventKind() string { return "subscription_gap" }

// SubscriptionStats 是订阅者的累计统计（用于观测丢弃情况）。
type SubscriptionStats struct {
	Delivered int64 // 成功投递给输出通道的事件数（含 gap 标记）
	Dropped   int64 // 因背压被丢弃的业务事件数
	GapCount  int64 // 发出的断档标记次数
	LastSeq   int64 // 引擎最近分配到的事件 Seq
}

// feedItem 是引擎到 feeder 的投递单元：gap 与“恢复后的首笔事件”原子绑定，
// 保证输出顺序严格为 gap（Seq=ToSeq）-> ev（Seq=ToSeq+1）。
type feedItem struct {
	gap *SubscriptionGapEvent
	ev  Event
}

// Subscription 表示一个增量订阅。
type Subscription struct {
	C      <-chan Event
	ch     chan Event    // 对外输出通道（== C）
	in     chan feedItem // 引擎 -> feeder 的邮箱
	buffer int
	hub    *hub
	done   sync.WaitGroup

	mu           sync.Mutex
	closed       bool
	lastSeq      int64 // 引擎分配到的最近事件 Seq
	deliveredSeq int64 // 最近一条已串行发送到输出通道的事件 Seq
	delivered    int64
	dropped      int64
	gaps         int64

	// 丢弃区间状态（dropFrom..dropTo 闭区间）。
	dropping bool
	dropFrom int64
	dropTo   int64
	// droppedAccounted 是已结算进 dropped 统计的最高丢弃 Seq，
	// 稳态慢消费会产生多个 gap，用它避免重复/遗漏计数。
	droppedAccounted int64
}

// offer 由写入端在 hub RLock 下调用：一次非阻塞入队，绝不阻塞写入端。
func (s *Subscription) offer(ev Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	seq := ev.eventSeq()
	s.lastSeq = seq

	if s.dropping {
		// 已在断档中：尝试把“覆盖到当前事件的 gap + 恢复首笔事件”原子入队。
		// 成功则断档结束；失败则继续丢弃并扩展区间。
		item := feedItem{gap: s.buildGapWith(seq), ev: ev}
		select {
		case s.in <- item:
			s.dropping = false
		default:
			s.dropTo = seq
		}
		return
	}
	select {
	case s.in <- feedItem{ev: ev}:
	default:
		// 邮箱已满：进入丢弃状态。
		s.dropping = true
		s.dropFrom = s.max64(s.deliveredSeq, s.droppedAccounted) + 1
		s.dropTo = seq
	}
}

func (s *Subscription) max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Stats 返回累计投递/丢弃统计。
func (s *Subscription) Stats() SubscriptionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SubscriptionStats{
		Delivered: s.delivered,
		Dropped:   s.dropped,
		GapCount:  s.gaps,
		LastSeq:   s.lastSeq,
	}
}

// Close 注销订阅、停止 feeder 并关闭输出通道。可重复调用。
func (s *Subscription) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.done.Wait()
		return
	}
	s.closed = true
	s.hub.mu.Lock()
	delete(s.hub.subs, s)
	s.hub.mu.Unlock()
	close(s.in)
	out := s.ch
	s.mu.Unlock()

	s.done.Wait()
	close(out)
}

// feed 是每订阅者唯一的串行投递者；输出满时只阻塞它自己。
func (s *Subscription) feed() {
	defer s.done.Done()
	for item := range s.in {
		if item.gap != nil {
			s.sendBlocking(*item.gap)
		}
		s.sendBlocking(item.ev)
		// 写出一批后，若期间又产生了丢弃（输出比写入慢的稳态），
		// 在这里补发覆盖区间的 gap，让恢复读取的订阅者尽快感知断档。
		for {
			gap := s.drainPendingGap()
			if gap == nil {
				break
			}
			s.sendBlocking(*gap)
		}
	}

	// 通道关闭时若仍挂着未补发的 gap（写入已停止、输出一直满），尽力补一次。
	s.mu.Lock()
	gap := s.pendingGapLocked()
	s.mu.Unlock()
	if gap != nil {
		select {
		case s.ch <- *gap:
			s.markDelivered(*gap)
		default:
		}
	}
}

// drainPendingGap 取出挂起的断档区间；调用点是 feeder，与 offer 并发安全。
func (s *Subscription) drainPendingGap() *SubscriptionGapEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingGapLocked()
}

// sendBlocking 将事件串行写入输出通道；输出满时在此等待，不影响写入端。
func (s *Subscription) sendBlocking(ev Event) {
	s.ch <- ev
	s.markDelivered(ev)
}

func (s *Subscription) markDelivered(ev Event) {
	s.mu.Lock()
	s.delivered++
	s.deliveredSeq = ev.eventSeq()
	if _, ok := ev.(SubscriptionGapEvent); ok {
		s.gaps++
	}
	s.mu.Unlock()
}

// buildGapWith 构造覆盖 [dropFrom, to] 的 gap（调用方持锁）。
func (s *Subscription) buildGapWith(to int64) *SubscriptionGapEvent {
	return &SubscriptionGapEvent{
		FromSeq: s.dropFrom - 1,
		ToSeq:   to,
		Dropped: to - s.dropFrom + 1,
		At:      s.hub.now(),
		Seq:     to,
	}
}

// pendingGapLocked 返回并清除挂起的 gap（调用方持锁）。
func (s *Subscription) pendingGapLocked() *SubscriptionGapEvent {
	if !s.dropping {
		return nil
	}
	gap := s.buildGapWith(s.dropTo)
	if gap.ToSeq > s.droppedAccounted {
		s.dropped += gap.ToSeq - s.max64(gap.FromSeq, s.droppedAccounted)
		s.droppedAccounted = gap.ToSeq
	}
	s.dropping = false
	return gap
}
