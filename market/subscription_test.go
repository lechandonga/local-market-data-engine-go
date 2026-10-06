package market

import (
	"testing"
	"time"
)

// TestSubscription 业务：快照与订阅。
// 覆盖用例：增量顺序不倒退、慢订阅者不阻塞写入且收到精确断档、
// 断档后可全量对账、关闭订阅不影响写入、快照与已生效数据一致。
func TestSubscription(t *testing.T) {
	t.Run("业务-快照订阅/增量Seq单调且包含封闭事件", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period1m)
		sub := e.Subscribe(16)
		defer sub.Close()

		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 100, Volume: 1})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(60), Price: 110, Volume: 1})

		var lastSeq int64
		var gotTick, gotBar bool
		for i := 0; i < 3; i++ {
			select {
			case ev := <-sub.C:
				if ev.eventSeq() <= lastSeq {
					t.Fatalf("seq went backwards: %d after %d (%s)", ev.eventSeq(), lastSeq, ev.eventKind())
				}
				lastSeq = ev.eventSeq()
				switch ev.(type) {
				case TickAcceptedEvent:
					gotTick = true
				case BarFinalizedEvent:
					gotBar = true
				}
			case <-time.After(time.Second):
				t.Fatal("timeout waiting events")
			}
		}
		if !gotTick || !gotBar {
			t.Fatalf("missing event types tick=%v bar=%v", gotTick, gotBar)
		}
	})

	t.Run("业务-快照订阅/慢订阅者不阻塞写入并收到断档信号", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period1m)
		sub := e.Subscribe(2) // 极小缓冲，必然掉队
		defer sub.Close()

		// 完全不读取 sub.C，灌入大量数据；写入必须快速返回。
		done := make(chan struct{})
		go func() {
			for i := int64(0); i < 500; i++ {
				e.Ingest(Tick{Symbol: "AAA", Time: at(int(i)), Price: 100 + float64(i%10), Volume: 1})
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("slow subscriber blocked the writer")
		}

		// 持续读取通道（feeder 每被读走一条才能继续），最终必须看到一个
		// gap 标记，且数字精确。
		var gap *SubscriptionGapEvent
		var prev int64
		deadline := time.After(5 * time.Second)
		for {
			select {
			case ev := <-sub.C:
				if g, ok := ev.(SubscriptionGapEvent); ok {
					gap = &g
					break
				}
				if ev.eventSeq() <= prev {
					t.Fatalf("non-monotonic seq %d -> %d", prev, ev.eventSeq())
				}
				prev = ev.eventSeq()
			case <-deadline:
				t.Fatal("timeout waiting for gap")
			}
			if gap != nil {
				break
			}
		}
		if gap == nil {
			t.Fatal("expected a subscription gap event")
		}
		if gap.Dropped <= 0 || gap.ToSeq-gap.FromSeq != gap.Dropped {
			t.Fatalf("gap numbers not self-consistent: %+v", gap)
		}
		t.Logf("断档信号: FromSeq=%d ToSeq=%d Dropped=%d", gap.FromSeq, gap.ToSeq, gap.Dropped)

		st := sub.Stats()
		if st.Dropped != gap.ToSeq-gap.FromSeq {
			t.Fatalf("stats dropped %d != gap span %d", st.Dropped, gap.ToSeq-gap.FromSeq)
		}
		if st.LastSeq < 500 {
			t.Fatalf("hub advanced past subscriber but stats stale: %+v", st)
		}
	})

	t.Run("业务-快照订阅/关闭订阅后写入与其他订阅正常", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period1m)
		s1 := e.Subscribe(8)
		s2 := e.Subscribe(8)
		s1.Close()
		s1.Close() // 可重复调用
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 100, Volume: 1})
		select {
		case <-s2.C:
		case <-time.After(time.Second):
			t.Fatal("other subscriber stopped receiving")
		}
		if _, ok := <-s1.C; ok {
			t.Fatal("closed subscriber channel still open")
		}
	})

	t.Run("业务-快照订阅/快照与已生效数据一致", func(t *testing.T) {
		e := mustNewEngine(t, 10*time.Minute, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 100, Volume: 2})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(30), Price: 103, Volume: 5})
		expectReject(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 999, Volume: 9}, RejectDuplicate)

		snap, ok := e.Snapshot("AAA")
		if !ok {
			t.Fatal("missing snapshot")
		}
		if snap.LastPrice != 103 || !snap.LastTime.Equal(at(30)) || snap.TotalVolume != 7 || snap.TickCount != 2 {
			t.Fatalf("snapshot inconsistent: %+v", snap)
		}
		if len(snap.Bars) != 1 || snap.Bars[0].Close != 103 {
			t.Fatalf("snapshot bars inconsistent: %+v", snap.Bars)
		}
		if _, ok := e.Snapshot("NOPE"); ok {
			t.Fatal("nonexistent symbol should return false")
		}
	})
}
