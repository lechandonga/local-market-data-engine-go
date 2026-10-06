package engine

import (
	"path/filepath"
	"sync"
	"testing"
)

// logCase 让 -v 日志按 “业务 -> 用例 -> 通过情况” 呈现。
func logCase(t *testing.T, business, name string) {
	t.Helper()
	t.Run(business+"/"+name, func(t *testing.T) {
		t.Logf("[业务] %s | [用例] %s | [结果] PASS", business, name)
	})
}

func newTestEngine(t *testing.T, period, maxLate int64) *Engine {
	t.Helper()
	e, err := New(Config{Period: period, MaxLate: maxLate})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

const (
	P  = 10 // 10ns 周期，测试用
	ML = 4  // 允许迟到 4ns
)

// 业务一：tick 入库与判重

func TestTickIngest(t *testing.T) {
	business := "tick入库与判重"

	t.Run("乱序到达均生效", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		for _, ts := range []int64{30, 10, 20} {
			r, err := e.Ingest(Tick{Symbol: "S", Time: ts, Price: 100, Volume: 1})
			if err != nil || !r.Accepted {
				t.Fatalf("tick %d 应生效, got %+v err=%v", ts, r, err)
			}
		}
		if snap := e.Snapshot("S"); snap.TickCount != 3 || snap.TotalVol != 3 {
			t.Fatalf("快照计数错误: %+v", snap)
		}
		logCase(t, business, "乱序到达均生效")
	})

	t.Run("完全重复tick被识别且不双计", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		tk := Tick{Symbol: "S", Time: 5, Price: 100, Volume: 7}
		if r, _ := e.Ingest(tk); !r.Accepted {
			t.Fatal("首次应生效")
		}
		r, _ := e.Ingest(tk)
		if r.Accepted || r.Reason != RejectDuplicate {
			t.Fatalf("重复 tick 应被识别为 %s, got %+v", RejectDuplicate, r)
		}
		if snap := e.Snapshot("S"); snap.TickCount != 1 || snap.TotalVol != 7 {
			t.Fatalf("重复 tick 被重复计入: %+v", snap)
		}
		logCase(t, business, "完全重复tick被识别且不双计")
	})

	t.Run("同时间戳内容冲突保留先到者", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		r1, _ := e.Ingest(Tick{Symbol: "S", Time: 5, Price: 100, Volume: 1})
		r2, _ := e.Ingest(Tick{Symbol: "S", Time: 5, Price: 999, Volume: 2})
		if !r1.Accepted || r2.Accepted || r2.Reason != RejectDuplicateConflict {
			t.Fatalf("冲突 tick 应以 %s 拒绝, got %+v", RejectDuplicateConflict, r2)
		}
		bar, ok := e.GetBar("S", 0)
		if !ok || bar.Open != 100 || bar.Volume != 1 {
			t.Fatalf("已有数据被改坏: %+v ok=%v", bar, ok)
		}
		logCase(t, business, "同时间戳内容冲突保留先到者")
	})

	t.Run("迟到超过允许范围被拒绝并给出原因", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		// 窗口 [0,10)：先 t=1，再把 watermark 推到 15（>=10+4+1）使其定稿。
		e.Ingest(Tick{Symbol: "S", Time: 1, Price: 100, Volume: 1})
		e.Ingest(Tick{Symbol: "S", Time: 15, Price: 101, Volume: 1})
		before := e.Bars("S")[0]
		r, _ := e.Ingest(Tick{Symbol: "S", Time: 2, Price: 50, Volume: 99})
		if r.Accepted || r.Reason != RejectTooLate {
			t.Fatalf("超范围迟到应 %s, got %+v", RejectTooLate, r)
		}
		after := e.Bars("S")[0]
		if after != before {
			t.Fatalf("定稿 bar 被迟到数据改坏: before=%+v after=%+v", before, after)
		}
		logCase(t, business, "迟到超过允许范围被拒绝并给出原因")
	})

	t.Run("零时间戳被拒绝", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		r, _ := e.Ingest(Tick{Symbol: "S", Time: 0, Price: 1})
		if r.Accepted || r.Reason != RejectInvalidTime {
			t.Fatalf("应拒绝零时间戳, got %+v", r)
		}
		logCase(t, business, "零时间戳被拒绝")
	})

	t.Run("不同标的同时间戳互不影响", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		e.Ingest(Tick{Symbol: "A", Time: 5, Price: 1, Volume: 1})
		e.Ingest(Tick{Symbol: "B", Time: 5, Price: 2, Volume: 2})
		r, _ := e.Ingest(Tick{Symbol: "A", Time: 5, Price: 1, Volume: 1})
		if r.Reason != RejectDuplicate {
			t.Fatalf("A 的重复应判重, got %+v", r)
		}
		if s := e.Snapshot("B"); s.LastPrice != 2 {
			t.Fatalf("标的间串数据: %+v", s)
		}
		logCase(t, business, "不同标的同时间戳互不影响")
	})
}

// 业务二：K线聚合

func TestBarAggregation(t *testing.T) {
	business := "K线聚合"

	t.Run("OHLCV口径与窗口左闭右开", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		// 窗口 [0,10)：t=1..9；t=10 属于下一窗。
		e.Ingest(Tick{Symbol: "S", Time: 3, Price: 100, Volume: 2})
		e.Ingest(Tick{Symbol: "S", Time: 1, Price: 110, Volume: 1}) // open
		e.Ingest(Tick{Symbol: "S", Time: 9, Price: 120, Volume: 3}) // close
		e.Ingest(Tick{Symbol: "S", Time: 5, Price: 90, Volume: 4})  // low
		e.Ingest(Tick{Symbol: "S", Time: 10, Price: 500, Volume: 9})
		b0, ok := e.GetBar("S", 0)
		if !ok {
			t.Fatal("窗口[0,10) 缺失")
		}
		if b0.Open != 110 || b0.Close != 120 || b0.High != 120 || b0.Low != 90 ||
			b0.Volume != 10 || b0.TickCount != 4 || b0.WindowEnd != 10 {
			t.Fatalf("OHLCV 口径错误: %+v", b0)
		}
		if b1, ok := e.GetBar("S", 10); !ok || b1.Open != 500 || b1.Volume != 9 {
			t.Fatalf("边界 tick t=10 应归属 [10,20): %+v ok=%v", b1, ok)
		}
		logCase(t, business, "OHLCV口径与窗口左闭右开")
	})

	t.Run("到达顺序不影响bar结果", func(t *testing.T) {
		ticksA := []Tick{
			{Symbol: "S", Time: 1, Price: 110, Volume: 1},
			{Symbol: "S", Time: 3, Price: 100, Volume: 2},
			{Symbol: "S", Time: 9, Price: 120, Volume: 3},
			{Symbol: "S", Time: 12, Price: 130, Volume: 1},
			{Symbol: "S", Time: 15, Price: 95, Volume: 2},
		}
		ticksB := []Tick{ticksA[4], ticksA[2], ticksA[0], ticksA[3], ticksA[1]}
		// MaxLate 取大，保证两种灌法期间窗口都不定稿，纯粹比较聚合口径。
		e1 := newTestEngine(t, P, 100)
		e2 := newTestEngine(t, P, 100)
		for _, tk := range ticksA {
			e1.Ingest(tk)
		}
		for _, tk := range ticksB {
			e2.Ingest(tk)
		}
		got, want := e2.Bars("S"), e1.Bars("S")
		if len(got) != len(want) {
			t.Fatalf("bar 数量不一致: %d vs %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("乱序导致 bar 漂移:\n顺序A=%+v\n顺序B=%+v", want[i], got[i])
			}
		}
		logCase(t, business, "到达顺序不影响bar结果")
	})

	t.Run("迟到tick修正已发布窗口且自洽", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		e.Ingest(Tick{Symbol: "S", Time: 1, Price: 100, Volume: 1})
		e.Ingest(Tick{Symbol: "S", Time: 12, Price: 101, Volume: 1})
		before, _ := e.GetBar("S", 0)
		if before.Revision != 0 {
			t.Fatalf("新 bar revision 应为 0, got %d", before.Revision)
		}
		// watermark=12 < 10+4=14，窗口 [0,10) 未定稿，允许修正。
		r, _ := e.Ingest(Tick{Symbol: "S", Time: 5, Price: 80, Volume: 5})
		if !r.Accepted || !r.Corrected || r.CorrectedWindowStart != 0 {
			t.Fatalf("迟到 tick 应触发修正, got %+v", r)
		}
		after, _ := e.GetBar("S", 0)
		// 窗口内时间戳最大的是 t=5(80)，故 Close=80；Open 仍是 t=1(100)。
		if after.Revision != 1 || after.Low != 80 || after.Open != 100 ||
			after.Close != 80 || after.Volume != 6 || after.TickCount != 2 {
			t.Fatalf("修正后 bar 不自洽: %+v", after)
		}
		// 再对同一窗口迟到修正一次，revision 继续递增且结果仍自洽。
		e.Ingest(Tick{Symbol: "S", Time: 9, Price: 130, Volume: 2})
		again, _ := e.GetBar("S", 0)
		if again.Revision != 2 || again.High != 130 || again.Close != 130 || again.Volume != 8 {
			t.Fatalf("二次修正不自洽: %+v", again)
		}
		logCase(t, business, "迟到tick修正已发布窗口且自洽")
	})

	t.Run("定稿后拒绝迟到且bar定稿事件发出", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		sub := e.Subscribe(64)
		defer sub.Close()
		e.Ingest(Tick{Symbol: "S", Time: 1, Price: 100, Volume: 1})
		e.Ingest(Tick{Symbol: "S", Time: 15, Price: 101, Volume: 1}) // watermark=15 >=14 定稿
		var finalized bool
		done := false
		for !done {
			ev := <-sub.Events()
			if ev.Type == EventBarFinalized && ev.Bar.WindowStart == 0 && ev.Bar.Final {
				finalized = true
				done = true
			}
		}
		if !finalized {
			t.Fatal("未收到窗口定稿事件")
		}
		if b, _ := e.GetBar("S", 0); !b.Final {
			t.Fatal("bar 应标记 Final")
		}
		logCase(t, business, "定稿后拒绝迟到且bar定稿事件发出")
	})
}

// 业务三：重启恢复

func TestRestartRecovery(t *testing.T) {
	business := "重启恢复"

	t.Run("WAL回放后bar序列与重启前完全一致", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tick.wal")
		seed := []Tick{
			{Symbol: "S", Time: 9, Price: 120, Volume: 3},
			{Symbol: "S", Time: 1, Price: 110, Volume: 1},
			{Symbol: "S", Time: 5, Price: 90, Volume: 4},
			{Symbol: "S", Time: 12, Price: 130, Volume: 2},
		}
		e1, err := New(Config{Period: P, MaxLate: ML, WALPath: path, FsyncWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, tk := range seed {
			if _, err := e1.Ingest(tk); err != nil {
				t.Fatal(err)
			}
		}
		// 重复数据不应落盘；也用于验证恢复后判重状态仍然生效。
		if r, _ := e1.Ingest(seed[1]); r.Reason != RejectDuplicate {
			t.Fatalf("重启前重复应判重, got %+v", r)
		}
		before := e1.Bars("S")
		snapBefore := e1.Snapshot("S")
		if err := e1.Close(); err != nil {
			t.Fatal(err)
		}

		e2, err := New(Config{Period: P, MaxLate: ML, WALPath: path, FsyncWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e2.Close()
		after := e2.Bars("S")
		if len(after) != len(before) {
			t.Fatalf("恢复后 bar 数量不一致: %d vs %d", len(after), len(before))
		}
		for i := range before {
			if after[i] != before[i] {
				t.Fatalf("恢复后 bar 不一致:\n重启前=%+v\n重启后=%+v", before[i], after[i])
			}
		}
		if s := e2.Snapshot("S"); s != snapBefore {
			t.Fatalf("恢复后快照不一致:\n重启前=%+v\n重启后=%+v", snapBefore, s)
		}
		// 继续灌“同样的数据”，重复 tick 仍被识别且不产生新 bar。
		for _, tk := range seed {
			if r, _ := e2.Ingest(tk); r.Reason != RejectDuplicate {
				t.Fatalf("恢复后重灌应全部判重, tick=%+v got=%+v", tk, r)
			}
		}
		if len(e2.Bars("S")) != len(before) {
			t.Fatal("恢复后重灌重复数据改变了 bar 序列")
		}
		logCase(t, business, "WAL回放后bar序列与重启前完全一致")
	})

	t.Run("恢复后继续灌数与不重启产出一致", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tick.wal")
		first := []Tick{
			{Symbol: "S", Time: 3, Price: 100, Volume: 2},
			{Symbol: "S", Time: 7, Price: 105, Volume: 1},
		}
		more := []Tick{
			{Symbol: "S", Time: 1, Price: 95, Volume: 3}, // 同窗口迟到修正
			{Symbol: "S", Time: 13, Price: 108, Volume: 2},
			{Symbol: "S", Time: 21, Price: 120, Volume: 5},
		}

		// 不重启对照：一次性灌全部。
		eRef := newTestEngine(t, P, ML)
		for _, tk := range append(append([]Tick{}, first...), more...) {
			eRef.Ingest(tk)
		}

		e1, err := New(Config{Period: P, MaxLate: ML, WALPath: path, FsyncWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, tk := range first {
			e1.Ingest(tk)
		}
		if err := e1.Close(); err != nil {
			t.Fatal(err)
		}
		e2, err := New(Config{Period: P, MaxLate: ML, WALPath: path, FsyncWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e2.Close()
		for _, tk := range more {
			e2.Ingest(tk)
		}
		got := e2.Bars("S")
		want := eRef.Bars("S")
		if len(got) != len(want) {
			t.Fatalf("bar 数量不一致: %d vs %d", len(got), len(want))
		}
		// 重启路径中 first 后窗口可能已被定稿事件标记，但 Final 不随产出内容变化；
		// 这里比较 OHLCV 内容（Final/Revision 单独看）。
		for i := range want {
			b1, b2 := got[i], want[i]
			if b1.Open != b2.Open || b1.High != b2.High || b1.Low != b2.Low ||
				b1.Close != b2.Close || b1.Volume != b2.Volume || b1.TickCount != b2.TickCount {
				t.Fatalf("重启继续灌数后 bar 内容不一致:\n重启=%+v\n对照=%+v", b1, b2)
			}
		}
		logCase(t, business, "恢复后继续灌数与不重启产出一致")
	})
}

// 业务四：快照与订阅

func TestSnapshotAndSubscription(t *testing.T) {
	business := "快照与订阅"

	t.Run("快照与已生效数据一致", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		e.Ingest(Tick{Symbol: "S", Time: 5, Price: 100, Volume: 3})
		e.Ingest(Tick{Symbol: "S", Time: 2, Price: 90, Volume: 2})  // 乱序更早
		e.Ingest(Tick{Symbol: "S", Time: 5, Price: 100, Volume: 3}) // 重复，不计
		s := e.Snapshot("S")
		// LastPrice 取 watermark（最大时间戳 tick）价格，不受乱序到达影响。
		if s.LastTime != 5 || s.LastPrice != 100 || s.TotalVol != 5 || s.TickCount != 2 {
			t.Fatalf("快照与已生效数据不一致: %+v", s)
		}
		if s0 := e.Snapshot("NOPE"); s0.TickCount != 0 || s0.LastTime != 0 {
			t.Fatalf("未知标的快照应为零值: %+v", s0)
		}
		logCase(t, business, "快照与已生效数据一致")
	})

	t.Run("订阅收到增量且Seq不倒退", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		sub := e.Subscribe(128)
		defer sub.Close()
		for i := int64(1); i <= 6; i++ {
			e.Ingest(Tick{Symbol: "S", Time: i, Price: uint64(100 + i), Volume: 1})
		}
		var lastSeq uint64
		ticks, upserts := 0, 0
		for ticks < 6 {
			ev := <-sub.Events()
			if ev.Seq <= lastSeq && ev.Type != EventGap {
				t.Fatalf("事件序号倒退/重复: last=%d ev=%d type=%s", lastSeq, ev.Seq, ev.Type)
			}
			lastSeq = ev.Seq
			switch ev.Type {
			case EventTickAccepted:
				if ev.Tick == nil {
					t.Fatal("tick 事件缺少 tick 负载")
				}
				ticks++
			case EventBarUpserted:
				upserts++
			}
		}
		if upserts < 1 {
			t.Fatal("至少应收到一次 bar upsert")
		}
		logCase(t, business, "订阅收到增量且Seq不倒退")
	})

	t.Run("慢订阅者被丢弃并收到可区分的gap信号", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		sub := e.Subscribe(2) // 极小缓冲
		defer sub.Close()
		// 灌大量事件且不消费。
		for i := int64(1); i <= 50; i++ {
			e.Ingest(Tick{Symbol: "S", Time: i, Price: 100, Volume: 1})
		}
		var gap *Event
		gotGap := false
	drain:
		for {
			select {
			case ev := <-sub.Events():
				if ev.Type == EventGap {
					cp := ev
					gap = &cp
					gotGap = true
					break drain
				}
			default:
				t.Fatal("缓冲已被压满却没有产生 gap 信号")
			}
		}
		if !gotGap || gap.GapDropped <= 0 || gap.GapFromSeq == 0 || gap.GapToSeq < gap.GapFromSeq {
			t.Fatalf("gap 信号内容不正确: %+v", gap)
		}
		// gap 之后写入仍应继续推进，且新事件序号在断点之后。
		before := gap.GapToSeq
		e.Ingest(Tick{Symbol: "S", Time: 51, Price: 101, Volume: 1})
		var sawNew bool
		for ev := range sub.Events() {
			if ev.Type == EventTickAccepted && ev.Seq > before {
				sawNew = true
				break
			}
			if ev.Type == EventTickAccepted {
				break
			}
		}
		if !sawNew {
			t.Fatal("gap 之后未收到序号正确的新事件")
		}
		logCase(t, business, "慢订阅者被丢弃并收到可区分的gap信号")
	})

	t.Run("订阅关闭后写入不阻塞不串数据", func(t *testing.T) {
		e := newTestEngine(t, P, ML)
		sub1 := e.Subscribe(8)
		sub2 := e.Subscribe(8)
		sub1.Close()
		for i := int64(1); i <= 3; i++ {
			if _, err := e.Ingest(Tick{Symbol: "S", Time: i, Price: 100, Volume: 1}); err != nil {
				t.Fatalf("关闭订阅后写入出错: %v", err)
			}
		}
		// sub1 通道已关闭且不会再收到数据；sub2 仍正常。
		if _, ok := <-sub1.Events(); ok {
			t.Fatal("已关闭订阅不应再收到事件")
		}
		n := 0
		for n < 3 {
			ev := <-sub2.Events()
			if ev.Type == EventTickAccepted {
				if ev.Tick.Symbol != "S" {
					t.Fatalf("事件串到其他标的: %+v", ev)
				}
				n++
			}
		}
		sub2.Close()
		logCase(t, business, "订阅关闭后写入不阻塞不串数据")
	})
}

// 业务五：并发读写

func TestConcurrency(t *testing.T) {
	business := "并发读写"

	t.Run("多写入方并发推送不乱序不丢判重", func(t *testing.T) {
		e := newTestEngine(t, P, 10_000)
		const writers = 8
		const perWriter = 200
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(base int64) {
				defer wg.Done()
				for i := int64(0); i < perWriter; i++ {
					// 每个 writer 写独立时间戳区间 => 全部应生效。
					ts := base + i
					e.Ingest(Tick{Symbol: "S", Time: ts + 1, Price: uint64(100 + ts%50), Volume: 1})
				}
			}(int64(w) * perWriter)
		}
		wg.Wait()
		s := e.Snapshot("S")
		if s.TickCount != int64(writers*perWriter) || s.TotalVol != uint64(writers*perWriter) {
			t.Fatalf("并发写入计数错误: %+v", s)
		}
		// 全部 tick 重放一遍：必须全部判重，计数不变。
		var wg2 sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg2.Add(1)
			go func(base int64) {
				defer wg2.Done()
				for i := int64(0); i < perWriter; i++ {
					ts := base + i
					r, _ := e.Ingest(Tick{Symbol: "S", Time: ts + 1, Price: uint64(100 + ts%50), Volume: 1})
					if r.Accepted {
						t.Errorf("并发重复 tick 被错误接受: ts=%d", ts+1)
					}
				}
			}(int64(w) * perWriter)
		}
		wg2.Wait()
		if s2 := e.Snapshot("S"); s2 != s {
			t.Fatalf("并发重放重复数据改变状态: before=%+v after=%+v", s, s2)
		}
		// 校验所有 bar 聚合自洽：总成交量与总笔数之和等于快照。
		var vol uint64
		var ticks int
		for _, b := range e.Bars("S") {
			vol += b.Volume
			ticks += b.TickCount
		}
		if vol != s.TotalVol || ticks != int(s.TickCount) {
			t.Fatalf("bar 聚合与 tick 总量不一致: vol=%d/%d ticks=%d/%d", vol, s.TotalVol, ticks, s.TickCount)
		}
		logCase(t, business, "多写入方并发推送不乱序不丢判重")
	})

	t.Run("并发订阅不阻塞写入且各自序号单调", func(t *testing.T) {
		e := newTestEngine(t, P, 10_000)
		const subs = 4
		var wg sync.WaitGroup
		results := make([]uint64, subs)
		for i := 0; i < subs; i++ {
			sub := e.Subscribe(64)
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				var last uint64
				for ev := range sub.Events() {
					if ev.Type == EventGap {
						last = ev.GapToSeq // gap 后以断点末尾续接
						continue
					}
					if ev.Seq < last {
						t.Errorf("订阅者 %d 序号倒退: %d < %d", idx, ev.Seq, last)
					}
					last = ev.Seq
					results[idx] = last
				}
			}(i)
		}
		for i := int64(1); i <= 500; i++ {
			if _, err := e.Ingest(Tick{Symbol: "S", Time: i, Price: 100 + uint64(i%30), Volume: 1}); err != nil {
				t.Fatal(err)
			}
		}
		// 关闭引擎会关闭全部订阅，消费 goroutine 随之结束。
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		// 至少一个订阅者跟到了最后的事件序号。
		var maxSeq uint64
		for _, s := range results {
			if s > maxSeq {
				maxSeq = s
			}
		}
		if maxSeq == 0 {
			t.Fatal("没有订阅者收到任何事件")
		}
		logCase(t, business, "并发订阅不阻塞写入且各自序号单调")
	})
}

// WAL 记录格式的独立往返测试。
func TestWALRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tick.wal")
	w, err := OpenWAL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	want := []Tick{
		{Symbol: "BTC", Time: 1_700_000_000_000_000_000, Price: 43_210_000_000_000, Volume: 123},
		{Symbol: "ETH-USDT", Time: 42, Price: 1, Volume: 0},
	}
	for _, tk := range want {
		if err := w.Append(tk); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var got []Tick
	n, err := ReplayWAL(path, func(tk Tick) error { got = append(got, tk); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n != len(want) || len(got) != len(want) {
		t.Fatalf("回放条数错误: n=%d got=%d want=%d", n, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 条记录不一致: got=%+v want=%+v", i, got[i], want[i])
		}
	}
	logCase(t, "本地持久化", "WAL记录追加与回放一致")
}
