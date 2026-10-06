package market

import (
	"testing"
	"time"
)

// TestIngest 业务：tick 入库与判重。
// 覆盖用例：非法输入拒绝、乱序在容忍内接收、重复精确判重、
// 重复不改已有数据、超容忍迟到拒绝、乱序窗口的 OHLC 口径自洽。
func TestIngest(t *testing.T) {
	t.Run("业务-入库判重/非法输入被拒绝", func(t *testing.T) {
		e := mustNewEngine(t, time.Minute, Period1m)
		cases := []struct {
			name string
			tick Tick
		}{
			{"空标的", Tick{Time: at(0), Price: 10}},
			{"零时间", Tick{Symbol: "S", Price: 10}},
			{"零价格", Tick{Symbol: "S", Time: at(0), Price: 0}},
			{"负价格", Tick{Symbol: "S", Time: at(0), Price: -1}},
			{"负成交量", Tick{Symbol: "S", Time: at(0), Price: 10, Volume: -1}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				expectReject(t, e, c.tick, RejectInvalid)
			})
		}
	})

	t.Run("业务-入库判重/乱序在容忍范围内接收且重复被识别", func(t *testing.T) {
		e := mustNewEngine(t, 2*time.Minute, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(30), Price: 100, Volume: 1})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(10), Price: 99, Volume: 2})
		r := expectReject(t, e, Tick{Symbol: "AAA", Time: at(10), Price: 123, Volume: 9}, RejectDuplicate)
		t.Logf("重复拒绝原因可区分: %s | %s", r.Reason, r.Detail)

		snap, ok := e.Snapshot("AAA")
		if !ok {
			t.Fatal("snapshot missing")
		}
		if snap.TickCount != 2 || snap.TotalVolume != 3 {
			t.Fatalf("duplicate was counted: %+v", snap)
		}
		if snap.LastPrice != 100 || !snap.LastTime.Equal(at(30)) {
			t.Fatalf("last tick corrupted: %+v", snap)
		}
	})

	t.Run("业务-入库判重/不同标的相同时间戳互不判重", func(t *testing.T) {
		e := mustNewEngine(t, time.Minute, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 10, Volume: 1})
		mustAccept(t, e, Tick{Symbol: "BBB", Time: at(0), Price: 20, Volume: 1})
		if got := e.Symbols(); len(got) != 2 {
			t.Fatalf("symbols = %v", got)
		}
	})

	t.Run("业务-入库判重/超过迟到容忍被拒绝且水位独立", func(t *testing.T) {
		e := mustNewEngine(t, 30*time.Second, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(60), Price: 100, Volume: 1})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(40), Price: 101, Volume: 1}) // 容忍内
		r := expectReject(t, e, Tick{Symbol: "AAA", Time: at(20), Price: 102, Volume: 1}, RejectTooLate)
		t.Logf("迟到拒绝边界: %s | %s", r.Reason, r.Detail)
		// 被拒绝后状态不变。
		snap, _ := e.Snapshot("AAA")
		if snap.TickCount != 2 {
			t.Fatalf("rejected tick changed state: %d", snap.TickCount)
		}
	})

	t.Run("业务-入库判重/零容忍只接受时间递增", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(1), Price: 100, Volume: 1})
		expectReject(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 101, Volume: 1}, RejectTooLate)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(2), Price: 102, Volume: 1})
	})

	t.Run("业务-入库判重/乱序到达的OHLC口径与接收顺序无关", func(t *testing.T) {
		// 同样三笔 tick，用两种到达顺序灌，最终 open bar 必须一致。
		build := func(order []int) Bar {
			e := mustNewEngine(t, time.Hour, Period1m)
			ticks := map[int]Tick{
				0: {Symbol: "AAA", Time: at(5), Price: 100, Volume: 1},
				1: {Symbol: "AAA", Time: at(15), Price: 90, Volume: 2},
				2: {Symbol: "AAA", Time: at(40), Price: 110, Volume: 3},
			}
			for _, i := range order {
				mustAccept(t, e, ticks[i])
			}
			snap, _ := e.Snapshot("AAA")
			return snap.Bars[0]
		}
		a := build([]int{0, 1, 2})
		b := build([]int{2, 0, 1})
		if a != b {
			t.Fatalf("bars drift with arrival order:\n%+v\n%+v", a, b)
		}
		if a.Open != 100 || a.High != 110 || a.Low != 90 || a.Close != 110 || a.Volume != 6 || a.TickCount != 3 {
			t.Fatalf("ohlc wrong: %+v", a)
		}
	})
}
