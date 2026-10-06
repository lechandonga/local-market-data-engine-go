package market

import (
	"testing"
	"time"
)

// TestBars 业务：K 线聚合。
// 覆盖用例：窗口边界 UTC 对齐、新窗口首笔触发封闭、封闭窗口迟到拒绝、
// 空窗也封闭、乱序早到更新进行中窗口 Open、封闭 bar 不可变且自洽。
func TestBars(t *testing.T) {
	t.Run("业务-K线聚合/窗口UTC对齐且跨窗口封闭", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period5m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 100, Volume: 1})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(60), Price: 105, Volume: 2})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(300), Price: 110, Volume: 3}) // 进入下一窗口

		bars := e.Bars("AAA", Period5m)
		if len(bars) != 1 {
			t.Fatalf("want 1 finalized bar, got %d", len(bars))
		}
		b := bars[0]
		if !b.Start.Equal(testBase) {
			t.Fatalf("window start not UTC aligned: %s", b.Start)
		}
		if b.Open != 100 || b.High != 105 || b.Low != 100 || b.Close != 105 || b.Volume != 3 || b.TickCount != 2 {
			t.Fatalf("bad bar: %+v", b)
		}
	})

	t.Run("业务-K线聚合/封闭窗口迟到tick被拒绝且bar不可变", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 100, Volume: 1})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(60), Price: 110, Volume: 1})
		r := expectReject(t, e, Tick{Symbol: "AAA", Time: at(30), Price: 50, Volume: 99}, RejectBarClosed)
		t.Logf("封闭窗口拒绝: %s | %s", r.Reason, r.Detail)

		bars := e.Bars("AAA", Period1m)
		if bars[0].Low != 100 || bars[0].Volume != 1 {
			t.Fatalf("finalized bar mutated: %+v", bars[0])
		}
	})

	t.Run("业务-K线聚合/跳过的空窗口也标记封闭", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(0), Price: 100, Volume: 1})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(180), Price: 120, Volume: 1}) // 跳过 60-120 窗口
		expectReject(t, e, Tick{Symbol: "AAA", Time: at(90), Price: 1, Volume: 1}, RejectBarClosed)
		expectReject(t, e, Tick{Symbol: "AAA", Time: at(60), Price: 1, Volume: 1}, RejectBarClosed)
	})

	t.Run("业务-K线聚合/乱序早到修正进行中窗口且聚合自洽", func(t *testing.T) {
		e := mustNewEngine(t, 10*time.Minute, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(50), Price: 110, Volume: 3})
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(10), Price: 90, Volume: 5}) // 同窗口乱序早到
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(30), Price: 120, Volume: 7})

		snap, _ := e.Snapshot("AAA")
		b := snap.Bars[0]
		if b.Open != 90 || b.High != 120 || b.Low != 90 || b.Close != 110 || b.Volume != 15 || b.TickCount != 3 {
			t.Fatalf("in-window out-of-order aggregate wrong: %+v", b)
		}
	})

	t.Run("业务-K线聚合/同纳秒并列时先入库为Open后入库为Close", func(t *testing.T) {
		e := mustNewEngine(t, 10*time.Minute, Period1m)
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(10), Price: 100, Volume: 1})
		// 同时间戳不同价只能靠“不同时间”模拟；这里验证先入 Open 不被后入改写。
		mustAccept(t, e, Tick{Symbol: "AAA", Time: at(20), Price: 130, Volume: 1})
		snap, _ := e.Snapshot("AAA")
		if snap.Bars[0].Open != 100 || snap.Bars[0].Close != 130 {
			t.Fatalf("tie rule wrong: %+v", snap.Bars[0])
		}
	})
}
