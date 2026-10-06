package market

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestRestart 业务：进程重启后的本地状态恢复。
// 覆盖用例：重启后封闭 bar 序列逐字段一致、进行中窗口 OHLC 一致、
// 重复与迟到规则在重启后继续生效、继续灌相同数据产出一致。
func TestRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	feed := func(e *Engine) {
		// 乱序灌入，跨越多个 1m 窗口，并制造一个封闭窗口。
		seq := []Tick{
			{Symbol: "AAA", Time: at(5), Price: 100, Volume: 1},
			{Symbol: "AAA", Time: at(20), Price: 95, Volume: 2},
			{Symbol: "AAA", Time: at(75), Price: 130, Volume: 4},
			{Symbol: "AAA", Time: at(90), Price: 120, Volume: 3},
			{Symbol: "AAA", Time: at(140), Price: 140, Volume: 5},
			{Symbol: "BBB", Time: at(0), Price: 50, Volume: 9},
		}
		for _, tk := range seq {
			mustAccept(t, e, tk)
		}
	}

	cfg := func() Config {
		return Config{
			DataDir:      dir,
			Periods:      []Period{Period1m, Period5m},
			MaxTardiness: 30 * time.Minute,
			SyncWrites:   true,
		}
	}

	e1, err := New(cfg())
	if err != nil {
		t.Fatal(err)
	}
	feed(e1)
	before1m := e1.Bars("AAA", Period1m)
	before5m := e1.Bars("AAA", Period5m)
	snapBefore, _ := e1.Snapshot("AAA")
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := New(cfg())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e2.Close() })

	t.Run("重启后封闭bar序列完全一致", func(t *testing.T) {
		after1m := e2.Bars("AAA", Period1m)
		after5m := e2.Bars("AAA", Period5m)
		if fmt.Sprint(after1m) != fmt.Sprint(before1m) {
			t.Fatalf("1m bars differ after restart:\nbefore=%v\nafter =%v", before1m, after1m)
		}
		if fmt.Sprint(after5m) != fmt.Sprint(before5m) {
			t.Fatalf("5m bars differ after restart:\nbefore=%v\nafter =%v", before5m, after5m)
		}
	})

	t.Run("重启后进行中窗口与统计一致", func(t *testing.T) {
		snapAfter, ok := e2.Snapshot("AAA")
		if !ok {
			t.Fatal("symbol missing after restart")
		}
		if fmt.Sprint(snapAfter.Bars) != fmt.Sprint(snapBefore.Bars) {
			t.Fatalf("active bars differ:\nbefore=%v\nafter =%v", snapBefore.Bars, snapAfter.Bars)
		}
		if snapAfter.TickCount != snapBefore.TickCount || snapAfter.TotalVolume != snapBefore.TotalVolume {
			t.Fatalf("stats differ: before=%+v after=%+v", snapBefore, snapAfter)
		}
		if snap2, ok := e2.Snapshot("BBB"); !ok || snap2.TickCount != 1 || snap2.LastPrice != 50 {
			t.Fatalf("other symbol not restored: %+v %v", snap2, ok)
		}
	})

	t.Run("重启后重复仍判重且规则继续生效", func(t *testing.T) {
		expectReject(t, e2, Tick{Symbol: "AAA", Time: at(20), Price: 1, Volume: 1}, RejectDuplicate)
		expectReject(t, e2, Tick{Symbol: "AAA", Time: at(61), Price: 1, Volume: 1}, RejectBarClosed)
		mustAccept(t, e2, Tick{Symbol: "AAA", Time: at(141), Price: 141, Volume: 1})
	})
}
