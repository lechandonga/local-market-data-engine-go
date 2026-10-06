// Command demo 演示本地行情引擎：乱序/重复/迟到 tick 的处理、
// K 线聚合修正、订阅增量与慢订阅者 gap 信号。全部数据本地模拟。
package main

import (
	"fmt"
	"time"

	"github.com/lechandonga/local-market-data-engine-go/engine"
)

func main() {
	const period = int64(time.Second)
	const maxLate = int64(500 * time.Millisecond)

	e, err := engine.New(engine.Config{
		Period:   period,
		MaxLate:  maxLate,
		WALPath:  "./demo-tick.wal", // 留文件：重启后再次运行可看到全部判重
		FsyncWAL: true,
	})
	if err != nil {
		panic(err)
	}
	defer e.Close()

	sub := e.Subscribe(16)
	go func() {
		for ev := range sub.Events() {
			switch ev.Type {
			case engine.EventTickAccepted:
				fmt.Printf("[事件] tick 生效 seq=%d %s t=%d price=%d vol=%d\n",
					ev.Seq, ev.Tick.Symbol, ev.Tick.Time, ev.Tick.Price, ev.Tick.Volume)
			case engine.EventBarUpserted:
				fmt.Printf("[事件] bar %s 窗口=[%d,%d) O=%d H=%d L=%d C=%d V=%d rev=%d\n",
					ev.Type, ev.Bar.WindowStart, ev.Bar.WindowEnd,
					ev.Bar.Open, ev.Bar.High, ev.Bar.Low, ev.Bar.Close, ev.Bar.Volume, ev.Bar.Revision)
			case engine.EventBarFinalized:
				fmt.Printf("[事件] bar_finalized 窗口=[%d,%d)\n", ev.Bar.WindowStart, ev.Bar.WindowEnd)
			case engine.EventGap:
				fmt.Printf("[事件] GAP 丢了 %d 条，序号区间 [%d,%d]，最后收到 seq=%d\n",
					ev.GapDropped, ev.GapFromSeq, ev.GapToSeq, ev.Seq)
			}
		}
	}()

	base := time.Now().UnixNano()
	t := func(offsetMs int64, price, vol uint64) engine.Tick {
		return engine.Tick{Symbol: "DEMO", Time: base + offsetMs*int64(time.Millisecond), Price: price, Volume: vol}
	}

	ingest := func(tk engine.Tick, label string) {
		r, err := e.Ingest(tk)
		switch {
		case err != nil:
			fmt.Printf("[写入] %-18s 错误: %v\n", label, err)
		case !r.Accepted:
			fmt.Printf("[写入] %-18s 拒绝 reason=%s\n", label, r.Reason)
		case r.Corrected:
			fmt.Printf("[写入] %-18s 生效(迟到修正 窗口起点=%d)\n", label, r.CorrectedWindowStart)
		default:
			fmt.Printf("[写入] %-18s 生效\n", label)
		}
	}

	ingest(t(200, 100, 1), "正常 tick")
	ingest(t(800, 110, 2), "同窗口 tick")
	ingest(t(200, 100, 1), "完全重复")                // duplicate
	ingest(t(200, 999, 9), "同时间戳冲突")              // duplicate_conflict
	ingest(t(100, 95, 3), "乱序更早(修正当前窗口)")         // 已发布窗口修正
	ingest(t(2100, 130, 1), "推进 watermark 到2.1s") // 定稿窗口0（1s+0.5s=1.5s）
	time.Sleep(50 * time.Millisecond)
	ingest(t(300, 1, 1), "迟到超过范围") // too_late：落入已定稿窗口0

	time.Sleep(100 * time.Millisecond)
	fmt.Println("\n最终 bar：")
	for _, b := range e.Bars("DEMO") {
		fmt.Printf("  窗口=[%d,%d) O=%d H=%d L=%d C=%d V=%d tick=%d final=%v rev=%d\n",
			b.WindowStart, b.WindowEnd, b.Open, b.High, b.Low, b.Close,
			b.Volume, b.TickCount, b.Final, b.Revision)
	}
	fmt.Printf("快照: %+v\n", e.Snapshot("DEMO"))
}
