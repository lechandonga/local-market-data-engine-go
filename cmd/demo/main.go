// demo 演示本地行情引擎的完整用法：灌 tick、看 bar、订阅增量、
// 慢消费者收到断档标记。全程纯内存模拟，不接任何外部数据源。
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/lechandonga/local-market-data-engine-go/market"
)

func main() {
	dir, err := os.MkdirTemp("", "local-mkt-demo-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	e, err := market.New(market.Config{
		DataDir:      dir,
		Periods:      []market.Period{market.Period1m, market.Period5m},
		MaxTardiness: 30 * time.Second,
		SyncWrites:   true,
	})
	if err != nil {
		panic(err)
	}
	defer e.Close()

	sub := e.Subscribe(8)
	go func() {
		for ev := range sub.C {
			switch v := ev.(type) {
			case market.TickAcceptedEvent:
				fmt.Printf("[事件] tick #%d %s @ %s price=%.0f vol=%.0f\n",
					v.Seq, v.Tick.Symbol, v.Tick.Time.Format("15:04:05"), v.Tick.Price, v.Tick.Volume)
			case market.BarFinalizedEvent:
				fmt.Printf("[事件] bar 封闭 #%d %s %s O=%.0f H=%.0f L=%.0f C=%.0f V=%.0f\n",
					v.Seq, v.Bar.Period, v.Bar.Start.Format("15:04:05"),
					v.Bar.Open, v.Bar.High, v.Bar.Low, v.Bar.Close, v.Bar.Volume)
			case market.SubscriptionGapEvent:
				fmt.Printf("[事件] 断档！(%d,%d] 丢弃 %d 条，请做全量对账\n",
					v.FromSeq, v.ToSeq, v.Dropped)
			}
		}
	}()

	base := time.Date(2026, 10, 6, 9, 30, 0, 0, time.UTC)
	tk := func(sec int, price, vol float64) market.Tick {
		return market.Tick{Symbol: "600000", Time: base.Add(time.Duration(sec) * time.Second), Price: price, Volume: vol}
	}

	feed := []market.Tick{
		tk(5, 100, 10),
		tk(20, 98, 15), // 同窗口乱序早到，Open 由它决定
		tk(40, 103, 8),
		tk(70, 110, 20), // 跨分钟，触发 1m bar 封闭
		tk(20, 98, 15),  // 重复 -> 拒绝
		tk(25, 1, 1),    // 超过 30s 迟到容忍（水位 70s），且窗口已封 -> bar_closed
		tk(35, 1, 1),    // 09:30 窗口已封 -> bar_closed
		tk(100, 200, 1), // 推进水位到 100s（仍在 09:31 窗口）
		tk(65, 1, 1),    // 迟到 35s > 30s；窗口已封 -> bar_closed（封闭优先）
		tk(85, 1, 1),    // 同窗口乱序 15s -> 正常接收，演示乱序可修正进行中 bar
		tk(150, 300, 1), // 推进水位到 150s，09:32 窗口进行中
		tk(115, 1, 1),   // 09:31 窗口已封 -> bar_closed
	}
	for _, t := range feed {
		if r := e.Ingest(t); !r.Accepted {
			fmt.Printf("[拒绝] %s: %s\n", r.Reason, r.Detail)
		}
	}

	for _, b := range e.Bars("600000", market.Period1m) {
		fmt.Printf("[1m bar] %s O=%.0f H=%.0f L=%.0f C=%.0f V=%.0f ticks=%d\n",
			b.Start.Format("15:04:05"), b.Open, b.High, b.Low, b.Close, b.Volume, b.TickCount)
	}
	if s, ok := e.Snapshot("600000"); ok {
		fmt.Printf("[快照] last=%.0f@%s totalVol=%.0f ticks=%d 进行中窗口=%d\n",
			s.LastPrice, s.LastTime.Format("15:04:05"), s.TotalVolume, s.TickCount, len(s.Bars))
	}

	time.Sleep(100 * time.Millisecond)
	sub.Close()
	fmt.Printf("[WAL] 已持久化到 %s/ticks.wal，重启后 bar 序列完全一致\n", dir)
}
