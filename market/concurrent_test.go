package market

import (
	"sync"
	"testing"
	"time"
)

// TestConcurrent 业务：多个写入方并发推送与并发读取。
// 用 -race 运行时验证内部状态不被写乱；本测试同时断言聚合结果与串行等价。
func TestConcurrent(t *testing.T) {
	t.Run("业务-并发读写/多写入方唯一时间戳全部生效且聚合确定", func(t *testing.T) {
		// 允许 1 小时乱序：8 个 writer 以任意交织顺序发送 0..399 秒的 tick。
		e := mustNewEngine(t, time.Hour, Period1h)
		const writers = 8
		const perWriter = 50

		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				// 每个 writer 拿到互不相交的秒数集合，避免跨 writer 的重复键。
				for i := 0; i < perWriter; i++ {
					sec := w*perWriter + i // 0..399，全在 5m 窗口内
					r := e.Ingest(Tick{Symbol: "AAA", Time: at(sec), Price: float64(100 + sec%7), Volume: 1})
					if !r.Accepted {
						t.Errorf("unexpected reject: %s %s", r.Reason, r.Detail)
						return
					}
				}
			}(w)
		}

		// 并发读者：持续拉快照与 bar 列表，不能 panic、不能读到撕裂状态。
		stop := make(chan struct{})
		var rwg sync.WaitGroup
		rwg.Add(2)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if s, ok := e.Snapshot("AAA"); ok {
						if s.LastPrice <= 0 && s.TickCount > 0 {
							t.Errorf("torn snapshot: %+v", s)
							return
						}
					}
				}
			}
		}()
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = e.Bars("AAA", Period1h)
					_ = e.Symbols()
				}
			}
		}()

		wg.Wait()
		close(stop)
		rwg.Wait()

		snap, ok := e.Snapshot("AAA")
		if !ok {
			t.Fatal("missing snapshot")
		}
		if snap.TickCount != writers*perWriter {
			t.Fatalf("tick count = %d, want %d", snap.TickCount, writers*perWriter)
		}
		if snap.TotalVolume != float64(writers*perWriter) {
			t.Fatalf("volume = %v", snap.TotalVolume)
		}
		b := snap.Bars[0]
		if len(snap.Bars) != 1 {
			t.Fatalf("unexpected active bars: %+v", snap.Bars)
		}
		if b.TickCount != writers*perWriter || b.Volume != float64(writers*perWriter) {
			t.Fatalf("bar aggregate torn: %+v", b)
		}
		if b.Open != 100 || b.High != 106 || b.Low != 100 || b.Close != 100 {
			// sec 覆盖 0..399：首笔成交时间为 0 -> Open=100；
			// 最大价 100+6=106，最小 100；最后一笔时间 399（399%7=0）-> Close=100。
			t.Fatalf("ohlc not deterministic: %+v", b)
		}
		t.Logf("并发聚合结果: ticks=%d volume=%.0f OHLC=%v/%v/%v/%v",
			b.TickCount, b.Volume, b.Open, b.High, b.Low, b.Close)
	})

	t.Run("业务-并发读写/同一时间戳并发推送恰好一笔生效", func(t *testing.T) {
		e := mustNewEngine(t, 0, Period1m)
		const n = 32
		var wg sync.WaitGroup
		var accepts, dups int64
		var mu sync.Mutex
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := e.Ingest(Tick{Symbol: "AAA", Time: at(0), Price: 100, Volume: 1})
				mu.Lock()
				if r.Accepted {
					accepts++
				} else if r.Reason == RejectDuplicate {
					dups++
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
		if accepts != 1 || dups != n-1 {
			t.Fatalf("accepts=%d dups=%d", accepts, dups)
		}
		if s, _ := e.Snapshot("AAA"); s.TickCount != 1 || s.TotalVolume != 1 {
			t.Fatalf("state corrupted: %+v", s)
		}
		t.Logf("并发判重: 生效=%d 判重=%d", accepts, dups)
	})
}
