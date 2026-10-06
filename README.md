# 本地行情数据引擎（Go，纯标准库）

给回测和盘面复盘用的本地行情引擎：接收逐笔行情（tick），做判重、迟到处理、
固定周期 K 线（bar）聚合，并提供一致性快照与增量订阅。**全部数据走本地模拟，
不接任何外部数据源，零第三方依赖**（仅 Go 标准库）。

## 快速开始

```bash
# 跑全部单测（含竞态检测）。需要 CGO（-race 要求），本机有 gcc 即可
./scripts/test.sh
# 等价于
CGO_ENABLED=1 go test -race -v -count=1 ./...

# 跑一遍演示：灌 tick、看三种拒绝原因、看封闭 bar / 快照 / 事件流
go run ./cmd/demo
```

`-v` 日志按中文业务名组织，形如
`TestBars/业务-K线聚合/封闭窗口迟到tick被拒绝且bar不可变`，
可直接看出覆盖了哪些业务、每个业务有哪些用例以及 PASS/FAIL。

## 数据模型

| 类型 | 说明 |
| --- | --- |
| `Tick` | 逐笔行情：`Symbol`、`Time`（成交时间，非到达时间）、`Price>0`、`Volume>=0` |
| `Bar` | K 线：`Symbol/Period/Start` 加 OHLC、`Volume`、`TickCount`；窗口左闭右开 `[Start, Start+Period)` |
| `Snapshot` | 某标的在某时刻的一致快照：最新价/最新时间、累计量、tick 数、各周期进行中 bar |
| `Event` | 订阅增量：`TickAcceptedEvent` / `BarFinalizedEvent` / `SubscriptionGapEvent` |

所有内部时间统一转成 **UTC** 存储与计算。

## 周期与时间边界规则

- 周期是固定时长（`1m/5m/15m/1h/1d`，也可自定义任意 `Period`）。
- 窗口起点只由 tick 的成交时间决定：
  `Start = Time.UTC().UnixNano()` 向下对齐到周期长度，即
  `5m` 窗口永远是 `:00–:05`、`:05–:10`。**与接收顺序无关，不会漂移。**
- 窗口左闭右开。一个窗口在“收到成交时间属于更后窗口的首笔 tick”时封闭，
  封闭前可以接收同窗口内的乱序 tick；被跳过、没有任何 tick 的空窗口同样标记封闭。
- OHLCV 口径（对窗口内所有已生效 tick）：
  - **Open**：成交时间最早一笔的价格；同纳秒并列时先入库者为 Open。
  - **Close**：成交时间最晚一笔的价格；同纳秒并列时后入库者为 Close。
  - **High / Low**：价格最大 / 最小值。
  - **Volume**：成交量求和（允许零量 tick 参与价格、不增量）。
  - `TickCount`：窗口内 tick 笔数，可用于审计聚合自洽。

## 判重与迟到策略

判定全部以**每个标的独立的状态**进行，按以下顺序执行：

1. `invalid`：空标的、零时间、`Price<=0`、`Volume<0`，拒绝。
2. `duplicate`：同一 `Symbol` + 同一 `Time`（纳秒）已存在。**保留先入库的一笔，
   后到者丢弃**，拒绝结果的 `Detail` 会带出已有价格/成交量，可区分、可审计。
3. `bar_closed`：tick 所属窗口已经封闭（bar 已通过 `Bars()`/事件对外给出）。
   本引擎对历史窗口采取**拒绝而非改写**策略，已给出的 bar 永不变更。
4. `too_late`：`Time < 该标的已接收最大成交时间 - MaxTardiness`。
   `MaxTardiness=0` 表示严格按时间递增；容忍范围内的乱序可以修正**尚未封闭**
   窗口的 OHLC（例如更晚到达的更早 tick 会改写进行中窗口的 Open）。

任何拒绝都不改内存状态、不写 WAL，不会把已有数据改坏。

### 关于“迟到修正”

- 落在**进行中窗口**且在迟到容忍内：接受并修正该窗口聚合，修正后
  Open/High/Low/Close/Volume/TickCount 仍满足上述口径（自洽）。
- 落在**已封闭窗口**：一律 `bar_closed` 拒绝，不做历史修正（回测可复现优先）。
- 显式收盘可调用 `FlushBars(symbol)`，立即封闭全部进行中窗口并发出事件。

## 重启恢复与确定性

- 生效的每笔 tick 先追加到本地 WAL（`DataDir/ticks.wal`，自描述帧 + gob，
  见 `market/wal.go`），再更新内存状态。`SyncWrites=true` 时每笔 `fsync`。
- 启动时按 WAL 的提交顺序重放。由于：
  - 窗口边界只由 tick.Time 决定；
  - 判重键是 `(Symbol, Time)`；
  - 同纳秒并列的 Open/Close 用“入库先后”决胜，而 WAL 保存了提交顺序；

  重启后恢复出的**封闭 bar 序列、进行中 bar、统计量与重启前逐字段一致**，
  继续灌同样的数据产出完全相同的 bar（由 `TestRestart` 锁定）。
- WAL 尾部损坏会返回错误而不是静默截断，避免误丢已提交数据。

## 并发约定

- 引擎用一把 `sync.RWMutex` 保护全部标的状态：写入（`Ingest`）串行化，
  读取（`Snapshot`/`Bars`/`Symbols`）并发；快照在读锁内一次性构建，
  不会读到“最新价是新的、bar 还是旧的”的撕裂状态。
- 多写入方并发推送同一时间戳时，恰好一笔生效，其余全部拿到 `duplicate`。

## 订阅与背压

- `Engine.Subscribe(buffer)` 返回带缓冲通道，事件带**全局单调递增 Seq**，
  按 Seq 投递，顺序不倒退。
- 写入端对每个订阅者只做一次**非阻塞**入队，慢订阅者绝不会拖住写入端。
- 订阅者有独立 feeder 协程负责向输出通道串行写出：
  - 输出/邮箱满时进入丢弃状态，连续丢弃合并为**一个**断档区间；
  - 区间结束后发出一条 `SubscriptionGapEvent{FromSeq, ToSeq, Dropped}`，
    精确说明丢了多少条、从哪里断（`(FromSeq, ToSeq]`），
    消费方应在此刻用 `Snapshot()`/`Bars()` 做全量对账，再从 `ToSeq+1` 续增量；
  - 即使写入已停止，只要订阅者恢复读取（或关闭订阅），挂起的 gap 也会补发。
- `Subscription.Stats()` 可查累计投递/丢弃数。关闭订阅不影响写入端和其他订阅者。

## 代码结构

```
market/
  model.go      数据模型、周期与窗口对齐
  engine.go     入库、判重、迟到拒绝、bar 聚合、查询
  events.go     事件类型、订阅状态与 feeder（背压/断档）
  hub.go        事件分发（Seq 分配、非阻塞投递）
  snapshot.go   订阅入口、快照、显式收盘
  wal.go        仅追加的 tick WAL（持久化与重放）
  *_test.go     按业务分组的单测（乱序/重复/迟到/重启/并发/订阅）
cmd/demo/       可运行演示
scripts/test.sh 带 -race 的测试入口
```

## 典型用法

```go
e, _ := market.New(market.Config{
    DataDir:      "./data",
    Periods:      []market.Period{market.Period1m, market.Period5m},
    MaxTardiness: 30 * time.Second,
    SyncWrites:   true,
})
defer e.Close()

sub := e.Subscribe(256)
go func() {
    for ev := range sub.C {
        switch v := ev.(type) {
        case market.SubscriptionGapEvent: // 丢增量：全量对账后再续
            _ = v
        }
    }
}()

r := e.Ingest(market.Tick{Symbol: "600000", Time: ts, Price: 10.2, Volume: 100})
if !r.Accepted {
    log.Println(r.Reason, r.Detail) // duplicate / bar_closed / too_late / invalid
}
bars := e.Bars("600000", market.Period1m) // 已封闭 bar
snap, _ := e.Snapshot("600000")          // 当前一致快照（含进行中 bar）
```
