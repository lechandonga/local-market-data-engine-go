# 本地行情数据引擎（Go，仅标准库）

面向回测与盘面复盘的本地行情引擎：tick 入库（乱序/判重/迟到）、固定周期
K 线聚合（含迟到修正与定稿）、行情快照与增量订阅（含慢订阅者背压信号），
并用本地 WAL 支持进程重启恢复。**全部行情数据本地模拟，不接任何外部数据源，
不引任何第三方依赖。**

## 目录

- `engine/model.go`：数据模型（`Tick` / `Bar` / `Snapshot` / 拒绝原因）
- `engine/engine.go`：引擎主体（入库判重、窗口聚合、定稿、快照）
- `engine/broker.go`：订阅总线（环形邮箱 + pump，非阻塞投递 + gap 信号）
- `engine/wal.go`：只追加 tick 日志（持久化与回放）
- `engine/*_test.go`：按业务分组的单测（乱序/重复/迟到修正/重启/并发）
- `cmd/demo/main.go`：可运行的端到端演示

## 数据模型

- `Tick{Symbol, Time, Price, Volume}`
  - `Time`：Unix 纳秒，UTC 语义；`0` 被视为非法时间戳拒绝。
  - `Price`：定点整数，放大倍数 `engine.PriceScale = 1e9`（9 位小数），
    避免浮点误差；聚合只做整数比较与加减。
  - 同一标的的数据（tick 表、bar 表、watermark、累计量）在引擎内独立存放，
    标的之间严格隔离。
- `Bar`：左闭右开窗口 `[WindowStart, WindowEnd)`，字段 O/H/L/C、Volume、
  TickCount，以及状态位 `Final` 和修正计数 `Revision`。
- `Snapshot{LastTime, LastPrice, TotalVol, TickCount}`：某标的当前已生效数据
  的一致视图。`LastTime` 是已生效 tick 的最大时间戳（watermark），
  `LastPrice` 是该 tick 的价格；因此乱序到达不会让快照“倒退”。

## 周期与时间边界规则

- 周期固定为 `Config.Period`（纳秒）。窗口起点 `w = floor(Time/Period)*Period`，
  窗口 `[w, w+Period)`，边界 tick 属于右侧窗口（例如 Period=1s 时，
  t=1.0s 归入 `[1s,2s)`）。
- 窗口归属**只由 tick 时间戳决定，与到达顺序无关**：任意到达顺序产出同一组 bar。
- OHLCV 口径（同标的同一时间戳至多一条 tick，时间戳唯一）：
  - Open  = 窗口内**最小时间戳** tick 的价格
  - Close = 窗口内**最大时间戳** tick 的价格
  - High/Low = 窗口内全部 tick 的最高/最低价
  - Volume = 窗口内全部 tick 成交量之和；TickCount = tick 条数

## 判重与迟到数据处理

`Ingest(Tick)` 返回 `IngestResult`，**任何不生效的数据都有明确结论，绝不静默丢弃**：

| 情况 | 结论 | `IngestResult` |
| --- | --- | --- |
| 时间戳为 0 | 拒绝 | `Accepted=false, Reason=invalid_timestamp` |
| 同标的同时间戳、内容完全相同 | 识别为重复，忽略不双计 | `Reason=duplicate` |
| 同标的同时间戳、价格/量冲突 | 拒绝，**保留先到者**，已有数据不改 | `Reason=duplicate_conflict` |
| 迟到 tick 落入**未定稿**窗口 | 生效并整窗重算修正 | `Accepted=true, Corrected=true` |
| 迟到 tick 落入**已定稿**窗口 | 拒绝，定稿 bar 不可改 | `Reason=too_late` |

- watermark：每标的已生效 tick 的最大时间戳。
- 定稿规则：当 `watermark >= w + Period + MaxLate` 时，窗口 `[w,w+Period)`
  定稿（`Final=true`，并发 `bar_finalized` 事件）。`MaxLate=0` 表示窗口一旦
  被后续数据越过边界立即定稿。
- 修正规则：对未定稿窗口的迟到 tick，用窗口内**全部** tick 重新聚合
  （O/H/L/C/V 全量重算），`Revision` 加 1，并再发一条 `bar_upserted`
  （订阅方据 `Revision` 判断是首次产出还是修正）。整窗重算保证修正后
  自洽，不存在增量修补造成的漂移。
- 定稿后该窗口只读，任何迟到都以 `too_late` 拒绝，不会改坏已确认数据。

## 并发与背压约定

- 引擎用一把 `sync.RWMutex` 保护全部内部状态：写（`Ingest`）互斥，
  读（快照、bar 查询）共享。多个写入方并发推送时，判重、聚合、watermark、
  定稿都在同一临界区内原子完成，状态不会写乱。
- 事件在写锁内按全局单调 `Seq` 编号后发布；业务事件序号严格递增、不重复。
- 每个订阅独立：一个有界环形邮箱 + 一个 pump goroutine。
  - 写入端投递**非阻塞**：邮箱满则丢弃最新事件并累计断点，绝不反压写入端。
  - 消费端腾出空间后，pump 会补发一条 `gap` 事件（无需等待新业务事件），
    告知 `GapDropped`（丢了多少条）、`GapFromSeq/GapToSeq`（从哪里断到哪里）；
    gap 是订阅者本地信号，不占引擎事件序号，其 `Seq` 为最近成功收到的
    业务事件序号（首条即丢时为 0）。订阅方可用区间做断线补齐决策。
  - 订阅彼此隔离，慢订阅者只阻塞它自己的 pump；关闭订阅/引擎会安全摘链
    并关闭通道，不影响写入和其他订阅。

## 本地持久化与重启恢复

- 配置 `WALPath` 后，只有**生效**的 tick 会顺序追加到 WAL（二进制记录：
  魔数 + 标的长度/标的 + time/price/volume 各 u64）。
- `FsyncWAL=true` 时每条记录 `fsync`，已确认生效的数据崩溃不丢；
  关闭时走吞吐优先（OS page cache）。
- 启动时按 WAL 原始写入顺序回放重建状态。因为只持久化生效 tick，
  重复/被拒绝数据从未落盘，回放出的 tick 集合、判重状态、watermark 与
  bar 序列（OHLCV/成交量/笔数）与重启前**完全一致**；重启后重灌同样数据，
  全部按 `duplicate` 拒绝，bar 序列不变。

## 本地验证

需要 Go 1.25+ 与 C 工具链（竞态检测依赖 cgo）。

```bash
# 全部单测 + 竞态检测；-v 日志按 “业务 -> 用例 -> PASS” 展示
CGO_ENABLED=1 go test ./... -race -v

# 端到端演示（会在当前目录生成 demo-tick.wal；再跑一次可看到全部 duplicate）
cd cmd/demo && go run .
```

测试覆盖的业务与用例：

- tick 入库与判重：乱序生效、完全重复不双计、同时间戳冲突保留先到者、
  超范围迟到拒绝且定稿 bar 不被改坏、非法时间戳、跨标的隔离
- K 线聚合：OHLCV 口径与左闭右开边界、到达顺序不影响结果、迟到修正
  （含二次修正）自洽、定稿后拒绝并发出定稿事件
- 重启恢复：WAL 回放后 bar/快照与重启前一致、恢复后继续灌数与不重启一致
- 快照与订阅：快照一致性、事件序号不倒退、慢订阅 gap 信号可区分、
  关闭订阅不阻塞写入不串数据
- 并发读写：8 写入方并发 + 并发重复重放、4 订阅者并发消费（`-race`）
- 本地持久化：WAL 追加/回放往返一致
