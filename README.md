# stream-fleet-autoscaler-go

实时流媒体实例的动态扩缩容与容量再平衡控制器。实例生命周期与负载指标由本地模拟
（`SimProvider`）或回环输入驱动，决策与收敛逻辑与底层平台解耦，可替换为真实云平台实现。

## 架构

```
autoscaler/
  types.go       实例/会话/操作记录等核心类型
  config.go      全部可调参数与校验（见下表）
  clock.go       时钟抽象：RealClock / FakeClock（测试确定性）
  metrics.go     负载指标滑动窗口（平均值/峰值，容忍乱序采样）
  decider.go     纯函数决策器：迟滞阈值 + 冷却 + 步长限制
  provider.go    容量平台接口 Provider 与本地模拟 SimProvider（支持故障注入）
  store.go       状态持久化：MemStore / FileStore（原子快照，支撑重启恢复）
  controller.go  控制器状态、重启恢复、会话路由入口
  lifecycle.go   会话调度、优雅排空、会话迁移、失联处理
  reconcile.go   收敛主循环 Reconcile：恢复→排空→扩缩→再平衡→落盘
  rebalance.go   多可用区容量再平衡
  run.go         真实时钟周期驱动入口
cmd/sim/         脚本化负载回环演示程序
```

## 收敛语义

控制器以 `Reconcile()` 为最小收敛步，每步依次：

1. **故障恢复**：失联节点（`Provider.Alive()==false`）标记为 `Lost`，其承载会话按
   “最少负载优先”重新安置到 Running 实例；无可用实例时进入等待队列，容量出现后自动
   安置——会话不丢失、不重复占用。创建超时（`OpTimeout`）的 Pending 实例尽力回收，
   不留悬挂实例。
2. **排空推进**：Draining 实例会话清空即回收；超过 `DrainGracePeriod` 仍有会话时强制
   迁移后回收。排空期间新会话不会调度到该实例，存量会话不被中断。
3. **期望容量计算**：`Decide` 为纯函数——相同输入序列必然产生相同决策（可复现）。
   - 窗口平均利用率 > `ScaleUpThreshold` 且过扩容冷却 → 扩容，目标利用率取扩容阈值；
   - 窗口平均利用率 < `ScaleDownThreshold` 且过缩容冷却 → 缩容，目标利用率取迟滞中点，
     使缩容后稳态落在迟滞带内部，避免来回抖动；
   - 两阈值之间为迟滞带，不做任何调整；
   - 单步变化受 `MaxScaleOutStep`/`MaxScaleInStep` 限制，结果夹在 `[Min,Max]` 内。
4. **扩容放置**：按“区域实例数最少、名称字典序”选择可用区，失败自动尝试次优区域；
   部分创建失败时本轮停止，下轮重新收敛——任何时刻不越上限、不越并发度
   （`MaxConcurrentOps`）。
5. **安全缩容**：仅选择**无在途会话**的 Running 实例进入优雅排空；计入在途排空后
   不得跌破 `MinInstances`。
6. **再平衡**：区域间实例数偏差 > `RebalanceImbalanceThreshold` 且过
   `RebalanceCooldown` 才触发，每次只迁移一个**空闲**实例（先建后排，瞬态受上限保护），
   稳定状态下不迁移；慢/故障区域只跳过自身，不阻塞其他区域。
7. **持久化**：每步结束把实例视图、操作日志、审计事件原子写入 `Store`。

### 重启恢复

控制器重启后从 `Store` 恢复实例视图与操作日志：

- 已完成的操作不会重复下发（操作记录带 `Done` 标记）；
- 进行中的排空继续推进；创建中途的 Pending 实例按 `OpTimeout` 重新校验，超时即放弃
  并迁移其会话；
- 实际容量与期望容量的差异在后续 `Reconcile` 中继续收敛；
- 审计事件随状态持久化，全过程跨重启可追溯。

## 配置项

| 字段 | 默认 | 说明 |
|---|---|---|
| `MinInstances` / `MaxInstances` | 1 / 100 | 弹性上下限，任何时刻容量不越界 |
| `InstanceCapacity` | 100 | 单实例会话容量，用于负载换算 |
| `ScaleUpThreshold` | 0.75 | 窗口平均利用率高于此值扩容 |
| `ScaleDownThreshold` | 0.40 | 低于此值才缩容；两阈值之间为迟滞带 |
| `MetricsWindow` | 2m | 指标滑动窗口 |
| `DecisionInterval` | 10s | 决策间隔（`Run` 的驱动周期） |
| `ScaleUpCooldown` / `ScaleDownCooldown` | 30s / 3m | 扩容/缩容冷却时间 |
| `MaxScaleOutStep` / `MaxScaleInStep` | 4 / 2 | 单步最大扩/缩数量 |
| `MaxConcurrentOps` | 4 | 在途创建+排空并发上限 |
| `OpTimeout` | 2m | 创建/排空操作超时，超时按策略恢复 |
| `DrainGracePeriod` | 5m | 排空宽限期，超时强制迁移会话 |
| `RebalanceImbalanceThreshold` | 2 | 区域间偏差超过此值才再平衡 |
| `RebalanceCooldown` | 10m | 再平衡最小间隔 |

## 运行

```bash
go test ./...      # 全部自动化测试
go run ./cmd/sim   # 回环演示：负载突增→扩容→回落→缩容，输出关键事件
```

## 测试覆盖

- `decider_test.go`：阈值触发、迟滞带保持、冷却约束、上下限夹取、决策可复现、
  抖动输入不振荡；
- `scale_test.go`：负载突增扩容、负载回落缩容、部分创建失败不越界、跨区均匀分布；
- `drain_test.go`：缩容保护在途会话、排空宽限期强制迁移、会话结束后完成回收；
- `recovery_test.go`：节点失联会话重排、排空中失联、控制器重启恢复、
  创建中途重启的悬挂实例清理；
- `rebalance_test.go`：失衡纠偏、稳定状态不迁移、慢区域不阻塞；
- `metrics_test.go`：窗口滑动、乱序采样、同输入序列重放决策一致。
