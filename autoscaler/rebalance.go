package autoscaler

import (
	"fmt"
	"sort"
	"time"
)

// rebalanceLocked 区域间再平衡：当各可用区实例数偏差超过阈值时，
// 从过载区域挑选空闲实例排空，并在欠载区域补建。
// 慢/故障区域只影响自身补建，不阻塞其他区域；稳定状态（偏差低于阈值）不做任何迁移。
func (c *Controller) rebalanceLocked(now time.Time) {
	zones := c.provider.Zones()
	if len(zones) < 2 {
		return
	}
	if now.Sub(c.lastRebalance) < c.cfg.RebalanceCooldown {
		return
	}
	counts := map[string]int{}
	for _, z := range zones {
		counts[z] = 0
	}
	for _, in := range c.instances {
		if in.State == StatePending || in.State == StateRunning {
			counts[in.Zone]++
		}
	}
	// 找最重与最轻的区域。
	sorted := append([]string(nil), zones...)
	sort.Slice(sorted, func(i, j int) bool {
		if counts[sorted[i]] != counts[sorted[j]] {
			return counts[sorted[i]] < counts[sorted[j]]
		}
		return sorted[i] < sorted[j]
	})
	light, heavy := sorted[0], sorted[len(sorted)-1]
	if counts[heavy]-counts[light] <= c.cfg.RebalanceImbalanceThreshold {
		return // 稳定状态，不迁移
	}
	// 只迁移空闲实例，不打断在途会话。
	var victim *Instance
	for _, in := range c.sortedInstances() {
		if in.Zone == heavy && in.State == StateRunning && len(in.Sessions) == 0 {
			victim = in
			break
		}
	}
	if victim == nil {
		return // 无可迁移的空闲实例，等待下次
	}
	if c.inFlightOps() >= c.cfg.MaxConcurrentOps {
		return
	}
	// 上限保护：迁移是“先建后排”，瞬态会 +1，达到上限时本轮跳过，
	// 待缩容释放容量后再迁移，保证任何时刻不越上限。
	if c.activeCount() >= c.cfg.MaxInstances {
		c.log("rebalance-capped", "max instances reached, defer rebalance")
		return
	}
	id, err := c.provider.Create(light)
	if err != nil {
		c.log("rebalance-skipped", fmt.Sprintf("zone %s unavailable: %v", light, err))
		return // 慢区域失败不影响其他区域后续收敛
	}
	c.instances[id] = &Instance{
		ID:        id,
		Zone:      light,
		State:     StateRunning,
		Sessions:  map[string]*Session{},
		CreatedAt: now,
	}
	c.startDrainLocked(victim, fmt.Sprintf("rebalance %s -> %s", heavy, light))
	op := c.newOp(OpRebalance, light, id, 1)
	op.Done = true
	op.FinishedAt = now
	c.lastRebalance = now
	c.log("rebalance", fmt.Sprintf("moved capacity %s(%s) -> %s(%s)", victim.ID, heavy, id, light))
}
