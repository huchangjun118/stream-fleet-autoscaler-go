package autoscaler

import (
	"fmt"
	"sort"
	"time"
)

// Reconcile 执行一次收敛。整个流程：
//  1. 处理失联节点与超时操作（恢复）；
//  2. 推进进行中的排空；
//  3. 依据指标窗口计算期望容量；
//  4. 扩容（考虑可用区均匀分布与并发度）或缩容（仅空闲实例、优雅排空）；
//  5. 区域间再平衡；
//  6. 安置等待会话并持久化状态。
//
// 相同输入序列下结果完全确定。
func (c *Controller) Reconcile() Decision {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()

	c.recoverFailuresLocked(now)
	c.progressDrainsLocked(now)

	snap := c.metrics.Snapshot(now)
	serving := c.servingCount()
	dec := Decide(c.cfg, snap, serving, now, c.lastScaleUp, c.lastScaleDown)

	switch {
	case dec.Desired > serving:
		c.scaleOutLocked(dec.Desired-serving, now)
		c.lastScaleUp = now
	case dec.Desired < serving:
		c.scaleInLocked(serving-dec.Desired, now)
		c.lastScaleDown = now
	}

	c.rebalanceLocked(now)
	c.flushUnplacedLocked()
	c.persist()
	return dec
}

// recoverFailuresLocked 处理失联节点与超时操作。
func (c *Controller) recoverFailuresLocked(now time.Time) {
	for _, in := range c.sortedInstances() {
		switch in.State {
		case StatePending:
			if !c.provider.Alive(in.ID) || now.Sub(in.CreatedAt) > c.cfg.OpTimeout {
				c.abandonPending(in, "create timed out or node lost")
			}
		case StateRunning, StateDraining:
			if !c.provider.Alive(in.ID) {
				c.handleLostLocked(in)
			}
		}
	}
}

// progressDrainsLocked 推进排空：会话清空则回收；超过宽限期则强制迁移。
func (c *Controller) progressDrainsLocked(now time.Time) {
	for _, in := range c.sortedInstances() {
		if in.State != StateDraining {
			continue
		}
		if len(in.Sessions) == 0 {
			c.finishDrainLocked(in)
			continue
		}
		if now.Sub(in.DrainStartedAt) > c.cfg.DrainGracePeriod {
			c.log("drain-force", fmt.Sprintf("instance %s drain grace expired, migrating %d sessions",
				in.ID, len(in.Sessions)))
			c.migrateSessionsLocked(in, "drain grace expired")
			c.finishDrainLocked(in)
		}
	}
}

// scaleOutLocked 扩容：按可用区均匀放置，受并发度与上限约束。
// 部分创建失败不会使总量越界：失败即停止本轮扩容，下轮重新收敛。
func (c *Controller) scaleOutLocked(need int, now time.Time) {
	for i := 0; i < need; i++ {
		if c.inFlightOps() >= c.cfg.MaxConcurrentOps {
			c.log("scale-out-throttle", "max concurrent ops reached")
			return
		}
		if c.activeCount() >= c.cfg.MaxInstances {
			c.log("scale-out-capped", "max instances reached")
			return
		}
		zone, id, err := c.createInBestZone()
		if err != nil {
			c.log("scale-out-failed", err.Error())
			return // 本轮停止，避免在故障可用区上空转
		}
		c.instances[id] = &Instance{
			ID:        id,
			Zone:      zone,
			State:     StateRunning, // 模拟平台创建即就绪
			Sessions:  map[string]*Session{},
			CreatedAt: now,
		}
		op := c.newOp(OpScaleOut, zone, id, 1)
		op.Done = true
		op.FinishedAt = now
		c.log("scale-out", fmt.Sprintf("instance %s created in %s", id, zone))
	}
}

// createInBestZone 按“实例数最少、名称字典序”选择可用区创建，
// 失败时尝试次优可用区，保证均匀分布且慢/故障区域不阻塞整体。
func (c *Controller) createInBestZone() (string, string, error) {
	counts := map[string]int{}
	for _, in := range c.instances {
		if in.State == StatePending || in.State == StateRunning {
			counts[in.Zone]++
		}
	}
	zones := c.provider.Zones()
	sort.Slice(zones, func(i, j int) bool {
		if counts[zones[i]] != counts[zones[j]] {
			return counts[zones[i]] < counts[zones[j]]
		}
		return zones[i] < zones[j]
	})
	var lastErr error
	for _, z := range zones {
		id, err := c.provider.Create(z)
		if err == nil {
			return z, id, nil
		}
		lastErr = err
		c.log("zone-skipped", fmt.Sprintf("zone %s create failed: %v", z, err))
	}
	return "", "", fmt.Errorf("all zones failed: %v", lastErr)
}

// scaleInLocked 安全缩容：只选择确实空闲（无在途会话）的 Running 实例排空。
// 数量受并发度与下限约束，不会跌破 MinInstances。
func (c *Controller) scaleInLocked(n int, now time.Time) {
	// 候选：空闲的 Running 实例，按创建时间倒序（新的先回收）、ID 排序保证确定性。
	var candidates []*Instance
	for _, in := range c.instances {
		if in.State == StateRunning && len(in.Sessions) == 0 {
			candidates = append(candidates, in)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
		}
		return candidates[i].ID > candidates[j].ID
	})
	drained := 0
	for _, in := range candidates {
		if drained >= n {
			break
		}
		if c.inFlightOps() >= c.cfg.MaxConcurrentOps {
			c.log("scale-in-throttle", "max concurrent ops reached")
			return
		}
		// 下限保护：计入即将排空的实例后不得跌破 Min。
		if c.activeCount()-c.drainingCount() <= c.cfg.MinInstances {
			c.log("scale-in-floored", "min instances reached")
			return
		}
		c.startDrainLocked(in, "scale-in: idle instance")
		drained++
	}
}

// drainingCount 正在排空的实例数。
func (c *Controller) drainingCount() int {
	n := 0
	for _, in := range c.instances {
		if in.State == StateDraining {
			n++
		}
	}
	return n
}
