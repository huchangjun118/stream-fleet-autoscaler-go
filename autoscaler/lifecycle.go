package autoscaler

import (
	"fmt"
	"sort"
)

// RouteSession 为一路新会话选择实例（回环输入驱动）。
// 只会调度到 Running 实例；排空中的实例不再接收新会话。
// 返回承载实例 ID；暂无可用实例时会话进入等待队列，容量出现后自动安置。
func (c *Controller) RouteSession(sessionID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.sessionHome[sessionID]; ok {
		return "", fmt.Errorf("session %s already routed", sessionID)
	}
	target := c.pickInstanceLocked()
	if target == "" {
		c.unplaced = append(c.unplaced, sessionID)
		c.log("session-wait", fmt.Sprintf("session %s queued: no running instance", sessionID))
		c.persist()
		return "", nil
	}
	c.placeSessionLocked(sessionID, target)
	c.persist()
	return target, nil
}

// placeSessionLocked 把会话放到指定实例（调用前须保证归属不冲突）。
func (c *Controller) placeSessionLocked(sessionID, instanceID string) {
	in := c.instances[instanceID]
	in.Sessions[sessionID] = &Session{
		ID:         sessionID,
		InstanceID: instanceID,
		StartedAt:  c.clock.Now(),
	}
	c.sessionHome[sessionID] = instanceID
}

// pickInstanceLocked 在 Running 实例中选负载最轻者（并列按 ID，保证确定性）。
func (c *Controller) pickInstanceLocked() string {
	var best *Instance
	for _, in := range c.instances {
		if in.State != StateRunning {
			continue
		}
		if best == nil ||
			len(in.Sessions) < len(best.Sessions) ||
			(len(in.Sessions) == len(best.Sessions) && in.ID < best.ID) {
			best = in
		}
	}
	if best == nil {
		return ""
	}
	return best.ID
}

// flushUnplacedLocked 尝试安置等待队列中的会话。
func (c *Controller) flushUnplacedLocked() {
	remaining := c.unplaced[:0]
	for _, sid := range c.unplaced {
		if _, ok := c.sessionHome[sid]; ok {
			continue // 已被安置
		}
		target := c.pickInstanceLocked()
		if target == "" {
			remaining = append(remaining, sid)
			continue
		}
		c.placeSessionLocked(sid, target)
		c.log("session-placed", fmt.Sprintf("queued session %s placed on %s", sid, target))
	}
	c.unplaced = remaining
}

// EndSession 结束一路会话（回环输入驱动）。
func (c *Controller) EndSession(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.endSessionLocked(sessionID)
	c.persist()
}

func (c *Controller) endSessionLocked(sessionID string) {
	home, ok := c.sessionHome[sessionID]
	if !ok {
		return
	}
	delete(c.sessionHome, sessionID)
	if in, ok := c.instances[home]; ok {
		delete(in.Sessions, sessionID)
	}
}

// startDrainLocked 让实例进入优雅排空：不再接收新会话，等待存量会话结束。
func (c *Controller) startDrainLocked(in *Instance, reason string) {
	if in.State != StateRunning {
		return
	}
	in.State = StateDraining
	in.DrainStartedAt = c.clock.Now()
	c.newOp(OpDrain, in.Zone, in.ID, 1)
	c.log("drain-start", fmt.Sprintf("instance %s draining: %s", in.ID, reason))
}

// finishDrainLocked 会话已清空，回收实例。
func (c *Controller) finishDrainLocked(in *Instance) {
	if err := c.provider.Terminate(in.ID); err != nil {
		c.log("terminate-retry", fmt.Sprintf("instance %s terminate failed: %v", in.ID, err))
		return // 下次 Reconcile 重试
	}
	in.State = StateTerminated
	c.completeOpFor(in.ID)
	c.log("drain-done", fmt.Sprintf("instance %s terminated after drain", in.ID))
}

// migrateSessionsLocked 把实例上的会话迁移到其他 Running 实例。
// 无可用实例时进入等待队列，保证会话不丢失、不重复占用。
func (c *Controller) migrateSessionsLocked(from *Instance, reason string) {
	sids := make([]string, 0, len(from.Sessions))
	for sid := range from.Sessions {
		sids = append(sids, sid)
	}
	sort.Strings(sids)
	for _, sid := range sids {
		delete(from.Sessions, sid)
		delete(c.sessionHome, sid)
		target := c.pickInstanceLocked()
		if target == "" {
			c.unplaced = append(c.unplaced, sid)
			c.log("session-wait", fmt.Sprintf("session %s queued during migration (%s)", sid, reason))
			continue
		}
		c.placeSessionLocked(sid, target)
		c.log("session-migrated", fmt.Sprintf("session %s migrated %s -> %s (%s)", sid, from.ID, target, reason))
	}
}

// abandonPending 放弃一个创建超时/失联的 Pending 实例。
func (c *Controller) abandonPending(in *Instance, reason string) {
	_ = c.provider.Terminate(in.ID) // 尽力回收，防止悬挂实例
	in.State = StateLost
	c.migrateSessionsLocked(in, reason)
	c.completeOpFor(in.ID)
	c.log("instance-lost", fmt.Sprintf("pending instance %s abandoned: %s", in.ID, reason))
}

// handleLostLocked 处理失联节点：重新安排其会话，按策略回收。
func (c *Controller) handleLostLocked(in *Instance) {
	_ = c.provider.Terminate(in.ID)
	in.State = StateLost
	c.migrateSessionsLocked(in, "node lost")
	c.completeOpFor(in.ID)
	c.log("instance-lost", fmt.Sprintf("instance %s lost, sessions rescheduled", in.ID))
}

// completeOpFor 将与实例关联的未完成操作标记完成。
func (c *Controller) completeOpFor(instanceID string) {
	for _, op := range c.ops {
		if !op.Done && op.InstanceID == instanceID {
			op.Done = true
			op.FinishedAt = c.clock.Now()
		}
	}
}
