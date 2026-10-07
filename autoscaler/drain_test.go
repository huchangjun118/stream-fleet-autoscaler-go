package autoscaler

import (
	"testing"
	"time"
)

// 负载回落触发缩容时，有会话的实例不得被立即回收，
// 排空期间新会话不得调度到该实例，存量会话不得被中断。
func TestDrainProtectsActiveSessions(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	cfg.ScaleDownCooldown = 30 * time.Second
	cfg.DrainGracePeriod = 10 * time.Minute
	c, _, clock := newTestController(t, cfg, []string{"zone-a"})

	// 扩容到 4 实例
	feed(c, clock, 200, 6)
	for i := 0; i < 4; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 200, 1)
	}
	if got := runningCount(c); got < 4 {
		t.Fatalf("setup: want >=4 instances, got %d", got)
	}

	// 手动注入会话：往每个实例放一路长会话
	ids := []string{}
	for _, in := range c.Instances() {
		if in.State == StateRunning {
			ids = append(ids, in.ID)
		}
	}
	for _, id := range ids {
		c.mu.Lock()
		c.placeSessionLocked("s-long-"+id, id)
		c.mu.Unlock()
	}

	// 负载回落：指标显示低负载，但实例上有真实会话
	feed(c, clock, 0, 6)
	for i := 0; i < 10; i++ {
		clock.Advance(cfg.ScaleDownCooldown + time.Second)
		c.Reconcile()
	}

	// 所有带会话的实例必须仍然存在（Draining 或 Running），会话未被中断
	c.mu.Lock()
	for _, id := range ids {
		in := c.instances[id]
		if in == nil || in.State == StateTerminated || in.State == StateLost {
			t.Fatalf("instance %s with active session was reclaimed", id)
		}
		if len(in.Sessions) == 0 {
			t.Fatalf("session on %s lost during scale-in", id)
		}
	}
	c.mu.Unlock()
}

// 排空宽限期超时后，会话被迁移到其他实例而不是丢失。
func TestDrainGraceExpiryMigratesSessions(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	cfg.ScaleDownCooldown = 30 * time.Second
	cfg.DrainGracePeriod = time.Minute
	c, _, clock := newTestController(t, cfg, []string{"zone-a"})

	feed(c, clock, 200, 6)
	for i := 0; i < 4; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 200, 1)
	}

	// 挑一个实例放会话，然后直接让它进入排空
	c.mu.Lock()
	var victim *Instance
	for _, in := range c.instances {
		if in.State == StateRunning {
			victim = in
			break
		}
	}
	c.placeSessionLocked("s-1", victim.ID)
	c.startDrainLocked(victim, "test")
	c.mu.Unlock()

	// 排空期间新会话不得调度到该实例
	if got, _ := c.RouteSession("s-new"); got == victim.ID {
		t.Fatalf("new session routed to draining instance")
	}

	// 推进超过宽限期，会话应被迁移且实例被回收
	clock.Advance(cfg.DrainGracePeriod + time.Second)
	c.Reconcile()

	c.mu.Lock()
	defer c.mu.Unlock()
	if victim.State != StateTerminated {
		t.Fatalf("victim state=%s, want Terminated", victim.State)
	}
	home, ok := c.sessionHome["s-1"]
	if !ok {
		t.Fatalf("session s-1 lost after forced drain")
	}
	if home == victim.ID {
		t.Fatalf("session s-1 still on terminated instance")
	}
}

// 会话自然结束后，排空实例被回收。
func TestDrainCompletesWhenSessionsEnd(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	c, _, _ := newTestController(t, cfg, []string{"zone-a"})

	c.Reconcile()
	if got := runningCount(c); got != cfg.MinInstances {
		t.Fatalf("setup: got %d instances", got)
	}
	sid, _ := c.RouteSession("s-x")
	if sid == "" {
		// 可能进入等待队列，再收敛一次
		c.Reconcile()
		sid, _ = c.RouteSession("s-x")
	}

	c.mu.Lock()
	home := c.sessionHome["s-x"]
	c.startDrainLocked(c.instances[home], "test")
	c.mu.Unlock()

	// 会话未结束时不得回收
	c.Reconcile()
	c.mu.Lock()
	if c.instances[home].State == StateTerminated {
		t.Fatalf("instance terminated while session active")
	}
	c.mu.Unlock()

	// 会话结束后回收
	c.EndSession("s-x")
	c.Reconcile()
	c.mu.Lock()
	if c.instances[home].State != StateTerminated {
		t.Fatalf("instance not terminated after drain completed, state=%s", c.instances[home].State)
	}
	c.mu.Unlock()
}
