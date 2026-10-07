package autoscaler

import (
	"path/filepath"
	"testing"
	"time"
)

// 节点失联：其承载会话必须被重新安排，不丢失、不重复占用。
func TestNodeLostReschedulesSessions(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	c, p, clock := newTestController(t, cfg, []string{"zone-a", "zone-b"})

	feed(c, clock, 100, 6)
	for i := 0; i < 4; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 100, 1)
	}

	// 注入若干会话
	sessions := []string{"s1", "s2", "s3", "s4", "s5"}
	for _, sid := range sessions {
		if _, err := c.RouteSession(sid); err != nil {
			t.Fatalf("route %s: %v", sid, err)
		}
	}

	// 找一个有会话的实例并注入失联
	c.mu.Lock()
	var lostID string
	for _, in := range c.sortedInstances() {
		if in.State == StateRunning && len(in.Sessions) > 0 {
			lostID = in.ID
			break
		}
	}
	affected := map[string]bool{}
	for sid, home := range c.sessionHome {
		if home == lostID {
			affected[sid] = true
		}
	}
	c.mu.Unlock()
	if lostID == "" {
		t.Skip("no instance with sessions")
	}
	p.LoseNode(lostID)
	c.Reconcile()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.instances[lostID].State != StateLost {
		t.Fatalf("lost instance state=%s", c.instances[lostID].State)
	}
	// 所有会话仍各有唯一归属，且不在失联实例上
	for _, sid := range sessions {
		home, ok := c.sessionHome[sid]
		if !ok {
			t.Fatalf("session %s lost after node failure", sid)
		}
		if home == lostID {
			t.Fatalf("session %s still on lost instance", sid)
		}
	}
	// 无重复占用：每个实例上的会话数与 sessionHome 一致
	seen := map[string]int{}
	for _, home := range c.sessionHome {
		seen[home]++
	}
	for id, n := range seen {
		if len(c.instances[id].Sessions) != n {
			t.Fatalf("instance %s session count mismatch", id)
		}
	}
	_ = affected
}

// 排空中途节点失联：会话迁移，实例按 Lost 处理，不悬挂。
func TestNodeLostDuringDrain(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	c, p, clock := newTestController(t, cfg, []string{"zone-a"})

	feed(c, clock, 100, 6)
	for i := 0; i < 3; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 100, 1)
	}
	c.RouteSession("s-a")
	c.mu.Lock()
	home := c.sessionHome["s-a"]
	c.startDrainLocked(c.instances[home], "test")
	c.mu.Unlock()

	p.LoseNode(home)
	c.Reconcile()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.instances[home].State != StateLost {
		t.Fatalf("state=%s, want Lost", c.instances[home].State)
	}
	newHome, ok := c.sessionHome["s-a"]
	if !ok {
		t.Fatalf("session lost during drain node failure")
	}
	if newHome == home {
		t.Fatalf("session not migrated off lost node")
	}
	// 无悬挂：所有未完成 op 都与存活实例关联
	for _, op := range c.ops {
		if !op.Done && op.InstanceID == home {
			t.Fatalf("op %s dangling on lost instance", op.ID)
		}
	}
}

// 控制器重启：从 FileStore 恢复后识别实际与期望差异并继续收敛，
// 不重复下发已完成的操作。
func TestControllerRestartRecovery(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	storePath := filepath.Join(t.TempDir(), "state.json")
	clock := NewFakeClock(t0)
	p := NewSimProvider(clock, []string{"zone-a", "zone-b"})

	// 第一代控制器：扩容并记录操作
	c1, err := NewController(cfg, p, NewFileStore(storePath), clock)
	if err != nil {
		t.Fatal(err)
	}
	feed(c1, clock, 100, 6)
	for i := 0; i < 3; i++ {
		c1.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c1, clock, 100, 1)
	}
	before := runningCount(c1)
	opsBefore := len(c1.ops)
	if before <= cfg.MinInstances {
		t.Fatalf("setup: expected scale out, got %d", before)
	}

	// 模拟重启：同一 store/provider/clock 新建控制器
	c2, err := NewController(cfg, p, NewFileStore(storePath), clock)
	if err != nil {
		t.Fatal(err)
	}
	if got := runningCount(c2); got != before {
		t.Fatalf("after restart instances=%d, want %d (state restored)", got, before)
	}
	// 恢复后 op 日志不丢、不重复
	if len(c2.ops) != opsBefore {
		t.Fatalf("ops count changed across restart: %d -> %d", opsBefore, len(c2.ops))
	}

	// 负载回落，重启后的控制器继续收敛缩容
	feed(c2, clock, 0, 6)
	for i := 0; i < 20; i++ {
		clock.Advance(cfg.ScaleDownCooldown + time.Second)
		feed(c2, clock, 0, 1)
		c2.Reconcile()
	}
	if got := runningCount(c2); got != cfg.MinInstances {
		t.Fatalf("post-restart convergence failed: instances=%d, want %d", got, cfg.MinInstances)
	}
	// 全程可追溯：有 scale-out 与 drain 事件
	var sawOut, sawDrain bool
	for _, e := range c2.Events() {
		if e.Kind == "scale-out" {
			sawOut = true
		}
		if e.Kind == "drain-start" || e.Kind == "drain-done" {
			sawDrain = true
		}
	}
	if !sawOut || !sawDrain {
		t.Fatalf("missing audit events: scale-out=%v drain=%v", sawOut, sawDrain)
	}
}

// 创建中途重启 + 超时：悬挂 Pending 实例被回收，不占用容量。
func TestRestartAbandonsStalePending(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	storePath := filepath.Join(t.TempDir(), "state.json")
	clock := NewFakeClock(t0)
	p := NewSimProvider(clock, []string{"zone-a"})

	c1, _ := NewController(cfg, p, NewFileStore(storePath), clock)
	// 手工塞入一个 Pending 实例并持久化，模拟创建中途宕机
	c1.mu.Lock()
	c1.instances["i-zone-a-pend"] = &Instance{
		ID: "i-zone-a-pend", Zone: "zone-a", State: StatePending,
		Sessions: map[string]*Session{}, CreatedAt: clock.Now(),
	}
	c1.newOp(OpScaleOut, "zone-a", "i-zone-a-pend", 1)
	c1.persist()
	c1.mu.Unlock()

	// 时钟推进超过 OpTimeout 后重启
	clock.Advance(cfg.OpTimeout + time.Second)
	c2, err := NewController(cfg, p, NewFileStore(storePath), clock)
	if err != nil {
		t.Fatal(err)
	}
	c2.Reconcile()
	c2.mu.Lock()
	defer c2.mu.Unlock()
	if c2.instances["i-zone-a-pend"].State != StateLost {
		t.Fatalf("stale pending not abandoned: %s", c2.instances["i-zone-a-pend"].State)
	}
	for _, op := range c2.ops {
		if !op.Done && op.InstanceID == "i-zone-a-pend" {
			t.Fatalf("op for stale pending still open")
		}
	}
}
