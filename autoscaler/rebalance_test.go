package autoscaler

import (
	"testing"
	"time"
)

func zoneCounts(c *Controller) map[string]int {
	counts := map[string]int{}
	for _, in := range c.Instances() {
		if in.State == StateRunning || in.State == StatePending {
			counts[in.Zone]++
		}
	}
	return counts
}

// 区域失衡超过阈值时触发再平衡，把容量从重区迁往轻区。
func TestRebalanceCorrectsImbalance(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	cfg.RebalanceImbalanceThreshold = 2
	cfg.RebalanceCooldown = time.Minute
	c, p, clock := newTestController(t, cfg, []string{"zone-a", "zone-b"})

	// 手工制造失衡：zone-a 5 个，zone-b 0 个
	c.mu.Lock()
	for i := 0; i < 5; i++ {
		id, _ := p.Create("zone-a")
		c.instances[id] = &Instance{
			ID: id, Zone: "zone-a", State: StateRunning,
			Sessions: map[string]*Session{}, CreatedAt: clock.Now(),
		}
	}
	c.persist()
	c.mu.Unlock()

	clock.Advance(cfg.RebalanceCooldown + time.Second)
	c.Reconcile()

	counts := zoneCounts(c)
	if counts["zone-b"] == 0 {
		t.Fatalf("rebalance did not add capacity to light zone: %v", counts)
	}
	// 重区应开始排空（迁移而非直接删除）
	var draining int
	for _, in := range c.Instances() {
		if in.Zone == "zone-a" && (in.State == StateDraining || in.State == StateTerminated) {
			draining++
		}
	}
	if draining == 0 {
		t.Fatalf("rebalance did not drain heavy zone: %v", zoneCounts(c))
	}
}

// 稳定状态（偏差在阈值内）不得迁移。
func TestRebalanceStableDoesNotMigrate(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	cfg.RebalanceImbalanceThreshold = 2
	cfg.RebalanceCooldown = time.Minute
	c, _, clock := newTestController(t, cfg, []string{"zone-a", "zone-b"})

	// 均衡布局：3/2，偏差 1 < 阈值 2
	c.mu.Lock()
	for i, z := range []string{"zone-a", "zone-b", "zone-a", "zone-b", "zone-a"} {
		id, _ := c.provider.Create(z)
		c.instances[id] = &Instance{
			ID: id, Zone: z, State: StateRunning,
			Sessions: map[string]*Session{}, CreatedAt: clock.Now(),
		}
		_ = i
	}
	c.persist()
	c.mu.Unlock()
	opsBefore := len(c.ops)

	for i := 0; i < 10; i++ {
		clock.Advance(cfg.RebalanceCooldown + time.Second)
		c.Reconcile()
	}
	// 不得产生任何 rebalance 操作
	for _, op := range c.ops[opsBefore:] {
		if op.Type == OpRebalance {
			t.Fatalf("rebalance triggered in stable state")
		}
	}
	if got := runningCount(c); got != 5 {
		t.Fatalf("stable state changed capacity: %d", got)
	}
}

// 慢/故障区域不阻塞其他区域的再平衡与扩容。
func TestSlowZoneDoesNotBlockOthers(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	cfg.MaxConcurrentOps = 10
	c, p, clock := newTestController(t, cfg, []string{"zone-a", "zone-slow"})

	// zone-slow 创建必失败
	p.FailZones["zone-slow"] = true

	feed(c, clock, 100, 6)
	for i := 0; i < 4; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 100, 1)
	}
	// 健康区域应正常扩容
	if got := runningCount(c); got <= cfg.MinInstances {
		t.Fatalf("slow zone blocked scale-out on healthy zone: instances=%d", got)
	}
	for _, in := range c.Instances() {
		if in.Zone == "zone-slow" && in.State != StateLost && in.State != StateTerminated {
			t.Fatalf("instance placed in failed zone: %s", in.ID)
		}
	}
}
