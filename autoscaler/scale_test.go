package autoscaler

import (
	"testing"
	"time"
)

// newTestController 创建使用假时钟与模拟平台的控制器。
func newTestController(t *testing.T, cfg Config, zones []string) (*Controller, *SimProvider, *FakeClock) {
	t.Helper()
	clock := NewFakeClock(t0)
	p := NewSimProvider(clock, zones)
	c, err := NewController(cfg, p, NewMemStore(), clock)
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	return c, p, clock
}

// feed 注入 n 个间隔 10s、负载为 load 的采样并推进时钟。
func feed(c *Controller, clock *FakeClock, load, n int) {
	for i := 0; i < n; i++ {
		clock.Advance(10 * time.Second)
		c.AddMetrics(MetricsSample{At: clock.Now(), ActiveStreams: load})
	}
}

func runningCount(c *Controller) int {
	n := 0
	for _, in := range c.Instances() {
		if in.State == StateRunning {
			n++
		}
	}
	return n
}

func TestScaleOutOnLoadSpike(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	c, _, clock := newTestController(t, cfg, []string{"zone-a", "zone-b"})

	// 初始：无指标，收敛到下限
	c.Reconcile()
	if got := runningCount(c); got != cfg.MinInstances {
		t.Fatalf("initial instances=%d, want min=%d", got, cfg.MinInstances)
	}

	// 负载突增：80 路流，容量 10/实例，阈值 0.8
	feed(c, clock, 80, 6)
	c.Reconcile()
	if got := runningCount(c); got <= cfg.MinInstances {
		t.Fatalf("load spike did not trigger scale out, instances=%d", got)
	}
	// 不得超过上限
	if got := runningCount(c); got > cfg.MaxInstances {
		t.Fatalf("instances=%d exceeds max=%d", got, cfg.MaxInstances)
	}
}

func TestScaleInAfterLoadDrop(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	cfg.ScaleDownCooldown = time.Minute
	c, _, clock := newTestController(t, cfg, []string{"zone-a"})

	// 先扩上去
	feed(c, clock, 90, 6)
	for i := 0; i < 5; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 90, 1)
	}
	peak := runningCount(c)
	if peak <= cfg.MinInstances {
		t.Fatalf("setup: expected scale out, got %d", peak)
	}

	// 负载回落到 0，窗口内旧样本过期后应逐步缩容
	for i := 0; i < 20; i++ {
		clock.Advance(cfg.ScaleDownCooldown + time.Second)
		feed(c, clock, 0, 1)
		c.Reconcile()
	}
	if got := runningCount(c); got != cfg.MinInstances {
		t.Fatalf("after load drop instances=%d, want min=%d", got, cfg.MinInstances)
	}
	// 所有缩容都应走优雅排空且最终回收
	for _, in := range c.Instances() {
		if in.State == StateDraining {
			t.Fatalf("instance %s stuck in draining", in.ID)
		}
	}
}

func TestPartialCreateFailureRespectsBounds(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	c, p, clock := newTestController(t, cfg, []string{"zone-a", "zone-b"})

	// zone-b 故障，zone-a 前 2 次创建失败
	p.FailZones["zone-b"] = true
	p.FailNextCreates = 2

	feed(c, clock, 95, 6)
	// 故障期间多轮收敛：任何时刻都不得越上限；
	// 平台恢复前无法保证下限，但恢复后必须收敛回 [min,max]。
	for i := 0; i < 3; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		if got := runningCount(c); got > cfg.MaxInstances {
			t.Fatalf("instances=%d exceeds max during partial failure", got)
		}
	}

	// 故障恢复后继续收敛，最终满足需求且不越界
	p.FailZones = map[string]bool{}
	for i := 0; i < 10; i++ {
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 95, 1)
		c.Reconcile()
	}
	got := runningCount(c)
	if got < cfg.MinInstances {
		t.Fatalf("instances=%d below min after recovery", got)
	}
	if got > cfg.MaxInstances {
		t.Fatalf("instances=%d exceeds max=%d", got, cfg.MaxInstances)
	}
	if got < 10 { // 95 路 / (10*0.8) ≈ 12，夹在 max=10
		t.Fatalf("expected convergence to max=10, got %d", got)
	}
	// 全程无悬挂实例
	for _, in := range c.Instances() {
		if in.State == StatePending {
			t.Fatalf("instance %s left pending", in.ID)
		}
	}
}

func TestScaleOutSpreadsAcrossZones(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsWindow = time.Minute
	cfg.MaxConcurrentOps = 10
	cfg.MaxScaleOutStep = 6
	c, _, clock := newTestController(t, cfg, []string{"zone-a", "zone-b", "zone-c"})

	feed(c, clock, 200, 6)
	for i := 0; i < 6; i++ {
		c.Reconcile()
		clock.Advance(cfg.ScaleUpCooldown + time.Second)
		feed(c, clock, 200, 1)
	}
	counts := map[string]int{}
	for _, in := range c.Instances() {
		if in.State == StateRunning {
			counts[in.Zone]++
		}
	}
	if len(counts) < 3 {
		t.Fatalf("instances not spread across zones: %v", counts)
	}
	// 均匀分布：任意两区差值不超过 1
	min, max := 1<<30, 0
	for _, n := range counts {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	if max-min > 1 {
		t.Fatalf("uneven zone distribution: %v", counts)
	}
}
