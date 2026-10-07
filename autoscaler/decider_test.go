package autoscaler

import (
	"testing"
	"time"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.MinInstances = 2
	cfg.MaxInstances = 10
	cfg.InstanceCapacity = 10
	cfg.ScaleUpThreshold = 0.8
	cfg.ScaleDownThreshold = 0.4
	cfg.ScaleUpCooldown = time.Minute
	cfg.ScaleDownCooldown = 2 * time.Minute
	cfg.MaxScaleOutStep = 3
	cfg.MaxScaleInStep = 2
	return cfg
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func snap(avg float64) Snapshot { return Snapshot{Samples: 5, Avg: avg} }

func TestDecideScaleUp(t *testing.T) {
	cfg := testConfig()
	// 4 实例 * 10 容量 = 40，平均 36 -> 利用率 0.9 > 0.8
	d := Decide(cfg, snap(36), 4, t0, t0.Add(-time.Hour), t0.Add(-time.Hour))
	if d.Desired <= 4 {
		t.Fatalf("expected scale up, got desired=%d", d.Desired)
	}
	if d.Desired > 4+cfg.MaxScaleOutStep {
		t.Fatalf("step limit violated: desired=%d", d.Desired)
	}
}

func TestDecideHysteresisHoldsSteady(t *testing.T) {
	cfg := testConfig()
	// 利用率 0.6，处于 [0.4, 0.8] 迟滞区间内：不动作
	d := Decide(cfg, snap(24), 4, t0, t0.Add(-time.Hour), t0.Add(-time.Hour))
	if d.Desired != 4 {
		t.Fatalf("hysteresis band should hold steady, got desired=%d", d.Desired)
	}
}

func TestDecideScaleDownCooldown(t *testing.T) {
	cfg := testConfig()
	// 利用率 0.1 < 0.4，但缩容冷却未过
	d := Decide(cfg, snap(4), 4, t0, t0.Add(-time.Hour), t0.Add(-time.Minute))
	if d.Desired != 4 {
		t.Fatalf("cooldown should block scale down, got desired=%d", d.Desired)
	}
	// 冷却过后允许缩容，且受步长限制
	d = Decide(cfg, snap(4), 4, t0, t0.Add(-time.Hour), t0.Add(-time.Hour))
	if d.Desired != 2 {
		t.Fatalf("expected scale down by step to 2, got %d", d.Desired)
	}
}

func TestDecideClampsToBounds(t *testing.T) {
	cfg := testConfig()
	d := Decide(cfg, snap(10000), 9, t0, t0.Add(-time.Hour), t0.Add(-time.Hour))
	if d.Desired > cfg.MaxInstances {
		t.Fatalf("desired %d exceeds max", d.Desired)
	}
	d = Decide(cfg, snap(0), 3, t0, t0.Add(-time.Hour), t0.Add(-time.Hour))
	if d.Desired < cfg.MinInstances {
		t.Fatalf("desired %d below min", d.Desired)
	}
}

func TestDecideDeterministic(t *testing.T) {
	cfg := testConfig()
	first := Decide(cfg, snap(33), 5, t0, t0.Add(-90*time.Second), t0.Add(-3*time.Minute))
	for i := 0; i < 100; i++ {
		got := Decide(cfg, snap(33), 5, t0, t0.Add(-90*time.Second), t0.Add(-3*time.Minute))
		if got != first {
			t.Fatalf("non-deterministic decision: %v vs %v", got, first)
		}
	}
}

func TestDecideFlappingInputDoesNotOscillate(t *testing.T) {
	cfg := testConfig()
	// 负载在阈值附近来回：0.85 与 0.5 交替（容量 40）
	current := 4
	lastUp, lastDown := t0.Add(-time.Hour), t0.Add(-time.Hour)
	changes := 0
	prev := current
	now := t0
	for i := 0; i < 20; i++ {
		now = now.Add(30 * time.Second)
		avg := 34.0 // 0.85
		if i%2 == 1 {
			avg = 20.0 // 0.5，仍在迟滞带内
		}
		d := Decide(cfg, snap(avg), current, now, lastUp, lastDown)
		if d.Desired > current {
			lastUp = now
		} else if d.Desired < current {
			lastDown = now
		}
		if d.Desired != prev {
			changes++
		}
		prev = d.Desired
		current = d.Desired
	}
	// 扩容后利用率回落到迟滞带内，不应再反复变化
	if changes > 2 {
		t.Fatalf("flapping input caused %d changes, want <= 2", changes)
	}
}
