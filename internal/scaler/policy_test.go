package scaler

import (
	"testing"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
)

func testCfg() *config.Config {
	c := config.Default()
	c.MinReplicas = 2
	c.MaxReplicas = 10
	c.ScaleUpThreshold = 0.75
	c.ScaleDownThreshold = 0.35
	c.ScaleUpAfter = 2
	c.ScaleDownAfter = 3
	c.ScaleUpCooldownTicks = 2
	c.ScaleDownCooldownTicks = 4
	c.StepSize = 2
	return c
}

func TestHysteresisNoFlapping(t *testing.T) {
	p := NewPolicy(testCfg())
	// Alternating values right at the boundary must never trigger a move.
	decisions := []Decision{}
	up, down := 0, 0
	for tick := int64(1); tick <= 12; tick++ {
		// Utilization alternates 0.8 and 0.3: neither streak completes.
		u := 0.8
		if tick%2 == 0 {
			u = 0.3
		}
		if u >= 0.75 {
			up++
			down = 0
		} else if u <= 0.35 {
			down++
			up = 0
		}
		d := p.Evaluate(Input{
			Tick: tick, AvgUtil: u, CurrentCount: 4, ServedCount: 4,
			UpBreaches: up, DownBreaches: down,
			LastUpTick: -10, LastDownTick: -10,
		})
		decisions = append(decisions, d)
	}
	for i, d := range decisions {
		if d.Direction != Hold {
			t.Fatalf("tick %d: expected Hold in flapping band, got %s", i+1, d.Direction)
		}
	}
}

func TestScaleUpRequiresStreakAndCooldown(t *testing.T) {
	p := NewPolicy(testCfg())
	// One high tick is not enough.
	if d := p.Evaluate(Input{Tick: 1, AvgUtil: 0.9, CurrentCount: 4, ServedCount: 4,
		UpBreaches: 1, LastUpTick: -10, LastDownTick: -10}); d.Direction != Hold {
		t.Fatalf("single breach must hold, got %s", d.Direction)
	}
	// Two consecutive high ticks => scale up by step.
	d := p.Evaluate(Input{Tick: 2, AvgUtil: 0.9, CurrentCount: 4, ServedCount: 4,
		UpBreaches: 2, LastUpTick: -10, LastDownTick: -10})
	if d.Direction != ScaleUp || d.Delta != 2 {
		t.Fatalf("expected ScaleUp delta 2, got %s %d (%s)", d.Direction, d.Delta, d.Reason)
	}
	// Immediately after, cooldown blocks another scale up even on streak.
	d = p.Evaluate(Input{Tick: 3, AvgUtil: 0.9, CurrentCount: 6, ServedCount: 6,
		UpBreaches: 2, LastUpTick: 2, LastDownTick: -10})
	if d.Direction != Hold {
		t.Fatalf("cooldown should block, got %s", d.Direction)
	}
}

func TestScaleDownRespectsMin(t *testing.T) {
	p := NewPolicy(testCfg())
	d := p.Evaluate(Input{Tick: 10, AvgUtil: 0.1, CurrentCount: 2, ServedCount: 2,
		DownBreaches: 3, LastUpTick: -10, LastDownTick: -10})
	if d.Direction != Hold {
		t.Fatalf("at min replicas must hold, got %s", d.Direction)
	}
	d = p.Evaluate(Input{Tick: 10, AvgUtil: 0.1, CurrentCount: 3, ServedCount: 3,
		DownBreaches: 3, LastUpTick: -10, LastDownTick: -10})
	if d.Direction != ScaleDown || d.Delta != 1 {
		t.Fatalf("expected ScaleDown delta 1, got %s %d", d.Direction, d.Delta)
	}
}

func TestScaleUpClampedToMax(t *testing.T) {
	p := NewPolicy(testCfg())
	d := p.Evaluate(Input{Tick: 10, AvgUtil: 0.99, CurrentCount: 9, ServedCount: 9,
		UpBreaches: 2, LastUpTick: -10, LastDownTick: -10})
	if d.Direction != ScaleUp || d.Delta != 1 {
		t.Fatalf("expected clamped ScaleUp delta 1, got %s %d", d.Direction, d.Delta)
	}
}

// TestReproducibleDecision verifies the pure-function contract: identical
// inputs produce identical outputs across repeated evaluations.
func TestReproducibleDecision(t *testing.T) {
	p := NewPolicy(testCfg())
	in := Input{Tick: 5, AvgUtil: 0.9, CurrentCount: 4, ServedCount: 4,
		UpBreaches: 2, LastUpTick: -10, LastDownTick: -10}
	first := p.Evaluate(in)
	for i := 0; i < 50; i++ {
		d := p.Evaluate(in)
		if d.Direction != first.Direction || d.Delta != first.Delta || d.Reason != first.Reason {
			t.Fatalf("decision not reproducible at iteration %d", i)
		}
	}
}
