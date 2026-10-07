// Package scaler contains the deterministic hysteresis scaling policy and
// the decision types it produces. It is a pure function of (state, metrics,
// fleet view), so the same input sequence always yields the same decision.
package scaler

import (
	"fmt"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
)

// Direction is a scaling decision direction.
type Direction int

// Scaling directions.
const (
	Hold Direction = iota
	ScaleUp
	ScaleDown
)

func (d Direction) String() string {
	switch d {
	case ScaleUp:
		return "ScaleUp"
	case ScaleDown:
		return "ScaleDown"
	default:
		return "Hold"
	}
}

// Decision is the policy output for one tick.
type Decision struct {
	Direction      Direction
	Delta          int // instances to add(+) or remove(-)
	Reason         string
	RegionPressure map[string]float64
}

// Policy holds tuning and evaluates observations.
type Policy struct {
	cfg *config.Config
}

// NewPolicy creates a Policy.
func NewPolicy(cfg *config.Config) *Policy { return &Policy{cfg: cfg} }

// Input bundles every piece of state the decision depends on.
type Input struct {
	Tick         int64
	AvgUtil      float64
	CurrentCount int // non-terminal instances (incl. Pending/Draining)
	ServedCount  int // Running instances
	UpBreaches   int
	DownBreaches int
	LastUpTick   int64
	LastDownTick int64
}

// Evaluate is the pure decision function.
//
// Hysteresis: up requires ScaleUpAfter consecutive ticks at/above the up
// threshold and the up cooldown elapsed; down requires ScaleDownAfter
// consecutive ticks at/below the down threshold and down cooldown elapsed.
// The magnitude is limited by StepSize and clamped to [Min,Max] and by
// in-flight capacity, so partial create failures can never push the fleet
// past the upper bound or under the lower bound.
func (p *Policy) Evaluate(in Input) Decision {
	cfg := p.cfg

	upCooldownOK := in.Tick-in.LastUpTick >= int64(cfg.ScaleUpCooldownTicks)
	downCooldownOK := in.Tick-in.LastDownTick >= int64(cfg.ScaleDownCooldownTicks)

	if in.AvgUtil >= cfg.ScaleUpThreshold &&
		in.UpBreaches >= cfg.ScaleUpAfter && upCooldownOK {
		delta := cfg.StepSize
		if in.CurrentCount+delta > cfg.MaxReplicas {
			delta = cfg.MaxReplicas - in.CurrentCount
		}
		if delta <= 0 {
			return Decision{Direction: Hold,
				Reason: fmt.Sprintf("at upper bound %d (util=%.2f)", cfg.MaxReplicas, in.AvgUtil)}
		}
		return Decision{
			Direction: ScaleUp,
			Delta:     delta,
			Reason: fmt.Sprintf("util %.2f >= up %.2f for %d ticks",
				in.AvgUtil, cfg.ScaleUpThreshold, in.UpBreaches),
		}
	}

	if in.AvgUtil <= cfg.ScaleDownThreshold &&
		in.DownBreaches >= cfg.ScaleDownAfter && downCooldownOK {
		// Only count Running idle-reclaimable capacity toward removal; the
		// controller further guarantees every victim is truly idle.
		delta := cfg.StepSize
		if in.ServedCount-delta < cfg.MinReplicas {
			delta = in.ServedCount - cfg.MinReplicas
		}
		if delta <= 0 {
			return Decision{Direction: Hold,
				Reason: fmt.Sprintf("at lower bound %d (util=%.2f)", cfg.MinReplicas, in.AvgUtil)}
		}
		return Decision{
			Direction: ScaleDown,
			Delta:     delta,
			Reason: fmt.Sprintf("util %.2f <= down %.2f for %d ticks",
				in.AvgUtil, cfg.ScaleDownThreshold, in.DownBreaches),
		}
	}

	return Decision{Direction: Hold,
		Reason: fmt.Sprintf("within hysteresis band (util=%.2f, up=%d/%d, down=%d/%d)",
			in.AvgUtil, in.UpBreaches, cfg.ScaleUpAfter, in.DownBreaches, cfg.ScaleDownAfter)}
}
