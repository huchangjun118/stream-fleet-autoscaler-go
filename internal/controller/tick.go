package controller

import (
	"context"
	"fmt"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/metrics"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/placement"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/provider"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/scaler"
)

// Tick executes exactly one deterministic reconcile iteration:
//  1. advance the simulated backend and observe authoritative reality;
//  2. adopt in-flight creates, recover lost nodes/orphan sessions, time out ops;
//  3. record the metric window and evaluate the hysteresis policy;
//  4. converge actual capacity toward desired via bounded creates/drains;
//  5. progress existing drains (graceful, migrate only on timeout);
//  6. rebalance regions when stable and imbalanced.
func (c *Controller) Tick(ctx context.Context, loads map[string]float64) error {
	c.st.Tick++
	tick := c.st.Tick

	// 1. Advance backend, then observe.
	if t, ok := c.prov.(interface{ Tick() }); ok {
		t.Tick()
	}
	snap, err := c.rec.Snapshot(ctx)
	if err != nil {
		return err
	}
	c.snap = snap

	// 2a. Adopt/restart recovery for in-flight creates.
	adopted := c.rec.AdoptCreates(ctx, c.st, snap)
	for _, id := range adopted {
		c.event("create.adopt", fmt.Sprintf("adopted in-flight create %s", id))
	}

	// 2b. Recover lost nodes and reschedule their detached sessions.
	lost := c.rec.RecoverLost(ctx, c.st, snap)
	for _, id := range lost {
		c.event("node.lost", fmt.Sprintf("instance %s lost contact", id))
	}
	if moved, err := c.rec.RescheduleOrphans(ctx, snap); err != nil {
		return err
	} else if len(moved) > 0 {
		c.event("session.reschedule", fmt.Sprintf("rescheduled %d orphan session(s): %v", len(moved), moved))
	}

	// 2c. Timed-out operations + compensation.
	timedOut := c.rec.TimeoutOps(ctx, c.st, snap, tick)
	for _, id := range timedOut {
		c.event("op.timeout", fmt.Sprintf("operation on %s timed out and compensated", id))
	}

	// 3. Metrics + hysteresis policy.
	//
	// Per-region utilization (offered load / that region's running capacity)
	// is averaged across regions, so the numerator and denominator use the
	// same scope and a saturated region is never hidden by spare capacity in
	// another region. Utilization is not capped at 1, keeping unmet demand
	// visible until scale-out creates real headroom.
	var avgLoad, utilSum float64
	pressure := map[string]float64{}
	regionCap := map[string]int{}
	for _, in := range snap.Instances {
		if in.State == fleet.StateRunning {
			regionCap[in.Region] += in.Capacity
		}
	}
	n := len(c.cfg.Regions)
	for _, r := range c.cfg.Regions {
		l := loads[r.Name]
		c.window.Add(metrics.Sample{Tick: tick, Region: r.Name, Load: l})
		avgLoad += l
		pressure[r.Name] = l
		ru := 1.0
		if cap := regionCap[r.Name]; cap > 0 {
			ru = l / float64(cap)
		} else if l <= 0 {
			ru = 0
		}
		utilSum += ru
	}
	if n > 0 {
		avgLoad /= float64(n)
	}
	util := 0.0
	if n > 0 {
		util = utilSum / float64(n)
	}
	c.window.Add(metrics.Sample{Tick: tick, Region: globalRegion, Load: util})

	c.updateBreaches(tick, util)

	count, served := c.counts(snap)
	upBreaches := 0
	for _, n := range c.st.UpBreaches {
		if n > upBreaches {
			upBreaches = n
		}
	}
	downBreaches := 0
	for _, n := range c.st.DownBreaches {
		if n > downBreaches {
			downBreaches = n
		}
	}
	dec := c.policy.Evaluate(scaler.Input{
		Tick:         tick,
		AvgUtil:      util,
		CurrentCount: count,
		ServedCount:  served,
		UpBreaches:   upBreaches,
		DownBreaches: downBreaches,
		LastUpTick:   c.st.LastScaleUpTick,
		LastDownTick: c.st.LastScaleDownTick,
	})
	c.event("policy", fmt.Sprintf("util=%.2f decision=%s delta=%d (%s)",
		util, dec.Direction, dec.Delta, dec.Reason))

	switch dec.Direction {
	case scaler.ScaleUp:
		c.st.DesiredReplicas = min(c.cfg.MaxReplicas, count+dec.Delta)
		c.st.LastScaleUpTick = tick
	case scaler.ScaleDown:
		c.st.DesiredReplicas = max(c.cfg.MinReplicas, served-dec.Delta)
		c.st.LastScaleDownTick = tick
	}

	// 4. Converge actual -> desired.
	if err := c.converge(ctx, snap); err != nil {
		return err
	}

	// 5. Progress drains.
	if err := c.progressDrains(ctx, snap, tick); err != nil {
		return err
	}

	// 6. Rebalance when stable.
	if err := c.rebalanceStep(ctx, snap, tick); err != nil {
		return err
	}

	c.save()
	return nil
}

// utilizationByRegion is kept for diagnostics: it computes the current
// offered-load utilization of one region (uncapped, so overload stays > 1).
func (c *Controller) utilizationByRegion(snap *fleet.Snapshot, region string, offered float64) float64 {
	capacity := 0
	for _, in := range snap.Instances {
		if in.State == fleet.StateRunning && in.Region == region {
			capacity += in.Capacity
		}
	}
	if capacity == 0 {
		if offered > 0 {
			return 1
		}
		return 0
	}
	return offered / float64(capacity)
}

func (c *Controller) updateBreaches(tick int64, util float64) {
	upFn := func(v float64) bool { return v >= c.cfg.ScaleUpThreshold }
	downFn := func(v float64) bool { return v <= c.cfg.ScaleDownThreshold }
	up := c.window.AverageRegionBreaches(globalRegion, c.cfg.ScaleUpAfter, upFn)
	down := c.window.AverageRegionBreaches(globalRegion, c.cfg.ScaleDownAfter, downFn)
	c.st.UpBreaches[globalRegion] = up
	c.st.DownBreaches[globalRegion] = down
}

func (c *Controller) counts(snap *fleet.Snapshot) (nonTerminal, running int) {
	for _, in := range snap.Instances {
		if in.State == fleet.StateStopped || in.State == fleet.StateFailed {
			continue
		}
		nonTerminal++
		if in.State == fleet.StateRunning {
			running++
		}
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var _ = placement.Allocation{}
var _ = provider.ErrCreateFailed
