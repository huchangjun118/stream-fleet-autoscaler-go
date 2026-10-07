package controller

import (
	"context"
	"fmt"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/placement"
)

// rebalanceStep performs at most one region-to-region move per tick. The new
// instance is created first and the surplus node is drained only afterward,
// so capacity never dips during the move. Each region keeps its own cooldown
// (a slow region therefore never blocks a fast one), and the planner refuses
// to act while the fleet is unstable or within the balance threshold.
func (c *Controller) rebalanceStep(ctx context.Context, snap *fleet.Snapshot, tick int64) error {
	if c.inFlight() >= c.cfg.MaxConcurrentOps {
		return nil
	}
	moves := c.rebal.Evaluate(snap, c.st.LastRebalanceTick, tick)
	if len(moves) == 0 {
		return nil
	}
	mv := moves[0]

	// Source must be a genuinely idle instance: we never interrupt sessions
	// to rebalance (they would be migrated only on drain timeout anyway).
	load := c.scheduler.LoadMap(snap)
	regionSnap := c.regionSnapshot(snap, mv.FromRegion)
	victims := c.drain.PickIdle(regionSnap, load, 1)
	if len(victims) == 0 {
		return nil
	}

	// Create into the under-weight region first (respects MaxReplicas).
	cap := c.cfg.Specs[mv.Spec]
	if err := c.createOne(ctx, snap, placement.Allocation{
		Region: mv.ToRegion, Spec: mv.Spec, Capacity: cap,
	}); err != nil {
		return err
	}

	if err := c.startDrain(ctx, snap, victims[0],
		fmt.Sprintf("rebalance %s->%s", mv.FromRegion, mv.ToRegion)); err != nil {
		return err
	}

	c.st.LastRebalanceTick[mv.FromRegion] = tick
	c.st.LastRebalanceTick[mv.ToRegion] = tick
	return nil
}

// regionSnapshot returns a view containing only the instances of one region;
// sessions are retained so idle detection sees real load.
func (c *Controller) regionSnapshot(snap *fleet.Snapshot, region string) *fleet.Snapshot {
	out := &fleet.Snapshot{
		Instances: map[string]*fleet.Instance{},
		Sessions:  snap.Sessions,
	}
	for id, in := range snap.Instances {
		if in.Region == region {
			out.Instances[id] = in
		}
	}
	return out
}
