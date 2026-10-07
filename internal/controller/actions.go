package controller

import (
	"context"
	"fmt"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/placement"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/provider"
)

// converge issues the minimum bounded set of creates/drains needed to move
// the actual non-terminal capacity toward DesiredReplicas. It is safe under
// partial create failure: a failed create consumes no capacity and is simply
// retried on a later tick, and PlanAdd never plans beyond MaxReplicas; drains
// only target provably idle instances so capacity never drops below Min.
func (c *Controller) converge(ctx context.Context, snap *fleet.Snapshot) error {
	nonTerminal, served := c.counts(snap)
	draining := 0
	for _, in := range snap.Instances {
		if in.State == fleet.StateDraining || in.State == fleet.StateDrained {
			draining++
		}
	}

	switch {
	case nonTerminal < c.st.DesiredReplicas:
		deficit := c.st.DesiredReplicas - nonTerminal
		allocations := c.planner.PlanAdd(snap, deficit)
		for _, a := range allocations {
			if c.inFlight() >= c.cfg.MaxConcurrentOps {
				break
			}
			if err := c.createOne(ctx, snap, a); err != nil {
				return err
			}
		}

	case nonTerminal > c.st.DesiredReplicas:
		// Instances already draining will close the gap when they terminate.
		want := nonTerminal - c.st.DesiredReplicas - draining
		if want <= 0 {
			break
		}
		load := c.scheduler.LoadMap(snap)
		// Prefer provably idle nodes (immediate, zero-disruption reclaim).
		victims := c.drain.PickIdle(snap, load, want)
		already := map[string]bool{}
		for _, id := range victims {
			already[id] = true
		}
		// If not enough idle nodes exist, begin graceful drain on the least
		// loaded remaining nodes: new sessions stop immediately, existing
		// sessions finish naturally, and only past the drain timeout are they
		// atomically migrated (never interrupted before that).
		if len(victims) < want {
			// Never drain down to fewer Running nodes than the lower bound in
			// one step; surviving capacity must keep serving.
			running := 0
			for _, in := range snap.Instances {
				if in.State == fleet.StateRunning {
					running++
				}
			}
			busyBudget := running - c.cfg.MinReplicas - len(victims)
			if busyBudget > want-len(victims) {
				busyBudget = want - len(victims)
			}
			// Consider extra candidates beyond the budget so un-migratable
			// (stuck) nodes can be skipped in favor of feasible ones.
			busyAll := c.drain.PickBusy(snap, load, running, already)
			var busy []string
			for _, id := range busyAll {
				if len(busy) >= busyBudget {
					break
				}
				// A node is drainable only if its sessions fit on the region's
				// surviving capacity (idle victims counted as removed).
				excl := map[string]bool{}
				for k := range already {
					excl[k] = true
				}
				for _, b := range busy {
					excl[b] = true
				}
				if c.drain.Migratable(snap, load, id, excl) {
					busy = append(busy, id)
				}
			}
			victims = append(victims, busy...)
		}
		started := 0
		for _, id := range victims {
			if c.inFlight() >= c.cfg.MaxConcurrentOps {
				break
			}
			if err := c.startDrain(ctx, snap, id, "scale-down"); err != nil {
				return err
			}
			started++
		}
		if started < want {
			c.event("scale-down.wait",
				fmt.Sprintf("want %d drain candidate(s), started %d; waiting for graceful drain/concurrency",
					want, started))
		}
	}

	// Even at target, retry previously failed creates is unnecessary because
	// failed instances are terminal: the deficit reappears here next tick.
	_ = served
	return nil
}

// createOne issues a single idempotent create and journals it. A scripted
// failure is recorded terminally so no dangling Pending op remains and no
// capacity is counted.
func (c *Controller) createOne(ctx context.Context, snap *fleet.Snapshot, a placement.Allocation) error {
	c.st.NextOpSeq++
	seq := c.st.NextOpSeq
	instanceID := fmt.Sprintf("inst-%s-%06d", a.Region, seq)
	in, err := c.prov.CreateInstance(ctx, provider.CreateRequest{
		InstanceID: instanceID,
		Region:     a.Region,
		Spec:       a.Spec,
		Capacity:   a.Capacity,
	})
	op := &fleet.OpRecord{
		ID:         fmt.Sprintf("op-%d", seq),
		Kind:       fleet.OpCreate,
		InstanceID: instanceID,
		Region:     a.Region,
		Spec:       a.Spec,
		CreatedAt:  tickStamp(c.st.Tick),
		UpdatedAt:  tickStamp(c.st.Tick),
	}
	if err != nil {
		op.Status = fleet.OpFailed
		c.st.Ops = append(c.st.Ops, op)
		if in != nil {
			snap.Instances[instanceID] = in
		}
		c.event("create.failed", fmt.Sprintf("create %s (%s/%s) failed: %v",
			instanceID, a.Region, a.Spec, err))
		return nil // bounded retry on a later tick
	}
	op.Status = fleet.OpPending
	c.st.Ops = append(c.st.Ops, op)
	if in != nil {
		snap.Instances[instanceID] = in
	}
	c.event("create.issue", fmt.Sprintf("creating %s in %s (%s, cap=%d)",
		instanceID, a.Region, a.Spec, a.Capacity))
	return nil
}

// startDrain marks an idle instance draining and journals the drain. New
// sessions are immediately rejected by the provider for a Draining node.
func (c *Controller) startDrain(ctx context.Context, snap *fleet.Snapshot, id, reason string) error {
	if err := c.drain.BeginDrain(ctx, snap, id); err != nil {
		return err
	}
	if in, ok := snap.Instances[id]; ok {
		in.State = fleet.StateDraining
	}
	c.st.Ops = append(c.st.Ops, &fleet.OpRecord{
		ID:         c.newOpID(),
		Kind:       fleet.OpDrain,
		InstanceID: id,
		Status:     fleet.OpPending,
		CreatedAt:  tickStamp(c.st.Tick),
		UpdatedAt:  tickStamp(c.st.Tick),
	})
	c.event("drain.begin", fmt.Sprintf("draining %s (%s)", id, reason))
	return nil
}

// progressDrains advances pending drains. A node is terminated only once it
// holds zero active sessions. Ongoing sessions are never interrupted: if the
// drain timeout is exceeded the controller atomically migrates them away
// first; if no destination has room it keeps waiting rather than dropping a
// session.
func (c *Controller) progressDrains(ctx context.Context, snap *fleet.Snapshot, tick int64) error {
	load := c.scheduler.LoadMap(snap)
	for _, op := range c.st.Ops {
		if op.Kind != fleet.OpDrain || op.Status != fleet.OpPending {
			continue
		}
		in, ok := snap.Instances[op.InstanceID]
		if !ok {
			op.Status = fleet.OpFailed
			continue
		}
		switch in.State {
		case fleet.StateStopped:
			op.Status = fleet.OpSucceeded
			op.UpdatedAt = tickStamp(tick)
			c.event("drain.done", fmt.Sprintf("instance %s terminated", in.ID))
			continue
		case fleet.StateLost:
			op.Status = fleet.OpFailed
			continue
		case fleet.StateRunning:
			// Defensive: backend lost the drain marker; re-assert it.
			_ = c.prov.StartDrain(ctx, in.ID)
			in.State = fleet.StateDraining
		}

		age := tick - op.CreatedAt.Unix()
		if load[in.ID] == 0 {
			if err := c.drain.Complete(ctx, in.ID); err != nil {
				return err
			}
			in.State = fleet.StateStopped
			op.Status = fleet.OpSucceeded
			op.UpdatedAt = tickStamp(tick)
			c.event("drain.done", fmt.Sprintf("instance %s terminated gracefully", in.ID))
			continue
		}

		if age >= int64(c.cfg.DrainTimeoutTicks) {
			moved, err := c.drain.MigrateSessions(ctx, snap, load, in.ID)
			for _, sid := range moved {
				c.event("session.migrate",
					fmt.Sprintf("session %s moved off draining instance %s", sid, in.ID))
			}
			if err != nil {
				c.event("drain.wait",
					fmt.Sprintf("instance %s drain timed out but migration blocked: %v", in.ID, err))
				continue // never abandon an in-flight session
			}
			if err := c.drain.Complete(ctx, in.ID); err != nil {
				return err
			}
			in.State = fleet.StateStopped
			op.Status = fleet.OpSucceeded
			op.UpdatedAt = tickStamp(tick)
			c.event("drain.done",
				fmt.Sprintf("instance %s terminated after migrating %d session(s)", in.ID, len(moved)))
		}
	}
	return nil
}
