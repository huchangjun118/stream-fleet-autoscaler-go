// Package controller is the single-threaded reconcile loop that each tick:
// observes backend reality, recovers lost/in-flight work, evaluates the
// deterministic scaling policy, executes bounded scale up/down actions with
// graceful drain and session migration, rebalances regions, and persists an
// auditable event + operation journal before the next tick.
package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/clock"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/drain"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/metrics"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/placement"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/provider"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/rebalance"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/reconcile"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/scaler"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/scheduler"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/store"
)

// globalRegion is the synthetic region under which aggregate offered load is
// tracked for the hysteresis policy.
const globalRegion = "_global"

// Controller runs the autoscaling reconcile loop.
type Controller struct {
	cfg       *config.Config
	prov      provider.Provider
	store     store.Store
	clk       clock.Clock
	policy    *scaler.Policy
	planner   *placement.Planner
	drain     *drain.Manager
	scheduler *scheduler.Scheduler
	rebal     *rebalance.Planner
	rec       *reconcile.Reconciler
	window    *metrics.Window
	st        *store.State
	snap      *fleet.Snapshot
}

// New wires a Controller. Durable state is loaded by Restore so the loop
// resumes rather than restarts its convergence.
func New(cfg *config.Config, prov provider.Provider, st store.Store, clk clock.Clock) *Controller {
	sch := scheduler.New()
	return &Controller{
		cfg:       cfg,
		prov:      prov,
		store:     st,
		clk:       clk,
		policy:    scaler.NewPolicy(cfg),
		planner:   placement.NewPlanner(cfg),
		drain:     drain.NewManager(prov, sch),
		scheduler: sch,
		rebal:     rebalance.NewPlanner(cfg),
		rec:       reconcile.NewReconciler(cfg, prov, sch),
		window:    metrics.NewWindow(cfg.WindowTicks),
	}
}

func (c *Controller) event(evType, msg string) {
	tick := int64(0)
	if c.st != nil {
		tick = c.st.Tick
	}
	_ = c.store.AppendEvent(context.Background(), c.st, tick, evType, msg)
}

func (c *Controller) save() { _ = c.store.Save(context.Background(), c.st) }

func tickStamp(tick int64) time.Time { return time.Unix(tick, 0).UTC() }

// Restore loads (or initializes) durable state and reconciles it with the
// backend: expected-vs-actual divergence is detected and convergence resumes
// without re-issuing completed scale actions.
func (c *Controller) Restore(ctx context.Context) error {
	st, err := c.store.Load(ctx)
	if err != nil {
		return err
	}
	var needFreshEvent, needRestartEvent bool
	if st == nil {
		st = store.NewState()
		st.DesiredReplicas = c.cfg.MinReplicas
		needFreshEvent = true
	} else {
		needRestartEvent = true
	}
	c.st = st
	if needFreshEvent {
		c.event("start", fmt.Sprintf("fresh start: desired=%d", st.DesiredReplicas))
	}
	if needRestartEvent {
		c.event("restart", fmt.Sprintf("restart: tick=%d desired=%d ops=%d",
			st.Tick, st.DesiredReplicas, len(st.Ops)))
	}
	c.save()
	return nil
}

// Run loops Tick until ctx is cancelled.
func (c *Controller) Run(ctx context.Context, loads func(tick int64) map[string]float64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := c.Tick(ctx, loads(c.st.Tick+1)); err != nil {
			return err
		}
	}
}

// Snapshot returns the last observed fleet view.
func (c *Controller) Snapshot() *fleet.Snapshot {
	if c.snap == nil {
		return &fleet.Snapshot{Instances: map[string]*fleet.Instance{}, Sessions: map[string]*fleet.Session{}}
	}
	return c.snap
}

// State returns a pointer to the current durable state.
func (c *Controller) State() *store.State { return c.st }

// inFlight counts pending creates + active drains occupying concurrency slots.
func (c *Controller) inFlight() int {
	n := 0
	for _, op := range c.st.Ops {
		if op.Status != fleet.OpPending {
			continue
		}
		if op.Kind == fleet.OpCreate || op.Kind == fleet.OpDrain {
			n++
		}
	}
	return n
}

func (c *Controller) newOpID() string {
	c.st.NextOpSeq++
	return fmt.Sprintf("op-%d", c.st.NextOpSeq)
}

// findPendingOp returns the pending op of kind for instanceID.
func (c *Controller) findPendingOp(kind fleet.OpKind, instanceID string) *fleet.OpRecord {
	for _, op := range c.st.Ops {
		if op.Kind == kind && op.InstanceID == instanceID && op.Status == fleet.OpPending {
			return op
		}
	}
	return nil
}
