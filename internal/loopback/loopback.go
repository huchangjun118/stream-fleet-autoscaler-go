// Package loopback provides a deterministic local driver for the controller:
// a scripted timeline of load observations, session arrivals/ends, partial
// create failures and node-loss events. Replaying the same script with the
// same config produces the identical sequence of decisions.
package loopback

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/clock"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/controller"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/provider"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/scheduler"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/store"
)

// EventKind enumerates scripted input events.
type EventKind string

// Scripted event kinds.
const (
	EvLoad    EventKind = "load"    // set offered session-slot load per region
	EvLose    EventKind = "lose"    // a node is lost (Ref=instanceID or any in region)
	EvRestart EventKind = "restart" // controller restart at this tick
	EvSustain EventKind = "sustain" // pin Value long-lived sessions (not ended on slump)
)

// Event is one point in the scripted timeline.
type Event struct {
	At     int64
	Kind   EventKind
	Region string
	Value  float64
	Ref    string
}

// Script is a full replay scenario plus initial desired size.
type Script struct {
	Name    string
	Initial int // desired replicas at start (0 -> config MinReplicas)
	Events  []Event
	Ticks   int64
}

// Runner replays a Script against a Sim-backed controller.
type Runner struct {
	cfg  *config.Config
	sim  *provider.Sim
	st   store.Store
	clk  *clock.Fake
	sch  *scheduler.Scheduler
	ctrl *controller.Controller

	loads      map[string]float64
	sticky     map[string]int // region -> number of long-lived sessions to keep
	sessionSeq int64
	restartAt  map[int64]bool
}

// NewRunner builds a runner for the script.
func NewRunner(cfg *config.Config, scr *Script) *Runner {
	sim := provider.NewSimFromConfig(cfg)
	clk := clock.NewFake()
	st := store.NewMemoryWithClock(clk)
	r := &Runner{
		cfg:       cfg,
		sim:       sim,
		st:        st,
		clk:       clk,
		sch:       scheduler.New(),
		loads:     map[string]float64{},
		sticky:    map[string]int{},
		restartAt: map[int64]bool{},
	}
	r.ctrl = controller.New(cfg, sim, st, clk)
	return r
}

// Sim exposes the simulated backend (tests use it to assert/lose nodes).
func (r *Runner) Sim() *provider.Sim { return r.sim }

// Controller exposes the active controller.
func (r *Runner) Controller() *controller.Controller { return r.ctrl }

// Run replays every tick and returns the trace of decisions/events.
func (r *Runner) Run(ctx context.Context, scr *Script) (*Report, error) {
	if err := r.cfg.Validate(); err != nil {
		return nil, err
	}
	if err := r.ctrl.Restore(ctx); err != nil {
		return nil, err
	}
	if scr.Initial > 0 {
		r.ctrl.State().DesiredReplicas = scr.Initial
	}

	// Index scripted events by tick for deterministic application order.
	at := map[int64][]Event{}
	for _, ev := range scr.Events {
		at[ev.At] = append(at[ev.At], ev)
	}

	for tick := int64(1); tick <= scr.Ticks; tick++ {
		r.clk.Advance(time.Second)
		for _, ev := range at[tick] {
			if err := r.apply(ctx, ev); err != nil {
				return nil, err
			}
		}

		if r.restartAt[tick] {
			r.ctrl = controller.New(r.cfg, r.sim, r.st, r.clk)
			if err := r.ctrl.Restore(ctx); err != nil {
				return nil, err
			}
		}

		if err := r.ctrl.Tick(ctx, r.loads); err != nil {
			return nil, fmt.Errorf("tick %d: %w", tick, err)
		}
		// After capacity adjusts, drive session arrivals/ends toward demand.
		if err := r.driveSessions(ctx); err != nil {
			return nil, err
		}
	}
	return r.report(scr.Name), nil
}

func (r *Runner) apply(ctx context.Context, ev Event) error {
	switch ev.Kind {
	case EvLoad:
		r.loads[ev.Region] = ev.Value
	case EvLose:
		id := ev.Ref
		if id == "" {
			id = r.pickInstance(ev.Region)
		}
		if id != "" {
			// The controller observes the loss on its next tick and
			// reschedules the detached sessions via the reconciler.
			r.sim.LoseInstance(id)
		}
	case EvRestart:
		r.restartAt[ev.At] = true
	case EvSustain:
		r.sticky[ev.Region] += int(ev.Value)
	}
	return nil
}

// pickInstance returns a non-terminal instance id in region, deterministic.
func (r *Runner) pickInstance(region string) string {
	snap, _ := r.sim.DescribeInstances(context.Background())
	var ids []string
	for id, in := range snap {
		if (region == "" || in.Region == region) &&
			in.State != fleet.StateStopped && in.State != fleet.StateFailed &&
			in.State != fleet.StateLost {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

// driveSessions reconciles the live session count in each region toward the
// offered load (rounded down). Excess sessions end naturally; missing demand
// is placed via the even-distribution scheduler.
func (r *Runner) driveSessions(ctx context.Context) error {
	snap, err := r.sim.DescribeInstances(ctx)
	if err != nil {
		return err
	}
	sess, err := r.sim.DescribeSessions(ctx)
	if err != nil {
		return err
	}
	view := &fleet.Snapshot{Instances: snap, Sessions: sess}

	for _, rc := range r.cfg.Regions {
		// Sticky long-lived sessions must not be ended when offered load falls;
		// they persist until the scenario ends (or the node is lost/migrated).
		target := int(r.loads[rc.Name])
		if st := r.sticky[rc.Name]; st > target {
			target = st
		}
		have := 0
		var mine []*fleet.Session
		for _, s := range sess {
			if s.Region == rc.Name && s.State == fleet.SessionActive {
				have++
				mine = append(mine, s)
			}
		}
		// End excess sessions (oldest first, deterministic).
		if have > target {
			sort.Slice(mine, func(i, j int) bool {
				if !mine[i].StartedAt.Equal(mine[j].StartedAt) {
					return mine[i].StartedAt.Before(mine[j].StartedAt)
				}
				return mine[i].ID < mine[j].ID
			})
			for i := 0; i < have-target; i++ {
				if err := r.sim.EndSession(ctx, mine[i].ID); err != nil {
					return err
				}
			}
		}
		// Place new demand.
		for have < target {
			load := r.sch.LoadMap(view)
			id := r.sch.Select(view, rc.Name, load)
			if id == "" {
				break // capacity pending; next tick retries (no session lost)
			}
			r.sessionSeq++
			sid := fmt.Sprintf("sess-%s-%06d", rc.Name, r.sessionSeq)
			if err := r.sim.PlaceSession(ctx, provider.PlaceSessionRequest{
				SessionID: sid, InstanceID: id, Region: rc.Name,
			}); err != nil {
				return err
			}
			view.Sessions[sid] = &fleet.Session{ID: sid, InstanceID: id, Region: rc.Name, State: fleet.SessionActive}
			have++
		}
	}
	return nil
}

func (r *Runner) report(name string) *Report {
	rep := &Report{Name: name}
	st := r.ctrl.State()
	for _, ev := range st.Events {
		rep.Events = append(rep.Events, fmt.Sprintf("[t%02d] %s: %s", ev.Tick, ev.Type, ev.Message))
		switch ev.Type {
		case "create.issue":
			rep.ScaleUps++
		case "drain.begin":
			if contains(ev.Message, "scale-down") {
				rep.ScaleDowns++
			}
		case "node.lost":
			rep.Lost++
		case "session.migrate", "session.reschedule":
			rep.Migrations++
		}
	}
	snap := r.ctrl.Snapshot()
	for _, in := range snap.Instances {
		if in.State != fleet.StateStopped && in.State != fleet.StateFailed && in.State != fleet.StateLost {
			rep.FinalCount++
		}
	}
	return rep
}

func contains(s, sub string) bool {
	return len(sub) > 0 && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// Report summarizes a replay for assertions and documentation.
type Report struct {
	Name       string
	ScaleUps   int
	ScaleDowns int
	Lost       int
	Migrations int
	FinalCount int
	Events     []string
}

// EventRecords returns the durable, tick-stamped audit events for assertions.
func (r *Runner) EventRecords() []*fleet.Event { return r.ctrl.State().Events }
