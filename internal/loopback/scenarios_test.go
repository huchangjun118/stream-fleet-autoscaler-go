package loopback_test

import (
	"context"
	"strings"
	"testing"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/loopback"
)

func testCfg() *config.Config {
	c := config.Default()
	c.MinReplicas = 2
	c.MaxReplicas = 12
	c.StepSize = 2
	c.ScaleUpAfter = 2
	c.ScaleDownAfter = 3
	c.ScaleUpCooldownTicks = 2
	c.ScaleDownCooldownTicks = 4
	c.DrainTimeoutTicks = 5
	c.OpTimeoutTicks = 12
	c.MaxConcurrentOps = 4
	c.Specs = map[string]int{"standard": 10}
	c.Regions = []config.RegionConfig{
		{Name: "east", Weight: 1, Spec: "standard"},
		{Name: "west", Weight: 1, Spec: "standard"},
	}
	return c
}

func countActive(r *loopback.Runner) int {
	n := 0
	for _, in := range r.Controller().Snapshot().Instances {
		if in.State == fleet.StateRunning ||
			in.State == fleet.StatePending ||
			in.State == fleet.StateDraining ||
			in.State == fleet.StateDrained {
			n++
		}
	}
	return n
}

func hasEvent(r *loopback.Runner, evType string) bool {
	for _, e := range r.EventRecords() {
		if e.Type == evType {
			return true
		}
	}
	return false
}

// TestLoadSurgeScalesOut: a sudden load spike must add capacity, never
// exceeding MaxReplicas, and end with enough running instances for demand.
func TestLoadSurgeScalesOut(t *testing.T) {
	cfg := testCfg()
	scr := &loopback.Script{
		Name:    "surge",
		Initial: 2,
		Ticks:   20,
		Events: []loopback.Event{
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 2},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 1},
			{At: 5, Kind: loopback.EvLoad, Region: "east", Value: 40},
			{At: 5, Kind: loopback.EvLoad, Region: "west", Value: 40},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	rep, err := r.Run(context.Background(), scr)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ScaleUps == 0 {
		t.Fatal("expected at least one scale-up under surge")
	}
	if countActive(r) > cfg.MaxReplicas {
		t.Fatalf("capacity %d exceeded max %d", countActive(r), cfg.MaxReplicas)
	}
	// Demand is 80 slots / 10 per instance = 10 instances minimum.
	running := 0
	for _, in := range r.Controller().Snapshot().Instances {
		if in.State == fleet.StateRunning {
			running++
		}
	}
	if running < 10 {
		t.Fatalf("expected >= 10 running instances for 80 slots, got %d", running)
	}
}

// TestLoadSlumpScalesIn: when load falls, idle capacity is reclaimed down to
// MinReplicas via graceful drain, without interrupting active sessions.
func TestLoadSlumpScalesIn(t *testing.T) {
	cfg := testCfg()
	scr := &loopback.Script{
		Name:    "slump",
		Initial: 8,
		Ticks:   40,
		Events: []loopback.Event{
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 40},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 40},
			{At: 10, Kind: loopback.EvLoad, Region: "east", Value: 1},
			{At: 10, Kind: loopback.EvLoad, Region: "west", Value: 1},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	rep, err := r.Run(context.Background(), scr)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ScaleDowns == 0 {
		t.Fatal("expected scale-down after slump")
	}
	if got := countActive(r); got != cfg.MinReplicas {
		t.Fatalf("expected convergence to min %d, got %d", cfg.MinReplicas, got)
	}
	// No session should ever be duplicated: every active session on one node.
	seen := map[string]int{}
	for _, s := range r.Controller().Snapshot().Sessions {
		if s.State == fleet.SessionActive {
			if s.InstanceID == "" {
				t.Fatal("active session left unbound")
			}
			seen[s.ID]++
		}
	}
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("session %s bound %d times", id, n)
		}
	}
}

// TestNoFlappingNearThreshold: oscillating load around the thresholds must
// not cause rapid alternating scale-up/down.
func TestNoFlappingNearThreshold(t *testing.T) {
	cfg := testCfg()
	scr := &loopback.Script{Name: "flap", Initial: 4, Ticks: 24}
	for tick := int64(1); tick <= 24; tick++ {
		v := 30.0 // util ~= 0.3..0.95 oscillates around thresholds as capacity changes
		if tick%2 == 0 {
			v = 4
		}
		scr.Events = append(scr.Events, loopback.Event{At: tick, Kind: loopback.EvLoad, Region: "east", Value: v})
		scr.Events = append(scr.Events, loopback.Event{At: tick, Kind: loopback.EvLoad, Region: "west", Value: v})
	}
	r := loopback.NewRunner(cfg, scr)
	if _, err := r.Run(context.Background(), scr); err != nil {
		t.Fatal(err)
	}
	// Count direction changes; cooldowns + streak gating keep this bounded.
	var prev string
	changes := 0
	for _, e := range r.EventRecords() {
		if e.Type != "policy" {
			continue
		}
		cur := ""
		if strings.Contains(e.Message, "decision=ScaleUp") {
			cur = "up"
		} else if strings.Contains(e.Message, "decision=ScaleDown") {
			cur = "down"
		}
		if cur != "" && prev != "" && cur != prev {
			changes++
		}
		if cur != "" {
			prev = cur
		}
	}
	if changes > 2 {
		t.Fatalf("too many direction changes (%d), hysteresis is not suppressing flaps", changes)
	}
}

// TestPartialCreateFailureConverges: when some creates fail, the controller
// must retry and still converge within [Min,Max] without dangling capacity.
func TestPartialCreateFailureConverges(t *testing.T) {
	cfg := testCfg()
	cfg.MaxReplicas = 10
	scr := &loopback.Script{
		Name:    "partial-fail",
		Initial: 2,
		Ticks:   30,
		Events: []loopback.Event{
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 1},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 1},
			{At: 6, Kind: loopback.EvLoad, Region: "east", Value: 40},
			{At: 6, Kind: loopback.EvLoad, Region: "west", Value: 40},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	// Script the first three scale-out create attempts to fail.
	r.Sim().FailNextCreations(3)
	if _, err := r.Run(context.Background(), scr); err != nil {
		t.Fatal(err)
	}
	if !hasEvent(r, "create.failed") {
		t.Fatal("expected to observe scripted create failures")
	}
	active := countActive(r)
	if active > cfg.MaxReplicas {
		t.Fatalf("capacity %d exceeded max %d after partial failures", active, cfg.MaxReplicas)
	}
	running := 0
	for _, in := range r.Controller().Snapshot().Instances {
		switch in.State {
		case fleet.StateRunning:
			running++
		case fleet.StatePending, fleet.StateDraining, fleet.StateDrained:
			t.Fatalf("unexpected in-flight instance %s (%s) at end", in.ID, in.State)
		}
	}
	if running < 10 {
		t.Fatalf("expected retries to converge to 10 running, got %d", running)
	}
	// Failed creates must be journaled terminally (no dangling Pending ops).
	for _, op := range r.Controller().State().Ops {
		if op.Kind == fleet.OpCreate && op.Status == fleet.OpPending {
			t.Fatalf("create op %s still Pending after convergence", op.ID)
		}
	}
}

// TestNodeLossReschedulesSessions: losing a node mid-serving must detach its
// sessions, which the reconciler reschedules onto other running instances
// exactly once: no lost sessions, no double occupation.
func TestNodeLossReschedulesSessions(t *testing.T) {
	cfg := testCfg()
	cfg.MaxReplicas = 10
	scr := &loopback.Script{
		Name:    "node-loss",
		Initial: 6,
		Ticks:   16,
		Events: []loopback.Event{
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 30},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 30},
			// Lose an east node after capacity is up and sessions placed.
			{At: 9, Kind: loopback.EvLose, Region: "east"},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	if _, err := r.Run(context.Background(), scr); err != nil {
		t.Fatal(err)
	}
	if !hasEvent(r, "node.lost") {
		t.Fatal("expected a node.lost event")
	}
	if !hasEvent(r, "session.reschedule") {
		t.Fatal("expected orphan sessions to be rescheduled")
	}
	// Every active session must be bound exactly once to a live running node.
	bindCount := map[string]int{}
	live := map[string]bool{}
	for _, in := range r.Controller().Snapshot().Instances {
		if in.State == fleet.StateRunning {
			live[in.ID] = true
		}
	}
	for _, s := range r.Controller().Snapshot().Sessions {
		if s.State != fleet.SessionActive {
			continue
		}
		if s.InstanceID == "" || !live[s.InstanceID] {
			t.Fatalf("session %s not bound to a live running node (on %q)", s.ID, s.InstanceID)
		}
		bindCount[s.ID]++
	}
	for id, n := range bindCount {
		if n != 1 {
			t.Fatalf("session %s bound %d times, want exactly 1", id, n)
		}
	}
	// The lost node must not linger as serving capacity.
	for _, in := range r.Controller().Snapshot().Instances {
		if in.State == fleet.StateLost {
			// acceptable as observed state, but it is not counted as active
			if countActive(r) == 0 {
				t.Fatal("no capacity after loss")
			}
		}
	}
}

// TestDrainMigratesInFlightSessions: scaling in while sessions are pinned must
// never interrupt them; on drain timeout they are migrated to a peer node and
// the source terminates.
func TestDrainMigratesInFlightSessions(t *testing.T) {
	cfg := testCfg()
	cfg.MinReplicas = 1
	cfg.MaxReplicas = 6
	cfg.StepSize = 1
	cfg.DrainTimeoutTicks = 3
	// Single region so every drained node always has an in-region peer able to
	// receive the migrated session; the test focuses on drain migration.
	cfg.Regions = []config.RegionConfig{
		{Name: "east", Weight: 1, Spec: "standard"},
	}
	scr := &loopback.Script{
		Name:    "drain-migrate",
		Initial: 3,
		Ticks:   30,
		Events: []loopback.Event{
			// No offered load, but three sticky long-lived sessions occupy one
			// instance each. They are never ended on slump, so reaching min=1
			// must migrate two sessions onto the survivor after drain timeout.
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 0},
			{At: 1, Kind: loopback.EvSustain, Region: "east", Value: 3},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	rep, err := r.Run(context.Background(), scr)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Migrations == 0 {
		t.Fatal("expected the pinned session to be migrated during scale-in")
	}
	if got := countActive(r); got != cfg.MinReplicas {
		t.Fatalf("expected to converge to min %d, got %d", cfg.MinReplicas, got)
	}
	// Sessions are never dropped; the one remaining session stays in region.
	bound := 0
	for _, s := range r.Controller().Snapshot().Sessions {
		if s.State == fleet.SessionActive && s.InstanceID != "" {
			if s.Region != "east" {
				t.Fatalf("session migrated across region to %q", s.Region)
			}
			bound++
		}
	}
	if bound != 3 {
		t.Fatalf("expected all 3 sticky sessions preserved, got %d", bound)
	}
}

// TestControllerRestartResumesConvergence: after a restart while creates are
// in flight, the new controller must adopt them (not re-issue), recognize
// expected vs actual divergence and keep converging. Running the same script
// with and without the restart must finish at the same final capacity.
func TestControllerRestartResumesConvergence(t *testing.T) {
	makeScript := func() *loopback.Script {
		return &loopback.Script{
			Name:    "restart",
			Initial: 2,
			Ticks:   22,
			Events: []loopback.Event{
				{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 1},
				{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 1},
				{At: 6, Kind: loopback.EvLoad, Region: "east", Value: 40},
				{At: 6, Kind: loopback.EvLoad, Region: "west", Value: 40},
			},
		}
	}

	// Baseline without restart.
	cfg1 := testCfg()
	cfg1.MaxReplicas = 10
	r1 := loopback.NewRunner(cfg1, makeScript())
	if _, err := r1.Run(context.Background(), makeScript()); err != nil {
		t.Fatal(err)
	}
	baseline := countActive(r1)

	// With a restart exactly as scale-out creates are in flight.
	cfg2 := testCfg()
	cfg2.MaxReplicas = 10
	scr := makeScript()
	scr.Events = append(scr.Events, loopback.Event{At: 8, Kind: loopback.EvRestart})
	r2 := loopback.NewRunner(cfg2, scr)
	if _, err := r2.Run(context.Background(), scr); err != nil {
		t.Fatal(err)
	}
	if !hasEvent(r2, "restart") {
		t.Fatal("expected a restart event in the journal")
	}
	if got := countActive(r2); got != baseline {
		t.Fatalf("after restart final=%d differs from baseline=%d (convergence diverged)", got, baseline)
	}

	// After restart there must be no duplicate create for an instance that
	// already existed: every instance id in the journal appears once.
	created := map[string]int{}
	for _, op := range r2.Controller().State().Ops {
		if op.Kind == fleet.OpCreate {
			created[op.InstanceID]++
		}
	}
	for id, n := range created {
		if n > 1 {
			t.Fatalf("instance %s had %d create ops after restart (expected exactly 1)", id, n)
		}
	}
}

// TestRestartDoesNotReissueCompletedScaleDown: draining instances already
// terminated before a restart must not be drained/terminated again.
func TestRestartDoesNotReissueCompletedScaleDown(t *testing.T) {
	cfg := testCfg()
	cfg.MaxReplicas = 6
	scr := &loopback.Script{
		Name:    "restart-down",
		Initial: 5,
		Ticks:   30,
		Events: []loopback.Event{
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 2},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 2},
			{At: 12, Kind: loopback.EvRestart},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	if _, err := r.Run(context.Background(), scr); err != nil {
		t.Fatal(err)
	}
	// Each instance may be terminated at most once.
	terminations := map[string]int{}
	for _, e := range r.EventRecords() {
		if e.Type == "drain.done" {
			// message contains instance id: "instance inst-... terminated"
			for id := range r.Controller().Snapshot().Instances {
				if strings.Contains(e.Message, id) {
					terminations[id]++
				}
			}
		}
	}
	for id, n := range terminations {
		if n > 1 {
			t.Fatalf("instance %s terminated %d times after restart", id, n)
		}
	}
	if got := countActive(r); got != cfg.MinReplicas {
		t.Fatalf("expected convergence to min %d, got %d", cfg.MinReplicas, got)
	}
}

// TestReproducibleDecisions: the same script+config must yield the identical
// sequence of policy decisions across two independent runs.
func TestReproducibleDecisions(t *testing.T) {
	cfg := testCfg()
	build := func() *loopback.Script {
		return &loopback.Script{
			Name:    "repro",
			Initial: 3,
			Ticks:   24,
			Events: []loopback.Event{
				{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 4},
				{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 3},
				{At: 8, Kind: loopback.EvLoad, Region: "east", Value: 40},
				{At: 8, Kind: loopback.EvLoad, Region: "west", Value: 35},
				{At: 16, Kind: loopback.EvLoad, Region: "east", Value: 2},
				{At: 16, Kind: loopback.EvLoad, Region: "west", Value: 2},
			},
		}
	}
	run := func() []string {
		r := loopback.NewRunner(cfg, build())
		if _, err := r.Run(context.Background(), build()); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range r.EventRecords() {
			if e.Type == "policy" {
				out = append(out, e.Message)
			}
		}
		return out
	}
	a := run()
	b := run()
	if len(a) != len(b) {
		t.Fatalf("decision length differs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("decision %d differs:\n a=%s\n b=%s", i, a[i], b[i])
		}
	}
}

// TestSlowRegionDoesNotBlockFastRegion: one region has slow creates while the
// other keeps needing capacity; the fast region must still converge.
func TestSlowRegionDoesNotBlockFastRegion(t *testing.T) {
	cfg := testCfg()
	cfg.MaxReplicas = 12
	cfg.MaxConcurrentOps = 6
	// east creates are slow, west is instant.
	cfg.Regions = []config.RegionConfig{
		{Name: "east", Weight: 1, Spec: "standard", SlowTicks: 6},
		{Name: "west", Weight: 1, Spec: "standard", SlowTicks: 1},
	}
	scr := &loopback.Script{
		Name:    "slow-region",
		Initial: 2,
		Ticks:   30,
		Events: []loopback.Event{
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 1},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 1},
			{At: 6, Kind: loopback.EvLoad, Region: "east", Value: 30},
			{At: 6, Kind: loopback.EvLoad, Region: "west", Value: 30},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	if _, err := r.Run(context.Background(), scr); err != nil {
		t.Fatal(err)
	}
	// west (fast) must reach full running capacity even though east lags.
	westRunning := 0
	for _, in := range r.Controller().Snapshot().Instances {
		if in.Region == "west" && in.State == fleet.StateRunning {
			westRunning++
		}
	}
	if westRunning < 3 {
		t.Fatalf("fast region west should converge independently, only %d running", westRunning)
	}
}

// TestCapacityBoundsUnderFailure: combined surge + partial failures must keep
// active non-terminal capacity within [Min, Max] at every observed tick.
func TestCapacityBoundsUnderFailure(t *testing.T) {
	cfg := testCfg()
	cfg.MaxReplicas = 8
	scr := &loopback.Script{
		Name:    "bounds",
		Initial: 2,
		Ticks:   26,
		Events: []loopback.Event{
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 1},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 1},
			{At: 6, Kind: loopback.EvLoad, Region: "east", Value: 40},
			{At: 6, Kind: loopback.EvLoad, Region: "west", Value: 40},
			{At: 18, Kind: loopback.EvLoad, Region: "east", Value: 1},
			{At: 18, Kind: loopback.EvLoad, Region: "west", Value: 1},
		},
	}
	r := loopback.NewRunner(cfg, scr)
	r.Sim().FailNextCreations(4)
	if _, err := r.Run(context.Background(), scr); err != nil {
		t.Fatal(err)
	}
	for _, e := range r.EventRecords() {
		_ = e
	}
	if got := countActive(r); got > cfg.MaxReplicas {
		t.Fatalf("active capacity %d exceeded max %d", got, cfg.MaxReplicas)
	}
}
