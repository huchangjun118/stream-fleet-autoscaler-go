package rebalance

import (
	"testing"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

func cfg() *config.Config {
	c := config.Default()
	c.MinReplicas = 0
	c.MaxReplicas = 10
	c.RebalanceThreshold = 0.2
	c.RebalanceCooldown = 5
	c.Specs = map[string]int{"s": 2}
	c.Regions = []config.RegionConfig{
		{Name: "east", Weight: 1, Spec: "s"},
		{Name: "west", Weight: 1, Spec: "s"},
	}
	return c
}

func inst(id, region string, state fleet.InstanceState) *fleet.Instance {
	return &fleet.Instance{ID: id, Region: region, State: state, Capacity: 2}
}

func TestStableFleetNoMoves(t *testing.T) {
	p := NewPlanner(cfg())
	sn := &fleet.Snapshot{Instances: map[string]*fleet.Instance{
		"e1": inst("e1", "east", fleet.StateRunning),
		"e2": inst("e2", "east", fleet.StateRunning),
		"w1": inst("w1", "west", fleet.StateRunning),
		"w2": inst("w2", "west", fleet.StateRunning),
	}}
	if moves := p.Evaluate(sn, map[string]int64{}, 10); len(moves) != 0 {
		t.Fatalf("balanced fleet must not move, got %v", moves)
	}
}

func TestImbalancedMovesOnceAndCooldown(t *testing.T) {
	p := NewPlanner(cfg())
	sn := &fleet.Snapshot{Instances: map[string]*fleet.Instance{
		"e1": inst("e1", "east", fleet.StateRunning),
		"e2": inst("e2", "east", fleet.StateRunning),
		"e3": inst("e3", "east", fleet.StateRunning),
		"w1": inst("w1", "west", fleet.StateRunning),
	}}
	last := map[string]int64{}
	moves := p.Evaluate(sn, last, 10)
	if len(moves) != 1 || moves[0].FromRegion != "east" || moves[0].ToRegion != "west" {
		t.Fatalf("expected one east->west move, got %v", moves)
	}
	// Immediately re-evaluating with an updated cooldown must hold.
	last["east"], last["west"] = 10, 10
	if moves := p.Evaluate(sn, last, 11); len(moves) != 0 {
		t.Fatalf("cooldown must suppress repeated moves, got %v", moves)
	}
}

func TestUnstableFleetBlocksRebalance(t *testing.T) {
	p := NewPlanner(cfg())
	sn := &fleet.Snapshot{Instances: map[string]*fleet.Instance{
		"e1": inst("e1", "east", fleet.StateRunning),
		"e2": inst("e2", "east", fleet.StateRunning),
		"e3": inst("e3", "east", fleet.StatePending), // create in flight
		"w1": inst("w1", "west", fleet.StateRunning),
	}}
	if moves := p.Evaluate(sn, map[string]int64{}, 10); len(moves) != 0 {
		t.Fatalf("mid-convergence fleet must not rebalance, got %v", moves)
	}
}

func TestNeverEmptyARegion(t *testing.T) {
	p := NewPlanner(cfg())
	sn := &fleet.Snapshot{Instances: map[string]*fleet.Instance{
		"e1": inst("e1", "east", fleet.StateRunning),
	}}
	// west has zero; the only east node carries 100% but moving it would empty
	// the sole serving region -> forbidden (source must keep >= 1 after).
	if moves := p.Evaluate(sn, map[string]int64{}, 10); len(moves) != 0 {
		t.Fatalf("must not drain the last instance of the only region, got %v", moves)
	}
}
