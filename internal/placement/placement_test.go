package placement

import (
	"testing"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

func cfg() *config.Config {
	c := config.Default()
	c.MinReplicas = 0
	c.MaxReplicas = 6
	c.Specs = map[string]int{"s": 4}
	c.Regions = []config.RegionConfig{
		{Name: "east", Weight: 2, Spec: "s"},
		{Name: "west", Weight: 1, Spec: "s"},
	}
	return c
}

func TestPlanAddWeightedEven(t *testing.T) {
	p := NewPlanner(cfg())
	sn := &fleet.Snapshot{Instances: map[string]*fleet.Instance{}}
	allocs := p.PlanAdd(sn, 3)
	// Weighted target 2:1 -> east,east,west (first slot tie broken by name).
	counts := map[string]int{}
	for _, a := range allocs {
		counts[a.Region]++
	}
	if counts["east"] != 2 || counts["west"] != 1 {
		t.Fatalf("expected 2 east / 1 west, got %v", counts)
	}
}

func TestPlanAddAccountsForInFlightAndMax(t *testing.T) {
	p := NewPlanner(cfg())
	sn := &fleet.Snapshot{Instances: map[string]*fleet.Instance{
		"e1": {ID: "e1", Region: "east", State: fleet.StateRunning},
		"e2": {ID: "e2", Region: "east", State: fleet.StateRunning},
		"w1": {ID: "w1", Region: "west", State: fleet.StatePending},
		"w2": {ID: "w2", Region: "west", State: fleet.StateRunning},
		"e3": {ID: "e3", Region: "east", State: fleet.StateStopped}, // terminal ignored
	}}
	allocs := p.PlanAdd(sn, 5) // only 2 slots remain up to max 6
	if len(allocs) != 2 {
		t.Fatalf("expected clamp to 2 (max 6 - 4 active), got %d", len(allocs))
	}
	counts := map[string]int{}
	for _, a := range allocs {
		counts[a.Region]++
	}
	if counts["east"] != 2 {
		t.Fatalf("remaining weight deficit is all in east, got %v", counts)
	}
}
