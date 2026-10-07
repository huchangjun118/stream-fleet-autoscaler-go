package scheduler

import (
	"testing"
	"time"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

func snap() *fleet.Snapshot {
	return &fleet.Snapshot{
		Instances: map[string]*fleet.Instance{
			"a": {ID: "a", Region: "east", Capacity: 3, State: fleet.StateRunning, CreatedAt: time.Unix(1, 0)},
			"b": {ID: "b", Region: "east", Capacity: 3, State: fleet.StateRunning, CreatedAt: time.Unix(2, 0)},
			"c": {ID: "c", Region: "east", Capacity: 3, State: fleet.StateDraining, CreatedAt: time.Unix(3, 0)},
			"w": {ID: "w", Region: "west", Capacity: 3, State: fleet.StateRunning, CreatedAt: time.Unix(4, 0)},
		},
		Sessions: map[string]*fleet.Session{
			"s1": {ID: "s1", InstanceID: "b", State: fleet.SessionActive},
		},
	}
}

func TestSelectEvenDistributionAndDeterministic(t *testing.T) {
	s := New()
	got := s.Select(snap(), "east", s.LoadMap(snap()))
	if got != "a" {
		t.Fatalf("expected least-loaded running node 'a', got %q", got)
	}
	// Repeat => same answer.
	for i := 0; i < 10; i++ {
		if s.Select(snap(), "east", s.LoadMap(snap())) != "a" {
			t.Fatal("selection not deterministic")
		}
	}
}

func TestSelectExcludesDrainingAndRegion(t *testing.T) {
	s := New()
	if got := s.Select(snap(), "west", map[string]int{}); got != "w" {
		t.Fatalf("expected region-scoped 'w', got %q", got)
	}
	if got := s.SelectForMigration(snap(), "east", map[string]int{}, "a"); got != "b" {
		t.Fatalf("expected 'b' after excluding a and draining c, got %q", got)
	}
}

func TestSelectFullReturnsEmpty(t *testing.T) {
	s := New()
	sn := snap()
	load := map[string]int{"a": 3, "b": 3, "w": 0}
	if got := s.Select(sn, "east", load); got != "" {
		t.Fatalf("full region must yield no target, got %q", got)
	}
}
