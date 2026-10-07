package drain

import (
	"testing"
	"time"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

func newSnap() *fleet.Snapshot {
	return &fleet.Snapshot{
		Instances: map[string]*fleet.Instance{
			"a": {ID: "a", Region: "east", Capacity: 3, State: fleet.StateRunning, CreatedAt: time.Unix(1, 0)},
			"b": {ID: "b", Region: "east", Capacity: 3, State: fleet.StateRunning, CreatedAt: time.Unix(2, 0)},
			"c": {ID: "c", Region: "east", Capacity: 3, State: fleet.StateRunning, CreatedAt: time.Unix(3, 0)},
		},
		Sessions: map[string]*fleet.Session{
			"s1": {ID: "s1", InstanceID: "c", State: fleet.SessionActive, Region: "east"},
			"s2": {ID: "s2", InstanceID: "c", State: fleet.SessionActive, Region: "east"},
		},
	}
}

func TestPickIdleOldestFirst(t *testing.T) {
	m := NewManager(nil, nil)
	sn := newSnap()
	load := map[string]int{"a": 0, "b": 0, "c": 2}
	got := m.PickIdle(sn, load, 5)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected idle [a b] oldest-first, got %v", got)
	}
}

func TestMigratableFeasibility(t *testing.T) {
	m := NewManager(nil, nil)
	sn := newSnap()
	// c holds 2; a+b have 6 free slots -> migratable.
	if !m.Migratable(sn, map[string]int{"a": 0, "b": 0, "c": 2}, "c", nil) {
		t.Fatal("c should be migratable onto a,b")
	}
	// Exclude both peers -> nowhere to go.
	if m.Migratable(sn, map[string]int{"a": 0, "b": 0, "c": 2}, "c",
		map[string]bool{"a": true, "b": true}) {
		t.Fatal("c must not be migratable when peers excluded")
	}
	// Peers full -> not migratable.
	if m.Migratable(sn, map[string]int{"a": 3, "b": 3, "c": 2}, "c", nil) {
		t.Fatal("c must not be migratable when peers are full")
	}
}
