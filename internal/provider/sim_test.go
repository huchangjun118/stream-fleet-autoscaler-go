package provider

import (
	"context"
	"errors"
	"testing"
)

func testSim() *Sim {
	return NewSim(
		map[string]int{"s": 2},
		map[string]simRegion{"east": {slowTicks: 1}},
	)
}

func TestCreateIdempotentAndLifecycle(t *testing.T) {
	s := testSim()
	ctx := context.Background()
	in, err := s.CreateInstance(ctx, CreateRequest{InstanceID: "i1", Region: "east", Spec: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if in.State != "Pending" {
		t.Fatalf("expected Pending, got %s", in.State)
	}
	// Re-issuing the same id must not create a duplicate.
	in2, err := s.CreateInstance(ctx, CreateRequest{InstanceID: "i1", Region: "east", Spec: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if in2.Generation != in.Generation {
		t.Fatalf("duplicate create issued: generations differ")
	}
	if got := len(s.instances); got != 1 {
		t.Fatalf("expected 1 instance, got %d", got)
	}
	s.Tick()
	got, _ := s.DescribeInstances(ctx)
	if got["i1"].State != "Running" {
		t.Fatalf("expected Running after tick, got %s", got["i1"].State)
	}
}

func TestScriptedCreateFailure(t *testing.T) {
	s := NewSim(map[string]int{"s": 2}, map[string]simRegion{"bad": {createFail: true}})
	_, err := s.CreateInstance(context.Background(),
		CreateRequest{InstanceID: "x", Region: "bad", Spec: "s"})
	if !errors.Is(err, ErrCreateFailed) {
		t.Fatalf("expected ErrCreateFailed, got %v", err)
	}
}

func TestPlacementRespectsStateAndCapacity(t *testing.T) {
	s := testSim()
	ctx := context.Background()
	_, _ = s.CreateInstance(ctx, CreateRequest{InstanceID: "i1", Region: "east", Spec: "s"})
	// Pending: cannot place.
	if err := s.PlaceSession(ctx, PlaceSessionRequest{SessionID: "a", InstanceID: "i1"}); err == nil {
		t.Fatal("placement on Pending must fail")
	}
	s.Tick()
	// Fill capacity 2.
	if err := s.PlaceSession(ctx, PlaceSessionRequest{SessionID: "a", InstanceID: "i1", Region: "east"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PlaceSession(ctx, PlaceSessionRequest{SessionID: "b", InstanceID: "i1", Region: "east"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PlaceSession(ctx, PlaceSessionRequest{SessionID: "c", InstanceID: "i1", Region: "east"}); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("full instance must reject, got %v", err)
	}
	if s.SessionCount("i1") != 2 {
		t.Fatalf("expected load 2, got %d", s.SessionCount("i1"))
	}
	// Draining rejects new sessions.
	_ = s.StartDrain(ctx, "i1")
	if err := s.PlaceSession(ctx, PlaceSessionRequest{SessionID: "z", InstanceID: "i1"}); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("draining instance must reject, got %v", err)
	}
}

func TestRebindAtomicAndLossOrphans(t *testing.T) {
	s := NewSim(map[string]int{"s": 5}, map[string]simRegion{"east": {}})
	ctx := context.Background()
	_, _ = s.CreateInstance(ctx, CreateRequest{InstanceID: "i1", Region: "east", Spec: "s"})
	_, _ = s.CreateInstance(ctx, CreateRequest{InstanceID: "i2", Region: "east", Spec: "s"})
	s.Tick()
	if err := s.PlaceSession(ctx, PlaceSessionRequest{SessionID: "a", InstanceID: "i1", Region: "east"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Rebind(ctx, "a", "i2"); err != nil {
		t.Fatal(err)
	}
	if s.SessionCount("i1") != 0 || s.SessionCount("i2") != 1 {
		t.Fatalf("atomic rebind did not move session: i1=%d i2=%d", s.SessionCount("i1"), s.SessionCount("i2"))
	}
	// Losing the node detaches its sessions exactly once.
	orphaned := s.LoseInstance("i2")
	if len(orphaned) != 1 || orphaned[0] != "a" {
		t.Fatalf("expected orphan [a], got %v", orphaned)
	}
	sess, _ := s.DescribeSessions(ctx)
	if sess["a"].InstanceID != "" {
		t.Fatalf("orphan session must be detached, still on %s", sess["a"].InstanceID)
	}
}
