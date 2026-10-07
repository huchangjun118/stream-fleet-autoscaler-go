package reconcile

import (
	"context"
	"testing"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/provider"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/scheduler"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/store"
)

func setup(t *testing.T) (*Reconciler, *provider.Sim) {
	t.Helper()
	cfg := config.Default()
	sim := provider.NewSimFromConfig(cfg)
	sch := scheduler.New()
	return NewReconciler(cfg, sim, sch), sim
}

func TestAdoptInFlightCreates(t *testing.T) {
	r, sim := setup(t)
	ctx := context.Background()
	_, _ = sim.CreateInstance(ctx, provider.CreateRequest{InstanceID: "i1", Region: "east", Spec: "standard"})
	sim.Tick() // becomes Running
	st := store.NewState()
	st.DesiredReplicas = 1
	st.Ops = []*fleet.OpRecord{{
		ID: "op-1", Kind: fleet.OpCreate, InstanceID: "i1", Status: fleet.OpPending,
	}}
	snap, _ := r.Snapshot(ctx)
	adopted := r.AdoptCreates(ctx, st, snap)
	if len(adopted) != 1 || adopted[0] != "i1" {
		t.Fatalf("expected adoption of i1, got %v", adopted)
	}
	if st.Ops[0].Status != fleet.OpSucceeded {
		t.Fatalf("expected op succeeded, got %s", st.Ops[0].Status)
	}
}

func TestRecoverLostAndRescheduleOrphans(t *testing.T) {
	r, sim := setup(t)
	ctx := context.Background()
	// Two running nodes; place a session, lose its node; a healthy peer exists.
	_, _ = sim.CreateInstance(ctx, provider.CreateRequest{InstanceID: "i1", Region: "east", Spec: "standard"})
	_, _ = sim.CreateInstance(ctx, provider.CreateRequest{InstanceID: "i2", Region: "east", Spec: "standard"})
	sim.Tick()
	if err := sim.PlaceSession(ctx, provider.PlaceSessionRequest{
		SessionID: "s1", InstanceID: "i1", Region: "east"}); err != nil {
		t.Fatal(err)
	}
	sim.LoseInstance("i1")

	st := store.NewState()
	st.Ops = []*fleet.OpRecord{{
		ID: "op-1", Kind: fleet.OpCreate, InstanceID: "i1", Status: fleet.OpPending,
	}}
	snap, _ := r.Snapshot(ctx)

	lost := r.RecoverLost(ctx, st, snap)
	if len(lost) != 1 || lost[0] != "i1" {
		t.Fatalf("expected [i1] lost, got %v", lost)
	}
	if st.Ops[0].Status != fleet.OpFailed {
		t.Fatalf("lost node's pending op must fail, got %s", st.Ops[0].Status)
	}
	moved, err := r.RescheduleOrphans(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(moved) != 1 || moved[0] != "s1" {
		t.Fatalf("expected s1 rescheduled, got %v", moved)
	}
	// Session must land on the surviving node exactly once.
	sess, _ := sim.DescribeSessions(ctx)
	if sess["s1"].InstanceID != "i2" {
		t.Fatalf("expected s1 on i2, got %q", sess["s1"].InstanceID)
	}
}

func TestOrphanWithNoCapacityFailsSafely(t *testing.T) {
	r, sim := setup(t)
	ctx := context.Background()
	_, _ = sim.CreateInstance(ctx, provider.CreateRequest{InstanceID: "i1", Region: "east", Spec: "standard"})
	sim.Tick()
	_ = sim.PlaceSession(ctx, provider.PlaceSessionRequest{SessionID: "s1", InstanceID: "i1", Region: "east"})
	sim.LoseInstance("i1")
	snap, _ := r.Snapshot(ctx)
	r.RecoverLost(ctx, store.NewState(), snap)
	// No surviving instance -> reschedule reports error but keeps the orphan
	// recorded, so a later tick after scale-out can retry without losing it.
	if _, err := r.RescheduleOrphans(ctx, snap); err == nil {
		t.Fatal("expected reschedule to fail when no capacity exists")
	}
	sess, _ := sim.DescribeSessions(ctx)
	if _, ok := sess["s1"]; !ok {
		t.Fatal("orphan session must be retained for retry, not dropped")
	}
}
