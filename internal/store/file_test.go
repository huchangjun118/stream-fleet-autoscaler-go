package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	fs := NewFileStore(path)
	ctx := context.Background()

	// Empty file => nil state (fresh start).
	st, err := fs.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st != nil {
		t.Fatal("expected nil state for absent file")
	}

	st = NewState()
	st.Tick = 7
	st.DesiredReplicas = 5
	st.Ops = []*fleet.OpRecord{{ID: "op-1", Kind: fleet.OpCreate, InstanceID: "i1", Status: fleet.OpPending}}
	if err := fs.AppendEvent(ctx, st, 7, "policy", "scale-up"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Save(ctx, st); err != nil {
		t.Fatal(err)
	}

	// Simulate restart: a brand new FileStore reads the same state.
	fs2 := NewFileStore(path)
	got, err := fs2.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Tick != 7 || got.DesiredReplicas != 5 {
		t.Fatalf("state mismatch: %+v", got)
	}
	if len(got.Ops) != 1 || got.Ops[0].InstanceID != "i1" {
		t.Fatalf("op journal lost: %+v", got.Ops)
	}
	if len(got.Events) != 1 || got.Events[0].Tick != 7 {
		t.Fatalf("event journal lost: %+v", got.Events)
	}
}
