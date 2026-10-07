// Package drain implements safe scale-in: selecting only idle instances with
// no in-flight sessions, marking them to stop taking new sessions, and
// waiting for graceful drain before termination. Ongoing sessions are never
// interrupted; sessions of a lost node are migrated atomically elsewhere.
package drain

import (
	"context"
	"sort"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/provider"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/scheduler"
)

// Manager performs drain selection and session migration.
type Manager struct {
	prov      provider.Provider
	scheduler *scheduler.Scheduler
}

// NewManager creates a Manager.
func NewManager(prov provider.Provider, sch *scheduler.Scheduler) *Manager {
	return &Manager{prov: prov, scheduler: sch}
}

// PickIdle returns up to want running instance IDs that hold zero active
// sessions, oldest first, so idle nodes are reclaimed first.
func (m *Manager) PickIdle(snap *fleet.Snapshot, load map[string]int, want int) []string {
	if want <= 0 {
		return nil
	}
	var idle []*fleet.Instance
	for _, in := range snap.Instances {
		if in.State == fleet.StateRunning && load[in.ID] == 0 {
			idle = append(idle, in)
		}
	}
	sort.Slice(idle, func(i, j int) bool {
		if !idle[i].CreatedAt.Equal(idle[j].CreatedAt) {
			return idle[i].CreatedAt.Before(idle[j].CreatedAt)
		}
		return idle[i].ID < idle[j].ID
	})
	if len(idle) > want {
		idle = idle[:want]
	}
	out := make([]string, len(idle))
	for i, in := range idle {
		out[i] = in.ID
	}
	return out
}

// PickBusy returns up to want running instance IDs that still hold active
// sessions, ordered oldest/least-loaded first. These candidates are drained
// gracefully (new sessions rejected, existing ones allowed to finish) and are
// only terminated after natural drain or a post-timeout atomic migration.
func (m *Manager) PickBusy(snap *fleet.Snapshot, load map[string]int, want int, exclude map[string]bool) []string {
	if want <= 0 {
		return nil
	}
	var busy []*fleet.Instance
	for _, in := range snap.Instances {
		if in.State == fleet.StateRunning && load[in.ID] > 0 && !exclude[in.ID] {
			busy = append(busy, in)
		}
	}
	sort.Slice(busy, func(i, j int) bool {
		li, lj := load[busy[i].ID], load[busy[j].ID]
		if li != lj {
			return li < lj // drain least-loaded first
		}
		if !busy[i].CreatedAt.Equal(busy[j].CreatedAt) {
			return busy[i].CreatedAt.Before(busy[j].CreatedAt)
		}
		return busy[i].ID < busy[j].ID
	})
	if len(busy) > want {
		busy = busy[:want]
	}
	out := make([]string, len(busy))
	for i, in := range busy {
		out[i] = in.ID
	}
	return out
}

// Migratable reports whether every active session of id can be placed on
// other running instances in the same region (excluding ids in excluding),
// i.e. whether it is safe to start draining id without risking a stuck drain.
func (m *Manager) Migratable(snap *fleet.Snapshot, load map[string]int, id string, excluding map[string]bool) bool {
	src := snap.Instances[id]
	if src == nil {
		return false
	}
	if load[id] == 0 {
		return true
	}
	// Simulate packing src's sessions onto the region's surviving free slots,
	// spreading evenly. This is a conservative feasibility test.
	type slot struct {
		id   string
		free int
	}
	var targets []slot
	for _, in := range snap.Instances {
		if in.ID == id || excluding[in.ID] {
			continue
		}
		if in.State == fleet.StateRunning && in.Region == src.Region {
			if f := in.Capacity - load[in.ID]; f > 0 {
				targets = append(targets, slot{in.ID, f})
			}
		}
	}
	remaining := load[id]
	// Greedy: repeatedly use the target with the most free slots; no actual
	// binding happens, this only proves room exists.
	for remaining > 0 {
		best := -1
		for i := range targets {
			if targets[i].free > 0 && (best == -1 || targets[i].free > targets[best].free) {
				best = i
			}
		}
		if best == -1 {
			return false
		}
		targets[best].free--
		remaining--
	}
	return true
}

// BeginDrain transitions an instance to Draining (rejecting new sessions).
func (m *Manager) BeginDrain(ctx context.Context, snap *fleet.Snapshot, id string) error {
	return m.prov.StartDrain(ctx, id)
}

// SessionsOn returns active session IDs bound to src, sorted for trace order.
func SessionsOn(snap *fleet.Snapshot, src string) []string {
	var ids []string
	for _, sess := range snap.Sessions {
		if sess.InstanceID == src && sess.State == fleet.SessionActive {
			ids = append(ids, sess.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// MigrateSessions atomically moves every active session of src to other
// running instances in the same region. The provider's atomic rebind means a
// session is either fully on the destination or still on src; on the first
// failure the migration aborts and the caller can retry or time out, so no
// session is lost or double-booked.
func (m *Manager) MigrateSessions(ctx context.Context, snap *fleet.Snapshot, load map[string]int, src string) ([]string, error) {
	srcInst := snap.Instances[src]
	if srcInst == nil {
		return nil, provider.ErrNotFound
	}
	var moved []string
	for _, sid := range SessionsOn(snap, src) {
		dst := m.scheduler.SelectForMigration(snap, srcInst.Region, load, src)
		if dst == "" {
			return moved, provider.ErrNoCapacity
		}
		if err := m.rebind(ctx, sid, dst); err != nil {
			return moved, err
		}
		load[src]--
		load[dst]++
		moved = append(moved, sid)
	}
	return moved, nil
}

// rebind uses the simulator's atomic move when available; callers in
// production provide a provider implementing Rebinder.
func (m *Manager) rebind(ctx context.Context, sessionID, dst string) error {
	if rb, ok := m.prov.(interface {
		Rebind(context.Context, string, string) error
	}); ok {
		return rb.Rebind(ctx, sessionID, dst)
	}
	// Fallback for generic providers: end+place is still performed by the
	// concrete provider transactionally; here we surface unsupported.
	return provider.ErrNoCapacity
}

// Complete terminates an instance that is fully drained.
func (m *Manager) Complete(ctx context.Context, id string) error {
	return m.prov.Terminate(ctx, id)
}
