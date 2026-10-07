// Package reconcile converges reality toward intent after failures and
// restarts: it adopts in-flight creates, recovers instances whose node was
// lost mid-drain/mid-create (rescheduling their sessions without loss or
// double booking), and fails timed-out operations with compensation, never
// leaving a dangling instance.
package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/provider"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/scheduler"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/store"
)

// Reconciler repairs divergence between durable state and backend reality.
type Reconciler struct {
	prov provider.Provider
	sch  *scheduler.Scheduler
	cfg  *config.Config
}

// NewReconciler creates a Reconciler.
func NewReconciler(cfg *config.Config, prov provider.Provider, sch *scheduler.Scheduler) *Reconciler {
	return &Reconciler{prov: prov, sch: sch, cfg: cfg}
}

// Snapshot reads the authoritative backend view.
func (r *Reconciler) Snapshot(ctx context.Context) (*fleet.Snapshot, error) {
	inst, err := r.prov.DescribeInstances(ctx)
	if err != nil {
		return nil, err
	}
	sess, err := r.prov.DescribeSessions(ctx)
	if err != nil {
		return nil, err
	}
	return &fleet.Snapshot{Instances: inst, Sessions: sess}, nil
}

// findOp returns the pending op of kind for instanceID.
func findOp(st *store.State, kind fleet.OpKind, instanceID string) *fleet.OpRecord {
	for _, op := range st.Ops {
		if op.Kind == kind && op.InstanceID == instanceID && op.Status == fleet.OpPending {
			return op
		}
	}
	return nil
}

func mark(op *fleet.OpRecord, status fleet.OpStatus, tick int64) {
	op.Status = status
	op.UpdatedAt = tickTime(tick)
}

// AdoptCreates matches Pending create ops to backend instances that have
// since become Running (or Failed), so the controller never re-issues a
// completed/in-flight create after restart. A create still Pending in the
// backend remains Pending and keeps being awaited.
func (r *Reconciler) AdoptCreates(ctx context.Context, st *store.State, snap *fleet.Snapshot) []string {
	var adopted []string
	for _, op := range st.Ops {
		if op.Kind != fleet.OpCreate || op.Status != fleet.OpPending {
			continue
		}
		in, ok := snap.Instances[op.InstanceID]
		if !ok {
			// Backend never saw it (lost before issue): allow re-issue.
			mark(op, fleet.OpFailed, st.Tick)
			continue
		}
		switch in.State {
		case fleet.StateRunning:
			mark(op, fleet.OpSucceeded, st.Tick)
			adopted = append(adopted, in.ID)
		case fleet.StateFailed:
			mark(op, fleet.OpFailed, st.Tick)
		case fleet.StateLost:
			mark(op, fleet.OpFailed, st.Tick)
		default:
			// Pending: still in flight, keep awaiting (no duplicate create).
		}
	}
	return adopted
}

// RescheduleOrphans places sessions that lost their instance (InstanceID ==
// "") onto healthy running instances exactly once. Returns migrated ids.
func (r *Reconciler) RescheduleOrphans(ctx context.Context, snap *fleet.Snapshot) ([]string, error) {
	load := r.sch.LoadMap(snap)
	// Deterministic iteration order.
	var orphans []*fleet.Session
	for _, sess := range snap.Sessions {
		if sess.State == fleet.SessionActive && sess.InstanceID == "" {
			orphans = append(orphans, sess)
		}
	}
	var migrated []string
	// sort orphans by id
	sortSessions(orphans)
	for _, sess := range orphans {
		dst := r.sch.Select(snap, sess.Region, load)
		if dst == "" {
			return migrated, fmt.Errorf("orphan session %s: %w", sess.ID, provider.ErrNoCapacity)
		}
		if err := r.prov.PlaceSession(ctx, provider.PlaceSessionRequest{
			SessionID: sess.ID, InstanceID: dst, Region: sess.Region,
		}); err != nil {
			return migrated, fmt.Errorf("reschedule session %s: %w", sess.ID, err)
		}
		sess.InstanceID = dst
		load[dst]++
		migrated = append(migrated, sess.ID)
	}
	return migrated, nil
}

// RecoverLost marks backend instances observed as Lost. Their sessions were
// detached by the provider; RescheduleOrphans then relocates them. Pending
// ops touching a lost instance are failed so capacity may be retried.
func (r *Reconciler) RecoverLost(ctx context.Context, st *store.State, snap *fleet.Snapshot) []string {
	var lost []string
	for _, in := range snap.Instances {
		if in.State != fleet.StateLost {
			continue
		}
		lost = append(lost, in.ID)
		for _, op := range st.Ops {
			if op.InstanceID == in.ID && op.Status == fleet.OpPending {
				mark(op, fleet.OpFailed, st.Tick)
			}
		}
	}
	return lost
}

// TimeoutOps fails pending ops older than the op timeout and compensates a
// confirmed create by terminating the unusable instance, preventing dangling
// capacity. Returns timed-out instance ids.
func (r *Reconciler) TimeoutOps(ctx context.Context, st *store.State, snap *fleet.Snapshot, tick int64) []string {
	var timedOut []string
	for _, op := range st.Ops {
		if op.Status != fleet.OpPending {
			continue
		}
		age := tick - op.CreatedAt.Unix()
		if age < int64(r.cfg.OpTimeoutTicks) {
			continue
		}
		mark(op, fleet.OpTimeout, tick)
		timedOut = append(timedOut, op.InstanceID)
		if op.Kind == fleet.OpCreate {
			if in, ok := snap.Instances[op.InstanceID]; ok &&
				(in.State == fleet.StatePending || in.State == fleet.StateRunning) {
				// Compensation: release confirmed-but-unusable create capacity
				// so the fleet is not left in an intermediate state. Drain
				// ops are never force-terminated here: an instance still
				// holding sessions keeps draining until it can migrate safely.
				_ = r.prov.Terminate(ctx, op.InstanceID)
				in.State = fleet.StateStopped
			}
		}
	}
	return timedOut
}

func sortSessions(s []*fleet.Session) {
	// simple insertion sort, ids unique
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1].ID > s[j].ID; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// tickTime maps a deterministic loop tick onto a timestamp. The loopback
// treats one tick as one second; this keeps journals interpretable.
func tickTime(tick int64) time.Time { return time.Unix(tick, 0).UTC() }
