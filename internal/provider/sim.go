package provider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

// ErrCreateFailed is returned for scripted creation failures.
var ErrCreateFailed = fmt.Errorf("instance creation failed")

// Sim is a deterministic in-process provider. All mutations are driven by
// Tick(): in-flight creates complete after the configured per-region latency,
// scripted failures/loss are applied, and session placement is atomic.
type Sim struct {
	mu        sync.Mutex
	instances map[string]*fleet.Instance
	sessions  map[string]*fleet.Session
	pending   map[string]int // instanceID -> ticks remaining
	specCap   map[string]int
	regionCfg map[string]simRegion
	tick      int64
	seq       int64
	failNext  int // scripted: next N creates fail, then recover
}

type simRegion struct {
	slowTicks  int
	createFail bool
	loseNodes  bool
}

// NewSim constructs an empty simulator with the given spec capacities and
// region behavior.
func NewSim(specCap map[string]int, regions map[string]simRegion) *Sim {
	return &Sim{
		instances: map[string]*fleet.Instance{},
		sessions:  map[string]*fleet.Session{},
		pending:   map[string]int{},
		specCap:   specCap,
		regionCfg: regions,
	}
}

// NewSimFromConfig builds a Sim from an autoscaler Config.
func NewSimFromConfig(cfg *config.Config) *Sim {
	regions := make(map[string]simRegion, len(cfg.Regions))
	for _, r := range cfg.Regions {
		regions[r.Name] = simRegion{slowTicks: r.SlowTicks, createFail: r.CreateFail, loseNodes: r.LoseNodes}
	}
	return NewSim(cfg.Specs, regions)
}

// Tick advances the simulated backend by one tick.
func (s *Sim) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tick++
	for id, left := range s.pending {
		left--
		if left <= 0 {
			delete(s.pending, id)
			if in, ok := s.instances[id]; ok && in.State == fleet.StatePending {
				in.State = fleet.StateRunning
			}
		} else {
			s.pending[id] = left
		}
	}
}

// CurrentTick returns the simulated tick counter.
func (s *Sim) CurrentTick() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}

// CreateInstance implements Provider.
func (s *Sim) CreateInstance(ctx context.Context, req CreateRequest) (*fleet.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Idempotent adoption: an existing instance with this ID is never created
	// twice, so restarting/adopting an op never duplicates capacity.
	if existing, ok := s.instances[req.InstanceID]; ok {
		cp := *existing
		return &cp, nil
	}
	cap := req.Capacity
	if cap == 0 {
		cap = s.specCap[req.Spec]
	}
	in := &fleet.Instance{
		ID:         req.InstanceID,
		Region:     req.Region,
		Spec:       req.Spec,
		Capacity:   cap,
		Generation: s.tick + 1,
		CreatedAt:  s.nowLocked(),
	}
	if rc, ok := s.regionCfg[req.Region]; ok && rc.createFail {
		in.State = fleet.StateFailed
		s.instances[req.InstanceID] = in
		return in, ErrCreateFailed
	}
	if s.failNext > 0 {
		s.failNext--
		in.State = fleet.StateFailed
		s.instances[req.InstanceID] = in
		return in, ErrCreateFailed
	}
	in.State = fleet.StatePending
	s.instances[req.InstanceID] = in
	latency := 1
	if rc, ok := s.regionCfg[req.Region]; ok && rc.slowTicks > 0 {
		latency = rc.slowTicks
	}
	s.pending[req.InstanceID] = latency
	cp := *in
	return &cp, nil
}

func (s *Sim) nowLocked() time.Time { return time.Unix(s.tick, 0).UTC() }

// DescribeInstances implements Provider.
func (s *Sim) DescribeInstances(ctx context.Context) (map[string]*fleet.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*fleet.Instance, len(s.instances))
	for k, v := range s.instances {
		cp := *v
		out[k] = &cp
	}
	return out, nil
}

// StartDrain implements Provider.
func (s *Sim) StartDrain(ctx context.Context, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.instances[instanceID]
	if !ok {
		return ErrNotFound
	}
	if in.State == fleet.StateRunning {
		in.State = fleet.StateDraining
	}
	return nil
}

// Terminate implements Provider.
func (s *Sim) Terminate(ctx context.Context, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.instances[instanceID]
	if !ok {
		return ErrNotFound
	}
	delete(s.pending, instanceID)
	in.State = fleet.StateStopped
	return nil
}

// DescribeSessions implements Provider.
func (s *Sim) DescribeSessions(ctx context.Context) (map[string]*fleet.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*fleet.Session, len(s.sessions))
	for k, v := range s.sessions {
		cp := *v
		out[k] = &cp
	}
	return out, nil
}

// PlaceSession implements Provider.
func (s *Sim) PlaceSession(ctx context.Context, req PlaceSessionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Idempotent: rebinding the same session to the same instance is a no-op.
	if existing, ok := s.sessions[req.SessionID]; ok && existing.InstanceID == req.InstanceID {
		return nil
	}
	in, ok := s.instances[req.InstanceID]
	if !ok {
		return ErrNotFound
	}
	if in.State != fleet.StateRunning {
		return fmt.Errorf("%w: instance %s is %s, not accepting sessions",
			ErrNoCapacity, req.InstanceID, in.State)
	}
	if s.loadLocked(req.InstanceID) >= in.Capacity {
		return fmt.Errorf("%w: instance %s full", ErrNoCapacity, req.InstanceID)
	}
	// A session already bound elsewhere must be moved via Rebind, preventing
	// duplicate occupation of two instances. An orphaned session (InstanceID
	// == "") following node loss is allowed to be re-placed exactly once.
	if existing, ok := s.sessions[req.SessionID]; ok && existing.InstanceID != "" && existing.InstanceID != req.InstanceID {
		return fmt.Errorf("session %s already bound to %s", req.SessionID, existing.InstanceID)
	}
	s.sessions[req.SessionID] = &fleet.Session{
		ID:         req.SessionID,
		InstanceID: req.InstanceID,
		Region:     req.Region,
		State:      fleet.SessionActive,
		StartedAt:  s.nowLocked(),
	}
	return nil
}

// Rebind atomically moves a session from src to dst. It is the only way a
// session changes instance, which guarantees it is never on two nodes at once
// and never lost: either the move commits or the session stays on src.
func (s *Sim) Rebind(ctx context.Context, sessionID, dst string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return ErrNotFound
	}
	dstInst, ok := s.instances[dst]
	if !ok {
		return ErrNotFound
	}
	if dstInst.State != fleet.StateRunning {
		return fmt.Errorf("%w: migration target %s not running", ErrNoCapacity, dst)
	}
	if s.loadLocked(dst) >= dstInst.Capacity {
		return fmt.Errorf("%w: migration target %s full", ErrNoCapacity, dst)
	}
	sess.InstanceID = dst
	return nil
}

func (s *Sim) loadLocked(instanceID string) int {
	n := 0
	for _, sess := range s.sessions {
		if sess.InstanceID == instanceID && sess.State == fleet.SessionActive {
			n++
		}
	}
	return n
}

// EndSession implements Provider.
func (s *Sim) EndSession(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return ErrNotFound
	}
	sess.State = fleet.SessionEnded
	delete(s.sessions, sessionID)
	return nil
}

// LoseInstance simulates a node losing contact: the instance is marked Lost
// and its bound sessions are detached (empty InstanceID) so the reconciler
// can reschedule each exactly once without losing or duplicating them.
func (s *Sim) LoseInstance(instanceID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.instances[instanceID]
	if !ok {
		return nil
	}
	delete(s.pending, instanceID)
	in.State = fleet.StateLost
	var orphaned []string
	for _, sess := range s.sessions {
		if sess.InstanceID == instanceID {
			orphaned = append(orphaned, sess.ID)
			sess.InstanceID = ""
		}
	}
	return orphaned
}

// SessionCount returns how many Active sessions are bound to an instance.
func (s *Sim) SessionCount(instanceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(instanceID)
}

// FailNextCreations scripts the next n create calls to fail (regardless of
// region). After n failures creation recovers, modeling a transient backend
// outage: the controller must retry and still converge within bounds.
func (s *Sim) FailNextCreations(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = n
}
