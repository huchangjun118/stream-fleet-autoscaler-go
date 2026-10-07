// Package store persists the controller's durable state: the desired
// capacity, pending/completed operation journal and audit event log. It is
// what lets a restarted controller adopt in-flight operations instead of
// re-issuing finished scale up/scale down actions.
package store

import (
	"context"
	"time"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/clock"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

// State is the durable controller state.
type State struct {
	Tick            int64 `json:"tick"`
	DesiredReplicas int   `json:"desiredReplicas"`
	NextOpSeq       int64 `json:"nextOpSeq"`
	NextEventSeq    int64 `json:"nextEventSeq"`

	// Last scale timestamps (in ticks) implement cooldown/flap suppression.
	LastScaleUpTick   int64 `json:"lastScaleUpTick"`
	LastScaleDownTick int64 `json:"lastScaleDownTick"`
	// Consecutive breaching observations, per region.
	UpBreaches   map[string]int `json:"upBreaches"`
	DownBreaches map[string]int `json:"downBreaches"`

	// Last rebalance tick, per region (slow region must not block fast one).
	LastRebalanceTick map[string]int64 `json:"lastRebalanceTick"`

	Ops    []*fleet.OpRecord `json:"ops"`
	Events []*fleet.Event    `json:"events"`
}

// NewState returns an initialized durable state.
func NewState() *State {
	return &State{
		UpBreaches:        map[string]int{},
		DownBreaches:      map[string]int{},
		LastRebalanceTick: map[string]int64{},
	}
}

// Store is durable state storage.
type Store interface {
	Load(ctx context.Context) (*State, error)
	Save(ctx context.Context, st *State) error
	AppendEvent(ctx context.Context, st *State, tick int64, evType, msg string) error
}

// Memory is a non-durable store for tests/loopback.
type Memory struct {
	st      *State
	clk     clock.Clock
	maxEvts int
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{clk: clock.NewFake(), maxEvts: 10000}
}

// NewMemoryWithClock returns an in-memory store using clk for event stamps.
func NewMemoryWithClock(clk clock.Clock) *Memory {
	return &Memory{clk: clk, maxEvts: 10000}
}

// Load implements Store.
func (m *Memory) Load(ctx context.Context) (*State, error) {
	if m.st == nil {
		return nil, nil
	}
	return m.st, nil
}

// Save implements Store.
func (m *Memory) Save(ctx context.Context, st *State) error {
	m.st = st
	return nil
}

// AppendEvent implements Store.
func (m *Memory) AppendEvent(ctx context.Context, st *State, tick int64, evType, msg string) error {
	appendEvent(st, tick, m.clk.Now(), evType, msg, m.maxEvts)
	return nil
}

func appendEvent(st *State, tick int64, now time.Time, evType, msg string, cap_ int) {
	st.NextEventSeq++
	st.Events = append(st.Events, &fleet.Event{
		Seq: st.NextEventSeq, Tick: tick, Time: now, Type: evType, Message: msg,
	})
	if cap_ > 0 && len(st.Events) > cap_ {
		st.Events = st.Events[len(st.Events)-cap_:]
	}
}
