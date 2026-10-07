// Package scheduler places sessions onto running instances using an even
// distribution (fewest active sessions, then lexicographic id), and never
// selects draining/pending instances.
package scheduler

import (
	"sort"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

// Scheduler selects target instances for sessions.
type Scheduler struct{}

// New creates a Scheduler.
func New() *Scheduler { return &Scheduler{} }

// candidates returns running, non-draining instances of region sorted by id
// (the lexicographic order makes every choice reproducible).
func (s *Scheduler) candidates(snap *fleet.Snapshot, region string) []*fleet.Instance {
	var out []*fleet.Instance
	for _, in := range snap.Instances {
		if in.State == fleet.StateRunning && in.Region == region {
			out = append(out, in)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Select returns the best target instance for a new session in region: the
// running, non-draining instance with the most free slots; ties break by id
// so the decision is reproducible. Returns "" if nothing fits.
func (s *Scheduler) Select(snap *fleet.Snapshot, region string, load map[string]int) string {
	return s.pick(snap, region, load, "")
}

// SelectForMigration is like Select but excludes excludeID (the draining or
// lost source instance) and only returns instances with a free slot.
func (s *Scheduler) SelectForMigration(snap *fleet.Snapshot, region string, load map[string]int, excludeID string) string {
	return s.pick(snap, region, load, excludeID)
}

func (s *Scheduler) pick(snap *fleet.Snapshot, region string, load map[string]int, exclude string) string {
	best := ""
	bestFree := -1
	bestLoad := 0
	for _, in := range s.candidates(snap, region) {
		if in.ID == exclude {
			continue
		}
		l := load[in.ID]
		if l >= in.Capacity {
			continue
		}
		free := in.Capacity - l
		// Most free slots first; on equal free slots, lowest load; ids are
		// already sorted ascending so the first match of a strictly better
		// score wins deterministically.
		if free > bestFree || (free == bestFree && l < bestLoad) {
			best = in.ID
			bestFree = free
			bestLoad = l
		}
	}
	return best
}

// LoadMap computes active session counts per instance from the snapshot.
func (s *Scheduler) LoadMap(snap *fleet.Snapshot) map[string]int {
	load := make(map[string]int)
	for _, sess := range snap.Sessions {
		if sess.State == fleet.SessionActive && sess.InstanceID != "" {
			load[sess.InstanceID]++
		}
	}
	return load
}
