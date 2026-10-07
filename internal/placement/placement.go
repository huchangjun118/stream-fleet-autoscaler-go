// Package placement turns "add N instances" into concrete per-region/spec
// allocations that respect capacity bounds and an even weighted distribution.
package placement

import (
	"sort"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

// Allocation is one decided instance placement.
type Allocation struct {
	Region   string
	Spec     string
	Capacity int
}

// Planner computes balanced placements.
type Planner struct {
	cfg *config.Config
}

// NewPlanner creates a Planner.
func NewPlanner(cfg *config.Config) *Planner { return &Planner{cfg: cfg} }

func nonTerminal(in *fleet.Instance) bool {
	return in.State != fleet.StateStopped && in.State != fleet.StateFailed
}

// regionCount counts non-terminal instances per region (Pending and Draining
// included, so in-flight capacity is never double allocated).
func regionCount(snap *fleet.Snapshot) map[string]int {
	m := map[string]int{}
	for _, in := range snap.Instances {
		if nonTerminal(in) {
			m[in.Region]++
		}
	}
	return m
}

// PlanAdd distributes delta new instances across regions by weighted even
// share, accounting for instances already present (including Pending so
// in-flight creates are never double counted). It never plans beyond
// cfg.MaxReplicas.
func (p *Planner) PlanAdd(snap *fleet.Snapshot, delta int) []Allocation {
	if delta <= 0 {
		return nil
	}
	counts := regionCount(snap)
	total := 0
	weights := map[string]int{}
	var regions []string
	for _, r := range p.cfg.Regions {
		regions = append(regions, r.Name)
		weights[r.Name] = r.Weight
		total += counts[r.Name]
	}
	// Never exceed the hard upper bound, including in-flight capacity.
	room := p.cfg.MaxReplicas - total
	if room <= 0 {
		return nil
	}
	if delta > room {
		delta = room
	}
	weightSum := 0
	for _, w := range weights {
		weightSum += w
	}

	var out []Allocation
	for i := 0; i < delta; i++ {
		// Pick the region with the greatest deficit vs weighted target share.
		// Deficit = expected(total+1) - current. Ties break by region name.
		best := ""
		bestDeficit := 0.0
		newTotal := total + len(out) + 1
		for _, name := range regions {
			want := float64(weights[name]) / float64(weightSum) * float64(newTotal)
			deficit := want - float64(counts[name])
			if deficit > bestDeficit || (deficit == bestDeficit && (best == "" || name < best)) {
				bestDeficit = deficit
				best = name
			}
		}
		rc := p.region(best)
		out = append(out, Allocation{
			Region:   best,
			Spec:     rc.Spec,
			Capacity: p.cfg.Specs[rc.Spec],
		})
		counts[best]++
	}
	return out
}

func (p *Planner) region(name string) config.RegionConfig {
	for _, r := range p.cfg.Regions {
		if r.Name == name {
			return r
		}
	}
	return config.RegionConfig{}
}

// RegionShare returns the current share (0..1) of non-terminal instances in
// the given region.
func (p *Planner) RegionShare(snap *fleet.Snapshot, region string) float64 {
	counts := regionCount(snap)
	total := 0
	for _, n := range counts {
		total += n
	}
	if total == 0 {
		return 0
	}
	return float64(counts[region]) / float64(total)
}

// SortedRegions returns region names in deterministic order.
func (p *Planner) SortedRegions() []string {
	names := make([]string, 0, len(p.cfg.Regions))
	for _, r := range p.cfg.Regions {
		names = append(names, r.Name)
	}
	sort.Strings(names)
	return names
}
