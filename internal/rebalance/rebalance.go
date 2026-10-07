// Package rebalance redistributes instances across regions/specs toward
// weighted even shares. Each region is processed independently with its own
// cooldown, so a slow region never blocks a fast one, and a stable fleet
// (deviation below threshold) is never migrated churningly.
package rebalance

import (
	"sort"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

// Move is one region-to-region redistribution step: drain an idle instance
// in FromRegion and create one in ToRegion.
type Move struct {
	FromRegion string
	ToRegion   string
	Spec       string
}

// Planner evaluates weighted balance and emits safe migration moves.
type Planner struct {
	cfg *config.Config
}

// NewPlanner creates a Planner.
func NewPlanner(cfg *config.Config) *Planner { return &Planner{cfg: cfg} }

// Imbalance returns the maximum absolute deviation between actual and
// weighted target share across regions (0 for an empty fleet).
func (p *Planner) Imbalance(snap *fleet.Snapshot) float64 {
	counts, total := p.counts(snap)
	if total == 0 {
		return 0
	}
	max := 0.0
	weightSum := p.weightSum()
	for _, r := range p.cfg.Regions {
		target := float64(r.Weight) / float64(weightSum)
		actual := float64(counts[r.Name]) / float64(total)
		d := actual - target
		if d < 0 {
			d = -d
		}
		if d > max {
			max = d
		}
	}
	return max
}

func (p *Planner) weightSum() int {
	w := 0
	for _, r := range p.cfg.Regions {
		w += r.Weight
	}
	return w
}

func (p *Planner) counts(snap *fleet.Snapshot) (map[string]int, int) {
	m := map[string]int{}
	for _, in := range snap.Instances {
		if in.State == fleet.StateRunning || in.State == fleet.StateDraining {
			m[in.Region]++
		}
	}
	total := 0
	for _, n := range m {
		total += n
	}
	return m, total
}

// Evaluate returns at most one move per call. A move is emitted only when:
//   - the fleet is otherwise stable (no Pending creates, no active drains),
//   - some region is over target by >= RebalanceThreshold and another is
//     under target by the same amount,
//   - the over-weight region has at least 2 instances (we never empty a
//     region) and its cooldown has elapsed,
//   - capacity bounds allow a new instance in the under-weight region.
//
// The result is deterministic: regions are inspected in name order and the
// same input always yields the same move.
func (p *Planner) Evaluate(snap *fleet.Snapshot, lastMoveTick map[string]int64, tick int64) []Move {
	if p.unstable(snap) {
		return nil
	}
	counts, total := p.counts(snap)
	if total < 2 {
		return nil
	}
	if p.Imbalance(snap) < p.cfg.RebalanceThreshold {
		return nil
	}
	weightSum := p.weightSum()
	// Over- and under-weight regions relative to target share.
	type rc struct {
		name   string
		spec   string
		share  float64
		target float64
		count  int
	}
	var rows []rc
	for _, r := range p.cfg.Regions {
		rows = append(rows, rc{
			name: r.Name, spec: r.Spec,
			share:  float64(counts[r.Name]) / float64(total),
			target: float64(r.Weight) / float64(weightSum),
			count:  counts[r.Name],
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

	var over, under *rc
	for i := range rows {
		if rows[i].share-rows[i].target >= p.cfg.RebalanceThreshold && rows[i].count >= 2 {
			if over == nil || rows[i].share-rows[i].target > over.share-over.target {
				over = &rows[i]
			}
		}
		if rows[i].target-rows[i].share >= p.cfg.RebalanceThreshold {
			if under == nil || rows[i].target-rows[i].share > under.target-under.share {
				under = &rows[i]
			}
		}
	}
	if over == nil || under == nil {
		return nil
	}
	// Per-region cooldown: either side recently migrated => hold. This also
	// prevents a slow (draining) region from being asked to move again while
	// it is still catching up.
	if tick-lastMoveTick[over.name] < int64(p.cfg.RebalanceCooldown) {
		return nil
	}
	if tick-lastMoveTick[under.name] < int64(p.cfg.RebalanceCooldown) {
		return nil
	}
	// Respect the hard upper bound for the destination.
	if total >= p.cfg.MaxReplicas {
		return nil
	}
	return []Move{{FromRegion: over.name, ToRegion: under.name, Spec: under.spec}}
}

// unstable reports whether the fleet is mid-convergence (creates in flight or
// drains ongoing). Rebalancing waits for stability to avoid churn.
func (p *Planner) unstable(snap *fleet.Snapshot) bool {
	for _, in := range snap.Instances {
		if in.State == fleet.StatePending || in.State == fleet.StateDrained {
			return true
		}
	}
	return false
}
