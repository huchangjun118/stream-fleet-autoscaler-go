// Package metrics maintains the bounded, deterministic sliding window of
// per-region load observations used by the scaling policy.
package metrics

import "sort"

// Sample is one per-region load observation.
type Sample struct {
	Tick   int64
	Region string
	Load   float64 // offered load in "session slots"
}

// Window is a bounded FIFO buffer of samples.
type Window struct {
	maxTicks int
	samples  []Sample
}

// NewWindow creates a window covering the last maxTicks ticks.
func NewWindow(maxTicks int) *Window {
	if maxTicks < 1 {
		maxTicks = 1
	}
	return &Window{maxTicks: maxTicks}
}

// Add appends one observation, evicting samples older than the window.
// Multiple samples per tick are allowed (one per region).
func (w *Window) Add(s Sample) {
	cutoff := s.Tick - int64(w.maxTicks) + 1
	kept := w.samples[:0]
	for _, old := range w.samples {
		if old.Tick >= cutoff {
			kept = append(kept, old)
		}
	}
	w.samples = append(kept, s)
}

// Values returns the samples currently retained, oldest first.
func (w *Window) Values() []Sample {
	out := make([]Sample, len(w.samples))
	copy(out, w.samples)
	return out
}

// byTick returns samples of region sorted by tick ascending.
func (w *Window) byTick(region string) []Sample {
	var out []Sample
	for _, s := range w.samples {
		if s.Region == region {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tick < out[j].Tick })
	return out
}

// Latest returns the most recent sample for region, if any.
func (w *Window) Latest(region string) (Sample, bool) {
	s := w.byTick(region)
	if len(s) == 0 {
		return Sample{}, false
	}
	return s[len(s)-1], true
}

// Average returns the mean load across all retained samples (0 if empty).
func (w *Window) Average() float64 {
	if len(w.samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range w.samples {
		sum += s.Load
	}
	return sum / float64(len(w.samples))
}

// AverageRegion returns the mean load of one region across the window.
func (w *Window) AverageRegion(region string) float64 {
	s := w.byTick(region)
	if len(s) == 0 {
		return 0
	}
	var sum float64
	for _, x := range s {
		sum += x.Load
	}
	return sum / float64(len(s))
}

// AverageRegionBreaches counts consecutive ticks (newest first) for which
// the region average satisfies fn, capped at n. The average is computed
// from the window ending at each tick so it mirrors the policy view exactly.
func (w *Window) AverageRegionBreaches(region string, n int, fn func(float64) bool) int {
	s := w.byTick(region)
	if n <= 0 || len(s) == 0 {
		return 0
	}
	count := 0
	// Walk newest sample backwards; require contiguous ticks.
	for i := len(s) - 1; i >= 0; i-- {
		// Average over samples within maxTicks ending at s[i].Tick.
		var sum float64
		k := 0
		for j := 0; j <= i; j++ {
			if s[j].Tick >= s[i].Tick-int64(w.maxTicks)+1 {
				sum += s[j].Load
				k++
			}
		}
		avg := 0.0
		if k > 0 {
			avg = sum / float64(k)
		}
		if !fn(avg) {
			break
		}
		count++
		if count >= n {
			break
		}
	}
	return count
}

// Reset clears all observations.
func (w *Window) Reset() { w.samples = nil }
