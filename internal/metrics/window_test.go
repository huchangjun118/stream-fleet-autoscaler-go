package metrics

import "testing"

func TestWindowEvictionAndAverage(t *testing.T) {
	w := NewWindow(3)
	for tk := int64(1); tk <= 5; tk++ {
		w.Add(Sample{Tick: tk, Region: "r", Load: float64(tk)})
	}
	v := w.Values()
	if len(v) != 3 || v[0].Tick != 3 {
		t.Fatalf("expected last 3 ticks starting at 3, got %+v", v)
	}
	if got := w.AverageRegion("r"); got != 4.0 {
		t.Fatalf("expected avg 4, got %v", got)
	}
}

func TestBreachStreak(t *testing.T) {
	w := NewWindow(3)
	// util values per tick; threshold high at 0.8.
	w.Add(Sample{Tick: 1, Region: "g", Load: 0.9})
	w.Add(Sample{Tick: 2, Region: "g", Load: 0.2})
	w.Add(Sample{Tick: 3, Region: "g", Load: 0.95})
	w.Add(Sample{Tick: 4, Region: "g", Load: 0.95})
	high := func(v float64) bool { return v >= 0.8 }
	// At tick 4, window [2,3,4] averages (0.2+0.95+0.95)/3=0.7, not high;
	// the newest raw streak via AverageRegionBreaches uses windowed avg.
	if got := w.AverageRegionBreaches("g", 3, high); got != 0 {
		t.Fatalf("windowed average at tick4 should not breach, got %d", got)
	}
	// Add two more sustained highs.
	w.Add(Sample{Tick: 5, Region: "g", Load: 0.95})
	w.Add(Sample{Tick: 6, Region: "g", Load: 0.95})
	if got := w.AverageRegionBreaches("g", 3, high); got < 1 {
		t.Fatalf("expected >=1 breaching tick by tick6, got %d", got)
	}
}
