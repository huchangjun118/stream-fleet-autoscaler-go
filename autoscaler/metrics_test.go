package autoscaler

import (
	"testing"
	"time"
)

func TestMetricsWindowSlides(t *testing.T) {
	m := NewMetricsWindowAgg(time.Minute)
	base := t0
	m.Add(MetricsSample{At: base, ActiveStreams: 10})
	m.Add(MetricsSample{At: base.Add(10 * time.Second), ActiveStreams: 20})
	m.Add(MetricsSample{At: base.Add(70 * time.Second), ActiveStreams: 40})

	snap := m.Snapshot(base.Add(80 * time.Second))
	// 窗口 (20s, 80s] 内只剩最后一个样本
	if snap.Samples != 1 || snap.Avg != 40 || snap.Peak != 40 {
		t.Fatalf("window slide wrong: %+v", snap)
	}
}

func TestMetricsOutOfOrder(t *testing.T) {
	m := NewMetricsWindowAgg(time.Minute)
	m.Add(MetricsSample{At: t0.Add(30 * time.Second), ActiveStreams: 30})
	m.Add(MetricsSample{At: t0.Add(10 * time.Second), ActiveStreams: 10})
	m.Add(MetricsSample{At: t0.Add(20 * time.Second), ActiveStreams: 20})
	snap := m.Snapshot(t0.Add(40 * time.Second))
	if snap.Samples != 3 || snap.Avg != 20 || snap.Peak != 30 {
		t.Fatalf("out-of-order aggregation wrong: %+v", snap)
	}
}

// 同一输入序列重放两次，决策序列必须完全一致（可复现性）。
func TestReplayDeterminism(t *testing.T) {
	run := func() []Decision {
		cfg := testConfig()
		cfg.MetricsWindow = time.Minute
		c, _, clock := newTestController(t, cfg, []string{"zone-a", "zone-b"})
		var out []Decision
		loads := []int{0, 30, 90, 95, 40, 10, 0, 0, 70, 20}
		for _, l := range loads {
			for i := 0; i < 6; i++ {
				clock.Advance(10 * time.Second)
				c.AddMetrics(MetricsSample{At: clock.Now(), ActiveStreams: l})
			}
			out = append(out, c.Reconcile())
		}
		return out
	}
	first := run()
	second := run()
	if len(first) != len(second) {
		t.Fatalf("replay length mismatch")
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("replay diverged at step %d: %+v vs %+v", i, first[i], second[i])
		}
	}
}
