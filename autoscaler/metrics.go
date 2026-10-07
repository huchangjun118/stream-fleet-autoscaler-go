package autoscaler

import (
	"sort"
	"time"
)

// MetricsWindow 滑动窗口指标聚合器。
// 只保留最近 window 时长内的采样，提供平均与峰值利用率。
// 对相同输入序列与参数，聚合结果完全确定。
type MetricsWindowAgg struct {
	window  time.Duration
	samples []MetricsSample // 按时间升序
}

// NewMetricsWindowAgg 创建窗口聚合器。
func NewMetricsWindowAgg(window time.Duration) *MetricsWindowAgg {
	return &MetricsWindowAgg{window: window}
}

// Add 追加一个采样。允许乱序到达，内部保持有序。
func (m *MetricsWindowAgg) Add(s MetricsSample) {
	i := sort.Search(len(m.samples), func(i int) bool {
		return !m.samples[i].At.Before(s.At)
	})
	m.samples = append(m.samples, MetricsSample{})
	copy(m.samples[i+1:], m.samples[i:])
	m.samples[i] = s
}

// prune 丢弃窗口之外的旧采样。
func (m *MetricsWindowAgg) prune(now time.Time) {
	cutoff := now.Add(-m.window)
	i := sort.Search(len(m.samples), func(i int) bool {
		return m.samples[i].At.After(cutoff)
	})
	m.samples = append([]MetricsSample(nil), m.samples[i:]...)
}

// Snapshot 窗口聚合结果。
type Snapshot struct {
	Samples int
	Avg     float64 // 窗口内平均活跃会话数
	Peak    int     // 窗口内峰值活跃会话数
	Oldest  time.Time
	Newest  time.Time
}

// Snapshot 返回 now 时刻窗口内的聚合结果。
func (m *MetricsWindowAgg) Snapshot(now time.Time) Snapshot {
	m.prune(now)
	snap := Snapshot{}
	if len(m.samples) == 0 {
		return snap
	}
	var sum float64
	for _, s := range m.samples {
		sum += float64(s.ActiveStreams)
		if s.ActiveStreams > snap.Peak {
			snap.Peak = s.ActiveStreams
		}
	}
	snap.Samples = len(m.samples)
	snap.Avg = sum / float64(len(m.samples))
	snap.Oldest = m.samples[0].At
	snap.Newest = m.samples[len(m.samples)-1].At
	return snap
}
