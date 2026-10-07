// Package autoscaler 提供实时流媒体实例的动态扩缩容与容量再平衡能力。
package autoscaler

import "time"

// InstanceState 实例生命周期状态。
type InstanceState string

const (
	// StatePending 已下发创建，尚未就绪。
	StatePending InstanceState = "Pending"
	// StateRunning 就绪，可接收会话。
	StateRunning InstanceState = "Running"
	// StateDraining 优雅排空中：不再接收新会话，等待存量会话结束。
	StateDraining InstanceState = "Draining"
	// StateTerminated 已回收。
	StateTerminated InstanceState = "Terminated"
	// StateLost 节点失联（创建或排空中途异常）。
	StateLost InstanceState = "Lost"
)

// Instance 一个流媒体实例。
type Instance struct {
	ID             string
	Zone           string
	State          InstanceState
	Sessions       map[string]*Session // 在途会话
	CreatedAt      time.Time
	DrainStartedAt time.Time // 进入排空的时间
}

// ActiveSessions 返回在途会话数。
func (in *Instance) ActiveSessions() int { return len(in.Sessions) }

// Session 一路在途流媒体会话。
type Session struct {
	ID         string
	InstanceID string
	StartedAt  time.Time
}

// MetricsSample 一次负载采样：给定时刻整个集群的活跃会话数。
type MetricsSample struct {
	At            time.Time
	ActiveStreams int
}

// OpType 扩缩容操作类型。
type OpType string

const (
	OpScaleOut  OpType = "ScaleOut"
	OpScaleIn   OpType = "ScaleIn"
	OpDrain     OpType = "Drain"
	OpRebalance OpType = "Rebalance"
)

// OpRecord 一条已下发的操作记录，用于重启恢复与审计追溯。
type OpRecord struct {
	ID         string
	Type       OpType
	Zone       string
	InstanceID string // 缩容/排空时为目标实例
	Count      int    // 扩容时为数量
	StartedAt  time.Time
	FinishedAt time.Time
	Done       bool
	Aborted    bool // 被取消或超时
}
