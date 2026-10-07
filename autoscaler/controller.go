package autoscaler

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Event 一条可追溯的控制器事件。
type Event struct {
	At     time.Time
	Kind   string
	Detail string
}

// Controller 扩缩容控制器。通过 Reconcile 单步收敛，
// 可用真实时钟周期性驱动，也可用假时钟在测试中确定性驱动。
type Controller struct {
	cfg      Config
	provider Provider
	store    Store
	clock    Clock
	metrics  *MetricsWindowAgg

	mu            sync.Mutex
	instances     map[string]*Instance
	ops           []*OpRecord
	opSeq         int
	lastScaleUp   time.Time
	lastScaleDown time.Time
	lastRebalance time.Time

	sessionHome map[string]string // sessionID -> instanceID，权威归属，防重复占用
	unplaced    []string          // 等待容量的会话
	events      []Event
}

// NewController 创建控制器并从 Store 恢复状态（支持重启恢复）。
func NewController(cfg Config, provider Provider, store Store, clock Clock) (*Controller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := &Controller{
		cfg:         cfg,
		provider:    provider,
		store:       store,
		clock:       clock,
		metrics:     NewMetricsWindowAgg(cfg.MetricsWindow),
		instances:   map[string]*Instance{},
		sessionHome: map[string]string{},
	}
	if store != nil {
		state, err := store.Load()
		if err != nil {
			return nil, fmt.Errorf("load state: %w", err)
		}
		if state != nil {
			c.recover(state)
		}
	}
	return c, nil
}

// recover 从持久化状态重建内存视图，并处理重启前未完成的操作。
func (c *Controller) recover(s *PersistentState) {
	c.instances = s.Instances
	if c.instances == nil {
		c.instances = map[string]*Instance{}
	}
	c.ops = s.Ops
	c.opSeq = s.OpSeq
	c.lastScaleUp = s.LastScaleUp
	c.lastScaleDown = s.LastScaleDown
	c.lastRebalance = s.LastRebalance
	c.events = append(c.events, s.Events...)
	for id, in := range c.instances {
		if in.Sessions == nil {
			in.Sessions = map[string]*Session{}
		}
		for sid := range in.Sessions {
			c.sessionHome[sid] = id
		}
		switch in.State {
		case StatePending:
			// 创建中途重启：按超时策略重新校验，超时则回收，
			// 未超时则保留 Pending 等待后续 Reconcile 收敛。
			if c.clock.Now().Sub(in.CreatedAt) > c.cfg.OpTimeout {
				c.abandonPending(in, "controller restarted, create op timed out")
			}
		case StateDraining:
			// 排空中途重启：继续排空，不重复下发。
			c.log("recover", fmt.Sprintf("resume draining instance %s", id))
		}
	}
	for _, op := range c.ops {
		if !op.Done {
			c.log("recover", fmt.Sprintf("op %s (%s) still in flight after restart", op.ID, op.Type))
		}
	}
}

// log 追加一条可追溯事件。
func (c *Controller) log(kind, detail string) {
	c.events = append(c.events, Event{At: c.clock.Now(), Kind: kind, Detail: detail})
}

// AddMetrics 注入一次负载采样（本地模拟或回环输入）。
func (c *Controller) AddMetrics(s MetricsSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics.Add(s)
}

// Events 返回事件日志快照。
func (c *Controller) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Event(nil), c.events...)
}

// Instances 返回实例快照（按 ID 排序，保证确定性）。
func (c *Controller) Instances() []*Instance {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sortedInstances()
}

func (c *Controller) sortedInstances() []*Instance {
	ids := make([]string, 0, len(c.instances))
	for id := range c.instances {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Instance, 0, len(ids))
	for _, id := range ids {
		out = append(out, c.instances[id])
	}
	return out
}

// activeCount 计入容量的实例数（Pending+Running+Draining，不含 Lost/Terminated）。
func (c *Controller) activeCount() int {
	n := 0
	for _, in := range c.instances {
		if in.State == StatePending || in.State == StateRunning || in.State == StateDraining {
			n++
		}
	}
	return n
}

// servingCount 可对外服务的实例数（Pending+Running）。
func (c *Controller) servingCount() int {
	n := 0
	for _, in := range c.instances {
		if in.State == StatePending || in.State == StateRunning {
			n++
		}
	}
	return n
}

// inFlightOps 在途操作数（未完成的创建 + 正在排空）。
func (c *Controller) inFlightOps() int {
	n := 0
	for _, in := range c.instances {
		if in.State == StatePending || in.State == StateDraining {
			n++
		}
	}
	return n
}

func (c *Controller) newOp(typ OpType, zone, instanceID string, count int) *OpRecord {
	c.opSeq++
	op := &OpRecord{
		ID:         fmt.Sprintf("op-%06d", c.opSeq),
		Type:       typ,
		Zone:       zone,
		InstanceID: instanceID,
		Count:      count,
		StartedAt:  c.clock.Now(),
	}
	c.ops = append(c.ops, op)
	return op
}

// persist 落盘当前状态。
func (c *Controller) persist() {
	if c.store == nil {
		return
	}
	_ = c.store.Save(&PersistentState{
		Instances:     c.instances,
		Ops:           c.ops,
		LastScaleUp:   c.lastScaleUp,
		LastScaleDown: c.lastScaleDown,
		LastRebalance: c.lastRebalance,
		OpSeq:         c.opSeq,
		Events:        c.events,
	})
}
