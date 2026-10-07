package autoscaler

import (
	"fmt"
	"time"
)

// Config 弹性伸缩与再平衡的全部可调参数。
type Config struct {
	// 弹性上下限（整个集群维度）。
	MinInstances int
	MaxInstances int

	// 每个实例的会话容量（用于计算期望实例数）。
	InstanceCapacity int

	// 扩容阈值：窗口内平均利用率（会话数/容量）高于该值则扩容。
	ScaleUpThreshold float64
	// 缩容阈值：窗口内平均利用率低于该值才允许缩容。
	// 必须与 ScaleUpThreshold 之间留有迟滞区间，避免阈值附近抖动。
	ScaleDownThreshold float64

	// 指标窗口：仅统计最近该时长内的采样。
	MetricsWindow time.Duration
	// 决策间隔。
	DecisionInterval time.Duration

	// 冷却时间：相邻两次扩容/缩容之间的最小间隔。
	ScaleUpCooldown   time.Duration
	ScaleDownCooldown time.Duration

	// 单步最大扩/缩数量，限制变化速率。
	MaxScaleOutStep int
	MaxScaleInStep  int

	// 并发度：同时在途的创建/排空操作数上限。
	MaxConcurrentOps int

	// 操作超时：创建或排空超过该时长视为失败并按策略恢复。
	OpTimeout time.Duration

	// 排空宽限：排空开始后等待会话自然结束的最长时间，超时强制迁移。
	DrainGracePeriod time.Duration

	// 再平衡：区域间实例数偏差超过该值才触发，避免稳定状态下频繁迁移。
	RebalanceImbalanceThreshold int
	// 再平衡最小间隔。
	RebalanceCooldown time.Duration
}

// DefaultConfig 返回一组合理默认参数。
func DefaultConfig() Config {
	return Config{
		MinInstances:                1,
		MaxInstances:                100,
		InstanceCapacity:            100,
		ScaleUpThreshold:            0.75,
		ScaleDownThreshold:          0.40,
		MetricsWindow:               2 * time.Minute,
		DecisionInterval:            10 * time.Second,
		ScaleUpCooldown:             30 * time.Second,
		ScaleDownCooldown:           3 * time.Minute,
		MaxScaleOutStep:             4,
		MaxScaleInStep:              2,
		MaxConcurrentOps:            4,
		OpTimeout:                   2 * time.Minute,
		DrainGracePeriod:            5 * time.Minute,
		RebalanceImbalanceThreshold: 2,
		RebalanceCooldown:           10 * time.Minute,
	}
}

// Validate 校验配置合法性。
func (c Config) Validate() error {
	if c.MinInstances < 0 {
		return fmt.Errorf("MinInstances 不能为负: %d", c.MinInstances)
	}
	if c.MaxInstances < c.MinInstances {
		return fmt.Errorf("MaxInstances(%d) 必须 >= MinInstances(%d)", c.MaxInstances, c.MinInstances)
	}
	if c.InstanceCapacity <= 0 {
		return fmt.Errorf("InstanceCapacity 必须为正: %d", c.InstanceCapacity)
	}
	if c.ScaleDownThreshold >= c.ScaleUpThreshold {
		return fmt.Errorf("ScaleDownThreshold(%v) 必须小于 ScaleUpThreshold(%v)，需要迟滞区间",
			c.ScaleDownThreshold, c.ScaleUpThreshold)
	}
	if c.ScaleUpThreshold <= 0 || c.ScaleUpThreshold > 1 {
		return fmt.Errorf("ScaleUpThreshold 必须在 (0,1]: %v", c.ScaleUpThreshold)
	}
	if c.ScaleDownThreshold < 0 {
		return fmt.Errorf("ScaleDownThreshold 不能为负: %v", c.ScaleDownThreshold)
	}
	if c.MetricsWindow <= 0 || c.DecisionInterval <= 0 {
		return fmt.Errorf("MetricsWindow 与 DecisionInterval 必须为正")
	}
	if c.MaxScaleOutStep <= 0 || c.MaxScaleInStep <= 0 {
		return fmt.Errorf("扩缩步长必须为正")
	}
	if c.MaxConcurrentOps <= 0 {
		return fmt.Errorf("MaxConcurrentOps 必须为正")
	}
	if c.OpTimeout <= 0 {
		return fmt.Errorf("OpTimeout 必须为正")
	}
	return nil
}
