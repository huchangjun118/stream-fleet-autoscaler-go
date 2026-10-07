package autoscaler

import (
	"math"
	"time"
)

// Decision 一次决策的输出。
type Decision struct {
	Desired int    // 期望实例总数（已夹在 [Min,Max] 内）
	Reason  string // 决策原因，用于追溯
}

// Decide 是纯函数：给定窗口聚合指标、当前实例数、上次扩缩时间与配置，
// 计算期望实例数。相同输入必然产生相同输出，保证决策可复现。
//
// 防抖动机制：
//  1. 扩容看平均利用率 > ScaleUpThreshold，缩容看平均利用率 < ScaleDownThreshold，
//     两者之间的迟滞区间内不做任何调整；
//  2. 扩容/缩容各自受冷却时间约束；
//  3. 单步变化量受 MaxScaleOutStep / MaxScaleInStep 限制。
func Decide(cfg Config, snap Snapshot, current int, now time.Time,
	lastScaleUp, lastScaleDown time.Time) Decision {

	// 期望数按窗口平均负载换算：ceil(avg / (capacity * targetUtil))。
	// 扩容时用 ScaleUpThreshold 作为目标利用率，缩容时用迟滞中点，
	// 使缩容后的稳态利用率落在迟滞区间内部，避免立即反弹扩容。
	mid := (cfg.ScaleUpThreshold + cfg.ScaleDownThreshold) / 2

	desired := current
	reason := "stable: utilization within hysteresis band"

	if snap.Samples == 0 {
		// 无指标：不动作，但收敛到下限。
		if current < cfg.MinInstances {
			return Decision{Desired: cfg.MinInstances, Reason: "no metrics: converge to min"}
		}
		return Decision{Desired: current, Reason: "no metrics: hold"}
	}

	util := snap.Avg / (float64(current) * float64(cfg.InstanceCapacity))
	if current == 0 {
		util = math.Inf(1)
	}

	switch {
	case util > cfg.ScaleUpThreshold:
		if now.Sub(lastScaleUp) < cfg.ScaleUpCooldown {
			reason = "scale-up needed but in cooldown"
			break
		}
		target := int(math.Ceil(snap.Avg / (float64(cfg.InstanceCapacity) * cfg.ScaleUpThreshold)))
		if target > current {
			desired = min(current+cfg.MaxScaleOutStep, target)
			reason = "scale up: avg utilization above threshold"
		}
	case util < cfg.ScaleDownThreshold:
		if now.Sub(lastScaleDown) < cfg.ScaleDownCooldown {
			reason = "scale-down needed but in cooldown"
			break
		}
		target := int(math.Ceil(snap.Avg / (float64(cfg.InstanceCapacity) * mid)))
		if target < current {
			desired = max(current-cfg.MaxScaleInStep, target)
			reason = "scale down: avg utilization below threshold"
		}
	}

	// 弹性上下限：无论扩缩，最终结果必须落在 [Min, Max]。
	desired = max(cfg.MinInstances, min(cfg.MaxInstances, desired))
	return Decision{Desired: desired, Reason: reason}
}
