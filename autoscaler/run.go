package autoscaler

import (
	"context"
	"time"
)

// Run 以真实时钟周期性驱动 Reconcile，直到 ctx 取消。
// 取消时执行最后一次收敛，确保不留下悬挂实例或中间状态。
func (c *Controller) Run(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.DecisionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.Reconcile() // 收尾收敛
			return
		case <-ticker.C:
			c.Reconcile()
		}
	}
}
