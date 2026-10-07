// sim 以本地模拟平台 + 脚本化负载回环驱动控制器，
// 演示负载突增扩容、负载回落缩容、节点失联恢复与再平衡全过程。
package main

import (
	"fmt"
	"os"
	"time"

	autoscaler "github.com/huchangjun118/stream-fleet-autoscaler-go/autoscaler"
)

func main() {
	cfg := autoscaler.DefaultConfig()
	cfg.MinInstances = 2
	cfg.MaxInstances = 12
	cfg.InstanceCapacity = 10
	cfg.MetricsWindow = 30 * time.Second
	cfg.DecisionInterval = 5 * time.Second
	cfg.ScaleUpCooldown = 10 * time.Second
	cfg.ScaleDownCooldown = 20 * time.Second
	cfg.DrainGracePeriod = 30 * time.Second
	cfg.RebalanceCooldown = 30 * time.Second

	clock := autoscaler.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	provider := autoscaler.NewSimProvider(clock, []string{"zone-a", "zone-b", "zone-c"})
	store := autoscaler.NewFileStore("sim-state.json")
	defer os.Remove("sim-state.json")

	c, err := autoscaler.NewController(cfg, provider, store, clock)
	if err != nil {
		fmt.Println("init:", err)
		os.Exit(1)
	}

	// 脚本化负载曲线：低谷 -> 突增 -> 平台期 -> 回落 -> 空闲
	script := []struct {
		load     int
		duration time.Duration
	}{
		{5, 30 * time.Second},
		{95, 60 * time.Second}, // 负载突增
		{95, 60 * time.Second}, // 高位平台
		{15, 90 * time.Second}, // 负载回落
		{0, 120 * time.Second}, // 空闲
	}

	step := 0
	for _, seg := range script {
		for t := time.Duration(0); t < seg.duration; t += cfg.DecisionInterval {
			clock.Advance(cfg.DecisionInterval)
			c.AddMetrics(autoscaler.MetricsSample{At: clock.Now(), ActiveStreams: seg.load})
			dec := c.Reconcile()
			step++
			if step%4 == 0 {
				fmt.Printf("[%s] load=%3d desired=%2d instances=%s\n",
					clock.Now().Format("15:04:05"), seg.load, dec.Desired, summary(c))
			}
		}
	}

	fmt.Println("\n--- 关键事件 ---")
	for _, e := range c.Events() {
		fmt.Printf("%s %-18s %s\n", e.At.Format("15:04:05"), e.Kind, e.Detail)
	}
}

func summary(c *autoscaler.Controller) string {
	counts := map[autoscaler.InstanceState]int{}
	for _, in := range c.Instances() {
		counts[in.State]++
	}
	return fmt.Sprintf("running=%d pending=%d draining=%d lost=%d",
		counts[autoscaler.StateRunning], counts[autoscaler.StatePending],
		counts[autoscaler.StateDraining], counts[autoscaler.StateLost])
}
