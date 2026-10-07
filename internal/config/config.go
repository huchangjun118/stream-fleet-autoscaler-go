// Package config defines the autoscaler configuration: elastic bounds,
// metric window, cooldowns, concurrency and per-region/spec topology.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// RegionConfig configures one region.
type RegionConfig struct {
	Name       string `json:"name"`
	Weight     int    `json:"weight"`     // relative target share of fleet
	Spec       string `json:"spec"`       // instance spec used in region
	SlowTicks  int    `json:"slowTicks"`  // simulated per-op latency in ticks
	CreateFail bool   `json:"createFail"` // scripted: creates fail in region
	LoseNodes  bool   `json:"loseNodes"`  // scripted: nodes may be lost
}

// Config is the full autoscaler configuration. Time fields are expressed in
// control-loop ticks; TickInterval maps ticks to wall-clock time.
type Config struct {
	MinReplicas int `json:"minReplicas"`
	MaxReplicas int `json:"maxReplicas"`

	ScaleUpThreshold   float64 `json:"scaleUpThreshold"`   // 0..1 avg utilization
	ScaleDownThreshold float64 `json:"scaleDownThreshold"` // 0..1 avg utilization

	WindowTicks    int `json:"windowTicks"`  // metric window length
	ScaleUpAfter   int `json:"scaleUpAfter"` // consecutive breaching ticks
	ScaleDownAfter int `json:"scaleDownAfter"`

	ScaleUpCooldownTicks   int `json:"scaleUpCooldownTicks"`
	ScaleDownCooldownTicks int `json:"scaleDownCooldownTicks"`

	DrainTimeoutTicks int `json:"drainTimeoutTicks"`
	OpTimeoutTicks    int `json:"opTimeoutTicks"`

	MaxConcurrentOps int `json:"maxConcurrentOps"`
	StepSize         int `json:"maxInstancesPerScale"`

	// Rebalance tuning.
	RebalanceThreshold float64 `json:"rebalanceThreshold"` // min deviation to act
	RebalanceCooldown  int     `json:"rebalanceCooldownTicks"`

	TickIntervalMS int            `json:"tickIntervalMs"`
	Regions        []RegionConfig `json:"regions"`
	Specs          map[string]int `json:"specs"` // spec name -> capacity
}

// Default returns a usable baseline configuration.
func Default() *Config {
	return &Config{
		MinReplicas:            2,
		MaxReplicas:            10,
		ScaleUpThreshold:       0.75,
		ScaleDownThreshold:     0.35,
		WindowTicks:            3,
		ScaleUpAfter:           2,
		ScaleDownAfter:         3,
		ScaleUpCooldownTicks:   2,
		ScaleDownCooldownTicks: 4,
		DrainTimeoutTicks:      10,
		OpTimeoutTicks:         12,
		MaxConcurrentOps:       2,
		StepSize:               2,
		RebalanceThreshold:     0.20,
		RebalanceCooldown:      6,
		TickIntervalMS:         500,
		Specs:                  map[string]int{"standard": 10},
		Regions: []RegionConfig{
			{Name: "east", Weight: 1, Spec: "standard"},
			{Name: "west", Weight: 1, Spec: "standard"},
		},
	}
}

// LoadFile reads JSON configuration from path.
func LoadFile(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	c := Default()
	if err := json.Unmarshal(b, c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	if c.MinReplicas < 0 {
		return fmt.Errorf("minReplicas must be >= 0")
	}
	if c.MaxReplicas < c.MinReplicas {
		return fmt.Errorf("maxReplicas (%d) must be >= minReplicas (%d)", c.MaxReplicas, c.MinReplicas)
	}
	if c.ScaleUpThreshold < c.ScaleDownThreshold {
		return fmt.Errorf("scaleUpThreshold (%.2f) must be >= scaleDownThreshold (%.2f) for hysteresis",
			c.ScaleUpThreshold, c.ScaleDownThreshold)
	}
	if c.WindowTicks <= 0 {
		return fmt.Errorf("windowTicks must be > 0")
	}
	if c.StepSize <= 0 {
		return fmt.Errorf("maxInstancesPerScale must be > 0")
	}
	if c.MaxConcurrentOps <= 0 {
		return fmt.Errorf("maxConcurrentOps must be > 0")
	}
	if len(c.Regions) == 0 {
		return fmt.Errorf("at least one region is required")
	}
	names := map[string]bool{}
	totalWeight := 0
	for _, r := range c.Regions {
		if r.Name == "" {
			return fmt.Errorf("region name must not be empty")
		}
		if names[r.Name] {
			return fmt.Errorf("duplicate region name %q", r.Name)
		}
		names[r.Name] = true
		if r.Weight <= 0 {
			return fmt.Errorf("region %q weight must be > 0", r.Name)
		}
		totalWeight += r.Weight
		if _, ok := c.Specs[r.Spec]; !ok {
			return fmt.Errorf("region %q references unknown spec %q", r.Name, r.Spec)
		}
	}
	if totalWeight <= 0 {
		return fmt.Errorf("total region weight must be > 0")
	}
	return nil
}
