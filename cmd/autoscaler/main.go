// Command autoscaler runs a loopback-driven demo of the streaming fleet
// autoscaler, printing the deterministic convergence trace.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/config"
	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/loopback"
)

func main() {
	cfg := config.Default()
	cfg.Regions = []config.RegionConfig{
		{Name: "east", Weight: 2, Spec: "standard"},
		{Name: "west", Weight: 1, Spec: "standard"},
	}

	// Offered load in session slots; each standard instance holds 10.
	scr := &loopback.Script{
		Name:    "surge-then-slump",
		Initial: cfg.MinReplicas,
		Ticks:   40,
		Events: []loopback.Event{
			// quiet start
			{At: 1, Kind: loopback.EvLoad, Region: "east", Value: 2},
			{At: 1, Kind: loopback.EvLoad, Region: "west", Value: 1},
			// sudden surge at tick 8
			{At: 8, Kind: loopback.EvLoad, Region: "east", Value: 26},
			{At: 8, Kind: loopback.EvLoad, Region: "west", Value: 13},
			// load slumps back at tick 22
			{At: 22, Kind: loopback.EvLoad, Region: "east", Value: 2},
			{At: 22, Kind: loopback.EvLoad, Region: "west", Value: 1},
		},
	}

	runner := loopback.NewRunner(cfg, scr)
	rep, err := runner.Run(context.Background(), scr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	fmt.Printf("scenario: %s\n", rep.Name)
	for _, e := range rep.Events {
		fmt.Println(" ", e)
	}
	fmt.Printf("summary: scaleUps=%d scaleDowns=%d migrations=%d finalInstances=%d\n",
		rep.ScaleUps, rep.ScaleDowns, rep.Migrations, rep.FinalCount)
}
