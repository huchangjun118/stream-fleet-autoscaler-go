package config

import "testing"

func TestValidateRejectsInconsistent(t *testing.T) {
	c := Default()
	c.MaxReplicas = 1
	c.MinReplicas = 2
	if err := c.Validate(); err == nil {
		t.Fatal("expected max<min validation error")
	}
	c2 := Default()
	c2.ScaleUpThreshold = 0.2
	c2.ScaleDownThreshold = 0.8
	if err := c2.Validate(); err == nil {
		t.Fatal("expected inverted thresholds validation error")
	}
	c3 := Default()
	c3.Regions = nil
	if err := c3.Validate(); err == nil {
		t.Fatal("expected empty regions validation error")
	}
}

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}
