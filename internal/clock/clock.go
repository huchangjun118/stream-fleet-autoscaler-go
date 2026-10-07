// Package clock provides an injectable clock so control-loop decisions are
// deterministic and reproducible in tests and loopback simulations.
package clock

import "time"

// Clock is the minimal time interface used across the controller.
type Clock interface {
	Now() time.Time
}

// System uses the real wall clock.
type System struct{}

// NewSystem returns a clock backed by time.Now.
func NewSystem() *System { return &System{} }

// Now returns the current wall-clock time.
func (System) Now() time.Time { return time.Now() }

// Fake is a manually advanced clock used by tests and the loopback input.
type Fake struct{ t time.Time }

// NewFake returns a fake clock set to a fixed epoch.
func NewFake() *Fake { return &Fake{t: time.Unix(1_700_000_000, 0).UTC()} }

// Now returns the fake current time.
func (f *Fake) Now() time.Time { return f.t }

// Advance moves the fake clock forward by d.
func (f *Fake) Advance(d time.Duration) { f.t = f.t.Add(d) }
