// Package provider abstracts the infrastructure backend that owns instances
// and sessions. The simulator in sim.go implements it locally with scripted
// latency, partial create failures and node loss for deterministic testing.
package provider

import (
	"context"
	"errors"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/fleet"
)

// ErrNoCapacity means no running, non-draining instance had room.
var ErrNoCapacity = errors.New("no instance capacity available")

// ErrNotFound means the referenced instance/session is unknown.
var ErrNotFound = errors.New("resource not found")

// CreateRequest asks the backend to create one instance.
type CreateRequest struct {
	InstanceID string
	Region     string
	Spec       string
	Capacity   int
}

// PlaceSessionRequest assigns a session to a specific instance.
type PlaceSessionRequest struct {
	SessionID  string
	InstanceID string
	Region     string
}

// Provider is the infrastructure backend interface.
type Provider interface {
	// CreateInstance starts an instance asynchronously. A Pending result means
	// the create is in flight; the same instanceID re-query later adopts it
	// instead of issuing a duplicate create.
	CreateInstance(ctx context.Context, req CreateRequest) (*fleet.Instance, error)

	// DescribeInstances returns the backend's authoritative instance view.
	DescribeInstances(ctx context.Context) (map[string]*fleet.Instance, error)

	// StartDrain marks an instance as draining: new sessions are rejected.
	StartDrain(ctx context.Context, instanceID string) error

	// Terminate stops a drained instance.
	Terminate(ctx context.Context, instanceID string) error

	// DescribeSessions returns the backend's authoritative session view.
	DescribeSessions(ctx context.Context) (map[string]*fleet.Session, error)

	// PlaceSession atomically binds a session to the given instance. It fails
	// if the instance is not Running or has no free slot, which guarantees no
	// duplicate placement of a session and no over-commitment.
	PlaceSession(ctx context.Context, req PlaceSessionRequest) error

	// EndSession removes a completed session.
	EndSession(ctx context.Context, sessionID string) error
}
