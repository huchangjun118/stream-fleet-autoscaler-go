// Package fleet contains the core domain model of the streaming fleet:
// instances, sessions, lifecycle operations and audit events.
package fleet

import "time"

// InstanceState is the lifecycle state of a streaming instance.
type InstanceState string

// Instance lifecycle states.
const (
	StatePending  InstanceState = "Pending"  // create issued, not yet serving
	StateRunning  InstanceState = "Running"  // healthy and accepting sessions
	StateDraining InstanceState = "Draining" // draining: no new sessions
	StateDrained  InstanceState = "Drained"  // drain finished, ready to terminate
	StateStopped  InstanceState = "Stopped"  // terminated successfully
	StateFailed   InstanceState = "Failed"   // create/terminate failed terminally
	StateLost     InstanceState = "Lost"     // node lost contact
)

// SessionState is the lifecycle state of a streaming session.
type SessionState string

// Session states.
const (
	SessionActive    SessionState = "Active"
	SessionMigrating SessionState = "Migrating"
	SessionEnded     SessionState = "Ended"
)

// OpKind identifies the kind of a lifecycle operation.
type OpKind string

// Operation kinds tracked in the durable journal.
const (
	OpCreate    OpKind = "Create"
	OpDrain     OpKind = "Drain"
	OpTerminate OpKind = "Terminate"
	OpMigrate   OpKind = "Migrate"
)

// OpStatus is the status of a lifecycle operation.
type OpStatus string

// Operation statuses.
const (
	OpPending   OpStatus = "Pending"
	OpSucceeded OpStatus = "Succeeded"
	OpFailed    OpStatus = "Failed"
	OpTimeout   OpStatus = "Timeout"
)

// Instance is one streaming server instance (a node in a region/zone).
type Instance struct {
	ID         string
	Region     string
	Spec       string
	Capacity   int // max concurrent sessions
	State      InstanceState
	Generation int64
	CreatedAt  time.Time
}

// Session is one long-lived streaming session pinned to an instance.
type Session struct {
	ID         string
	InstanceID string
	Region     string
	State      SessionState
	StartedAt  time.Time
}

// OpRecord is a durable, idempotent lifecycle operation record.
type OpRecord struct {
	ID         string
	Kind       OpKind
	InstanceID string
	SessionID  string
	Region     string
	Spec       string
	Status     OpStatus
	Attempts   int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Event is an append-only audit/trace entry.
type Event struct {
	Seq     int64
	Tick    int64
	Time    time.Time
	Type    string
	Message string
}

// Spec describes one instance size.
type Spec struct {
	Name     string
	Capacity int
}

// Snapshot is a point-in-time view of the whole fleet.
type Snapshot struct {
	Instances map[string]*Instance
	Sessions  map[string]*Session
}

// Clone returns a deep copy of the snapshot.
func (s *Snapshot) Clone() *Snapshot {
	if s == nil {
		return nil
	}
	c := &Snapshot{
		Instances: make(map[string]*Instance, len(s.Instances)),
		Sessions:  make(map[string]*Session, len(s.Sessions)),
	}
	for k, v := range s.Instances {
		iv := *v
		c.Instances[k] = &iv
	}
	for k, v := range s.Sessions {
		sv := *v
		c.Sessions[k] = &sv
	}
	return c
}

// ActiveInstances returns instances not in a terminal state.
func (s *Snapshot) ActiveInstances() []*Instance {
	var out []*Instance
	for _, in := range s.Instances {
		if in.State != StateStopped && in.State != StateFailed {
			out = append(out, in)
		}
	}
	return out
}
