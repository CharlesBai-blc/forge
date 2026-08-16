package job

import "time"

type WorkerState string

const (
	WorkerActive   WorkerState = "active"
	WorkerCordoned WorkerState = "cordoned"
	WorkerDraining WorkerState = "draining"
	WorkerLost     WorkerState = "lost"
	WorkerRemoved  WorkerState = "removed"
)

// Worker is one enrolled machine (FR-18).
type Worker struct {
	ID        string
	Name      string
	Labels    []string
	Capacity  int
	State     WorkerState
	Burst     bool
	Healthy   bool
	LastSeen  time.Time
	TokenHash []byte
	Arch      string
	Version   string
}
