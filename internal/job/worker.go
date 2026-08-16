package job

import (
	"fmt"
	"time"
)

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

// workerTransitions is tdd.md §4.3. A lost worker revives to the
// operational state it held before going lost (FR-19, FR-20).
var workerTransitions = map[WorkerState]map[WorkerState]bool{
	WorkerActive:   {WorkerCordoned: true, WorkerDraining: true, WorkerLost: true},
	WorkerCordoned: {WorkerActive: true, WorkerDraining: true, WorkerLost: true, WorkerRemoved: true},
	WorkerDraining: {WorkerCordoned: true, WorkerLost: true},
	WorkerLost:     {WorkerActive: true, WorkerCordoned: true, WorkerDraining: true, WorkerRemoved: true},
	WorkerRemoved:  {},
}

func ValidateWorkerTransition(from, to WorkerState) error {
	if workerTransitions[from][to] {
		return nil
	}
	return fmt.Errorf("invalid worker transition: %s -> %s", from, to)
}
