package job

import (
	"errors"
	"fmt"
)

// ErrInvalidTransition is returned by ValidateTransition for illegal edges.
var ErrInvalidTransition = errors.New("invalid transition")

// transitions is the state machine from docs/design/tdd.md §4.2. It
// covers the full diagram, including the lost/reclaim edges that are
// unreachable until M3's sweeper exists: rejecting them now costs
// nothing and means this table never needs revisiting.
var transitions = map[JobState]map[JobState]bool{
	JobQueued:    {JobAssigned: true, JobFailed: true},
	JobAssigned:  {JobRunning: true, JobQueued: true, JobLost: true},
	JobRunning:   {JobSucceeded: true, JobFailed: true, JobLost: true},
	JobLost:      {JobQueued: true, JobFailed: true},
	JobFailed:    {},
	JobSucceeded: {},
}

// ValidateTransition reports whether from -> to is a legal edge in
// the job state machine. Any pair not in transitions is rejected,
// including unknown "from" states.
func ValidateTransition(from, to JobState) error {
	if transitions[from][to] {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
}
