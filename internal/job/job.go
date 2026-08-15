// Package job defines Forge's core job record and its state machine
package job

import "time"

type JobState string

const (
	JobQueued    JobState = "queued"
	JobAssigned  JobState = "assigned"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"
	JobLost      JobState = "lost"
)

// Job is the control plane's record of one unit of work.
type Job struct {
	ID           string
	Source       string
	ExternalID   int64
	Repo         string
	RunID        int64
	Labels       []string
	State        JobState
	Attempt      int
	WorkerID     string
	DeadLettered bool
	Reason       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Transition is one row of append-only state history (FR-9).
type Transition struct {
	JobID   string
	Attempt int
	From    JobState
	To      JobState
	Reason  string
	At      time.Time
}
