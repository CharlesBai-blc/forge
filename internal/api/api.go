// Package api is the agent HTTP contract (tdd.md §4.6) and dashboard (FR-24).
package api

import (
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

// EnrollRequest is the body of POST /v1/agents/enroll (FR-3).
type EnrollRequest struct {
	Token   string `json:"token"`
	Name    string `json:"name"`
	Arch    string `json:"arch"`
	Version string `json:"version"`
}

// EnrollResponse is the per-machine credential issued at enrollment.
type EnrollResponse struct {
	WorkerID string `json:"worker_id"`
	Token    string `json:"token"`
}

// ClaimResponse is delivered when the long-poll matches a job.
type ClaimResponse struct {
	JobID   string       `json:"job_id"`
	Attempt int          `json:"attempt"`
	JIT     string       `json:"jit"`
	Spec    sandbox.Spec `json:"spec"`
}

// StatusReport is a running or terminal update for one attempt.
type StatusReport struct {
	State    job.JobState `json:"state"`
	ExitCode *int         `json:"exit_code,omitempty"`
	Reason   string       `json:"reason,omitempty"`
}

// Heartbeat is the agent's liveness report (FR-18, FR-20).
type Heartbeat struct {
	Capacity int      `json:"capacity"`
	Running  []string `json:"running"`
	Healthy  bool     `json:"healthy"`
}

// Dashboard is GET /v1/dashboard (FR-24).
type Dashboard struct {
	QueueDepth int               `json:"queue_depth"`
	Jobs       []DashboardJob    `json:"jobs"`
	Workers    []DashboardWorker `json:"workers"`
	Burst      *BurstStatus      `json:"burst,omitempty"`
}

// BurstStatus is the dashboard's burst activity panel (FR-23, FR-24):
// desired instance count, caps, and any cap or apply-failure banner.
type BurstStatus struct {
	Instances      int     `json:"instances"`
	MaxInstances   int     `json:"max_instances"`
	HoursToday     float64 `json:"hours_today"`
	MaxHoursPerDay float64 `json:"max_hours_per_day"`
	Banner         string  `json:"banner,omitempty"`
}

// DashboardJob is one row of the job list.
type DashboardJob struct {
	ID           string       `json:"id"`
	Repo         string       `json:"repo"`
	State        job.JobState `json:"state"`
	Attempt      int          `json:"attempt"`
	WorkerID     string       `json:"worker_id,omitempty"`
	DeadLettered bool         `json:"dead_lettered"`
	Reason       string       `json:"reason,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
	DurationMS   int64        `json:"duration_ms"`
}

// DashboardWorker is one row of the fleet list. Token hashes are omitted (FR-27).
type DashboardWorker struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	State       job.WorkerState `json:"state"`
	Capacity    int             `json:"capacity"`
	Running     int             `json:"running"`
	Healthy     bool            `json:"healthy"`
	LastSeen    time.Time       `json:"last_seen"`
	Arch        string          `json:"arch"`
	Utilization float64         `json:"utilization"`
	Burst       bool            `json:"burst"`
}

// JobDetail is GET /v1/jobs/{id} (FR-9, FR-26).
type JobDetail struct {
	Job         DashboardJob          `json:"job"`
	Transitions []DashboardTransition `json:"transitions"`
}

// DashboardTransition is one append-only history row.
type DashboardTransition struct {
	Attempt int          `json:"attempt"`
	From    job.JobState `json:"from"`
	To      job.JobState `json:"to"`
	Reason  string       `json:"reason,omitempty"`
	At      time.Time    `json:"at"`
}
