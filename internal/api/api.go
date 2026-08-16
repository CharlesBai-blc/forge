// Package api is the agent HTTP contract (tdd.md §4.6).
package api

import (
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
