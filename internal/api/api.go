// Package api is the agent HTTP contract (tdd.md §4.6).
package api

import (
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

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
