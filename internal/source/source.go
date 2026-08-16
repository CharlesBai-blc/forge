// Package source is the CI provider seam (RunnerSource: FR-4, FR-5).
package source

import (
	"context"
	"net/http"

	"github.com/CharlesBai-blc/forge/internal/job"
)

// JobEvent is a provider-side job notification, normalized.
type JobEvent struct {
	Kind       string // queued | in_progress | completed
	ExternalID int64
	Repo       string
	RunID      int64
	Labels     []string
	Conclusion string // set when Kind == completed
}

// JITConfig is a single-use runner credential bound to one job attempt.
type JITConfig struct {
	RunnerID int64
	Encoded  string
}

// RunnerSource is the seam between Forge and a CI provider.
type RunnerSource interface {
	VerifyAndParse(r *http.Request) ([]JobEvent, error)
	RegisterJIT(ctx context.Context, j *job.Job) (*JITConfig, error)
	Unregister(ctx context.Context, runnerID int64) error
	ListQueued(ctx context.Context) ([]JobEvent, error)
}
