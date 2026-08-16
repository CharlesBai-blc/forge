// Package runner is the M1 in-process claim loop: queue, sandbox, store.
package runner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/queue"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	"github.com/CharlesBai-blc/forge/internal/store"
)

// Runner claims jobs and runs each to a terminal state in a fresh sandbox.
type Runner struct {
	Queue    *queue.Queue
	Store    *store.Store
	Provider sandbox.Provider
	Log      *slog.Logger

	Image   string
	Command []string
}

// Run claims jobs until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	if r.Log == nil {
		r.Log = slog.Default()
	}
	for {
		j, err := r.Queue.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.Log.Error("claim", "err", err)
			continue
		}
		r.runOne(ctx, j)
	}
}

// runOne runs one job to a terminal state. Destroy runs on every exit path (FR-13).
func (r *Runner) runOne(ctx context.Context, j *job.Job) {
	storeCtx := context.Background()
	spec := sandbox.Spec{Image: r.Image, Command: r.Command}

	sb, err := r.Provider.Create(ctx, spec)
	if err != nil {
		r.transition(storeCtx, j.ID, job.JobFailed, "sandbox_error: "+err.Error())
		return
	}
	defer func() {
		if err := sb.Destroy(context.Background()); err != nil {
			r.Log.Error("destroy", "job", j.ID, "err", err)
		}
	}()

	r.transition(storeCtx, j.ID, job.JobAssigned, "")

	if err := sb.Start(ctx); err != nil {
		r.transition(storeCtx, j.ID, job.JobLost, "start: "+err.Error())
		r.transition(storeCtx, j.ID, job.JobFailed, "start: "+err.Error())
		return
	}

	r.transition(storeCtx, j.ID, job.JobRunning, "")

	code, err := sb.Wait(ctx)
	if err != nil {
		r.transition(storeCtx, j.ID, job.JobFailed, err.Error())
		return
	}
	if code == 0 {
		r.transition(storeCtx, j.ID, job.JobSucceeded, "")
		return
	}
	r.transition(storeCtx, j.ID, job.JobFailed, fmt.Sprintf("exit %d", code))
}

func (r *Runner) transition(ctx context.Context, id string, to job.JobState, reason string) {
	if err := r.Store.Transition(ctx, id, to, reason); err != nil {
		r.Log.Error("transition", "job", id, "to", to, "err", err)
	}
}
