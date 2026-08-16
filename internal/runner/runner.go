// Package runner is the in-process claim loop: queue, JIT, sandbox, store.
package runner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/queue"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/store"
)

// Runner claims jobs and runs each to a terminal state in a fresh sandbox.
type Runner struct {
	Queue    *queue.Queue
	Store    *store.Store
	Provider sandbox.Provider
	Source   source.RunnerSource
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
// JIT is registered at claim time (FR-4, FR-5). Unconsumed credentials are unregistered.
func (r *Runner) runOne(ctx context.Context, j *job.Job) {
	storeCtx := context.Background()

	jit, err := r.Source.RegisterJIT(ctx, j)
	if err != nil {
		r.Log.Error("register jit", "job", j.ID, "err", err)
		if err := r.Queue.Enqueue(j); err != nil {
			r.Log.Error("requeue", "job", j.ID, "err", err)
			r.transition(storeCtx, j.ID, job.JobFailed, "register_jit: "+err.Error())
		}
		return
	}

	consumed := false
	defer func() {
		if consumed {
			return
		}
		if err := r.Source.Unregister(context.Background(), jit.RunnerID); err != nil {
			r.Log.Error("unregister", "job", j.ID, "runner", jit.RunnerID, "err", err)
		}
	}()

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

	if err := sb.Start(ctx, jit.Encoded); err != nil {
		r.transition(storeCtx, j.ID, job.JobLost, "start: "+err.Error())
		r.transition(storeCtx, j.ID, job.JobFailed, "start: "+err.Error())
		return
	}
	consumed = true

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
