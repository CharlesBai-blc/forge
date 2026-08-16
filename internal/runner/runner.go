// Package runner is forge-agent's claim loop: HTTP claim, sandbox, status.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

// Runner claims jobs from the control plane and runs each in a fresh sandbox.
type Runner struct {
	Client   *api.Client
	Provider sandbox.Provider
	Log      *slog.Logger
}

// Run claims jobs until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	if r.Log == nil {
		r.Log = slog.Default()
	}
	for {
		cl, err := r.Client.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, api.ErrNoJob) {
				continue
			}
			r.Log.Error("claim", "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		r.runOne(ctx, cl)
	}
}

// runOne runs one claimed job. Destroy runs on every exit path (FR-13).
func (r *Runner) runOne(ctx context.Context, cl *api.ClaimResponse) {
	report := func(rep api.StatusReport) {
		if err := r.Client.Status(context.Background(), cl.JobID, cl.Attempt, rep); err != nil {
			r.Log.Error("status", "job", cl.JobID, "err", err)
		}
	}

	sb, err := r.Provider.Create(ctx, cl.Spec)
	if err != nil {
		report(api.StatusReport{State: job.JobFailed, Reason: "sandbox_error: " + err.Error()})
		return
	}
	defer func() {
		if err := sb.Destroy(context.Background()); err != nil {
			r.Log.Error("destroy", "job", cl.JobID, "err", err)
		}
	}()

	if err := sb.Start(ctx, cl.JIT); err != nil {
		report(api.StatusReport{State: job.JobFailed, Reason: "start: " + err.Error()})
		return
	}
	report(api.StatusReport{State: job.JobRunning})

	code, waitErr := sb.Wait(ctx)
	r.uploadLogs(context.Background(), sb, cl)
	if waitErr != nil {
		report(api.StatusReport{State: job.JobFailed, Reason: waitErr.Error()})
		return
	}
	if code == 0 {
		report(api.StatusReport{State: job.JobSucceeded})
		return
	}
	c := code
	report(api.StatusReport{State: job.JobFailed, ExitCode: &c, Reason: fmt.Sprintf("exit %d", code)})
}

func (r *Runner) uploadLogs(ctx context.Context, sb sandbox.Sandbox, cl *api.ClaimResponse) {
	rc, err := sb.Logs(ctx)
	if err != nil {
		r.Log.Error("logs", "job", cl.JobID, "err", err)
		return
	}
	defer rc.Close()
	if err := r.Client.Logs(ctx, cl.JobID, cl.Attempt, rc); err != nil {
		r.Log.Error("logs upload", "job", cl.JobID, "err", err)
	}
}
