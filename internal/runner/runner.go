// Package runner is forge-agent's claim loop: HTTP claim, sandbox, status.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

const (
	defaultHeartbeat = 10 * time.Second
	defaultCapacity  = 1
)

// Runner claims jobs from the control plane and runs each in a fresh sandbox.
type Runner struct {
	Client         *api.Client
	Provider       sandbox.Provider
	Log            *slog.Logger
	HeartbeatEvery time.Duration

	mu      sync.Mutex
	current string
	healthy bool
}

// Run claims jobs until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	if r.Log == nil {
		r.Log = slog.Default()
	}
	r.setHealthy(true)
	go r.heartbeatLoop(ctx)
	for {
		if !r.isHealthy() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.heartbeatEvery()):
			}
			continue
		}
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
		r.setCurrent(cl.JobID)
		r.runOne(ctx, cl)
		r.setCurrent("")
	}
}

func (r *Runner) heartbeatEvery() time.Duration {
	if r.HeartbeatEvery > 0 {
		return r.HeartbeatEvery
	}
	return defaultHeartbeat
}

func (r *Runner) heartbeatLoop(ctx context.Context) {
	r.sendHeartbeat(ctx)
	t := time.NewTicker(r.heartbeatEvery())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sendHeartbeat(ctx)
		}
	}
}

func (r *Runner) sendHeartbeat(ctx context.Context) {
	ok := r.dockerHealthy(ctx)
	r.setHealthy(ok)
	hb := api.Heartbeat{Capacity: defaultCapacity, Healthy: ok}
	if id := r.currentJob(); id != "" {
		hb.Running = []string{id}
	}
	if err := r.Client.Heartbeat(ctx, hb); err != nil && ctx.Err() == nil {
		r.Log.Error("heartbeat", "err", err)
	}
}

func (r *Runner) dockerHealthy(ctx context.Context) bool {
	p, ok := r.Provider.(interface{ Ping(context.Context) error })
	if !ok {
		return true
	}
	return p.Ping(ctx) == nil
}

func (r *Runner) setHealthy(v bool) {
	r.mu.Lock()
	r.healthy = v
	r.mu.Unlock()
}

func (r *Runner) isHealthy() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.healthy
}

func (r *Runner) setCurrent(id string) {
	r.mu.Lock()
	r.current = id
	r.mu.Unlock()
}

func (r *Runner) currentJob() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current
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
