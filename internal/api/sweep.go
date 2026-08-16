package api

import (
	"context"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

func (h *Handler) lostAfter() time.Duration {
	if h.LostAfter > 0 {
		return h.LostAfter
	}
	return defaultLostAfter
}

func (h *Handler) visibility() time.Duration {
	if h.Visibility > 0 {
		return h.Visibility
	}
	return defaultVisibility
}

func (h *Handler) maxAttempts() int {
	if h.MaxAttempts > 0 {
		return h.MaxAttempts
	}
	return defaultMaxAttempts
}

func (h *Handler) sweepEvery() time.Duration {
	if h.SweepEvery > 0 {
		return h.SweepEvery
	}
	return defaultSweepEvery
}

// RunSweep ticks until ctx is cancelled (tdd.md §4.9).
func (h *Handler) RunSweep(ctx context.Context) {
	t := time.NewTicker(h.sweepEvery())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := h.Sweep(ctx); err != nil {
				h.log().Error("sweep", "err", err)
			}
		}
	}
}

// Sweep marks stale workers lost, reclaims idle stream entries, and
// progresses drains (FR-11, FR-19, FR-20).
func (h *Handler) Sweep(ctx context.Context) error {
	if err := h.markLostWorkers(ctx); err != nil {
		return err
	}
	if err := h.reclaimIdle(ctx); err != nil {
		return err
	}
	return h.progressDrains(ctx)
}

func (h *Handler) markLostWorkers(ctx context.Context) error {
	workers, err := h.Store.ListWorkers(ctx)
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-h.lostAfter())
	for _, w := range workers {
		if w.State != job.WorkerActive && w.State != job.WorkerCordoned && w.State != job.WorkerDraining {
			continue
		}
		if !w.LastSeen.Before(cutoff) {
			continue
		}
		if err := h.Store.MarkLost(ctx, w.ID); err != nil {
			h.log().Error("mark lost", "worker", w.ID, "err", err)
		}
	}
	return nil
}

func (h *Handler) reclaimIdle(ctx context.Context) error {
	msgs, err := h.Stream.AutoClaim(ctx, h.visibility())
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		if err := h.reclaim(ctx, msg.JobID, msg.ID); err != nil {
			h.log().Error("reclaim", "job", msg.JobID, "err", err)
		}
	}
	return nil
}

func (h *Handler) reclaim(ctx context.Context, jobID, msgID string) error {
	j, err := h.Store.GetJob(ctx, jobID)
	if err != nil {
		return h.Stream.Ack(ctx, msgID)
	}
	switch j.State {
	case job.JobSucceeded, job.JobFailed:
		return h.Stream.Ack(ctx, msgID)
	case job.JobQueued:
		if err := h.Stream.Ack(ctx, msgID); err != nil {
			return err
		}
		return h.Stream.Add(ctx, jobID)
	}

	acquired := j.State == job.JobRunning
	if j.State != job.JobLost {
		if err := h.Store.Transition(ctx, jobID, job.JobLost, "visibility_timeout"); err != nil {
			return err
		}
		h.forgetJIT(jobID, j.Attempt, !acquired)
	}
	if acquired {
		if err := h.Store.Transition(ctx, jobID, job.JobFailed, "worker_lost"); err != nil {
			return err
		}
		h.forgetMsg(jobID)
		return h.Stream.Ack(ctx, msgID)
	}
	if j.Attempt >= h.maxAttempts() {
		if err := h.Store.DeadLetter(ctx, jobID, "max_attempts"); err != nil {
			return err
		}
		h.forgetMsg(jobID)
		return h.Stream.Ack(ctx, msgID)
	}
	if err := h.Store.Requeue(ctx, jobID); err != nil {
		return err
	}
	h.forgetMsg(jobID)
	if err := h.Stream.Ack(ctx, msgID); err != nil {
		return err
	}
	return h.Stream.Add(ctx, jobID)
}

func (h *Handler) forgetMsg(jobID string) {
	h.mu.Lock()
	delete(h.msgs, jobID)
	h.mu.Unlock()
}

func (h *Handler) progressDrains(ctx context.Context) error {
	workers, err := h.Store.ListWorkers(ctx)
	if err != nil {
		return err
	}
	for _, w := range workers {
		if w.State != job.WorkerDraining {
			continue
		}
		assigned, err := h.Store.JobsByWorker(ctx, w.ID, job.JobAssigned)
		if err != nil {
			return err
		}
		for _, j := range assigned {
			if err := h.drainAssigned(ctx, w.ID, j); err != nil {
				h.log().Error("drain requeue", "job", j.ID, "err", err)
			}
		}
		running, err := h.Store.JobsByWorker(ctx, w.ID, job.JobRunning)
		if err != nil {
			return err
		}
		if len(running) > 0 {
			continue
		}
		if err := h.Store.TransitionWorker(ctx, w.ID, job.WorkerCordoned); err != nil {
			h.log().Error("drain complete", "worker", w.ID, "err", err)
		}
	}
	return nil
}

func (h *Handler) drainAssigned(ctx context.Context, workerID string, j *job.Job) error {
	if err := h.Store.DrainRequeue(ctx, j.ID); err != nil {
		return err
	}
	h.forgetJIT(j.ID, j.Attempt, true)
	h.forgetMsg(j.ID)
	pending, err := h.Stream.PendingFor(ctx, workerID)
	if err != nil {
		return err
	}
	acked := false
	for _, msg := range pending {
		if msg.JobID != j.ID {
			continue
		}
		if err := h.Stream.Ack(ctx, msg.ID); err != nil {
			return err
		}
		acked = true
		break
	}
	if !acked {
		h.log().Error("drain ack missing", "job", j.ID, "worker", workerID)
	}
	return h.Stream.Add(ctx, j.ID)
}
