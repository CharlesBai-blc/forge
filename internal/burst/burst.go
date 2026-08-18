// Package burst provisions and retires AWS burst workers from queue
// pressure (FR-21, FR-22, FR-23, tdd.md §4.10).
package burst

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/store"
)

// Defaults are tdd.md Appendix B.
const (
	DefaultUpWindow       = 120 * time.Second
	DefaultDownWindow     = 600 * time.Second
	DefaultDownThreshold  = 1
	DefaultMaxInstances   = 2
	DefaultMaxHoursPerDay = 12
	defaultEvery          = 30 * time.Second
)

// Terraform applies the bundled module with a desired instance count.
// Implemented by CLI; faked in tests.
type Terraform interface {
	Apply(ctx context.Context, count int, vars map[string]string) error
}

// Controller compares queue depth to idle fleet capacity and runs
// Terraform to add or remove burst instances. State that must survive
// a restart (desired count, instance-hours) lives in the store's
// burst_events ledger; sustained-window tracking is in memory, so a
// restart at worst re-waits one window.
type Controller struct {
	Store           *store.Store
	Terraform       Terraform
	Log             *slog.Logger
	ControlPlaneURL string // reachable from AWS; passed to cloud-init

	UpWindow       time.Duration // depth > capacity this long triggers scale-up (FR-21)
	DownWindow     time.Duration // depth < DownThreshold this long triggers scale-down (FR-22)
	DownThreshold  int
	MaxInstances   int     // FR-23
	MaxHoursPerDay float64 // FR-23
	Every          time.Duration
	Now            func() time.Time // injectable clock for tests

	mu         sync.Mutex
	overSince  time.Time
	underSince time.Time
	draining   string // worker ID mid scale-down; empty when none
	banner     string
}

// Status is the dashboard's burst panel (FR-23, FR-24).
type Status struct {
	Instances      int
	MaxInstances   int
	HoursToday     float64
	MaxHoursPerDay float64
	Banner         string
}

func (c *Controller) log() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.Default()
}

func (c *Controller) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Controller) upWindow() time.Duration {
	if c.UpWindow > 0 {
		return c.UpWindow
	}
	return DefaultUpWindow
}

func (c *Controller) downWindow() time.Duration {
	if c.DownWindow > 0 {
		return c.DownWindow
	}
	return DefaultDownWindow
}

func (c *Controller) downThreshold() int {
	if c.DownThreshold > 0 {
		return c.DownThreshold
	}
	return DefaultDownThreshold
}

func (c *Controller) maxInstances() int {
	if c.MaxInstances > 0 {
		return c.MaxInstances
	}
	return DefaultMaxInstances
}

func (c *Controller) maxHoursPerDay() float64 {
	if c.MaxHoursPerDay > 0 {
		return c.MaxHoursPerDay
	}
	return DefaultMaxHoursPerDay
}

// Run evaluates until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	every := c.Every
	if every <= 0 {
		every = defaultEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Evaluate(ctx); err != nil && ctx.Err() == nil {
				c.log().Error("burst evaluate", "err", err)
			}
		}
	}
}

// Status returns the current burst panel data.
func (c *Controller) Status(ctx context.Context) Status {
	count, err := c.Store.BurstCount(ctx)
	if err != nil {
		c.log().Error("burst status count", "err", err)
	}
	hours, err := c.hoursToday(ctx, c.now())
	if err != nil {
		c.log().Error("burst status hours", "err", err)
	}
	c.mu.Lock()
	banner := c.banner
	c.mu.Unlock()
	return Status{
		Instances:      count,
		MaxInstances:   c.maxInstances(),
		HoursToday:     hours,
		MaxHoursPerDay: c.maxHoursPerDay(),
		Banner:         banner,
	}
}

// Evaluate is one tick: track sustained windows and act when one
// elapses. Exported so tests drive it with an injected clock.
func (c *Controller) Evaluate(ctx context.Context) error {
	now := c.now()
	depth, err := c.Store.CountQueued(ctx)
	if err != nil {
		return err
	}
	capacity, err := c.idleCapacity(ctx)
	if err != nil {
		return err
	}
	count, err := c.Store.BurstCount(ctx)
	if err != nil {
		return err
	}
	instancesGauge.Set(float64(count))

	c.mu.Lock()
	if depth > capacity {
		if c.overSince.IsZero() {
			c.overSince = now
		}
	} else {
		c.overSince = time.Time{}
	}
	if depth < c.downThreshold() {
		if c.underSince.IsZero() {
			c.underSince = now
		}
	} else if c.draining == "" {
		// A drain in progress finishes even if load returns: the
		// worker is already draining and its jobs are finishing
		// elsewhere or on it (FR-22 semantics).
		c.underSince = time.Time{}
	}
	scaleUp := !c.overSince.IsZero() && now.Sub(c.overSince) >= c.upWindow()
	scaleDown := c.draining != "" ||
		(!c.underSince.IsZero() && now.Sub(c.underSince) >= c.downWindow() && count > 0)
	c.mu.Unlock()

	if scaleUp {
		if err := c.scaleUp(ctx, count, now); err != nil {
			return err
		}
	}
	if scaleDown {
		if err := c.scaleDown(ctx); err != nil {
			return err
		}
	}
	return nil
}

// idleCapacity is spare slots on live workers: capacity minus assigned
// and running jobs, over active healthy workers.
func (c *Controller) idleCapacity(ctx context.Context) (int, error) {
	workers, err := c.Store.ListWorkers(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, w := range workers {
		if w.State != job.WorkerActive || !w.Healthy {
			continue
		}
		busy := 0
		for _, s := range []job.JobState{job.JobAssigned, job.JobRunning} {
			jobs, err := c.Store.JobsByWorker(ctx, w.ID, s)
			if err != nil {
				return 0, err
			}
			busy += len(jobs)
		}
		if spare := w.Capacity - busy; spare > 0 {
			total += spare
		}
	}
	return total, nil
}

// scaleUp checks caps, pre-issues a burst enrollment token, and applies
// count+1. A failure or cap hit raises the banner and re-waits a full
// window before retrying (tdd.md §6.7).
func (c *Controller) scaleUp(ctx context.Context, count int, now time.Time) error {
	c.resetOver()
	if count >= c.maxInstances() {
		c.setBanner(fmt.Sprintf("max instances cap reached (%d)", c.maxInstances()))
		capHits.Inc()
		return nil
	}
	hours, err := c.hoursToday(ctx, now)
	if err != nil {
		return err
	}
	if hours >= c.maxHoursPerDay() {
		c.setBanner(fmt.Sprintf("daily instance-hours cap reached (%.1fh of %.1fh)", hours, c.maxHoursPerDay()))
		capHits.Inc()
		return nil
	}
	tok, err := c.Store.IssueBurstEnrollmentToken(ctx)
	if err != nil {
		return err
	}
	if err := c.Terraform.Apply(ctx, count+1, map[string]string{
		"control_plane_url": c.ControlPlaneURL,
		"enroll_token":      tok,
	}); err != nil {
		c.setBanner("terraform apply failed: " + err.Error())
		applyFailures.Inc()
		c.log().Error("burst scale up", "err", err)
		return nil
	}
	if err := c.Store.AppendBurstEvent(ctx, count+1); err != nil {
		return err
	}
	instancesGauge.Set(float64(count + 1))
	c.setBanner("")
	c.log().Info("burst scale up", "instances", count+1)
	return nil
}

// scaleDown drains the newest burst worker, then, once its jobs are
// done, removes it and applies count-1. Instances are count-indexed,
// so decrementing destroys the newest (FR-22). An instance that never
// enrolled has no worker to drain and is destroyed directly (tdd.md §6.7).
func (c *Controller) scaleDown(ctx context.Context) error {
	c.mu.Lock()
	draining := c.draining
	c.mu.Unlock()

	if draining == "" {
		w, ok, err := c.Store.NewestBurstWorker(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return c.destroyNewest(ctx, "")
		}
		switch w.State {
		case job.WorkerActive:
			if err := c.Store.TransitionWorker(ctx, w.ID, job.WorkerDraining); err != nil {
				return err
			}
			c.log().Info("burst drain", "worker", w.ID)
		case job.WorkerDraining, job.WorkerCordoned, job.WorkerLost:
			// Already on its way out; track it below.
		default:
			return nil
		}
		c.mu.Lock()
		c.draining = w.ID
		c.mu.Unlock()
		return nil
	}

	// Drain in progress: the sweeper moves draining -> cordoned once
	// running jobs finish (FR-19); a lost worker's jobs are reclaimed.
	w, err := c.Store.GetWorker(ctx, draining)
	if err != nil {
		return err
	}
	switch w.State {
	case job.WorkerDraining:
		return nil // still draining
	case job.WorkerCordoned, job.WorkerLost:
		if err := c.Store.RemoveWorker(ctx, draining); err != nil {
			return err
		}
	case job.WorkerRemoved:
		// Removed out of band (dashboard); still destroy the instance.
	default:
		// Back to active (operator uncordoned): abandon the scale-down.
		c.mu.Lock()
		c.draining = ""
		c.mu.Unlock()
		return nil
	}
	return c.destroyNewest(ctx, draining)
}

func (c *Controller) destroyNewest(ctx context.Context, drained string) error {
	count, err := c.Store.BurstCount(ctx)
	if err != nil {
		return err
	}
	if count == 0 {
		c.clearDrain()
		return nil
	}
	if err := c.Terraform.Apply(ctx, count-1, map[string]string{
		"control_plane_url": c.ControlPlaneURL,
	}); err != nil {
		c.setBanner("terraform apply failed: " + err.Error())
		applyFailures.Inc()
		c.log().Error("burst scale down", "err", err)
		return nil // retry next tick; the worker stays drained
	}
	if err := c.Store.AppendBurstEvent(ctx, count-1); err != nil {
		return err
	}
	instancesGauge.Set(float64(count - 1))
	c.setBanner("")
	c.clearDrain()
	c.log().Info("burst scale down", "instances", count-1, "worker", drained)
	return nil
}

// clearDrain finishes a scale-down: one instance per down-window, so
// the window restarts.
func (c *Controller) clearDrain() {
	c.mu.Lock()
	c.draining = ""
	c.underSince = time.Time{}
	c.mu.Unlock()
}

func (c *Controller) resetOver() {
	c.mu.Lock()
	c.overSince = time.Time{}
	c.mu.Unlock()
}

func (c *Controller) setBanner(s string) {
	c.mu.Lock()
	c.banner = s
	c.mu.Unlock()
}

// hoursToday integrates instance-hours since UTC midnight from the
// burst_events ledger (FR-23).
func (c *Controller) hoursToday(ctx context.Context, now time.Time) (float64, error) {
	now = now.UTC()
	midnight := now.Truncate(24 * time.Hour)
	baseline, events, err := c.Store.BurstEventsSince(ctx, midnight)
	if err != nil {
		return 0, err
	}
	var acc time.Duration
	t, cur := midnight, baseline
	for _, e := range events {
		if e.At.After(now) {
			break
		}
		acc += time.Duration(cur) * e.At.Sub(t)
		t, cur = e.At, e.Count
	}
	acc += time.Duration(cur) * now.Sub(t)
	return acc.Hours(), nil
}
