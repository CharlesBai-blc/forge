package runner

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

const defaultPoolRetry = 30 * time.Second

// Pool keeps up to Size pre-created, pre-started sandboxes so a claim
// only injects the JIT credential (FR-16, tdd.md §4.5). Pooled
// sandboxes are ordinary sandbox.Sandbox values: taken at most once,
// destroyed after their one job, so FR-13's single-use invariant is
// unchanged. v1.0 has one configured label set, so one pool suffices.
type Pool struct {
	Provider sandbox.Provider
	Size     int
	Log      *slog.Logger
	// RetryEvery bounds how long a failed refill (e.g. Docker down)
	// waits before retrying. Zero means the default.
	RetryEvery time.Duration

	mu    sync.Mutex
	spec  sandbox.Spec
	valid bool
	idle  []sandbox.Sandbox
	wake  chan struct{}
}

func (p *Pool) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

func (p *Pool) notify() {
	p.mu.Lock()
	if p.wake == nil {
		p.wake = make(chan struct{}, 1)
	}
	ch := p.wake
	p.mu.Unlock()
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (p *Pool) wakeCh() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wake == nil {
		p.wake = make(chan struct{}, 1)
	}
	return p.wake
}

// SetSpec records the sandbox configuration to warm. A change destroys
// idle sandboxes built from the old spec and triggers a refill.
func (p *Pool) SetSpec(spec sandbox.Spec) {
	p.mu.Lock()
	if p.valid && specEqual(p.spec, spec) {
		p.mu.Unlock()
		return
	}
	stale := p.idle
	p.idle = nil
	p.spec = spec
	p.valid = true
	p.mu.Unlock()
	for _, sb := range stale {
		if err := sb.Destroy(context.Background()); err != nil {
			p.log().Error("pool destroy stale", "sandbox", sb.ID(), "err", err)
		}
	}
	p.setIdleGauge()
	p.notify()
}

// Take pops a warm sandbox if one matches spec. The sandbox leaves the
// pool permanently; a refill is triggered asynchronously.
func (p *Pool) Take(spec sandbox.Spec) (sandbox.Sandbox, bool) {
	p.mu.Lock()
	if !p.valid || !specEqual(p.spec, spec) || len(p.idle) == 0 {
		p.mu.Unlock()
		return nil, false
	}
	sb := p.idle[len(p.idle)-1]
	p.idle = p.idle[:len(p.idle)-1]
	p.mu.Unlock()
	p.setIdleGauge()
	p.notify()
	return sb, true
}

// Run refills the pool until ctx is cancelled, then destroys all idle
// sandboxes. Sandboxes are single-use, so shutdown never hands one to
// another process; it removes them.
func (p *Pool) Run(ctx context.Context) {
	retry := p.RetryEvery
	if retry <= 0 {
		retry = defaultPoolRetry
	}
	wake := p.wakeCh()
	for {
		p.fill(ctx)
		select {
		case <-ctx.Done():
			p.drain()
			return
		case <-wake:
		case <-time.After(retry):
		}
	}
}

// fill creates and warm-starts sandboxes until the pool holds Size.
// Creation happens outside the lock; if the spec changed meanwhile the
// new sandbox is destroyed rather than pooled.
func (p *Pool) fill(ctx context.Context) {
	for {
		p.mu.Lock()
		if !p.valid || len(p.idle) >= p.Size {
			p.mu.Unlock()
			return
		}
		spec := p.spec
		p.mu.Unlock()

		sb, err := p.Provider.Create(ctx, spec)
		if err != nil {
			if ctx.Err() == nil {
				p.log().Error("pool create", "err", err)
			}
			return
		}
		if w, ok := sb.(sandbox.Warmable); ok {
			if err := w.WarmStart(ctx); err != nil {
				if ctx.Err() == nil {
					p.log().Error("pool warm start", "err", err)
				}
				_ = sb.Destroy(context.Background())
				return
			}
		}

		p.mu.Lock()
		if p.valid && specEqual(p.spec, spec) && len(p.idle) < p.Size {
			p.idle = append(p.idle, sb)
			p.mu.Unlock()
			p.setIdleGauge()
			continue
		}
		p.mu.Unlock()
		_ = sb.Destroy(context.Background())
	}
}

func (p *Pool) drain() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.valid = false
	p.mu.Unlock()
	for _, sb := range idle {
		if err := sb.Destroy(context.Background()); err != nil {
			p.log().Error("pool drain destroy", "sandbox", sb.ID(), "err", err)
		}
	}
	p.setIdleGauge()
}

func (p *Pool) idleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

func specEqual(a, b sandbox.Spec) bool {
	return a.Image == b.Image &&
		slices.Equal(a.Command, b.Command) &&
		a.CPU == b.CPU &&
		a.MemoryBytes == b.MemoryBytes &&
		a.PIDs == b.PIDs &&
		a.DiskBytes == b.DiskBytes &&
		a.Hardened == b.Hardened
}
