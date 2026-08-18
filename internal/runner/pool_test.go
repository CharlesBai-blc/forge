package runner

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

// poolSandbox is a warm-capable fake. All state is mutex-guarded since
// the pool's refill goroutine races the test goroutine.
type poolSandbox struct {
	id   string
	spec sandbox.Spec

	mu        sync.Mutex
	warmed    bool
	started   bool
	destroyed bool
}

func (s *poolSandbox) ID() string { return s.id }

func (s *poolSandbox) WarmStart(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.warmed || s.started {
		return fmt.Errorf("already started")
	}
	s.warmed = true
	return nil
}

func (s *poolSandbox) Start(_ context.Context, jit string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return fmt.Errorf("already started")
	}
	s.started = true
	return nil
}

func (s *poolSandbox) Wait(context.Context) (int, error) { return 0, nil }

func (s *poolSandbox) Logs(context.Context, bool) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s *poolSandbox) Destroy(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.destroyed = true
	return nil
}

func (s *poolSandbox) isWarmed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.warmed
}

func (s *poolSandbox) isDestroyed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.destroyed
}

type poolProvider struct {
	mu      sync.Mutex
	created []*poolSandbox
}

func (p *poolProvider) Create(_ context.Context, spec sandbox.Spec) (sandbox.Sandbox, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sb := &poolSandbox{id: fmt.Sprintf("warm-%d", len(p.created)+1), spec: spec}
	p.created = append(p.created, sb)
	return sb, nil
}

func (p *poolProvider) all() []*poolSandbox {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*poolSandbox, len(p.created))
	copy(out, p.created)
	return out
}

func startPool(t *testing.T, pool *Pool) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("pool.Run did not return after cancel")
		}
	})
	return cancel
}

func waitCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func poolSpec() sandbox.Spec {
	return sandbox.Spec{Image: "img", Command: []string{"run"}, Hardened: true}
}

func TestPoolFillsToSizeAndWarmStarts(t *testing.T) {
	p := &poolProvider{}
	pool := &Pool{Provider: p, Size: 2}
	pool.SetSpec(poolSpec())
	startPool(t, pool)

	waitCond(t, "pool filled", func() bool { return pool.idleCount() == 2 })
	for _, sb := range p.all() {
		if !sb.isWarmed() {
			t.Errorf("sandbox %s not warm-started", sb.ID())
		}
	}
}

func TestPoolTakeIsSingleUseAndRefills(t *testing.T) {
	p := &poolProvider{}
	pool := &Pool{Provider: p, Size: 1}
	pool.SetSpec(poolSpec())
	startPool(t, pool)
	waitCond(t, "pool filled", func() bool { return pool.idleCount() == 1 })

	sb1, ok := pool.Take(poolSpec())
	if !ok {
		t.Fatal("Take returned no sandbox")
	}
	waitCond(t, "pool refilled", func() bool { return pool.idleCount() == 1 })
	sb2, ok := pool.Take(poolSpec())
	if !ok {
		t.Fatal("second Take returned no sandbox")
	}
	if sb1.ID() == sb2.ID() {
		t.Fatalf("pool handed out sandbox %s twice (FR-13)", sb1.ID())
	}
}

func TestPoolTakeRejectsSpecMismatch(t *testing.T) {
	p := &poolProvider{}
	pool := &Pool{Provider: p, Size: 1}
	pool.SetSpec(poolSpec())
	startPool(t, pool)
	waitCond(t, "pool filled", func() bool { return pool.idleCount() == 1 })

	other := poolSpec()
	other.Image = "other-img"
	if _, ok := pool.Take(other); ok {
		t.Fatal("Take matched a sandbox built from a different spec")
	}
}

func TestPoolSpecChangeDestroysStaleIdle(t *testing.T) {
	p := &poolProvider{}
	pool := &Pool{Provider: p, Size: 1}
	pool.SetSpec(poolSpec())
	startPool(t, pool)
	waitCond(t, "pool filled", func() bool { return pool.idleCount() == 1 })
	old := p.all()[0]

	next := poolSpec()
	next.Image = "next-img"
	pool.SetSpec(next)

	waitCond(t, "stale sandbox destroyed", old.isDestroyed)
	waitCond(t, "pool refilled with new spec", func() bool {
		if pool.idleCount() != 1 {
			return false
		}
		sb, ok := pool.Take(next)
		if ok {
			// Put nothing back: verify then let the pool refill.
			_ = sb.Destroy(context.Background())
		}
		return ok
	})
}

func TestPoolDrainsOnCancel(t *testing.T) {
	p := &poolProvider{}
	pool := &Pool{Provider: p, Size: 2}
	pool.SetSpec(poolSpec())
	cancel := startPool(t, pool)
	waitCond(t, "pool filled", func() bool { return pool.idleCount() == 2 })

	cancel()
	waitCond(t, "idle sandboxes destroyed", func() bool {
		for _, sb := range p.all() {
			if !sb.isDestroyed() {
				return false
			}
		}
		return true
	})
}

// warmProvider adapts poolProvider to the full sandbox.Sandbox interface
// used by the runner harness: its sandboxes carry fakeSandbox behavior
// plus WarmStart.
type warmRunnerSandbox struct {
	fakeSandbox
	mu2    sync.Mutex
	warmed bool
	ran    bool
}

func (s *warmRunnerSandbox) WarmStart(context.Context) error {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	if s.warmed {
		return fmt.Errorf("already warmed")
	}
	s.warmed = true
	return nil
}

func (s *warmRunnerSandbox) Start(ctx context.Context, jit string) error {
	s.mu2.Lock()
	s.ran = true
	s.mu2.Unlock()
	return s.fakeSandbox.Start(ctx, jit)
}

func (s *warmRunnerSandbox) isWarmed() bool {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	return s.warmed
}

func (s *warmRunnerSandbox) didRun() bool {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	return s.ran
}

type warmProvider struct {
	mu      sync.Mutex
	created []*warmRunnerSandbox
}

func (p *warmProvider) Create(_ context.Context, spec sandbox.Spec) (sandbox.Sandbox, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sb := &warmRunnerSandbox{fakeSandbox: fakeSandbox{id: fmt.Sprintf("wsb-%d", len(p.created)+1)}}
	p.created = append(p.created, sb)
	return sb, nil
}

func (p *warmProvider) first() *warmRunnerSandbox {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.created[0]
}

// TestRunnerUsesWarmSandbox runs a job end to end through the harness
// with a warm pool attached and verifies the job executed in the
// pre-started sandbox rather than a cold create (FR-16).
func TestRunnerUsesWarmSandbox(t *testing.T) {
	p := &warmProvider{}
	st, jobs, r, _, _ := openHarness(t, p)

	spec, err := r.Client.Spec(context.Background())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}
	pool := &Pool{Provider: p, Size: 1, Log: r.Log}
	pool.SetSpec(*spec)
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()
	go pool.Run(poolCtx)
	waitCond(t, "pool warmed", func() bool { return pool.idleCount() == 1 })
	warmed := p.first()

	r.Pool = pool
	startRunner(t, r)

	j := testJob("job-warm", 42)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitState(t, st, j.ID, job.JobSucceeded)
	if !warmed.isWarmed() {
		t.Error("job's sandbox was not warm-started")
	}
	if !warmed.didRun() {
		t.Error("warm sandbox was not the one that ran the job")
	}
	waitDestroyed(t, &warmed.fakeSandbox, 1)
}
