package burst

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/store"
)

// fakeTF records applies and can be told to fail.
type fakeTF struct {
	mu      sync.Mutex
	applies []int
	vars    []map[string]string
	err     error
}

func (f *fakeTF) Apply(_ context.Context, count int, vars map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.applies = append(f.applies, count)
	f.vars = append(f.vars, vars)
	return nil
}

func (f *fakeTF) counts() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.applies...)
}

func (f *fakeTF) lastVars() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.vars) == 0 {
		return nil
	}
	return f.vars[len(f.vars)-1]
}

func (f *fakeTF) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

type fixture struct {
	st    *store.Store
	tf    *fakeTF
	c     *Controller
	clock time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "forge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f := &fixture{
		st:    st,
		tf:    &fakeTF{},
		clock: time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC),
	}
	f.c = &Controller{
		Store:           st,
		Terraform:       f.tf,
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		ControlPlaneURL: "https://forge.example:8080",
		UpWindow:        120 * time.Second,
		DownWindow:      600 * time.Second,
		MaxInstances:    2,
		MaxHoursPerDay:  12,
		Now:             func() time.Time { return f.clock },
	}
	return f
}

func (f *fixture) advance(d time.Duration) { f.clock = f.clock.Add(d) }

func (f *fixture) eval(t *testing.T) {
	t.Helper()
	if err := f.c.Evaluate(context.Background()); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
}

func (f *fixture) queueJobs(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		j := &job.Job{
			ID: fmt.Sprintf("j-%d-%d", f.clock.Unix(), i), Source: "github",
			ExternalID: f.clock.UnixNano() + int64(i), Repo: "o/r", RunID: 1,
			State: job.JobQueued,
		}
		if err := f.st.CreateJob(context.Background(), j); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *fixture) enrollBurstWorker(t *testing.T) string {
	t.Helper()
	vars := f.tf.lastVars()
	tok := vars["enroll_token"]
	if tok == "" {
		t.Fatal("no enroll_token passed to terraform")
	}
	id, _, err := f.st.Enroll(context.Background(), tok, "burst", "arm64", "test")
	if err != nil {
		t.Fatalf("Enroll with burst token: %v", err)
	}
	return id
}

func TestScaleUpAfterSustainedWindow(t *testing.T) {
	f := newFixture(t)
	f.queueJobs(t, 3) // no workers: depth > capacity immediately

	f.eval(t) // starts the window
	if got := f.tf.counts(); len(got) != 0 {
		t.Fatalf("applied before window elapsed: %v", got)
	}
	f.advance(60 * time.Second)
	f.eval(t)
	if got := f.tf.counts(); len(got) != 0 {
		t.Fatalf("applied at 60s of 120s window: %v", got)
	}
	f.advance(61 * time.Second)
	f.eval(t)
	got := f.tf.counts()
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("applies = %v, want [1]", got)
	}
	vars := f.tf.lastVars()
	if vars["control_plane_url"] != "https://forge.example:8080" || vars["enroll_token"] == "" {
		t.Fatalf("vars = %v", vars)
	}
	n, err := f.st.BurstCount(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("BurstCount = %d, %v", n, err)
	}

	// The pre-issued token enrolls a burst-flagged worker (FR-21).
	id := f.enrollBurstWorker(t)
	w, err := f.st.GetWorker(context.Background(), id)
	if err != nil || !w.Burst {
		t.Fatalf("burst worker = %+v, %v", w, err)
	}

	// A second scale-up needs a fresh full window.
	f.advance(30 * time.Second)
	f.eval(t)
	if got := f.tf.counts(); len(got) != 1 {
		t.Fatalf("scaled again without a fresh window: %v", got)
	}
}

func TestBacklogResetClearsWindow(t *testing.T) {
	f := newFixture(t)
	f.queueJobs(t, 1)
	f.eval(t)
	f.advance(100 * time.Second)

	// Backlog drains before the window elapses.
	if err := f.st.Transition(context.Background(), fmt.Sprintf("j-%d-0", f.clock.Add(-100*time.Second).Unix()), job.JobFailed, "test"); err != nil {
		t.Fatal(err)
	}
	f.eval(t) // depth 0 clears the over-window
	f.queueJobs(t, 1)
	f.eval(t) // backlog again: window restarts
	f.advance(100 * time.Second)
	f.eval(t)
	if got := f.tf.counts(); len(got) != 0 {
		t.Fatalf("applied without a sustained window: %v", got)
	}
}

func TestMaxInstancesCap(t *testing.T) {
	f := newFixture(t)
	f.c.MaxInstances = 1
	if err := f.st.AppendBurstEvent(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	f.queueJobs(t, 5)
	f.eval(t)
	f.advance(121 * time.Second)
	f.eval(t)
	if got := f.tf.counts(); len(got) != 0 {
		t.Fatalf("applied past the cap: %v", got)
	}
	s := f.c.Status(context.Background())
	if s.Banner == "" {
		t.Fatal("cap hit raised no banner")
	}
	if s.Instances != 1 || s.MaxInstances != 1 {
		t.Fatalf("status = %+v", s)
	}
}

func TestDailyHoursCap(t *testing.T) {
	f := newFixture(t)
	f.c.MaxHoursPerDay = 2
	if err := f.st.AppendBurstEvent(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	// The ledger row is stamped with wall-clock time. Move the injected
	// clock 3h past the next UTC midnight: the event is then the
	// baseline (1 instance since midnight), 3 instance-hours > 2h cap.
	f.clock = time.Now().UTC().Truncate(24 * time.Hour).Add(24*time.Hour + 3*time.Hour)

	f.queueJobs(t, 5)
	f.eval(t)
	f.advance(121 * time.Second)
	f.eval(t)
	if got := f.tf.counts(); len(got) != 0 {
		t.Fatalf("applied past the daily hours cap: %v", got)
	}
	s := f.c.Status(context.Background())
	if s.Banner == "" {
		t.Fatal("hours cap hit raised no banner")
	}
}

func TestApplyFailureBannerAndRetry(t *testing.T) {
	f := newFixture(t)
	f.queueJobs(t, 3)
	f.tf.setErr(fmt.Errorf("spot capacity"))

	f.eval(t)
	f.advance(121 * time.Second)
	f.eval(t)
	if n, _ := f.st.BurstCount(context.Background()); n != 0 {
		t.Fatalf("count advanced despite apply failure: %d", n)
	}
	if s := f.c.Status(context.Background()); s.Banner == "" {
		t.Fatal("apply failure raised no banner")
	}

	// Next full window retries and succeeds; the banner clears.
	f.tf.setErr(nil)
	f.eval(t)
	f.advance(121 * time.Second)
	f.eval(t)
	if got := f.tf.counts(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("retry applies = %v, want [1]", got)
	}
	if s := f.c.Status(context.Background()); s.Banner != "" {
		t.Fatalf("banner not cleared: %q", s.Banner)
	}
}

// TestScaleDownLifecycle covers the full FR-22 path: sustained low
// queue -> drain newest burst worker -> wait for drain -> remove ->
// terraform count-1.
func TestScaleDownLifecycle(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Scale up and enroll the burst worker.
	f.queueJobs(t, 3)
	f.eval(t)
	f.advance(121 * time.Second)
	f.eval(t)
	id := f.enrollBurstWorker(t)

	// Queue empties (depth 0 < threshold 1): the down-window accrues.
	for i := 0; i < 3; i++ {
		jobID := fmt.Sprintf("j-%d-%d", f.clock.Add(-121*time.Second).Unix(), i)
		if err := f.st.Transition(ctx, jobID, job.JobFailed, "test"); err != nil {
			t.Fatal(err)
		}
	}
	f.eval(t)
	f.advance(300 * time.Second)
	f.eval(t)
	w, err := f.st.GetWorker(ctx, id)
	if err != nil || w.State != job.WorkerActive {
		t.Fatalf("drained at 300s of 600s window: %s, %v", w.State, err)
	}

	f.advance(301 * time.Second)
	f.eval(t)
	w, err = f.st.GetWorker(ctx, id)
	if err != nil || w.State != job.WorkerDraining {
		t.Fatalf("worker state = %s, %v; want draining", w.State, err)
	}
	if got := f.tf.counts(); len(got) != 1 {
		t.Fatalf("destroyed before drain finished: %v", got)
	}

	// Sweeper-equivalent: drain completes (draining -> cordoned).
	if err := f.st.TransitionWorker(ctx, id, job.WorkerCordoned); err != nil {
		t.Fatal(err)
	}
	f.advance(30 * time.Second)
	f.eval(t)

	w, err = f.st.GetWorker(ctx, id)
	if err != nil || w.State != job.WorkerRemoved {
		t.Fatalf("worker state = %s, %v; want removed", w.State, err)
	}
	got := f.tf.counts()
	if len(got) != 2 || got[1] != 0 {
		t.Fatalf("applies = %v, want [1 0]", got)
	}
	if n, _ := f.st.BurstCount(ctx); n != 0 {
		t.Fatalf("BurstCount = %d, want 0", n)
	}
}

// TestScaleDownNeverEnrolled destroys an instance that never enrolled:
// there is no worker to drain (tdd.md §6.7).
func TestScaleDownNeverEnrolled(t *testing.T) {
	f := newFixture(t)
	if err := f.st.AppendBurstEvent(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	f.eval(t)
	f.advance(601 * time.Second)
	f.eval(t)
	got := f.tf.counts()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("applies = %v, want [0]", got)
	}
	if n, _ := f.st.BurstCount(context.Background()); n != 0 {
		t.Fatalf("BurstCount = %d, want 0", n)
	}
}

func TestHoursToday(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// The event lands at wall-clock now. Measure 30 minutes after the
	// next UTC midnight: the event is the baseline, so exactly
	// 2 instances x 0.5h = 1.0 instance-hour.
	if err := f.st.AppendBurstEvent(ctx, 2); err != nil {
		t.Fatal(err)
	}
	f.clock = time.Now().UTC().Truncate(24 * time.Hour).Add(24*time.Hour + 30*time.Minute)
	h, err := f.c.hoursToday(ctx, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	if h < 0.999 || h > 1.001 {
		t.Fatalf("hoursToday = %f, want 1.0", h)
	}
}
