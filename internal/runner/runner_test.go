package runner

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/queue"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	"github.com/CharlesBai-blc/forge/internal/store"
)

type fakeSandbox struct {
	id       string
	startErr error
	exitCode int
	waitErr  error
	started  bool
	destroys int
}

func (s *fakeSandbox) ID() string { return s.id }

func (s *fakeSandbox) Start(context.Context) error {
	if s.started {
		return fmt.Errorf("already started")
	}
	if s.startErr != nil {
		return s.startErr
	}
	s.started = true
	return nil
}

func (s *fakeSandbox) Wait(context.Context) (int, error) {
	return s.exitCode, s.waitErr
}

func (s *fakeSandbox) Logs(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s *fakeSandbox) Destroy(context.Context) error {
	s.destroys++
	return nil
}

type fakeProvider struct {
	createErr error
	sb        *fakeSandbox
}

func (p *fakeProvider) Create(context.Context, sandbox.Spec) (sandbox.Sandbox, error) {
	if p.createErr != nil {
		return nil, p.createErr
	}
	if p.sb == nil {
		p.sb = &fakeSandbox{id: "sb-1"}
	}
	return p.sb, nil
}

func testJob(id string, external int64) *job.Job {
	return &job.Job{
		ID:         id,
		Source:     "github",
		ExternalID: external,
		Repo:       "owner/name",
		RunID:      1,
		State:      job.JobQueued,
	}
}

func startRunner(t *testing.T, r *Runner) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return cancel, done
}

func openHarness(t *testing.T, p sandbox.Provider) (*store.Store, *queue.Queue, *Runner) {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "forge.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	q := queue.New()
	r := &Runner{
		Queue:    q,
		Store:    st,
		Provider: p,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Image:    "alpine:3.20",
		Command:  []string{"true"},
	}
	return st, q, r
}

func waitState(t *testing.T, st *store.Store, id string, want job.JobState) *job.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := st.GetJob(context.Background(), id)
		if err == nil && got.State == want {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := st.GetJob(context.Background(), id)
	t.Fatalf("job %s did not reach %s: got %+v err=%v", id, want, got, err)
	return nil
}

func TestRunSucceeded(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1"}}
	st, q, r := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-ok", 1)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got := waitState(t, st, j.ID, job.JobSucceeded)
	if p.sb.destroys != 1 {
		t.Errorf("destroys = %d, want 1", p.sb.destroys)
	}
	if got.State != job.JobSucceeded {
		t.Errorf("State = %s, want succeeded", got.State)
	}
}

func TestRunNonzeroExitFailed(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1", exitCode: 7}}
	st, q, r := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-fail", 2)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got := waitState(t, st, j.ID, job.JobFailed)
	if p.sb.destroys != 1 {
		t.Errorf("destroys = %d, want 1", p.sb.destroys)
	}
	if got.Reason != "exit 7" {
		t.Errorf("Reason = %q, want exit 7", got.Reason)
	}
}

func TestCreateErrorFailsJob(t *testing.T) {
	p := &fakeProvider{createErr: fmt.Errorf("no docker")}
	st, q, r := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-create", 3)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitState(t, st, j.ID, job.JobFailed)
}

func TestStartErrorDestroysAndFails(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1", startErr: fmt.Errorf("start failed")}}
	st, q, r := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-start", 4)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitState(t, st, j.ID, job.JobFailed)
	if p.sb.destroys != 1 {
		t.Errorf("destroys = %d, want 1", p.sb.destroys)
	}
}

func TestRunCancelled(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1"}}
	_, _, r := openHarness(t, p)
	cancel, _ := startRunner(t, r)
	cancel()
	// Cleanup waits for Run to return.
}
