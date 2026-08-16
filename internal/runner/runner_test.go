package runner

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/store"
	"github.com/CharlesBai-blc/forge/internal/stream"
	"github.com/alicebob/miniredis/v2"
)

type fakeSandbox struct {
	id       string
	startErr error
	exitCode int
	waitErr  error
	started  bool
	destroys int
	jit      string
	logs     string
}

func (s *fakeSandbox) ID() string { return s.id }

func (s *fakeSandbox) Start(_ context.Context, jit string) error {
	if s.started {
		return fmt.Errorf("already started")
	}
	s.jit = jit
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
	return io.NopCloser(strings.NewReader(s.logs)), nil
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

type fakeSource struct {
	mu          sync.Mutex
	jit         *source.JITConfig
	registerErr error
	registers   int
	unregisters []int64
}

func (s *fakeSource) VerifyAndParse(*http.Request) ([]source.JobEvent, error) {
	return nil, nil
}

func (s *fakeSource) RegisterJIT(context.Context, *job.Job) (*source.JITConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registers++
	if s.registerErr != nil {
		return nil, s.registerErr
	}
	if s.jit == nil {
		s.jit = &source.JITConfig{RunnerID: 1, Encoded: "jit-blob"}
	}
	return s.jit, nil
}

func (s *fakeSource) Unregister(_ context.Context, runnerID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unregisters = append(s.unregisters, runnerID)
	return nil
}

func (s *fakeSource) ListQueued(context.Context) ([]source.JobEvent, error) {
	return nil, nil
}

func (s *fakeSource) registerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registers
}

func (s *fakeSource) unregisterIDs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.unregisters))
	copy(out, s.unregisters)
	return out
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

func startRunner(t *testing.T, r *Runner) context.CancelFunc {
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
	return cancel
}

func openHarness(t *testing.T, p sandbox.Provider) (*store.Store, *stream.Stream, *Runner, *fakeSource, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(dir, "forge.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	src := &fakeSource{jit: &source.JITConfig{RunnerID: 1, Encoded: "jit-blob"}}
	mr := miniredis.RunT(t)
	jobs, err := stream.Open(context.Background(), mr.Addr())
	if err != nil {
		t.Fatalf("stream.Open: %v", err)
	}
	t.Cleanup(func() { jobs.Close() })
	logDir := filepath.Join(dir, "logs")
	h := &api.Handler{
		Stream:    jobs,
		Store:     st,
		Source:    src,
		Token:     "tok",
		Image:     "alpine:3.20",
		Command:   []string{"true"},
		LogDir:    logDir,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		ClaimWait: 50 * time.Millisecond,
	}
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	r := &Runner{
		Client:   &api.Client{BaseURL: srv.URL, Token: "tok", WorkerID: "w1", HTTP: srv.Client()},
		Provider: p,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return st, jobs, r, src, logDir
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
	st, jobs, r, src, _ := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-ok", 1)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got := waitState(t, st, j.ID, job.JobSucceeded)
	if p.sb.destroys != 1 {
		t.Errorf("destroys = %d, want 1", p.sb.destroys)
	}
	if p.sb.jit != "jit-blob" {
		t.Errorf("Start jit = %q, want jit-blob", p.sb.jit)
	}
	if n := src.registerCount(); n != 1 {
		t.Errorf("RegisterJIT calls = %d, want 1", n)
	}
	if ids := src.unregisterIDs(); len(ids) != 0 {
		t.Errorf("Unregister = %v, want none after Start", ids)
	}
	if got.State != job.JobSucceeded {
		t.Errorf("State = %s, want succeeded", got.State)
	}
}

func TestRunNonzeroExitFailed(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1", exitCode: 7}}
	st, jobs, r, _, _ := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-fail", 2)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
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
	st, jobs, r, src, _ := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-create", 3)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitState(t, st, j.ID, job.JobFailed)
	if ids := src.unregisterIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Unregister = %v, want [1]", ids)
	}
}

func TestStartErrorDestroysAndFails(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1", startErr: fmt.Errorf("start failed")}}
	st, jobs, r, src, _ := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-start", 4)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitState(t, st, j.ID, job.JobFailed)
	if p.sb.destroys != 1 {
		t.Errorf("destroys = %d, want 1", p.sb.destroys)
	}
	if ids := src.unregisterIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Unregister = %v, want [1]", ids)
	}
}

func TestRegisterJITErrorLeavesQueued(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1"}}
	st, jobs, r, src, _ := openHarness(t, p)
	src.registerErr = fmt.Errorf("github down")
	startRunner(t, r)

	j := testJob("job-jit", 5)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if src.registerCount() >= 1 {
			got, err := st.GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if got.State != job.JobQueued {
				t.Fatalf("State = %s, want queued", got.State)
			}
			if p.sb.started || p.sb.destroys != 0 {
				t.Fatalf("sandbox used on register failure")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("RegisterJIT not called")
}

func TestRunWritesLogs(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1", logs: "hello-forge\n"}}
	st, jobs, r, _, logDir := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-logs", 6)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitState(t, st, j.ID, job.JobSucceeded)
	b, err := os.ReadFile(filepath.Join(logDir, "job-logs-1.log"))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if string(b) != "hello-forge\n" {
		t.Errorf("log = %q, want hello-forge\\n", b)
	}
}

func TestRunCancelled(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1"}}
	_, _, r, _, _ := openHarness(t, p)
	cancel := startRunner(t, r)
	cancel()
}
