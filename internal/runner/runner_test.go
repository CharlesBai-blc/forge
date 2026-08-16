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
	jit      string
	logs     string

	mu       sync.Mutex
	destroys int
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

func (s *fakeSandbox) Logs(context.Context, bool) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.logs)), nil
}

func (s *fakeSandbox) Destroy(context.Context) error {
	s.mu.Lock()
	s.destroys++
	s.mu.Unlock()
	return nil
}

func (s *fakeSandbox) destroyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.destroys
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
	id, tok := enrollWorker(t, st)
	r := &Runner{
		Client:   &api.Client{BaseURL: srv.URL, Token: tok, WorkerID: id, HTTP: srv.Client()},
		Provider: p,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return st, jobs, r, src, logDir
}

func enrollWorker(t *testing.T, st *store.Store) (id, token string) {
	t.Helper()
	ctx := context.Background()
	enroll, err := st.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatalf("IssueEnrollmentToken: %v", err)
	}
	id, token, err = st.Enroll(ctx, enroll, "test", "amd64", "test")
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	return id, token
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

func waitDestroyed(t *testing.T, sb *fakeSandbox, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sb.destroyCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("destroys = %d, want %d", sb.destroyCount(), want)
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
	waitDestroyed(t, p.sb, 1)
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
	waitDestroyed(t, p.sb, 1)
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

	// Attempt 1 requeues (pre-acquisition sandbox error), attempt 2
	// dead-letters at the default max of 2.
	got := waitState(t, st, j.ID, job.JobFailed)
	if !got.DeadLettered || got.Attempt != 2 {
		t.Errorf("job = %+v, want dead-lettered on attempt 2", got)
	}
	if ids := src.unregisterIDs(); len(ids) != 2 {
		t.Errorf("Unregister = %v, want one per attempt", ids)
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

	// Both attempts fail at Start, destroy their sandbox, and the job
	// dead-letters on the second.
	waitState(t, st, j.ID, job.JobFailed)
	waitDestroyed(t, p.sb, 2)
	if ids := src.unregisterIDs(); len(ids) != 2 {
		t.Errorf("Unregister = %v, want one per attempt", ids)
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
			if p.sb.started || p.sb.destroyCount() != 0 {
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

// liveSandbox emits follow-mode logs through a pipe and blocks Wait
// until the test releases it, simulating a long-running job.
type liveSandbox struct {
	fakeSandbox
	pr     *io.PipeReader
	waitCh chan int
}

func (s *liveSandbox) Logs(_ context.Context, follow bool) (io.ReadCloser, error) {
	if follow {
		return s.pr, nil
	}
	return io.NopCloser(strings.NewReader(s.logs)), nil
}

func (s *liveSandbox) Wait(ctx context.Context) (int, error) {
	select {
	case code := <-s.waitCh:
		return code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

type liveProvider struct{ sb sandbox.Sandbox }

func (p *liveProvider) Create(context.Context, sandbox.Spec) (sandbox.Sandbox, error) {
	return p.sb, nil
}

func TestRunStreamsLogsWhileRunning(t *testing.T) {
	pr, pw := io.Pipe()
	sb := &liveSandbox{fakeSandbox: fakeSandbox{id: "sb-live"}, pr: pr, waitCh: make(chan int)}
	p := &liveProvider{sb: sb}
	st, jobs, r, _, logDir := openHarness(t, p)
	startRunner(t, r)

	j := testJob("job-live", 9)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitState(t, st, j.ID, job.JobRunning)

	if _, err := pw.Write([]byte("tick-1\n")); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logDir, "job-live-1.log")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if b, err := os.ReadFile(logPath); err == nil && strings.Contains(string(b), "tick-1") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("log chunk did not reach the control plane while the job was running")
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := st.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != job.JobRunning {
		t.Fatalf("state = %s, want still running while logs stream", got.State)
	}

	pw.Close()
	sb.logs = "tick-1\n"
	sb.waitCh <- 0
	waitState(t, st, j.ID, job.JobSucceeded)
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "tick-1\n" {
		t.Errorf("final log = %q, want tick-1 only (no duplicate snapshot)", b)
	}
}

func TestRunCancelled(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1"}}
	_, _, r, _, _ := openHarness(t, p)
	cancel := startRunner(t, r)
	cancel()
}

type failStatusTransport struct {
	inner http.RoundTripper
	mu    sync.Mutex
	fails int
}

func (t *failStatusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, "/status") {
		t.mu.Lock()
		n := t.fails
		if n > 0 {
			t.fails--
		}
		t.mu.Unlock()
		if n > 0 {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("unavailable")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}
	}
	return t.inner.RoundTrip(req)
}

func TestStatusRetriesThenSucceeds(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1"}}
	st, jobs, r, _, _ := openHarness(t, p)
	r.StatusBackoff = 10 * time.Millisecond
	inner := r.Client.HTTP.Transport
	if inner == nil {
		inner = http.DefaultTransport
	}
	r.Client.HTTP.Transport = &failStatusTransport{inner: inner, fails: 1}
	startRunner(t, r)

	j := testJob("job-retry", 7)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitState(t, st, j.ID, job.JobSucceeded)
}

func TestStatusReplayFromOutbox(t *testing.T) {
	p := &fakeProvider{sb: &fakeSandbox{id: "sb-1"}}
	st, _, r, _, _ := openHarness(t, p)
	path := filepath.Join(t.TempDir(), "status.json")
	box, err := OpenOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	j := testJob("job-replay", 8)
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	assigned, err := st.Assign(context.Background(), j.ID, r.Client.WorkerID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Push(assigned.ID, assigned.Attempt, api.StatusReport{State: job.JobRunning}); err != nil {
		t.Fatal(err)
	}
	if err := box.Push(assigned.ID, assigned.Attempt, api.StatusReport{State: job.JobSucceeded}); err != nil {
		t.Fatal(err)
	}
	r.Outbox = box
	r.StatusBackoff = 10 * time.Millisecond
	startRunner(t, r)
	waitState(t, st, j.ID, job.JobSucceeded)
	if p.sb.started {
		t.Fatal("sandbox started for replayed status")
	}
}

func TestOutboxPersistsAcrossOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")
	box, err := OpenOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	rep := api.StatusReport{State: job.JobRunning}
	if err := box.Push("job-1", 1, rep); err != nil {
		t.Fatal(err)
	}
	got, err := OpenOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	item, ok := got.Peek()
	if !ok || item.JobID != "job-1" || item.Attempt != 1 || item.Report.State != job.JobRunning {
		t.Fatalf("peek = %+v ok=%v", item, ok)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 0600", info.Mode().Perm())
	}
}
