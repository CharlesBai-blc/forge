package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/store"
	"github.com/CharlesBai-blc/forge/internal/stream"
	"github.com/alicebob/miniredis/v2"
)

const testToken = "agent-token"

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

func openAPI(t *testing.T, src *fakeSource) (*store.Store, *stream.Stream, string, *Client, *fakeSource) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(dir, "forge.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if src == nil {
		src = &fakeSource{jit: &source.JITConfig{RunnerID: 1, Encoded: "jit-blob"}}
	}
	mr := miniredis.RunT(t)
	jobs, err := stream.Open(context.Background(), mr.Addr())
	if err != nil {
		t.Fatalf("stream.Open: %v", err)
	}
	t.Cleanup(func() { jobs.Close() })
	logDir := filepath.Join(dir, "logs")
	h := &Handler{
		Stream:    jobs,
		Store:     st,
		Source:    src,
		Token:     testToken,
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
	c := &Client{BaseURL: srv.URL, Token: testToken, WorkerID: "w1", HTTP: srv.Client()}
	return st, jobs, logDir, c, src
}

func putQueued(t *testing.T, st *store.Store, jobs *stream.Stream, j *job.Job) {
	t.Helper()
	if err := st.CreateJob(context.Background(), j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := jobs.Add(context.Background(), j.ID); err != nil {
		t.Fatalf("Add: %v", err)
	}
}

func TestClaimUnauthorized(t *testing.T) {
	_, _, _, c, _ := openAPI(t, nil)
	req, _ := http.NewRequest(http.MethodGet, c.BaseURL+"/v1/agents/w1/claim", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestClaimAssignsAndReturnsJIT(t *testing.T) {
	st, jobs, _, c, _ := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-1", 1))
	got, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.JobID != "job-1" || got.Attempt != 1 || got.JIT != "jit-blob" {
		t.Fatalf("claim = %+v", got)
	}
	if got.Spec.Image != "alpine:3.20" || len(got.Spec.Command) != 1 || got.Spec.Command[0] != "true" {
		t.Fatalf("spec = %+v", got.Spec)
	}
	row, err := st.GetJob(context.Background(), "job-1")
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobAssigned || row.WorkerID != "w1" || row.Attempt != 1 {
		t.Fatalf("job = %+v", row)
	}
}

func TestClaimTimeoutNoJob(t *testing.T) {
	_, _, _, c, _ := openAPI(t, nil)
	_, err := c.Claim(context.Background())
	if err != ErrNoJob {
		t.Fatalf("err = %v, want ErrNoJob", err)
	}
}

func TestRegisterJITErrorLeavesQueued(t *testing.T) {
	src := &fakeSource{registerErr: fmt.Errorf("github down")}
	st, jobs, _, c, src := openAPI(t, src)
	putQueued(t, st, jobs, testJob("job-jit", 2))
	_, err := c.Claim(context.Background())
	if err != ErrNoJob {
		t.Fatalf("err = %v, want ErrNoJob", err)
	}
	if src.registerCount() < 1 {
		t.Fatal("RegisterJIT not called")
	}
	row, err := st.GetJob(context.Background(), "job-jit")
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobQueued {
		t.Fatalf("State = %s, want queued", row.State)
	}
}

func TestStatusRunningSucceededAndLogs(t *testing.T) {
	st, jobs, logDir, c, src := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-ok", 3))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobRunning}); err != nil {
		t.Fatalf("running: %v", err)
	}
	if err := c.Logs(context.Background(), cl.JobID, cl.Attempt, bytes.NewReader([]byte("hello-forge\n"))); err != nil {
		t.Fatalf("logs: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobSucceeded}); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	row, err := st.GetJob(context.Background(), cl.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobSucceeded {
		t.Fatalf("State = %s", row.State)
	}
	if ids := src.unregisterIDs(); len(ids) != 0 {
		t.Errorf("Unregister = %v, want none after running", ids)
	}
	b, err := os.ReadFile(filepath.Join(logDir, "job-ok-1.log"))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if string(b) != "hello-forge\n" {
		t.Fatalf("log = %q", b)
	}
}

func TestFailedFromAssignedUnregisters(t *testing.T) {
	st, jobs, _, c, src := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-start", 4))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobFailed, Reason: "start failed"}); err != nil {
		t.Fatalf("failed: %v", err)
	}
	row, err := st.GetJob(context.Background(), cl.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobFailed {
		t.Fatalf("State = %s", row.State)
	}
	if ids := src.unregisterIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Unregister = %v, want [1]", ids)
	}
}

func TestStaleAttemptRejected(t *testing.T) {
	st, jobs, _, c, _ := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-stale", 5))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	err = c.Status(context.Background(), cl.JobID, cl.Attempt+1, StatusReport{State: job.JobRunning})
	if err == nil {
		t.Fatal("expected error for stale attempt")
	}
}
