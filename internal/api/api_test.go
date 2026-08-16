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

func openAPI(t *testing.T, src *fakeSource) (*store.Store, *stream.Stream, string, *Client, *fakeSource, *Handler) {
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
	c := &Client{BaseURL: srv.URL, Token: tok, WorkerID: id, HTTP: srv.Client()}
	return st, jobs, logDir, c, src, h
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
	_, _, _, c, _, _ := openAPI(t, nil)
	req, _ := http.NewRequest(http.MethodGet, c.BaseURL+"/v1/agents/"+c.WorkerID+"/claim", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestClaimWrongWorkerID(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	c.WorkerID = "not-me"
	_, err := c.Claim(context.Background())
	if err == nil {
		t.Fatal("expected error for mismatched worker id")
	}
}

func TestClaimRemovedWorker(t *testing.T) {
	st, _, _, c, _, _ := openAPI(t, nil)
	if err := st.SetWorkerState(context.Background(), c.WorkerID, job.WorkerRemoved); err != nil {
		t.Fatal(err)
	}
	_, err := c.Claim(context.Background())
	if err == nil {
		t.Fatal("expected error for removed worker")
	}
}

func TestClaimCordonedWorker(t *testing.T) {
	st, _, _, c, _, _ := openAPI(t, nil)
	if err := st.SetWorkerState(context.Background(), c.WorkerID, job.WorkerCordoned); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, c.BaseURL+"/v1/agents/"+c.WorkerID+"/claim", nil)
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestEnrollHTTP(t *testing.T) {
	st, _, _, c, _, _ := openAPI(t, nil)
	tok, err := st.IssueEnrollmentToken(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	anon := &Client{BaseURL: c.BaseURL, HTTP: c.HTTP}
	got, err := anon.Enroll(context.Background(), tok, "host-b", "arm64", "test")
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if got.WorkerID == "" || got.Token == "" {
		t.Fatalf("enroll = %+v", got)
	}
	w, err := st.GetWorker(context.Background(), got.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerActive || w.Name != "host-b" {
		t.Fatalf("worker = %+v", w)
	}
	_, err = anon.Enroll(context.Background(), tok, "host-c", "arm64", "test")
	if err == nil {
		t.Fatal("expected second enroll to fail")
	}
}

func TestEnrollRejectsUnknownToken(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	anon := &Client{BaseURL: c.BaseURL, HTTP: c.HTTP}
	_, err := anon.Enroll(context.Background(), "nope", "h", "amd64", "test")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClaimAssignsAndReturnsJIT(t *testing.T) {
	st, jobs, _, c, _, _ := openAPI(t, nil)
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
	if row.State != job.JobAssigned || row.WorkerID != c.WorkerID || row.Attempt != 1 {
		t.Fatalf("job = %+v", row)
	}
}

func TestClaimTimeoutNoJob(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	_, err := c.Claim(context.Background())
	if err != ErrNoJob {
		t.Fatalf("err = %v, want ErrNoJob", err)
	}
}

func TestRegisterJITErrorLeavesQueued(t *testing.T) {
	src := &fakeSource{registerErr: fmt.Errorf("github down")}
	st, jobs, _, c, src, _ := openAPI(t, src)
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
	st, jobs, logDir, c, src, _ := openAPI(t, nil)
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
	st, jobs, _, c, src, _ := openAPI(t, nil)
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
	st, jobs, _, c, _, _ := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-stale", 5))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	err = c.Status(context.Background(), cl.JobID, cl.Attempt+1, StatusReport{State: job.JobRunning})
	if err == nil {
		t.Fatal("expected error for stale attempt")
	}
	if StatusRetryable(err) {
		t.Fatalf("stale attempt retryable: %v", err)
	}
}

func TestStatusWrongWorker(t *testing.T) {
	st, jobs, _, c, _, _ := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-fence", 6))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	id2, tok2 := enrollWorker(t, st)
	other := &Client{BaseURL: c.BaseURL, Token: tok2, WorkerID: id2, HTTP: c.HTTP}
	err = other.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobRunning})
	if err == nil {
		t.Fatal("expected error for other worker")
	}
	if StatusRetryable(err) {
		t.Fatalf("wrong worker retryable: %v", err)
	}
}

func TestStatusDuplicateIsNoContent(t *testing.T) {
	st, jobs, _, c, _, _ := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-dup", 21))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	rep := StatusReport{State: job.JobRunning}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, rep); err != nil {
		t.Fatalf("running: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, rep); err != nil {
		t.Fatalf("duplicate running: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobSucceeded}); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobSucceeded}); err != nil {
		t.Fatalf("duplicate succeeded: %v", err)
	}
	row, err := st.GetJob(context.Background(), cl.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobSucceeded {
		t.Fatalf("State = %s", row.State)
	}
}

func TestHeartbeatRestoresLost(t *testing.T) {
	st, _, _, c, _, _ := openAPI(t, nil)
	ctx := context.Background()
	if err := st.SetWorkerState(ctx, c.WorkerID, job.WorkerLost); err != nil {
		t.Fatal(err)
	}
	if err := c.Heartbeat(ctx, Heartbeat{Capacity: 1, Healthy: true}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	w, err := st.GetWorker(ctx, c.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerActive || !w.Healthy || w.Capacity != 1 {
		t.Fatalf("worker = %+v", w)
	}
}

func TestHeartbeatUnauthorized(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	req, _ := http.NewRequest(http.MethodPost, c.BaseURL+"/v1/agents/"+c.WorkerID+"/heartbeat", bytes.NewReader([]byte(`{"capacity":1,"healthy":true}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestClaimUnhealthyNoJob(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	if err := c.Heartbeat(context.Background(), Heartbeat{Capacity: 1, Healthy: false}); err != nil {
		t.Fatal(err)
	}
	_, err := c.Claim(context.Background())
	if err != ErrNoJob {
		t.Fatalf("err = %v, want ErrNoJob", err)
	}
}

func TestSweepMarksStaleWorkerLost(t *testing.T) {
	st, _, _, c, _, h := openAPI(t, nil)
	ctx := context.Background()
	if err := st.SetLastSeen(ctx, c.WorkerID, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	h.LostAfter = 30 * time.Second
	if err := h.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	w, err := st.GetWorker(ctx, c.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerLost {
		t.Fatalf("state = %s, want lost", w.State)
	}
}

func TestSweepRequeuesAssigned(t *testing.T) {
	st, jobs, _, c, src, h := openAPI(t, nil)
	h.Visibility = time.Millisecond
	putQueued(t, st, jobs, testJob("job-reclaim", 7))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	row, err := st.GetJob(context.Background(), cl.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobQueued || row.Attempt != 1 || row.WorkerID != "" {
		t.Fatalf("job = %+v", row)
	}
	if ids := src.unregisterIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Unregister = %v, want [1]", ids)
	}
	cl2, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if cl2.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", cl2.Attempt)
	}
}

func TestSweepDeadLettersAfterMaxAttempts(t *testing.T) {
	st, jobs, _, c, _, h := openAPI(t, nil)
	h.Visibility = time.Millisecond
	h.MaxAttempts = 1
	putQueued(t, st, jobs, testJob("job-dl", 8))
	if _, err := c.Claim(context.Background()); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	row, err := st.GetJob(context.Background(), "job-dl")
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobFailed || !row.DeadLettered || row.Reason != "max_attempts" {
		t.Fatalf("job = %+v", row)
	}
}

func TestSweepFailsRunningAsWorkerLost(t *testing.T) {
	st, jobs, _, c, src, h := openAPI(t, nil)
	h.Visibility = time.Millisecond
	putQueued(t, st, jobs, testJob("job-run", 9))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobRunning}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	row, err := st.GetJob(context.Background(), cl.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobFailed || row.Reason != "worker_lost" || row.DeadLettered {
		t.Fatalf("job = %+v", row)
	}
	if ids := src.unregisterIDs(); len(ids) != 0 {
		t.Errorf("Unregister = %v, want none after running", ids)
	}
}

func TestSweepLeavesFreshClaim(t *testing.T) {
	st, jobs, _, c, _, h := openAPI(t, nil)
	h.Visibility = time.Minute
	putQueued(t, st, jobs, testJob("job-fresh", 10))
	if _, err := c.Claim(context.Background()); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	row, err := st.GetJob(context.Background(), "job-fresh")
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobAssigned {
		t.Fatalf("state = %s, want assigned", row.State)
	}
}

func TestSweepDrainRequeuesAssigned(t *testing.T) {
	st, jobs, _, c, src, h := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-drain", 11))
	if _, err := c.Claim(context.Background()); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := st.TransitionWorker(context.Background(), c.WorkerID, job.WorkerDraining); err != nil {
		t.Fatal(err)
	}
	if err := h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	row, err := st.GetJob(context.Background(), "job-drain")
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobQueued || row.Attempt != 0 || row.WorkerID != "" {
		t.Fatalf("job = %+v", row)
	}
	if ids := src.unregisterIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Unregister = %v, want [1]", ids)
	}
	id2, tok2 := enrollWorker(t, st)
	other := &Client{BaseURL: c.BaseURL, Token: tok2, WorkerID: id2, HTTP: c.HTTP}
	cl, err := other.Claim(context.Background())
	if err != nil {
		t.Fatalf("other Claim: %v", err)
	}
	if cl.JobID != "job-drain" || cl.Attempt != 1 {
		t.Fatalf("claim = %+v", cl)
	}
	w, err := st.GetWorker(context.Background(), c.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerCordoned {
		t.Fatalf("state = %s, want cordoned", w.State)
	}
}

func TestSweepDrainWaitsForRunning(t *testing.T) {
	st, jobs, _, c, _, h := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-run-drain", 12))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobRunning}); err != nil {
		t.Fatal(err)
	}
	if err := st.TransitionWorker(context.Background(), c.WorkerID, job.WorkerDraining); err != nil {
		t.Fatal(err)
	}
	if err := h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	row, err := st.GetJob(context.Background(), cl.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != job.JobRunning {
		t.Fatalf("state = %s, want running", row.State)
	}
	w, err := st.GetWorker(context.Background(), c.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerDraining {
		t.Fatalf("worker = %s, want draining", w.State)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobSucceeded}); err != nil {
		t.Fatal(err)
	}
	if err := h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep after succeed: %v", err)
	}
	w, err = st.GetWorker(context.Background(), c.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerCordoned {
		t.Fatalf("state = %s, want cordoned", w.State)
	}
}
