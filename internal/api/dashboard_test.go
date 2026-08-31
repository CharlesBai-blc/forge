package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

func TestDashboardPage(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)

	// Without a session the page route serves the login form (tdd.md §7).
	resp, err := http.Get(c.BaseURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !bytes.Contains(b, []byte("Log in")) || bytes.Contains(b, []byte("Queue depth")) {
		t.Fatalf("expected login page, got: %.100s", b)
	}

	hc := adminHTTP(t, c.BaseURL)
	resp, err = hc.Get(c.BaseURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("Content-Type = %q", ct)
	}
	b, _ = io.ReadAll(resp.Body)
	if !bytes.Contains(b, []byte("Queue depth")) {
		t.Fatalf("body missing queue depth")
	}
}

func TestDashboardJSON(t *testing.T) {
	st, jobs, _, c, _, _ := openAPI(t, nil)
	putQueued(t, st, jobs, testJob("job-q", 10))
	putQueued(t, st, jobs, testJob("job-run", 11))
	cl, err := c.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := c.Status(context.Background(), cl.JobID, cl.Attempt, StatusReport{State: job.JobRunning}); err != nil {
		t.Fatalf("running: %v", err)
	}

	hc := adminHTTP(t, c.BaseURL)
	resp, err := hc.Get(c.BaseURL + "/v1/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if bytes.Contains(raw, []byte("token_hash")) || bytes.Contains(raw, []byte("TokenHash")) {
		t.Fatal("dashboard leaked token hash")
	}
	var d Dashboard
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.QueueDepth != 1 {
		t.Fatalf("queue_depth = %d, want 1", d.QueueDepth)
	}
	if len(d.Jobs) != 2 {
		t.Fatalf("len(jobs) = %d, want 2", len(d.Jobs))
	}
	byID := map[string]DashboardJob{}
	for _, j := range d.Jobs {
		byID[j.ID] = j
		if j.DurationMS < 0 {
			t.Fatalf("duration_ms = %d", j.DurationMS)
		}
	}
	if byID[cl.JobID].State != job.JobRunning {
		t.Fatalf("%s state = %s, want running", cl.JobID, byID[cl.JobID].State)
	}
	other := "job-q"
	if cl.JobID == "job-q" {
		other = "job-run"
	}
	if byID[other].State != job.JobQueued {
		t.Fatalf("%s state = %s, want queued", other, byID[other].State)
	}
	if len(d.Workers) != 1 {
		t.Fatalf("len(workers) = %d, want 1", len(d.Workers))
	}
	w := d.Workers[0]
	if w.ID != c.WorkerID || w.Running != 1 || w.Capacity != 1 || w.Utilization != 1 {
		t.Fatalf("worker = %+v", w)
	}
}

func TestDashboardDeadLetter(t *testing.T) {
	st, _, _, c, _, _ := openAPI(t, nil)
	ctx := context.Background()
	if err := st.CreateJob(ctx, testJob("job-dl", 12)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Assign(ctx, "job-dl", "w1", 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Transition(ctx, "job-dl", job.JobLost, "visibility_timeout"); err != nil {
		t.Fatal(err)
	}
	if err := st.DeadLetter(ctx, "job-dl", "max_attempts"); err != nil {
		t.Fatal(err)
	}

	hc := adminHTTP(t, c.BaseURL)
	resp, err := hc.Get(c.BaseURL + "/v1/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var d Dashboard
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatal(err)
	}
	var found *DashboardJob
	for i := range d.Jobs {
		if d.Jobs[i].ID == "job-dl" {
			found = &d.Jobs[i]
			break
		}
	}
	if found == nil || !found.DeadLettered || found.State != job.JobFailed {
		t.Fatalf("dead letter job = %+v", found)
	}

	detail, err := hc.Get(c.BaseURL + "/v1/jobs/job-dl")
	if err != nil {
		t.Fatal(err)
	}
	defer detail.Body.Close()
	if detail.StatusCode != http.StatusOK {
		t.Fatalf("detail status = %d", detail.StatusCode)
	}
	var jd JobDetail
	if err := json.NewDecoder(detail.Body).Decode(&jd); err != nil {
		t.Fatal(err)
	}
	if len(jd.Transitions) < 2 {
		t.Fatalf("transitions = %+v", jd.Transitions)
	}
}

func TestLogStream(t *testing.T) {
	st, _, logDir, c, _, h := openAPI(t, nil)
	h.LogPoll = 10 * time.Millisecond
	ctx := context.Background()
	if err := st.CreateJob(ctx, testJob("job-log", 13)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(logDir, "job-log-0.log")
	if err := os.WriteFile(path, []byte("hello-forge\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := st.Transition(ctx, "job-log", job.JobFailed, "test"); err != nil {
		t.Fatal(err)
	}

	hc := adminHTTP(t, c.BaseURL)
	resp, err := hc.Get(c.BaseURL + "/v1/jobs/job-log/logs/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("Content-Type = %q", resp.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte("hello-forge")) {
		t.Fatalf("stream = %q", body)
	}
	if !bytes.Contains(body, []byte("event: end")) {
		t.Fatalf("missing end event: %q", body)
	}
}

func TestLogStreamLastEventID(t *testing.T) {
	st, _, logDir, c, _, h := openAPI(t, nil)
	h.LogPoll = 10 * time.Millisecond
	ctx := context.Background()
	if err := st.CreateJob(ctx, testJob("job-off", 14)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "job-off-0.log"), []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := st.Transition(ctx, "job-off", job.JobFailed, "test"); err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodGet, c.BaseURL+"/v1/jobs/job-off/logs/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Last-Event-ID", "6")
	resp, err := adminHTTP(t, c.BaseURL).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte("world")) {
		t.Fatalf("stream = %q", body)
	}
	if bytes.Contains(body, []byte("hello")) {
		t.Fatalf("replayed prefix: %q", body)
	}
}

func TestJobDetailNotFound(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	resp, err := adminHTTP(t, c.BaseURL).Get(c.BaseURL + "/v1/jobs/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
