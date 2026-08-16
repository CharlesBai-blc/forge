package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	dockersandbox "github.com/CharlesBai-blc/forge/internal/sandbox/docker"

	_ "modernc.org/sqlite"
)

const testSecret = "test-secret"

func sign(body []byte) string {
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

type recordingProvider struct {
	inner sandbox.Provider
	mu    sync.Mutex
	ids   []string
}

func (p *recordingProvider) Create(ctx context.Context, spec sandbox.Spec) (sandbox.Sandbox, error) {
	sb, err := p.inner.Create(ctx, spec)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.ids = append(p.ids, sb.ID())
	p.mu.Unlock()
	return sb, nil
}

func (p *recordingProvider) Close() error {
	if c, ok := p.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func (p *recordingProvider) idsCopy() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.ids))
	copy(out, p.ids)
	return out
}

func TestValidateRequiredFlags(t *testing.T) {
	cases := []config{
		{dataDir: t.TempDir(), webhookSecret: "s", command: []string{"true"}},
		{dataDir: t.TempDir(), webhookSecret: "s", image: "alpine:3.20"},
		{dataDir: t.TempDir(), image: "alpine:3.20", command: []string{"true"}},
	}
	for _, cfg := range cases {
		if err := cfg.validate(); err == nil {
			t.Fatalf("expected error for %+v", cfg)
		}
	}
	ok := config{
		dataDir: t.TempDir(), webhookSecret: "s", image: "alpine:3.20", command: []string{"true"},
	}
	if err := ok.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestRunRejectsMissingImage(t *testing.T) {
	err := run(context.Background(), config{
		addr:          "127.0.0.1:0",
		dataDir:       t.TempDir(),
		webhookSecret: "s",
		command:       []string{"true"},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWebhookRunsJobAndDestroysSandbox(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skipf("docker client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pingCancel()
	if _, err := cli.Ping(pingCtx); err != nil {
		t.Skipf("docker daemon: %v", err)
	}

	inner, err := dockersandbox.NewProvider()
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	prov := &recordingProvider{inner: inner}

	cfg := config{
		dataDir:       t.TempDir(),
		webhookSecret: testSecret,
		image:         "alpine:3.20",
		command:       []string{"true"},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	appCtx, appCancel := context.WithCancel(context.Background())
	a, err := newApp(appCtx, cfg, log, prov)
	if err != nil {
		appCancel()
		t.Fatalf("newApp: %v", err)
	}
	t.Cleanup(func() {
		appCancel()
		a.Close()
	})

	go func() { _ = a.runner.Run(appCtx) }()

	srv := httptest.NewServer(a.mux)
	t.Cleanup(srv.Close)

	body := []byte(`{
		"action": "queued",
		"workflow_job": {"id": 11, "run_id": 22, "labels": ["self-hosted"]},
		"repository": {"full_name": "owner/name"}
	}`)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/webhook/github", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-GitHub-Event", "workflow_job")
	req.Header.Set("X-Hub-Signature-256", sign(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	state, jobID := waitJobState(t, cfg.dataDir, job.JobSucceeded, job.JobFailed)
	if state != job.JobSucceeded {
		t.Fatalf("state = %s, want succeeded (job %s)", state, jobID)
	}

	ids := prov.idsCopy()
	if len(ids) == 0 {
		t.Fatal("no sandbox created")
	}
	for _, id := range ids {
		_, err := cli.ContainerInspect(context.Background(), id)
		if err == nil {
			t.Fatalf("container %s still present", id)
		}
		if !errdefs.IsNotFound(err) {
			t.Fatalf("inspect %s: %v", id, err)
		}
	}
}

func waitJobState(t *testing.T, dataDir string, want ...job.JobState) (job.JobState, string) {
	t.Helper()
	path := filepath.Join(dataDir, "forge.db")
	allowed := make(map[job.JobState]bool, len(want))
	for _, s := range want {
		allowed[s] = true
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		var id, state string
		err = db.QueryRow(`SELECT id, state FROM jobs LIMIT 1`).Scan(&id, &state)
		db.Close()
		if err == nil && allowed[job.JobState(state)] {
			return job.JobState(state), id
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job did not reach %v", want)
	return "", ""
}
