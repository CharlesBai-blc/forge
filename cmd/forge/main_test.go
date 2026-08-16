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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/runner"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	dockersandbox "github.com/CharlesBai-blc/forge/internal/sandbox/docker"
	"github.com/CharlesBai-blc/forge/internal/secret"
	"github.com/CharlesBai-blc/forge/internal/source/github"
	"github.com/CharlesBai-blc/forge/internal/store"

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
		dataDir:       t.TempDir(),
		webhookSecret: "s",
		githubToken:   "tok",
		githubOwner:   "owner",
		githubRepo:    "name",
		image:         "alpine:3.20",
		command:       []string{"true"},
	}
	if err := ok.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	ok.command = nil
	if err := ok.validate(); err != nil {
		t.Fatalf("validate without command: %v", err)
	}
}

func TestJobCommandDefaultsToRunnerJIT(t *testing.T) {
	got := jobCommand(nil)
	if len(got) != 3 || got[0] != "sh" || got[1] != "-c" {
		t.Fatalf("jobCommand(nil) = %v", got)
	}
	if got[2] != `./run.sh --jitconfig "$(cat /jitconfig)"` {
		t.Fatalf("script = %q", got[2])
	}
	if got := jobCommand([]string{"true"}); len(got) != 1 || got[0] != "true" {
		t.Fatalf("jobCommand(true) = %v", got)
	}
}

func TestValidateCredsRequired(t *testing.T) {
	cfg := config{webhookSecret: "s"}
	if err := cfg.validateCreds(); err == nil {
		t.Fatal("expected error for missing token")
	}
	cfg = config{githubToken: "tok"}
	if err := cfg.validateCreds(); err == nil {
		t.Fatal("expected error for missing webhook secret")
	}
	cfg = config{webhookSecret: "s", githubToken: "tok"}
	if err := cfg.validateCreds(); err == nil {
		t.Fatal("expected error for missing agent token")
	}
	cfg = config{webhookSecret: "s", githubToken: "tok", agentToken: "a"}
	if err := cfg.validateCreds(); err != nil {
		t.Fatalf("validateCreds: %v", err)
	}
}

func TestResolveCredsPersistsEncrypted(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	key, err := secret.LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	st, err := store.Open(ctx, filepath.Join(dir, "forge.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const webhook, token, agent = "super-secret-webhook", "ghp_plaintext_token", "agent-secret"
	got, err := resolveCreds(ctx, st, key, config{webhookSecret: webhook, githubToken: token, agentToken: agent})
	if err != nil {
		t.Fatalf("resolveCreds: %v", err)
	}
	if got.webhookSecret != webhook || got.githubToken != token || got.agentToken != agent {
		t.Fatalf("resolved = %+v", got)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "forge.db"))
	if err != nil {
		t.Fatalf("read db: %v", err)
	}
	if bytes.Contains(raw, []byte(webhook)) || bytes.Contains(raw, []byte(token)) || bytes.Contains(raw, []byte(agent)) {
		t.Fatal("plaintext credential found in database file")
	}

	st.Close()
	st2, err := store.Open(ctx, filepath.Join(dir, "forge.db"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { st2.Close() })
	key2, err := secret.LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("reload key: %v", err)
	}
	loaded, err := resolveCreds(ctx, st2, key2, config{})
	if err != nil {
		t.Fatalf("reload creds: %v", err)
	}
	if loaded.webhookSecret != webhook || loaded.githubToken != token || loaded.agentToken != agent {
		t.Fatalf("loaded = %+v", loaded)
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

func TestStartupReconcilesQueued(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(dir, "forge.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	j := &job.Job{ID: "job-r", Source: "github", ExternalID: 99, Repo: "owner/name", RunID: 1, State: job.JobQueued}
	if err := st.CreateJob(ctx, j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	st.Close()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/generate-jitconfig") {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"runner":{"id":1},"encoded_jit_config":"jit-blob"}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(gh.Close)
	src := &github.Source{
		Secret:  testSecret,
		Token:   "tok",
		Owner:   "owner",
		Repo:    "name",
		BaseURL: gh.URL,
		Client:  gh.Client(),
	}
	mr := miniredis.RunT(t)
	cfg := config{
		dataDir:       dir,
		webhookSecret: testSecret,
		githubToken:   "tok",
		githubOwner:   "owner",
		githubRepo:    "name",
		image:         "alpine:3.20",
		agentToken:    "agent-tok",
		redis:         mr.Addr(),
	}
	a, err := newApp(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), src)
	if err != nil {
		t.Fatalf("newApp: %v", err)
	}
	t.Cleanup(func() { a.Close() })
	srv := httptest.NewServer(a.mux)
	t.Cleanup(srv.Close)
	c := &api.Client{BaseURL: srv.URL, Token: cfg.agentToken, WorkerID: "w1", HTTP: srv.Client()}
	cl, err := c.Claim(ctx)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if cl.JobID != "job-r" || cl.JIT != "jit-blob" {
		t.Fatalf("claim = %+v", cl)
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

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/generate-jitconfig") {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"runner":{"id":1},"encoded_jit_config":"jit-blob"}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(gh.Close)

	src := &github.Source{
		Secret:  testSecret,
		Token:   "tok",
		Owner:   "owner",
		Repo:    "name",
		BaseURL: gh.URL,
		Client:  gh.Client(),
	}

	cfg := config{
		dataDir:       t.TempDir(),
		webhookSecret: testSecret,
		githubToken:   "tok",
		githubOwner:   "owner",
		githubRepo:    "name",
		image:         "alpine:3.20",
		command:       []string{"true"},
		agentToken:    "agent-tok",
	}
	mr := miniredis.RunT(t)
	cfg.redis = mr.Addr()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	appCtx, appCancel := context.WithCancel(context.Background())
	a, err := newApp(appCtx, cfg, log, src)
	if err != nil {
		appCancel()
		t.Fatalf("newApp: %v", err)
	}
	t.Cleanup(func() {
		appCancel()
		a.Close()
	})

	srv := httptest.NewServer(a.mux)
	t.Cleanup(srv.Close)

	agent := &runner.Runner{
		Client:   &api.Client{BaseURL: srv.URL, Token: cfg.agentToken, WorkerID: "w1", HTTP: srv.Client()},
		Provider: prov,
		Log:      log,
	}
	go func() { _ = agent.Run(appCtx) }()

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
	if _, err := os.Stat(filepath.Join(cfg.dataDir, "logs", jobID+"-1.log")); err != nil {
		t.Fatalf("job log: %v", err)
	}

	ids := prov.idsCopy()
	if len(ids) == 0 {
		t.Fatal("no sandbox created")
	}
	waitContainersGone(t, cli, ids)
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

func waitContainersGone(t *testing.T, cli *client.Client, ids []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allGone := true
		for _, id := range ids {
			_, err := cli.ContainerInspect(context.Background(), id)
			if err == nil {
				allGone = false
				break
			}
			if !errdefs.IsNotFound(err) {
				t.Fatalf("inspect %s: %v", id, err)
			}
		}
		if allGone {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("containers still present: %v", ids)
}
