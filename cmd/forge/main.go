package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/queue"
	"github.com/CharlesBai-blc/forge/internal/secret"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/source/github"
	"github.com/CharlesBai-blc/forge/internal/store"
)

type config struct {
	addr          string
	dataDir       string
	webhookSecret string
	githubToken   string
	githubOwner   string
	githubRepo    string
	githubOrg     string
	image         string
	command       []string
	agentToken    string
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := parseFlags()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil {
		log.Error("forge", "err", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	addr := flag.String("addr", envOr("FORGE_ADDR", ":8080"), "listen address")
	dataDir := flag.String("data-dir", envOr("FORGE_DATA_DIR", "./data"), "data directory")
	webhookSecret := flag.String("webhook-secret", os.Getenv("FORGE_WEBHOOK_SECRET"), "GitHub webhook secret")
	githubToken := flag.String("github-token", os.Getenv("FORGE_GITHUB_TOKEN"), "GitHub PAT or installation token")
	githubOwner := flag.String("github-owner", envOr("FORGE_GITHUB_OWNER", ""), "GitHub repo owner")
	githubRepo := flag.String("github-repo", envOr("FORGE_GITHUB_REPO", ""), "GitHub repo name")
	githubOrg := flag.String("github-org", envOr("FORGE_GITHUB_ORG", ""), "GitHub org (org-level registration)")
	image := flag.String("image", envOr("FORGE_JOB_IMAGE", ""), "sandbox image")
	command := flag.String("command", envOr("FORGE_JOB_COMMAND", ""), "sandbox command; default is actions/runner JIT")
	agentToken := flag.String("agent-token", os.Getenv("FORGE_AGENT_TOKEN"), "shared token for forge-agent")
	flag.Parse()
	return config{
		addr:          *addr,
		dataDir:       *dataDir,
		webhookSecret: *webhookSecret,
		githubToken:   *githubToken,
		githubOwner:   *githubOwner,
		githubRepo:    *githubRepo,
		githubOrg:     *githubOrg,
		image:         *image,
		command:       strings.Fields(*command),
		agentToken:    *agentToken,
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// jobCommand is the sandbox process. Empty means the official
// actions/runner JIT invocation (FR-6).
func jobCommand(cmd []string) []string {
	if len(cmd) > 0 {
		return cmd
	}
	return []string{"sh", "-c", `./run.sh --jitconfig "$(cat /jitconfig)"`}
}

func (c config) validate() error {
	if c.image == "" {
		return fmt.Errorf("forge: -image is required")
	}
	if c.githubOrg == "" && (c.githubOwner == "" || c.githubRepo == "") {
		return fmt.Errorf("forge: -github-owner and -github-repo, or -github-org, are required")
	}
	if c.dataDir == "" {
		return fmt.Errorf("forge: -data-dir is required")
	}
	return nil
}

func (c config) validateCreds() error {
	if c.webhookSecret == "" {
		return fmt.Errorf("forge: -webhook-secret is required")
	}
	if c.githubToken == "" {
		return fmt.Errorf("forge: -github-token is required")
	}
	if c.agentToken == "" {
		return fmt.Errorf("forge: -agent-token is required")
	}
	return nil
}

const (
	secretWebhook = "webhook_secret"
	secretToken   = "github_token"
	secretAgent   = "agent_token"
)

func resolveCreds(ctx context.Context, st *store.Store, key *[secret.KeySize]byte, cfg config) (config, error) {
	var err error
	if cfg.webhookSecret, err = resolveOne(ctx, st, key, secretWebhook, cfg.webhookSecret); err != nil {
		return cfg, err
	}
	if cfg.githubToken, err = resolveOne(ctx, st, key, secretToken, cfg.githubToken); err != nil {
		return cfg, err
	}
	if cfg.agentToken, err = resolveOne(ctx, st, key, secretAgent, cfg.agentToken); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func resolveOne(ctx context.Context, st *store.Store, key *[secret.KeySize]byte, name, provided string) (string, error) {
	if provided != "" {
		box, err := secret.Seal(key, []byte(provided))
		if err != nil {
			return "", err
		}
		if err := st.PutSecret(ctx, name, box); err != nil {
			return "", err
		}
		return provided, nil
	}
	box, err := st.GetSecret(ctx, name)
	if errors.Is(err, store.ErrSecretNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	pt, err := secret.Open(key, box)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

type app struct {
	store *store.Store
	mux   *http.ServeMux
}

func newApp(ctx context.Context, cfg config, log *slog.Logger, src source.RunnerSource) (*app, error) {
	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("forge: data dir: %w", err)
	}
	key, err := secret.LoadOrCreate(cfg.dataDir)
	if err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, filepath.Join(cfg.dataDir, "forge.db"))
	if err != nil {
		return nil, err
	}
	cfg, err = resolveCreds(ctx, st, key, cfg)
	if err != nil {
		st.Close()
		return nil, err
	}
	if src == nil {
		if err := cfg.validateCreds(); err != nil {
			st.Close()
			return nil, err
		}
		src = &github.Source{
			Secret: cfg.webhookSecret,
			Token:  cfg.githubToken,
			Owner:  cfg.githubOwner,
			Repo:   cfg.githubRepo,
			Org:    cfg.githubOrg,
		}
	}
	q := queue.New()
	h := &webhookHandler{
		src: src,
		onJob: func(j *job.Job) error {
			if err := st.CreateJob(ctx, j); err != nil {
				return err
			}
			return q.Enqueue(j)
		},
	}
	mux := http.NewServeMux()
	mux.Handle("/webhook/github", h)
	apiH := &api.Handler{
		Queue:   q,
		Store:   st,
		Source:  src,
		Token:   cfg.agentToken,
		Image:   cfg.image,
		Command: jobCommand(cfg.command),
		LogDir:  filepath.Join(cfg.dataDir, "logs"),
		Log:     log,
	}
	apiH.Register(mux)
	return &app{store: st, mux: mux}, nil
}

func (a *app) Close() error {
	return a.store.Close()
}

func run(ctx context.Context, cfg config, log *slog.Logger) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	a, err := newApp(ctx, cfg, log, nil)
	if err != nil {
		return err
	}
	defer a.Close()

	srv := &http.Server{Addr: cfg.addr, Handler: a.mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("forge starting", "addr", cfg.addr, "data_dir", cfg.dataDir, "image", cfg.image)
	err = srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
