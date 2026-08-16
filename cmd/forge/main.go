package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/secret"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/source/github"
	"github.com/CharlesBai-blc/forge/internal/store"
	"github.com/CharlesBai-blc/forge/internal/stream"
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
	redis         string
	cpu           float64
	memoryMB      int64
	pids          int64
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if len(os.Args) > 1 {
		if run, ok := cliCommands[os.Args[1]]; ok {
			if err := run(os.Args[2:], os.Stdout); err != nil {
				log.Error("forge", "err", err)
				os.Exit(1)
			}
			return
		}
	}
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
	redisAddr := flag.String("redis", envOr("FORGE_REDIS", "127.0.0.1:6379"), "redis address")
	cpu := flag.Float64("cpu", envOrFloat("FORGE_JOB_CPU", defaultCPU), "sandbox CPU limit in cores; 0 disables (FR-14)")
	memoryMB := flag.Int64("memory-mb", envOrInt("FORGE_JOB_MEMORY_MB", defaultMemoryMB), "sandbox memory limit in MiB; 0 disables (FR-14)")
	pids := flag.Int64("pids", envOrInt("FORGE_JOB_PIDS", defaultPIDs), "sandbox PID limit; 0 disables (FR-14)")
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
		redis:         *redisAddr,
		cpu:           *cpu,
		memoryMB:      *memoryMB,
		pids:          *pids,
	}
}

// Sandbox resource defaults (FR-14, tdd.md Appendix B).
const (
	defaultCPU      = 2.0
	defaultMemoryMB = 4096
	defaultPIDs     = 4096
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func envOrInt(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
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
	return nil
}

const (
	secretWebhook = "webhook_secret"
	secretToken   = "github_token"
)

func resolveCreds(ctx context.Context, st *store.Store, key *[secret.KeySize]byte, cfg config) (config, error) {
	var err error
	if cfg.webhookSecret, err = resolveOne(ctx, st, key, secretWebhook, cfg.webhookSecret); err != nil {
		return cfg, err
	}
	if cfg.githubToken, err = resolveOne(ctx, st, key, secretToken, cfg.githubToken); err != nil {
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
	store  *store.Store
	mux    *http.ServeMux
	stream *stream.Stream
	api    *api.Handler
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
	jobs, err := stream.Open(ctx, cfg.redis)
	if err != nil {
		st.Close()
		return nil, err
	}
	queued, err := st.QueuedIDs(ctx)
	if err != nil {
		jobs.Close()
		st.Close()
		return nil, err
	}
	if err := jobs.Reconcile(ctx, queued); err != nil {
		jobs.Close()
		st.Close()
		return nil, err
	}
	h := &webhookHandler{
		src: src,
		onJob: func(j *job.Job) error {
			err := st.CreateJob(ctx, j)
			if errors.Is(err, store.ErrDuplicateJob) {
				// Webhook redelivery. Repair a still-queued job whose
				// stream entry is missing, then report success so GitHub
				// stops retrying.
				existing, gerr := st.GetJobBySource(ctx, j.Source, j.ExternalID)
				if gerr != nil {
					return gerr
				}
				if existing.State != job.JobQueued {
					return nil
				}
				ok, herr := jobs.Has(ctx, existing.ID)
				if herr != nil {
					return herr
				}
				if ok {
					return nil
				}
				return jobs.Add(ctx, existing.ID)
			}
			if err != nil {
				return err
			}
			return jobs.Add(ctx, j.ID)
		},
	}
	mux := http.NewServeMux()
	mux.Handle("/webhook/github", h)
	apiH := &api.Handler{
		Stream:      jobs,
		Store:       st,
		Source:      src,
		Image:       cfg.image,
		Command:     jobCommand(cfg.command),
		CPU:         cfg.cpu,
		MemoryBytes: cfg.memoryMB << 20,
		PIDs:        cfg.pids,
		LogDir:      filepath.Join(cfg.dataDir, "logs"),
		Log:         log,
	}
	apiH.Register(mux)
	return &app{store: st, mux: mux, stream: jobs, api: apiH}, nil
}

func (a *app) Close() error {
	if a.stream != nil {
		_ = a.stream.Close()
	}
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

	go a.api.RunSweep(ctx)

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

func runEnrollToken(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("enroll-token", flag.ContinueOnError)
	dataDir := fs.String("data-dir", envOr("FORGE_DATA_DIR", "./data"), "data directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := openCLIStore(*dataDir)
	if err != nil {
		return err
	}
	defer st.Close()
	tok, err := st.IssueEnrollmentToken(context.Background())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, tok)
	return err
}

func openCLIStore(dataDir string) (*store.Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("forge: -data-dir is required")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("forge: data dir: %w", err)
	}
	return store.Open(context.Background(), filepath.Join(dataDir, "forge.db"))
}

var cliCommands = map[string]func([]string, io.Writer) error{
	"enroll-token": runEnrollToken,
	"workers":      runWorkers,
	"cordon":       runCordon,
	"uncordon":     runUncordon,
	"drain":        runDrain,
	"remove":       runRemove,
}
