package main

import (
	"context"
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

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/queue"
	"github.com/CharlesBai-blc/forge/internal/runner"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	dockersandbox "github.com/CharlesBai-blc/forge/internal/sandbox/docker"
	"github.com/CharlesBai-blc/forge/internal/store"
	"github.com/CharlesBai-blc/forge/internal/trigger"
)

type config struct {
	addr          string
	dataDir       string
	webhookSecret string
	image         string
	command       []string
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
	image := flag.String("image", envOr("FORGE_JOB_IMAGE", ""), "fixed sandbox image")
	command := flag.String("command", envOr("FORGE_JOB_COMMAND", ""), "fixed command to run")
	flag.Parse()
	return config{
		addr:          *addr,
		dataDir:       *dataDir,
		webhookSecret: *webhookSecret,
		image:         *image,
		command:       strings.Fields(*command),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (c config) validate() error {
	if c.image == "" {
		return fmt.Errorf("forge: -image is required")
	}
	if len(c.command) == 0 {
		return fmt.Errorf("forge: -command is required")
	}
	if c.webhookSecret == "" {
		return fmt.Errorf("forge: -webhook-secret is required")
	}
	if c.dataDir == "" {
		return fmt.Errorf("forge: -data-dir is required")
	}
	return nil
}

type app struct {
	store    *store.Store
	runner   *runner.Runner
	mux      *http.ServeMux
	provider sandbox.Provider
}

func newApp(ctx context.Context, cfg config, log *slog.Logger, provider sandbox.Provider) (*app, error) {
	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("forge: data dir: %w", err)
	}
	st, err := store.Open(ctx, filepath.Join(cfg.dataDir, "forge.db"))
	if err != nil {
		return nil, err
	}
	q := queue.New()
	r := &runner.Runner{
		Queue:    q,
		Store:    st,
		Provider: provider,
		Log:      log,
		Image:    cfg.image,
		Command:  cfg.command,
	}
	h := &trigger.Handler{
		Config: trigger.Config{WebhookSecret: cfg.webhookSecret, Image: cfg.image, Command: cfg.command},
		OnJob: func(j *job.Job) error {
			if err := st.CreateJob(ctx, j); err != nil {
				return err
			}
			return q.Enqueue(j)
		},
	}
	mux := http.NewServeMux()
	mux.Handle("/webhook/github", h)
	return &app{store: st, runner: r, mux: mux, provider: provider}, nil
}

func (a *app) Close() error {
	if c, ok := a.provider.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	return a.store.Close()
}

func run(ctx context.Context, cfg config, log *slog.Logger) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	p, err := dockersandbox.NewProvider()
	if err != nil {
		return err
	}
	a, err := newApp(ctx, cfg, log, p)
	if err != nil {
		p.Close()
		return err
	}
	defer a.Close()

	go func() {
		if err := a.runner.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("runner", "err", err)
		}
	}()

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
