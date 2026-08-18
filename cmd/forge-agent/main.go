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
	"runtime"
	"strconv"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/runner"
	dockersandbox "github.com/CharlesBai-blc/forge/internal/sandbox/docker"
)

const agentVersion = "0.0.1"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	addr := flag.String("addr", envOr("FORGE_AGENT_ADDR", "http://127.0.0.1:8080"), "control plane URL")
	dataDir := flag.String("data-dir", envOr("FORGE_AGENT_DATA_DIR", "./agent-data"), "agent data directory")
	enrollToken := flag.String("enroll-token", os.Getenv("FORGE_ENROLL_TOKEN"), "one-time enrollment token (first run)")
	warmPool := flag.Int("warm-pool", envOrInt("FORGE_AGENT_WARM_POOL", defaultWarmPool), "warm sandboxes to keep ready; 0 disables (FR-16)")
	metricsAddr := flag.String("metrics-addr", envOr("FORGE_AGENT_METRICS_ADDR", "127.0.0.1:9091"), "metrics listen address; empty disables (FR-25)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := resolveClient(ctx, *addr, *dataDir, *enrollToken)
	if err != nil {
		log.Error("forge-agent", "err", err)
		os.Exit(1)
	}

	box, err := runner.OpenOutbox(filepath.Join(*dataDir, "status.json"))
	if err != nil {
		log.Error("forge-agent", "err", err)
		os.Exit(1)
	}

	p, err := dockersandbox.NewProvider()
	if err != nil {
		log.Error("forge-agent", "err", err)
		os.Exit(1)
	}
	defer p.Close()
	p.Log = log

	if *metricsAddr != "" {
		go serveMetrics(log, *metricsAddr)
	}

	r := &runner.Runner{
		Client:   c,
		Provider: p,
		Log:      log,
		Outbox:   box,
	}
	if *warmPool > 0 {
		pool := &runner.Pool{Provider: p, Size: *warmPool, Log: log}
		// Warm before the first claim; a failure is not fatal because
		// the first claim's spec also feeds the pool.
		if spec, err := c.Spec(ctx); err == nil {
			pool.SetSpec(*spec)
		} else if ctx.Err() == nil {
			log.Warn("warm pool: spec fetch failed, warming after first claim", "err", err)
		}
		go pool.Run(ctx)
		r.Pool = pool
	}
	log.Info("forge-agent starting", "addr", *addr, "id", c.WorkerID, "warm_pool", *warmPool)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("forge-agent", "err", err)
		os.Exit(1)
	}
}

// defaultWarmPool is the per-label-set pool size (FR-16, tdd.md Appendix B).
const defaultWarmPool = 2

func serveMetrics(log *slog.Logger, addr string) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("metrics listener", "addr", addr, "err", err)
	}
}

func resolveClient(ctx context.Context, addr, dataDir, enrollToken string) (*api.Client, error) {
	path := workerPath(dataDir)
	cred, err := loadWorker(path)
	if err == nil {
		return &api.Client{BaseURL: addr, Token: cred.Token, WorkerID: cred.ID}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if enrollToken == "" {
		return nil, fmt.Errorf("forge-agent: -enroll-token is required on first run")
	}
	name, herr := os.Hostname()
	if herr != nil {
		name = "unknown"
	}
	c := &api.Client{BaseURL: addr}
	out, err := c.Enroll(ctx, enrollToken, name, runtime.GOARCH, agentVersion)
	if err != nil {
		return nil, err
	}
	if err := saveWorker(path, workerCred{ID: out.WorkerID, Token: out.Token}); err != nil {
		return nil, err
	}
	c.WorkerID = out.WorkerID
	c.Token = out.Token
	return c, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
