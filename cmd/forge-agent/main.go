package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

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
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := resolveClient(ctx, *addr, *dataDir, *enrollToken)
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

	r := &runner.Runner{
		Client:   c,
		Provider: p,
		Log:      log,
	}
	log.Info("forge-agent starting", "addr", *addr, "id", c.WorkerID)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("forge-agent", "err", err)
		os.Exit(1)
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
