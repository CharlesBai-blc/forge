package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/runner"
	dockersandbox "github.com/CharlesBai-blc/forge/internal/sandbox/docker"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	addr := flag.String("addr", envOr("FORGE_AGENT_ADDR", "http://127.0.0.1:8080"), "control plane URL")
	token := flag.String("token", os.Getenv("FORGE_AGENT_TOKEN"), "shared agent token")
	id := flag.String("id", envOr("FORGE_AGENT_ID", ""), "worker id")
	flag.Parse()
	if *token == "" {
		log.Error("forge-agent", "err", fmt.Errorf("forge-agent: -token is required"))
		os.Exit(1)
	}
	workerID := *id
	if workerID == "" {
		h, err := os.Hostname()
		if err != nil {
			log.Error("forge-agent", "err", err)
			os.Exit(1)
		}
		workerID = h
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p, err := dockersandbox.NewProvider()
	if err != nil {
		log.Error("forge-agent", "err", err)
		os.Exit(1)
	}
	defer p.Close()

	r := &runner.Runner{
		Client:   &api.Client{BaseURL: *addr, Token: *token, WorkerID: workerID},
		Provider: p,
		Log:      log,
	}
	log.Info("forge-agent starting", "addr", *addr, "id", workerID)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("forge-agent", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
