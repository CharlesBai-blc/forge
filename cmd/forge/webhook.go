package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/source/github"
)

type webhookHandler struct {
	src   source.RunnerSource
	onJob func(*job.Job) error
}

func (h *webhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	events, err := h.src.VerifyAndParse(r)
	if errors.Is(err, github.ErrUnauthorized) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	accepted := false
	for _, ev := range events {
		if ev.Kind != "queued" || !selfHosted(ev.Labels) {
			continue
		}
		j, err := jobFromEvent(ev)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if h.onJob == nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if err := h.onJob(j); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		accepted = true
	}
	if accepted {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func selfHosted(labels []string) bool {
	for _, l := range labels {
		if l == "self-hosted" {
			return true
		}
	}
	return false
}

func jobFromEvent(ev source.JobEvent) (*job.Job, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	return &job.Job{
		ID:         id,
		Source:     "github",
		ExternalID: ev.ExternalID,
		Repo:       ev.Repo,
		RunID:      ev.RunID,
		Labels:     ev.Labels,
		State:      job.JobQueued,
	}, nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("forge: id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
