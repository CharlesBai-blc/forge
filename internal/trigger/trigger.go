// Package trigger is the interim M1 GitHub webhook path (FR-8).
// It is deleted at M2 and is not documented for users.
package trigger

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/CharlesBai-blc/forge/internal/job"
)

const maxBodyBytes = 1 << 20

// Config is the fixed M1 trigger: every job uses the same image and command.
type Config struct {
	WebhookSecret string
	Image         string
	Command       []string
}

// Handler verifies a GitHub webhook and, on workflow_job queued, calls OnJob.
type Handler struct {
	Config Config
	OnJob  func(*job.Job) error
}

type webhookPayload struct {
	Action      string `json:"action"`
	WorkflowJob struct {
		ID     int64    `json:"id"`
		RunID  int64    `json:"run_id"`
		Labels []string `json:"labels"`
	} `json:"workflow_job"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if len(body) > maxBodyBytes {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}

	if !verifySignature(h.Config.WebhookSecret, body, r.Header.Get("X-Hub-Signature-256")) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Header.Get("X-GitHub-Event") != "workflow_job" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	var p webhookPayload
	if err := json.Unmarshal(body, &p); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if p.Action != "queued" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	id, err := newID()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	j := &job.Job{
		ID:         id,
		Source:     "github",
		ExternalID: p.WorkflowJob.ID,
		Repo:       p.Repository.FullName,
		RunID:      p.WorkflowJob.RunID,
		Labels:     p.WorkflowJob.Labels,
		State:      job.JobQueued,
	}
	if h.OnJob == nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := h.OnJob(j); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func verifySignature(secret string, body []byte, signatureHeader string) bool {
	const prefix = "sha256="
	if secret == "" || !strings.HasPrefix(signatureHeader, prefix) {
		return false
	}
	got, err := hex.DecodeString(signatureHeader[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hmac.Equal(mac.Sum(nil), got)
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("trigger: id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
