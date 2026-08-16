package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/sandbox"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/store"
	"github.com/CharlesBai-blc/forge/internal/stream"
)

const (
	defaultClaimWait   = 30 * time.Second
	defaultLostAfter   = 30 * time.Second
	defaultVisibility  = 60 * time.Second
	defaultMaxAttempts = 2
	defaultSweepEvery  = 10 * time.Second
)

// Handler serves claim, status, and log upload (FR-10, FR-9, FR-26).
type Handler struct {
	Store       *store.Store
	Stream      *stream.Stream
	Source      source.RunnerSource
	Image       string
	Command     []string
	LogDir      string
	Log         *slog.Logger
	ClaimWait   time.Duration
	LostAfter   time.Duration
	Visibility  time.Duration
	MaxAttempts int
	SweepEvery  time.Duration

	mu   sync.Mutex
	jits map[string]int64  // jobID/attempt -> GitHub runner ID, until consumed
	msgs map[string]string // jobID -> stream message ID, until XACK
}

// Register attaches agent routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/agents/enroll", h.enroll)
	mux.HandleFunc("POST /v1/agents/{id}/heartbeat", h.heartbeat)
	mux.HandleFunc("GET /v1/agents/{id}/claim", h.claim)
	mux.HandleFunc("POST /v1/jobs/{id}/attempts/{n}/status", h.status)
	mux.HandleFunc("POST /v1/jobs/{id}/attempts/{n}/logs", h.logs)
}

func (h *Handler) log() *slog.Logger {
	if h.Log == nil {
		return slog.Default()
	}
	return h.Log
}

func (h *Handler) enroll(w http.ResponseWriter, r *http.Request) {
	var req EnrollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Token == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id, tok, err := h.Store.Enroll(r.Context(), req.Token, req.Name, req.Arch, req.Version)
	if errors.Is(err, store.ErrEnrollmentUsed) {
		http.Error(w, "conflict", http.StatusConflict)
		return
	}
	if errors.Is(err, store.ErrEnrollmentExpired) || errors.Is(err, store.ErrEnrollmentInvalid) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err != nil {
		h.log().Error("enroll", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(EnrollResponse{WorkerID: id, Token: tok})
}

func (h *Handler) heartbeat(w http.ResponseWriter, r *http.Request) {
	wk, ok := h.worker(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.PathValue("id") != wk.ID {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var hb Heartbeat
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := h.Store.Heartbeat(r.Context(), wk.ID, hb.Capacity, hb.Healthy); err != nil {
		h.log().Error("heartbeat", "worker", wk.ID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) worker(r *http.Request) (*job.Worker, bool) {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || got == "" {
		return nil, false
	}
	wk, err := h.Store.WorkerByToken(r.Context(), got)
	if err != nil || wk.State == job.WorkerRemoved {
		return nil, false
	}
	return wk, true
}

func (h *Handler) claim(w http.ResponseWriter, r *http.Request) {
	wk, ok := h.worker(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	workerID := r.PathValue("id")
	if workerID == "" || workerID != wk.ID {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if wk.State != job.WorkerActive {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !wk.Healthy {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	wait := h.ClaimWait
	if wait == 0 {
		wait = defaultClaimWait
	}
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	msg, err := h.Stream.Claim(ctx, workerID, wait)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	j, err := h.Store.GetJob(r.Context(), msg.JobID)
	if err != nil || j.State != job.JobQueued {
		if aerr := h.Stream.Ack(context.Background(), msg.ID); aerr != nil {
			h.log().Error("ack skip", "job", msg.JobID, "err", aerr)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	jit, err := h.Source.RegisterJIT(r.Context(), j)
	if err != nil {
		h.log().Error("register jit", "job", j.ID, "err", err)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	assigned, err := h.Store.Assign(r.Context(), j.ID, workerID)
	if err != nil {
		h.log().Error("assign", "job", j.ID, "err", err)
		if uerr := h.Source.Unregister(context.Background(), jit.RunnerID); uerr != nil {
			h.log().Error("unregister", "job", j.ID, "err", uerr)
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.mu.Lock()
	if h.jits == nil {
		h.jits = make(map[string]int64)
	}
	if h.msgs == nil {
		h.msgs = make(map[string]string)
	}
	h.jits[jitKey(assigned.ID, assigned.Attempt)] = jit.RunnerID
	h.msgs[assigned.ID] = msg.ID
	h.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ClaimResponse{
		JobID:   assigned.ID,
		Attempt: assigned.Attempt,
		JIT:     jit.Encoded,
		Spec: sandbox.Spec{
			Image:   h.Image,
			Command: h.Command,
		},
	})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	wk, ok := h.worker(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	jobID := r.PathValue("id")
	n, err := strconv.Atoi(r.PathValue("n"))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var rep StatusReport
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	j, err := h.Store.GetJob(r.Context(), jobID)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if j.WorkerID != wk.ID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if j.Attempt != n {
		http.Error(w, "conflict", http.StatusConflict)
		return
	}

	switch rep.State {
	case job.JobRunning:
		if err := h.Store.Transition(r.Context(), jobID, job.JobRunning, ""); err != nil {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		h.forgetJIT(jobID, n, false)
	case job.JobSucceeded:
		if err := h.Store.Transition(r.Context(), jobID, job.JobSucceeded, rep.Reason); err != nil {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		h.forgetJIT(jobID, n, false)
		h.ackJob(jobID)
	case job.JobFailed:
		reason := rep.Reason
		if reason == "" && rep.ExitCode != nil {
			reason = fmt.Sprintf("exit %d", *rep.ExitCode)
		}
		if j.State == job.JobAssigned {
			if err := h.Store.Transition(r.Context(), jobID, job.JobLost, reason); err != nil {
				http.Error(w, "conflict", http.StatusConflict)
				return
			}
			h.forgetJIT(jobID, n, true)
		}
		if err := h.Store.Transition(r.Context(), jobID, job.JobFailed, reason); err != nil {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		h.forgetJIT(jobID, n, false)
		h.ackJob(jobID)
	default:
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) logs(w http.ResponseWriter, r *http.Request) {
	wk, ok := h.worker(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if h.LogDir == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	jobID := r.PathValue("id")
	n, err := strconv.Atoi(r.PathValue("n"))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	j, err := h.Store.GetJob(r.Context(), jobID)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if j.WorkerID != wk.ID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if err := os.MkdirAll(h.LogDir, 0o755); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	path := filepath.Join(h.LogDir, fmt.Sprintf("%s-%d.log", jobID, n))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer f.Close()
	if _, err := io.Copy(f, r.Body); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) forgetJIT(jobID string, attempt int, unregister bool) {
	key := jitKey(jobID, attempt)
	h.mu.Lock()
	id, ok := h.jits[key]
	if ok {
		delete(h.jits, key)
	}
	h.mu.Unlock()
	if !unregister || !ok {
		return
	}
	if err := h.Source.Unregister(context.Background(), id); err != nil {
		h.log().Error("unregister", "job", jobID, "runner", id, "err", err)
	}
}

func (h *Handler) ackJob(jobID string) {
	h.mu.Lock()
	id, ok := h.msgs[jobID]
	if ok {
		delete(h.msgs, jobID)
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	if err := h.Stream.Ack(context.Background(), id); err != nil {
		h.log().Error("ack", "job", jobID, "err", err)
	}
}

func jitKey(jobID string, attempt int) string {
	return jobID + "/" + strconv.Itoa(attempt)
}

// ErrNoJob is returned by Client.Claim when the long-poll times out (204).
var ErrNoJob = errors.New("api: no job")
