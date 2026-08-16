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
	maxLogBytes        = 100 << 20 // per upload request
)

// Handler serves claim, status, and log upload (FR-10, FR-9, FR-26).
type Handler struct {
	Store       *store.Store
	Stream      *stream.Stream
	Source      source.RunnerSource
	Image       string
	Command     []string
	CPU         float64 // sandbox limits (FR-14); zero disables
	MemoryBytes int64
	PIDs        int64
	LogDir      string
	Log         *slog.Logger
	ClaimWait   time.Duration
	LostAfter   time.Duration
	Visibility  time.Duration
	MaxAttempts int
	SweepEvery  time.Duration
	LogPoll     time.Duration

	mu   sync.Mutex
	msgs map[string]string          // jobID -> stream message ID, until XACK
	hbs  map[string]map[string]bool // workerID -> job IDs reported running in the latest heartbeat
}

// Register attaches agent and dashboard routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/agents/enroll", h.enroll)
	mux.HandleFunc("POST /v1/agents/{id}/heartbeat", h.heartbeat)
	mux.HandleFunc("GET /v1/agents/{id}/claim", h.claim)
	mux.HandleFunc("POST /v1/jobs/{id}/attempts/{n}/status", h.status)
	mux.HandleFunc("POST /v1/jobs/{id}/attempts/{n}/logs", h.logs)
	mux.HandleFunc("GET /{$}", h.page)
	mux.HandleFunc("GET /v1/dashboard", h.dashboard)
	mux.HandleFunc("GET /v1/jobs/{id}", h.jobDetail)
	mux.HandleFunc("GET /v1/jobs/{id}/logs/stream", h.logStream)
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
	running := make(map[string]bool, len(hb.Running))
	for _, id := range hb.Running {
		running[id] = true
	}
	h.mu.Lock()
	if h.hbs == nil {
		h.hbs = make(map[string]map[string]bool)
	}
	h.hbs[wk.ID] = running
	h.mu.Unlock()
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
	deadline := time.Now().Add(wait)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()

	var (
		j     *job.Job
		msgID string
	)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		msg, err := h.Stream.Claim(ctx, workerID, remaining)
		if err != nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		j, err = h.Store.GetJob(r.Context(), msg.JobID)
		if err != nil {
			if errors.Is(err, store.ErrJobNotFound) {
				if aerr := h.Stream.Ack(context.Background(), msg.ID); aerr != nil {
					h.log().Error("ack skip", "job", msg.JobID, "err", aerr)
				}
				continue
			}
			// Transient store error: leave the entry pending for retry.
			h.log().Error("claim job lookup", "job", msg.JobID, "err", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if j.State == job.JobQueued {
			msgID = msg.ID
			break
		}
		if err := h.redelivered(r.Context(), workerID, j, msg.ID); err != nil {
			h.log().Error("claim redelivery", "job", j.ID, "err", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	jit, err := h.Source.RegisterJIT(r.Context(), j)
	if err != nil {
		h.log().Error("register jit", "job", j.ID, "err", err)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	assigned, err := h.Store.Assign(r.Context(), j.ID, workerID, jit.RunnerID)
	if err != nil {
		h.log().Error("assign", "job", j.ID, "err", err)
		if uerr := h.Source.Unregister(context.Background(), jit.RunnerID); uerr != nil {
			h.log().Error("unregister", "job", j.ID, "err", uerr)
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.mu.Lock()
	if h.msgs == nil {
		h.msgs = make(map[string]string)
	}
	h.msgs[assigned.ID] = msgID
	h.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ClaimResponse{
		JobID:   assigned.ID,
		Attempt: assigned.Attempt,
		JIT:     jit.Encoded,
		Spec: sandbox.Spec{
			Image:       h.Image,
			Command:     h.Command,
			CPU:         h.CPU,
			MemoryBytes: h.MemoryBytes,
			PIDs:        h.PIDs,
		},
	})
}

// redelivered resolves a pending stream entry whose job is no longer
// queued. The important case is a job this worker itself was assigned
// or running: the agent is claiming again, so it restarted and
// abandoned the attempt. Acking without resolving would strand the job.
func (h *Handler) redelivered(ctx context.Context, workerID string, j *job.Job, msgID string) error {
	if j.WorkerID != workerID {
		// Terminal, or owned by another worker: drop the stale entry.
		return h.Stream.Ack(ctx, msgID)
	}
	switch j.State {
	case job.JobAssigned:
		if err := h.Store.Transition(ctx, j.ID, job.JobLost, "worker_restart"); err != nil {
			return err
		}
		h.forgetJIT(j.ID, true)
		return h.finishLost(ctx, j, msgID)
	case job.JobRunning:
		// Post-acquisition abandonment: GitHub bound the job to the dead
		// runner, so it cannot be re-dispatched (tdd.md §6.1).
		if err := h.Store.Transition(ctx, j.ID, job.JobLost, "worker_restart"); err != nil {
			return err
		}
		if err := h.Store.Transition(ctx, j.ID, job.JobFailed, "worker_lost"); err != nil {
			return err
		}
		h.forgetJIT(j.ID, false)
		h.forgetMsg(j.ID)
		return h.Stream.Ack(ctx, msgID)
	case job.JobLost:
		// A previous reclamation was interrupted; finish it.
		return h.finishLost(ctx, j, msgID)
	default:
		return h.Stream.Ack(ctx, msgID)
	}
}

// finishLost completes a lost job's reclamation: dead-letter at max
// attempts, otherwise requeue and redeliver (FR-11, FR-12).
func (h *Handler) finishLost(ctx context.Context, j *job.Job, msgID string) error {
	h.forgetMsg(j.ID)
	if j.Attempt >= h.maxAttempts() {
		if err := h.Store.DeadLetter(ctx, j.ID, "max_attempts"); err != nil {
			return err
		}
		return h.Stream.Ack(ctx, msgID)
	}
	if err := h.Store.Requeue(ctx, j.ID); err != nil {
		return err
	}
	if err := h.Stream.Ack(ctx, msgID); err != nil {
		return err
	}
	return h.Stream.Add(ctx, j.ID)
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

	if j.State == rep.State && statusIdempotent(rep.State) {
		if rep.State == job.JobSucceeded || rep.State == job.JobFailed {
			h.ackJob(jobID)
		}
		if rep.State == job.JobRunning {
			h.forgetJIT(jobID, false)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	switch rep.State {
	case job.JobRunning:
		if err := h.Store.Transition(r.Context(), jobID, job.JobRunning, ""); err != nil {
			h.statusStoreError(w, jobID, err)
			return
		}
		h.forgetJIT(jobID, false)
	case job.JobSucceeded:
		if err := h.Store.Transition(r.Context(), jobID, job.JobSucceeded, rep.Reason); err != nil {
			h.statusStoreError(w, jobID, err)
			return
		}
		h.forgetJIT(jobID, false)
		h.ackJob(jobID)
	case job.JobFailed:
		reason := rep.Reason
		if reason == "" && rep.ExitCode != nil {
			reason = fmt.Sprintf("exit %d", *rep.ExitCode)
		}
		if j.State == job.JobAssigned {
			// Pre-acquisition failure (sandbox error): the JIT runner was
			// never consumed, so the attempt requeues to the rest of the
			// fleet, or dead-letters at max attempts (FR-12, tdd.md §6.5).
			if err := h.Store.Transition(r.Context(), jobID, job.JobLost, reason); err != nil {
				h.statusStoreError(w, jobID, err)
				return
			}
			h.forgetJIT(jobID, true)
			if j.Attempt >= h.maxAttempts() {
				if err := h.Store.DeadLetter(r.Context(), jobID, "max_attempts"); err != nil {
					h.statusStoreError(w, jobID, err)
					return
				}
				h.ackJob(jobID)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if err := h.Store.Requeue(r.Context(), jobID); err != nil {
				h.statusStoreError(w, jobID, err)
				return
			}
			h.ackJob(jobID)
			if err := h.Stream.Add(r.Context(), jobID); err != nil {
				h.log().Error("requeue add", "job", jobID, "err", err)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err := h.Store.Transition(r.Context(), jobID, job.JobFailed, reason); err != nil {
			h.statusStoreError(w, jobID, err)
			return
		}
		h.forgetJIT(jobID, false)
		h.ackJob(jobID)
	default:
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func statusIdempotent(s job.JobState) bool {
	return s == job.JobRunning || s == job.JobSucceeded || s == job.JobFailed
}

func (h *Handler) statusStoreError(w http.ResponseWriter, jobID string, err error) {
	if errors.Is(err, job.ErrInvalidTransition) {
		http.Error(w, "conflict", http.StatusConflict)
		return
	}
	h.log().Error("status", "job", jobID, "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
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
	if j.Attempt != n {
		http.Error(w, "conflict", http.StatusConflict)
		return
	}
	if err := os.MkdirAll(h.LogDir, 0o755); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	flags := os.O_CREATE | os.O_WRONLY
	if r.URL.Query().Get("append") == "1" {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	path := filepath.Join(h.LogDir, fmt.Sprintf("%s-%d.log", jobID, n))
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer f.Close()
	body := http.MaxBytesReader(w, r.Body, maxLogBytes)
	if _, err := io.Copy(f, body); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// forgetJIT clears the persisted JIT registration for jobID's current
// attempt, unregistering it at the provider when it was never consumed.
// The registration lives in SQLite, so this survives a restart (FR-5).
func (h *Handler) forgetJIT(jobID string, unregister bool) {
	id, ok, err := h.Store.TakeRunnerID(context.Background(), jobID)
	if err != nil {
		h.log().Error("take runner", "job", jobID, "err", err)
		return
	}
	if !ok || !unregister {
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
		// A restart lost the in-memory map; find the entry in the PEL.
		msgs, err := h.Stream.PendingAll(context.Background())
		if err != nil {
			h.log().Error("ack scan", "job", jobID, "err", err)
			return
		}
		for _, m := range msgs {
			if m.JobID == jobID {
				id = m.ID
				ok = true
				break
			}
		}
		if !ok {
			return
		}
	}
	if err := h.Stream.Ack(context.Background(), id); err != nil {
		h.log().Error("ack", "job", jobID, "err", err)
	}
}

// ErrNoJob is returned by Client.Claim when the long-poll times out (204).
var ErrNoJob = errors.New("api: no job")
