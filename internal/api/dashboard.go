package api

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

//go:embed static
var dashboardFS embed.FS

const defaultLogPoll = 200 * time.Millisecond

// page redirects to setup before an admin exists (FR-2), serves the
// login page without a session, and the dashboard otherwise (tdd.md §7).
func (h *Handler) page(w http.ResponseWriter, r *http.Request) {
	name := "static/index.html"
	if pending, err := h.SetupPending(r.Context()); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	} else if pending {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	} else if !h.session(r) {
		name = "static/login.html"
	}
	b, err := dashboardFS.ReadFile(name)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

// setupPage exists only until the first admin account is created.
func (h *Handler) setupPage(w http.ResponseWriter, r *http.Request) {
	pending, err := h.SetupPending(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !pending {
		http.NotFound(w, r)
		return
	}
	b, err := dashboardFS.ReadFile("static/setup.html")
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

func (h *Handler) dashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	n, err := h.Store.CountQueued(ctx)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	jobs, err := h.Store.ListJobs(ctx, 100)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	workers, err := h.Store.ListWorkers(ctx)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	now := time.Now().UTC()
	out := Dashboard{
		QueueDepth: n,
		Jobs:       make([]DashboardJob, 0, len(jobs)),
		Workers:    make([]DashboardWorker, 0, len(workers)),
	}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, dashboardJob(j, now))
	}
	for _, wk := range workers {
		running, err := h.Store.JobsByWorker(ctx, wk.ID, job.JobRunning)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		out.Workers = append(out.Workers, dashboardWorker(wk, len(running)))
	}
	if h.BurstStatus != nil {
		s := h.BurstStatus(ctx)
		out.Burst = &s
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (h *Handler) jobDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	j, err := h.Store.GetJob(ctx, r.PathValue("id"))
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	trs, err := h.Store.ListTransitions(ctx, j.ID)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	out := JobDetail{
		Job:         dashboardJob(j, time.Now().UTC()),
		Transitions: make([]DashboardTransition, 0, len(trs)),
	}
	for _, tr := range trs {
		out.Transitions = append(out.Transitions, DashboardTransition{
			Attempt: tr.Attempt,
			From:    tr.From,
			To:      tr.To,
			Reason:  tr.Reason,
			At:      tr.At,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (h *Handler) logStream(w http.ResponseWriter, r *http.Request) {
	if h.LogDir == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	jobID := r.PathValue("id")
	if _, err := h.Store.GetJob(r.Context(), jobID); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	offset := 0
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		if n, err := strconv.Atoi(id); err == nil && n > 0 {
			offset = n
		}
	}
	poll := h.LogPoll
	if poll <= 0 {
		poll = defaultLogPoll
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		done, err := h.sendLogDelta(r.Context(), w, flusher, jobID, &offset)
		if err != nil {
			return
		}
		if done {
			fmt.Fprintf(w, "event: end\ndata: {}\n\n")
			flusher.Flush()
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
	}
}

func (h *Handler) sendLogDelta(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, jobID string, offset *int) (done bool, err error) {
	j, err := h.Store.GetJob(ctx, jobID)
	if err != nil {
		return true, nil
	}
	path := filepath.Join(h.LogDir, fmt.Sprintf("%s-%d.log", jobID, j.Attempt))
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if os.IsNotExist(err) {
		b = nil
	}
	if len(b) < *offset {
		*offset = 0
	}
	if len(b) > *offset {
		chunk := string(b[*offset:])
		*offset = len(b)
		payload, merr := json.Marshal(chunk)
		if merr != nil {
			return false, merr
		}
		if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", *offset, payload); err != nil {
			return false, err
		}
		flusher.Flush()
	}
	return jobTerminal(j.State) && len(b) <= *offset, nil
}

func jobTerminal(s job.JobState) bool {
	return s == job.JobSucceeded || s == job.JobFailed
}

func dashboardJob(j *job.Job, now time.Time) DashboardJob {
	end := now
	if jobTerminal(j.State) {
		end = j.UpdatedAt
	}
	dur := end.Sub(j.CreatedAt)
	if dur < 0 {
		dur = 0
	}
	return DashboardJob{
		ID:           j.ID,
		Repo:         j.Repo,
		State:        j.State,
		Attempt:      j.Attempt,
		WorkerID:     j.WorkerID,
		DeadLettered: j.DeadLettered,
		Reason:       j.Reason,
		CreatedAt:    j.CreatedAt,
		UpdatedAt:    j.UpdatedAt,
		DurationMS:   dur.Milliseconds(),
	}
}

func dashboardWorker(w *job.Worker, running int) DashboardWorker {
	util := 0.0
	if w.Capacity > 0 {
		util = float64(running) / float64(w.Capacity)
	}
	return DashboardWorker{
		ID:          w.ID,
		Name:        w.Name,
		State:       w.State,
		Capacity:    w.Capacity,
		Running:     running,
		Healthy:     w.Healthy,
		LastSeen:    w.LastSeen,
		Arch:        w.Arch,
		Utilization: util,
		Burst:       w.Burst,
	}
}
