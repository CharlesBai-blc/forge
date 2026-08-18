package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/secret"
	"github.com/CharlesBai-blc/forge/internal/store"
)

// Admin session auth (FR-2, tdd.md §7): one admin account, Argon2id
// password hash, random session token held in an HttpOnly cookie with
// only its SHA-256 hash stored server-side.

const (
	sessionCookie     = "forge_session"
	defaultSessionTTL = 24 * time.Hour
	minPasswordLen    = 8
)

func (h *Handler) sessionTTL() time.Duration {
	if h.SessionTTL > 0 {
		return h.SessionTTL
	}
	return defaultSessionTTL
}

// session reports whether the request carries a valid, unexpired
// session cookie.
func (h *Handler) session(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return false
	}
	exp, err := h.Store.SessionExpiresAt(r.Context(), hashSession(c.Value))
	if err != nil {
		return false
	}
	return time.Now().UTC().Before(exp)
}

// requireSession gates dashboard-serving JSON routes (tdd.md §7).
func (h *Handler) requireSession(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.session(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// startSession issues a session token and sets the cookie. Secure is
// set when the request arrived over TLS; on plain HTTP (local install
// before TLS setup) a Secure cookie would be dropped by the browser.
func (h *Handler) startSession(w http.ResponseWriter, r *http.Request) error {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	tok := hex.EncodeToString(b)
	ttl := h.sessionTTL()
	if err := h.Store.CreateSession(r.Context(), hashSession(tok), time.Now().UTC().Add(ttl)); err != nil {
		return err
	}
	// Opportunistic cleanup; login is rare.
	if err := h.Store.DeleteExpiredSessions(r.Context(), time.Now().UTC()); err != nil {
		h.log().Error("delete expired sessions", "err", err)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    tok,
		Path:     "/",
		MaxAge:   int(ttl / time.Second),
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteStrictMode,
	})
	return nil
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (h *Handler) adminLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Username == "" || req.Password == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	username, hash, err := h.Store.GetAdmin(r.Context())
	if errors.Is(err, store.ErrAdminNotFound) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err != nil {
		h.log().Error("login", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Verify the password even on a username mismatch so both failure
	// modes take the same time.
	ok := secret.VerifyPassword(hash, req.Password)
	if req.Username != username || !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := h.startSession(w, r); err != nil {
		h.log().Error("login session", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) adminLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" {
		if err := h.Store.DeleteSession(r.Context(), hashSession(c.Value)); err != nil {
			h.log().Error("logout", "err", err)
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode,
	})
	w.WriteHeader(http.StatusNoContent)
}

type setupRequest struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	WebhookSecret string `json:"webhook_secret,omitempty"`
	GitHubToken   string `json:"github_token,omitempty"`
}

// setupSubmit is the one-shot first-run setup (FR-2 web flow at M5):
// it creates the admin account, optionally stores GitHub credentials,
// and logs the browser in. Once an admin exists it returns 404.
func (h *Handler) setupSubmit(w http.ResponseWriter, r *http.Request) {
	h.setupMu.Lock()
	defer h.setupMu.Unlock()

	exists, err := h.Store.AdminExists(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if exists {
		http.NotFound(w, r)
		return
	}
	var req setupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Username == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if len(req.Password) < minPasswordLen {
		http.Error(w, "password too short", http.StatusBadRequest)
		return
	}
	hash, err := secret.HashPassword(req.Password)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Persist and validate credentials before creating the one-shot
	// admin row. If this fails, setup remains available for retry.
	if h.SaveCreds != nil {
		if err := h.SaveCreds(r.Context(), req.WebhookSecret, req.GitHubToken); err != nil {
			h.log().Error("setup creds", "err", err)
			http.Error(w, "GitHub credentials required", http.StatusBadRequest)
			return
		}
	}
	if err := h.Store.CreateAdmin(r.Context(), req.Username, hash); err != nil {
		if errors.Is(err, store.ErrAdminExists) {
			http.NotFound(w, r)
			return
		}
		h.log().Error("setup admin", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := h.startSession(w, r); err != nil {
		h.log().Error("setup session", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// adminWorkerAction is fleet control from the dashboard: cordon,
// uncordon, drain, and remove (FR-19). Remove also revokes the
// machine token (FR-27).
func (h *Handler) adminWorkerAction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var err error
	switch r.PathValue("action") {
	case "cordon":
		err = h.Store.TransitionWorker(r.Context(), id, job.WorkerCordoned)
	case "uncordon":
		err = h.Store.TransitionWorker(r.Context(), id, job.WorkerActive)
	case "drain":
		err = h.Store.TransitionWorker(r.Context(), id, job.WorkerDraining)
	case "remove":
		err = h.Store.RemoveWorker(r.Context(), id)
	default:
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err != nil {
		h.log().Error("worker action", "worker", id, "err", err)
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func hashSession(tok string) []byte {
	sum := sha256.Sum256([]byte(tok))
	return sum[:]
}

// SetupPending reports whether first-run setup has not completed.
func (h *Handler) SetupPending(ctx context.Context) (bool, error) {
	exists, err := h.Store.AdminExists(ctx)
	return !exists, err
}
