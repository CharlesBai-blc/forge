package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/store"
)

func postJSON(t *testing.T, hc *http.Client, url, body string) *http.Response {
	t.Helper()
	resp, err := hc.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestLoginRejectsBadCredentials(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	for _, body := range []string{
		fmt.Sprintf(`{"username":%q,"password":"wrong-password"}`, testAdminUser),
		fmt.Sprintf(`{"username":"nobody","password":%q}`, testAdminPassword),
	} {
		resp := postJSON(t, http.DefaultClient, c.BaseURL+"/v1/admin/login", body)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("login %s: status = %d, want 401", body, resp.StatusCode)
		}
	}
	resp := postJSON(t, http.DefaultClient, c.BaseURL+"/v1/admin/login", `{"username":""}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty login: status = %d, want 400", resp.StatusCode)
	}
}

func TestSessionGatesDashboardRoutes(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	gated := []struct{ method, path string }{
		{http.MethodGet, "/v1/dashboard"},
		{http.MethodGet, "/v1/jobs/some-id"},
		{http.MethodGet, "/v1/jobs/some-id/logs/stream"},
		{http.MethodPost, "/v1/admin/workers/w1/cordon"},
	}
	for _, g := range gated {
		req, err := http.NewRequest(g.method, c.BaseURL+g.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s %s without session: status = %d, want 401", g.method, g.path, resp.StatusCode)
		}
	}

	// A garbage cookie is as good as none.
	req, _ := http.NewRequest(http.MethodGet, c.BaseURL+"/v1/dashboard", nil)
	req.AddCookie(&http.Cookie{Name: "forge_session", Value: "not-a-real-token"})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("forged cookie: status = %d, want 401", resp.StatusCode)
	}
}

func TestSessionExpires(t *testing.T) {
	_, _, _, c, _, h := openAPI(t, nil)
	h.SessionTTL = 500 * time.Millisecond
	hc := adminHTTP(t, c.BaseURL) // includes a real Argon2id login; leave TTL headroom for that

	resp, err := hc.Get(c.BaseURL + "/v1/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fresh session: status = %d", resp.StatusCode)
	}

	time.Sleep(700 * time.Millisecond)
	resp, err = hc.Get(c.BaseURL + "/v1/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired session: status = %d, want 401", resp.StatusCode)
	}
}

func TestLogoutInvalidatesSession(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	hc := adminHTTP(t, c.BaseURL)

	resp := postJSON(t, hc, c.BaseURL+"/v1/admin/logout", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logout: status = %d", resp.StatusCode)
	}
	r, err := hc.Get(c.BaseURL + "/v1/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("after logout: status = %d, want 401", r.StatusCode)
	}
}

func TestSessionCookieHardened(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	body := fmt.Sprintf(`{"username":%q,"password":%q}`, testAdminUser, testAdminPassword)
	resp := postJSON(t, http.DefaultClient, c.BaseURL+"/v1/admin/login", body)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("login: status = %d", resp.StatusCode)
	}
	var cookie *http.Cookie
	for _, ck := range resp.Cookies() {
		if ck.Name == "forge_session" {
			cookie = ck
		}
	}
	if cookie == nil {
		t.Fatal("no forge_session cookie")
	}
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("cookie = %+v; want HttpOnly SameSite=Strict", cookie)
	}
}

// setupServer is openAPI without the pre-seeded admin, for FR-2 tests.
func setupServer(t *testing.T) (*store.Store, string, *Handler) {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "forge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	h := &Handler{Store: st, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return st, srv.URL, h
}

func TestSetupFlowOneShot(t *testing.T) {
	st, base, h := setupServer(t)

	var savedWebhook, savedToken string
	h.SaveCreds = func(_ context.Context, webhook, token string) error {
		savedWebhook, savedToken = webhook, token
		return nil
	}

	// Before setup, the page route serves the setup form.
	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "Forge setup") {
		t.Fatalf("expected setup page, got: %.100s", b)
	}

	// Short passwords are rejected.
	r := postJSON(t, http.DefaultClient, base+"/setup", `{"username":"admin","password":"short"}`)
	if r.StatusCode != http.StatusBadRequest {
		t.Fatalf("short password: status = %d, want 400", r.StatusCode)
	}

	jar, _ := cookiejar.New(nil)
	hc := &http.Client{Jar: jar}
	r = postJSON(t, hc, base+"/setup",
		`{"username":"admin","password":"longenough","webhook_secret":"whs","github_token":"ghp_x"}`)
	if r.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("setup: status = %d: %s", r.StatusCode, body)
	}
	if savedWebhook != "whs" || savedToken != "ghp_x" {
		t.Fatalf("SaveCreds got %q, %q", savedWebhook, savedToken)
	}
	if exists, err := st.AdminExists(context.Background()); err != nil || !exists {
		t.Fatalf("admin after setup: %v, %v", exists, err)
	}

	// Setup logs the browser in.
	resp, err = hc.Get(base + "/v1/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dashboard after setup: status = %d", resp.StatusCode)
	}

	// One-shot: a second POST and the setup page 404 / stop serving.
	r = postJSON(t, http.DefaultClient, base+"/setup", `{"username":"evil","password":"longenough"}`)
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("second setup: status = %d, want 404", r.StatusCode)
	}
	resp, err = http.Get(base + "/setup")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("setup page after completion: status = %d, want 404", resp.StatusCode)
	}
	resp, err = http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.Contains(string(b), "Forge setup") {
		t.Fatal("setup page still served after setup completed")
	}
}

func TestSetupCredentialFailureLeavesSetupAvailable(t *testing.T) {
	st, base, h := setupServer(t)
	h.SaveCreds = func(context.Context, string, string) error {
		return fmt.Errorf("credential persistence failed")
	}

	resp := postJSON(t, http.DefaultClient, base+"/setup",
		`{"username":"admin","password":"longenough","webhook_secret":"whs","github_token":"ghp_x"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("setup failure: status = %d, want 400", resp.StatusCode)
	}
	exists, err := st.AdminExists(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("admin was created despite credential failure")
	}
	resp, err = http.Get(base + "/setup")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setup page after failure: status = %d, want 200", resp.StatusCode)
	}
}

func TestAdminWorkerActions(t *testing.T) {
	st, _, _, c, _, _ := openAPI(t, nil)
	hc := adminHTTP(t, c.BaseURL)
	ctx := context.Background()

	id, tok := enrollWorker(t, st)
	worker := &Client{BaseURL: c.BaseURL, Token: tok, WorkerID: id, HTTP: c.HTTP}
	do := func(action string, want int) {
		t.Helper()
		resp := postJSON(t, hc, c.BaseURL+"/v1/admin/workers/"+id+"/"+action, "")
		if resp.StatusCode != want {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("%s: status = %d, want %d: %s", action, resp.StatusCode, want, body)
		}
	}
	state := func(want job.WorkerState) {
		t.Helper()
		w, err := st.GetWorker(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if w.State != want {
			t.Fatalf("state = %s, want %s", w.State, want)
		}
	}

	do("cordon", http.StatusNoContent)
	state(job.WorkerCordoned)
	do("uncordon", http.StatusNoContent)
	state(job.WorkerActive)
	do("drain", http.StatusNoContent)
	state(job.WorkerDraining)
	do("cordon", http.StatusNoContent)
	do("remove", http.StatusNoContent)
	state(job.WorkerRemoved)

	// Removal revoked the machine token (FR-27): the worker's old
	// token no longer authenticates.
	if _, err := worker.Claim(ctx); err == nil || errors.Is(err, ErrNoJob) {
		t.Fatalf("claim with revoked token: err = %v, want unauthorized", err)
	}

	// Invalid transitions and unknown actions are rejected.
	do("uncordon", http.StatusConflict)
	do("explode", http.StatusBadRequest)
}

func TestDashboardShowsBurstStatus(t *testing.T) {
	_, _, _, c, _, h := openAPI(t, nil)
	h.BurstStatus = func(context.Context) BurstStatus {
		return BurstStatus{Instances: 1, MaxInstances: 2, HoursToday: 0.5, MaxHoursPerDay: 12, Banner: "cap"}
	}
	hc := adminHTTP(t, c.BaseURL)
	resp, err := hc.Get(c.BaseURL + "/v1/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, want := range []string{`"instances":1`, `"max_instances":2`, `"banner":"cap"`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("dashboard missing %s: %s", want, b)
		}
	}
}
