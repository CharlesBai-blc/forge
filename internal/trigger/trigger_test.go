package trigger

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CharlesBai-blc/forge/internal/job"
)

const testSecret = "test-secret"

func sign(body []byte) string {
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func queuedBody() []byte {
	return []byte(`{
		"action": "queued",
		"workflow_job": {"id": 11, "run_id": 22, "labels": ["self-hosted"]},
		"repository": {"full_name": "owner/name"}
	}`)
}

func do(h http.Handler, method, event, sig string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/webhook/github", bytes.NewReader(body))
	if event != "" {
		req.Header.Set("X-GitHub-Event", event)
	}
	if sig != "" {
		req.Header.Set("X-Hub-Signature-256", sig)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestQueuedWorkflowJobAccepted(t *testing.T) {
	var got *job.Job
	h := &Handler{
		Config: Config{WebhookSecret: testSecret},
		OnJob: func(j *job.Job) error {
			got = j
			return nil
		},
	}
	body := queuedBody()
	rec := do(h, http.MethodPost, "workflow_job", sign(body), body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if got == nil {
		t.Fatal("OnJob not called")
	}
	if got.Source != "github" || got.State != job.JobQueued {
		t.Errorf("Source/State = %s %s, want github queued", got.Source, got.State)
	}
	if got.ExternalID != 11 || got.RunID != 22 || got.Repo != "owner/name" {
		t.Errorf("ids = %+v", got)
	}
	if got.ID == "" {
		t.Error("ID not set")
	}
}

func TestMissingSignatureUnauthorized(t *testing.T) {
	called := false
	h := &Handler{
		Config: Config{WebhookSecret: testSecret},
		OnJob:  func(*job.Job) error { called = true; return nil },
	}
	body := queuedBody()
	rec := do(h, http.MethodPost, "workflow_job", "", body)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("OnJob called")
	}
}

func TestBadSignatureUnauthorized(t *testing.T) {
	called := false
	h := &Handler{
		Config: Config{WebhookSecret: testSecret},
		OnJob:  func(*job.Job) error { called = true; return nil },
	}
	body := queuedBody()
	rec := do(h, http.MethodPost, "workflow_job", "sha256=00", body)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("OnJob called")
	}
}

func TestOtherEventNoContent(t *testing.T) {
	called := false
	h := &Handler{
		Config: Config{WebhookSecret: testSecret},
		OnJob:  func(*job.Job) error { called = true; return nil },
	}
	body := []byte(`{"zen": "ok"}`)
	rec := do(h, http.MethodPost, "ping", sign(body), body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if called {
		t.Fatal("OnJob called")
	}
}

func TestCompletedActionNoContent(t *testing.T) {
	called := false
	h := &Handler{
		Config: Config{WebhookSecret: testSecret},
		OnJob:  func(*job.Job) error { called = true; return nil },
	}
	body := []byte(`{"action": "completed", "workflow_job": {"id": 1}}`)
	rec := do(h, http.MethodPost, "workflow_job", sign(body), body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if called {
		t.Fatal("OnJob called")
	}
}

func TestOnJobErrorInternal(t *testing.T) {
	h := &Handler{
		Config: Config{WebhookSecret: testSecret},
		OnJob:  func(*job.Job) error { return errors.New("boom") },
	}
	body := queuedBody()
	rec := do(h, http.MethodPost, "workflow_job", sign(body), body)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
