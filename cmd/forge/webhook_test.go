package main

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
	"github.com/CharlesBai-blc/forge/internal/source/github"
)

func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
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

func doWebhook(h http.Handler, method, event, sig string, body []byte) *httptest.ResponseRecorder {
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

func testWebhook(onJob func(*job.Job) error) *webhookHandler {
	return &webhookHandler{
		src:   &github.Source{Secret: testSecret},
		onJob: onJob,
	}
}

func TestQueuedWorkflowJobAccepted(t *testing.T) {
	var got *job.Job
	h := testWebhook(func(j *job.Job) error {
		got = j
		return nil
	})
	body := queuedBody()
	rec := doWebhook(h, http.MethodPost, "workflow_job", signBody(testSecret, body), body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if got == nil {
		t.Fatal("onJob not called")
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
	h := testWebhook(func(*job.Job) error { called = true; return nil })
	body := queuedBody()
	rec := doWebhook(h, http.MethodPost, "workflow_job", "", body)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("onJob called")
	}
}

func TestBadSignatureUnauthorized(t *testing.T) {
	called := false
	h := testWebhook(func(*job.Job) error { called = true; return nil })
	body := queuedBody()
	rec := doWebhook(h, http.MethodPost, "workflow_job", "sha256=00", body)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("onJob called")
	}
}

func TestOtherEventNoContent(t *testing.T) {
	called := false
	h := testWebhook(func(*job.Job) error { called = true; return nil })
	body := []byte(`{"zen": "ok"}`)
	rec := doWebhook(h, http.MethodPost, "ping", signBody(testSecret, body), body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if called {
		t.Fatal("onJob called")
	}
}

func TestHostedQueuedJobIgnored(t *testing.T) {
	called := false
	h := testWebhook(func(*job.Job) error { called = true; return nil })
	body := []byte(`{
		"action": "queued",
		"workflow_job": {"id": 11, "run_id": 22, "labels": ["ubuntu-latest"]},
		"repository": {"full_name": "owner/name"}
	}`)
	rec := doWebhook(h, http.MethodPost, "workflow_job", signBody(testSecret, body), body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if called {
		t.Fatal("onJob called for ubuntu-latest job")
	}
}

func TestCompletedActionNoContent(t *testing.T) {
	called := false
	h := testWebhook(func(*job.Job) error { called = true; return nil })
	body := []byte(`{"action": "completed", "workflow_job": {"id": 1}}`)
	rec := doWebhook(h, http.MethodPost, "workflow_job", signBody(testSecret, body), body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if called {
		t.Fatal("onJob called")
	}
}

func TestOnJobErrorInternal(t *testing.T) {
	h := testWebhook(func(*job.Job) error { return errors.New("boom") })
	body := queuedBody()
	rec := doWebhook(h, http.MethodPost, "workflow_job", signBody(testSecret, body), body)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
