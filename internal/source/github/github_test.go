package github

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CharlesBai-blc/forge/internal/job"
)

const testSecret = "test-secret"

func sign(body []byte) string {
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyAndParseQueued(t *testing.T) {
	s := &Source{Secret: testSecret}
	body := []byte(`{
		"action": "queued",
		"workflow_job": {"id": 11, "run_id": 22, "labels": ["self-hosted"]},
		"repository": {"full_name": "owner/name"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/webhook/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "workflow_job")
	req.Header.Set("X-Hub-Signature-256", sign(body))

	events, err := s.VerifyAndParse(req)
	if err != nil {
		t.Fatalf("VerifyAndParse: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	e := events[0]
	if e.Kind != "queued" || e.ExternalID != 11 || e.RunID != 22 || e.Repo != "owner/name" {
		t.Errorf("event = %+v", e)
	}
}

func TestVerifyAndParseBadSignature(t *testing.T) {
	s := &Source{Secret: testSecret}
	body := []byte(`{"action":"queued"}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "workflow_job")
	req.Header.Set("X-Hub-Signature-256", "sha256=00")
	_, err := s.VerifyAndParse(req)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
}

func TestVerifyAndParseOtherEvent(t *testing.T) {
	s := &Source{Secret: testSecret}
	body := []byte(`{"zen":"ok"}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set("X-Hub-Signature-256", sign(body))
	events, err := s.VerifyAndParse(req)
	if err != nil {
		t.Fatalf("VerifyAndParse: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("len = %d, want 0", len(events))
	}
}

func TestRegisterJIT(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"runner":{"id":99},"encoded_jit_config":"jit-blob"}`))
	}))
	t.Cleanup(srv.Close)

	s := &Source{
		Token:   "tok",
		Owner:   "owner",
		Repo:    "name",
		Labels:  []string{"forge"},
		BaseURL: srv.URL,
		Client:  srv.Client(),
	}
	cfg, err := s.RegisterJIT(context.Background(), &job.Job{
		ID:     "abc",
		Labels: []string{"self-hosted", "linux"},
	})
	if err != nil {
		t.Fatalf("RegisterJIT: %v", err)
	}
	if cfg.RunnerID != 99 || cfg.Encoded != "jit-blob" {
		t.Errorf("cfg = %+v", cfg)
	}
	if gotPath != "/repos/owner/name/actions/runners/generate-jitconfig" {
		t.Errorf("path = %s", gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("Authorization = %s", gotAuth)
	}
	labels, _ := gotBody["labels"].([]any)
	if len(labels) != 2 {
		t.Errorf("labels = %v, want job labels", gotBody["labels"])
	}
}

func TestUnregister(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	s := &Source{Token: "tok", Owner: "owner", Repo: "name", BaseURL: srv.URL, Client: srv.Client()}
	if err := s.Unregister(context.Background(), 99); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method = %s, want DELETE", gotMethod)
	}
	if gotPath != "/repos/owner/name/actions/runners/99" {
		t.Errorf("path = %s", gotPath)
	}
}

func TestListQueued(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/name/actions/runs" || r.URL.RawQuery != "status=queued" {
			t.Errorf("url = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"workflow_runs":[{"id":7,"repository":{"full_name":"owner/name"}}]}`))
	}))
	t.Cleanup(srv.Close)

	s := &Source{
		Token:   "tok",
		Owner:   "owner",
		Repo:    "name",
		Labels:  []string{"forge"},
		BaseURL: srv.URL,
		Client:  srv.Client(),
	}
	events, err := s.ListQueued(context.Background())
	if err != nil {
		t.Fatalf("ListQueued: %v", err)
	}
	if len(events) != 1 || events[0].RunID != 7 || events[0].Kind != "queued" {
		t.Fatalf("events = %+v", events)
	}
}

func TestRegisterJITOrg(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"runner":{"id":1},"encoded_jit_config":"x"}`))
	}))
	t.Cleanup(srv.Close)

	s := &Source{Token: "tok", Org: "acme", BaseURL: srv.URL, Client: srv.Client()}
	if _, err := s.RegisterJIT(context.Background(), &job.Job{ID: "j1"}); err != nil {
		t.Fatalf("RegisterJIT: %v", err)
	}
	if !strings.HasPrefix(gotPath, "/orgs/acme/actions/runners/") {
		t.Errorf("path = %s, want org-level", gotPath)
	}
}
