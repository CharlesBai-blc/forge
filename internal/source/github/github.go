// Package github implements source.RunnerSource against GitHub's API.
package github

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/source"
)

const (
	maxBodyBytes = 1 << 20
	defaultAPI   = "https://api.github.com"
)

// ErrUnauthorized is returned when webhook HMAC verification fails.
var ErrUnauthorized = errors.New("github: unauthorized")

var _ source.RunnerSource = (*Source)(nil)

// Source talks to one GitHub repo or org (FR-4, FR-7). Token is a PAT
// or installation token; encryption at rest is FR-27.
type Source struct {
	Secret        string
	Token         string
	Owner         string
	Repo          string // empty with Org set means org-level registration
	Org           string
	Labels        []string
	RunnerGroupID int64
	Client        *http.Client
	BaseURL       string // override for tests
}

type webhookPayload struct {
	Action      string `json:"action"`
	WorkflowJob struct {
		ID         int64    `json:"id"`
		RunID      int64    `json:"run_id"`
		Labels     []string `json:"labels"`
		Conclusion string   `json:"conclusion"`
	} `json:"workflow_job"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

func (s *Source) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return http.DefaultClient
}

func (s *Source) apiBase() string {
	if s.BaseURL != "" {
		return strings.TrimRight(s.BaseURL, "/")
	}
	return defaultAPI
}

func (s *Source) runnersURL() string {
	if s.Org != "" && s.Repo == "" {
		return s.apiBase() + "/orgs/" + s.Org + "/actions/runners"
	}
	return s.apiBase() + "/repos/" + s.Owner + "/" + s.Repo + "/actions/runners"
}

func (s *Source) runsURL() string {
	if s.Org != "" && s.Repo == "" {
		return s.apiBase() + "/orgs/" + s.Org + "/actions/runs"
	}
	return s.apiBase() + "/repos/" + s.Owner + "/" + s.Repo + "/actions/runs"
}

func (s *Source) runnerGroupID() int64 {
	if s.RunnerGroupID == 0 {
		return 1
	}
	return s.RunnerGroupID
}

func (s *Source) VerifyAndParse(r *http.Request) ([]source.JobEvent, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("github: read body: %w", err)
	}
	if len(body) > maxBodyBytes {
		return nil, fmt.Errorf("github: payload too large")
	}
	if !verifySignature(s.Secret, body, r.Header.Get("X-Hub-Signature-256")) {
		return nil, ErrUnauthorized
	}
	if r.Header.Get("X-GitHub-Event") != "workflow_job" {
		return nil, nil
	}
	var p webhookPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("github: parse: %w", err)
	}
	kind := p.Action
	switch kind {
	case "queued", "in_progress", "completed":
	default:
		return nil, nil
	}
	return []source.JobEvent{{
		Kind:       kind,
		ExternalID: p.WorkflowJob.ID,
		Repo:       p.Repository.FullName,
		RunID:      p.WorkflowJob.RunID,
		Labels:     p.WorkflowJob.Labels,
		Conclusion: p.WorkflowJob.Conclusion,
	}}, nil
}

func (s *Source) RegisterJIT(ctx context.Context, j *job.Job) (*source.JITConfig, error) {
	labels := j.Labels
	if len(labels) == 0 {
		labels = s.Labels
	}
	name := j.ID
	if name == "" {
		name = fmt.Sprintf("forge-%d", j.ExternalID)
	}
	payload, err := json.Marshal(struct {
		Name          string   `json:"name"`
		RunnerGroupID int64    `json:"runner_group_id"`
		Labels        []string `json:"labels"`
		WorkFolder    string   `json:"work_folder"`
	}{
		Name:          "forge-" + name,
		RunnerGroupID: s.runnerGroupID(),
		Labels:        labels,
		WorkFolder:    "_work",
	})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Runner struct {
			ID int64 `json:"id"`
		} `json:"runner"`
		EncodedJITConfig string `json:"encoded_jit_config"`
	}
	if err := s.doJSON(ctx, http.MethodPost, s.runnersURL()+"/generate-jitconfig", payload, http.StatusCreated, &resp); err != nil {
		return nil, err
	}
	if resp.EncodedJITConfig == "" {
		return nil, fmt.Errorf("github: empty jit config")
	}
	return &source.JITConfig{RunnerID: resp.Runner.ID, Encoded: resp.EncodedJITConfig}, nil
}

func (s *Source) Unregister(ctx context.Context, runnerID int64) error {
	return s.doJSON(ctx, http.MethodDelete, fmt.Sprintf("%s/%d", s.runnersURL(), runnerID), nil, http.StatusNoContent, nil)
}

func (s *Source) ListQueued(ctx context.Context) ([]source.JobEvent, error) {
	var resp struct {
		WorkflowRuns []struct {
			ID         int64 `json:"id"`
			Repository struct {
				FullName string `json:"full_name"`
			} `json:"repository"`
		} `json:"workflow_runs"`
	}
	if err := s.doJSON(ctx, http.MethodGet, s.runsURL()+"?status=queued", nil, http.StatusOK, &resp); err != nil {
		return nil, err
	}
	out := make([]source.JobEvent, 0, len(resp.WorkflowRuns))
	for _, run := range resp.WorkflowRuns {
		repo := run.Repository.FullName
		if repo == "" && s.Owner != "" && s.Repo != "" {
			repo = s.Owner + "/" + s.Repo
		}
		out = append(out, source.JobEvent{
			Kind:       "queued",
			ExternalID: run.ID,
			Repo:       repo,
			RunID:      run.ID,
			Labels:     s.Labels,
		})
	}
	return out, nil
}

func (s *Source) doJSON(ctx context.Context, method, url string, body []byte, want int, dest any) error {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+s.Token)
	req.Header.Set("User-Agent", "forge")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return fmt.Errorf("github: %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return err
	}
	if resp.StatusCode != want {
		return fmt.Errorf("github: %s %s: status %d: %s", method, url, resp.StatusCode, bytes.TrimSpace(b))
	}
	if dest == nil || len(b) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("github: decode: %w", err)
	}
	return nil
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
