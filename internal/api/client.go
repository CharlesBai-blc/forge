package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// StatusError is a control-plane HTTP failure for a status report.
type StatusError struct {
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("api: status: status %d: %s", e.StatusCode, e.Body)
}

// StatusRetryable reports whether the agent should retry the status POST.
func StatusRetryable(err error) bool {
	if err == nil {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.StatusCode >= 500 || se.StatusCode == http.StatusTooManyRequests
	}
	return true
}

// Client is forge-agent's view of the control plane.
type Client struct {
	BaseURL  string
	Token    string
	WorkerID string
	HTTP     *http.Client
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.http().Do(req)
}

// Enroll exchanges a one-time enrollment token for a worker ID and per-machine token (FR-3).
func (c *Client) Enroll(ctx context.Context, token, name, arch, version string) (*EnrollResponse, error) {
	b, err := json.Marshal(EnrollRequest{Token: token, Name: name, Arch: arch, Version: version})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/agents/enroll", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("api: enroll: status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	var out EnrollResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("api: enroll decode: %w", err)
	}
	return &out, nil
}

// Claim long-polls for work. Returns ErrNoJob on 204.
func (c *Client) Claim(ctx context.Context) (*ClaimResponse, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/agents/"+c.WorkerID+"/claim", nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, ErrNoJob
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("api: claim: status %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	var out ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("api: claim decode: %w", err)
	}
	return &out, nil
}

// Status posts a running or terminal report for an attempt.
func (c *Client) Status(ctx context.Context, jobID string, attempt int, rep StatusReport) error {
	b, err := json.Marshal(rep)
	if err != nil {
		return err
	}
	path := "/v1/jobs/" + jobID + "/attempts/" + strconv.Itoa(attempt) + "/status"
	resp, err := c.do(ctx, http.MethodPost, path, bytes.NewReader(b), "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &StatusError{StatusCode: resp.StatusCode, Body: string(bytes.TrimSpace(body))}
	}
	return nil
}

// Logs uploads a full log snapshot for an attempt, replacing any
// previously uploaded content.
func (c *Client) Logs(ctx context.Context, jobID string, attempt int, r io.Reader) error {
	return c.logs(ctx, jobID, attempt, r, false)
}

// AppendLogs uploads one log chunk while the attempt runs (FR-24).
// Chunks are appended in call order.
func (c *Client) AppendLogs(ctx context.Context, jobID string, attempt int, r io.Reader) error {
	return c.logs(ctx, jobID, attempt, r, true)
}

func (c *Client) logs(ctx context.Context, jobID string, attempt int, r io.Reader, appendChunk bool) error {
	path := "/v1/jobs/" + jobID + "/attempts/" + strconv.Itoa(attempt) + "/logs"
	if appendChunk {
		path += "?append=1"
	}
	resp, err := c.do(ctx, http.MethodPost, path, r, "application/octet-stream")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("api: logs: status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	return nil
}

// Heartbeat posts liveness, capacity, and Docker health (FR-18, FR-20).
func (c *Client) Heartbeat(ctx context.Context, hb Heartbeat) error {
	b, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodPost, "/v1/agents/"+c.WorkerID+"/heartbeat", bytes.NewReader(b), "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("api: heartbeat: status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	return nil
}
