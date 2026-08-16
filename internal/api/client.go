package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

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
		return fmt.Errorf("api: status: status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	return nil
}

// Logs uploads sandbox stdout/stderr for an attempt.
func (c *Client) Logs(ctx context.Context, jobID string, attempt int, r io.Reader) error {
	path := "/v1/jobs/" + jobID + "/attempts/" + strconv.Itoa(attempt) + "/logs"
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
