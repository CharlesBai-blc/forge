package api

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestSpecEndpoint verifies agents can fetch the sandbox configuration
// before any claim, which feeds the warm pool (FR-16).
func TestSpecEndpoint(t *testing.T) {
	_, _, _, c, _, h := openAPI(t, nil)
	h.DiskBytes = 20 << 30
	h.Hardened = true

	spec, err := c.Spec(context.Background())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}
	if spec.Image != h.Image || !spec.Hardened || spec.DiskBytes != h.DiskBytes {
		t.Errorf("spec = %+v, want image=%s hardened disk=%d", spec, h.Image, h.DiskBytes)
	}
	if spec.CPU != h.CPU || spec.MemoryBytes != h.MemoryBytes || spec.PIDs != h.PIDs {
		t.Errorf("spec limits = %+v, want FR-14 limits from handler", spec)
	}
}

func TestSpecEndpointRequiresWorkerToken(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	bad := &Client{BaseURL: c.BaseURL, Token: "wrong", WorkerID: c.WorkerID, HTTP: c.HTTP}
	if _, err := bad.Spec(context.Background()); err == nil {
		t.Fatal("expected error for bad token")
	}
}

// TestMetricsEndpoint verifies /metrics serves Prometheus text with the
// FR-25 control-plane series.
func TestMetricsEndpoint(t *testing.T) {
	_, _, _, c, _, _ := openAPI(t, nil)
	resp, err := http.Get(c.BaseURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(b)
	if !strings.Contains(body, "forge_queue_depth") {
		t.Errorf("metrics missing forge_queue_depth:\n%s", body)
	}
}
