// Package sandbox is the seam between Forge and a container runtime
// (FR-13, FR-14). v1.0 ships one implementation: docker.
package sandbox

import (
	"context"
	"io"
)

// Spec is the per-job sandbox configuration.
// Command is the process to run. Hardened is unused until M4.
type Spec struct {
	Image       string
	Command     []string
	CPU         float64
	MemoryBytes int64
	PIDs        int64
	DiskBytes   int64
	Hardened    bool
}

// Provider creates sandboxes. Create is the only source and always
// returns a fresh container; there is no reuse method (FR-13).
type Provider interface {
	Create(ctx context.Context, spec Spec) (Sandbox, error)
}

// Sandbox is a single-use job execution environment.
type Sandbox interface {
	ID() string

	// Start launches Spec.Command. If jitEncoded is non-empty, it is
	// written into the sandbox first (FR-4). Errors if called twice.
	Start(ctx context.Context, jitEncoded string) error

	// Wait blocks until the process exits and returns its exit code.
	Wait(ctx context.Context) (int, error)

	// Logs streams combined stdout and stderr from process start.
	Logs(ctx context.Context) (io.ReadCloser, error)

	// Destroy force-removes the sandbox and its writable layer.
	// Idempotent. No Reset or Release (FR-13).
	Destroy(ctx context.Context) error
}
