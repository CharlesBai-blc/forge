// Package docker implements sandbox.Provider using the Docker Engine API.
package docker

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"

	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

var (
	_ sandbox.Provider = (*Provider)(nil)
	_ sandbox.Sandbox  = (*dockerSandbox)(nil)
)

// Provider drives container lifecycle via the Docker Engine API.
type Provider struct {
	cli *client.Client
}

// NewProvider connects to the local Docker daemon.
func NewProvider() (*Provider, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker: client: %w", err)
	}
	return &Provider{cli: cli}, nil
}

// Close closes the Docker client.
func (p *Provider) Close() error {
	return p.cli.Close()
}

// Create pulls spec.Image if needed and creates a fresh, not-yet-started
// container with spec's resource limits. It does not start the container.
func (p *Provider) Create(ctx context.Context, spec sandbox.Spec) (sandbox.Sandbox, error) {
	if spec.Image == "" {
		return nil, fmt.Errorf("docker: image required")
	}
	if err := p.ensureImage(ctx, spec.Image); err != nil {
		return nil, err
	}

	host := &container.HostConfig{}
	if spec.CPU > 0 {
		host.NanoCPUs = int64(spec.CPU * 1e9)
	}
	if spec.MemoryBytes > 0 {
		host.Memory = spec.MemoryBytes
	}
	if spec.PIDs > 0 {
		pids := spec.PIDs
		host.PidsLimit = &pids
	}

	resp, err := p.cli.ContainerCreate(ctx, &container.Config{
		Image: spec.Image,
		Cmd:   spec.Command,
	}, host, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("docker: create: %w", err)
	}
	return &dockerSandbox{cli: p.cli, containerID: resp.ID}, nil
}

func (p *Provider) ensureImage(ctx context.Context, ref string) error {
	_, _, err := p.cli.ImageInspectWithRaw(ctx, ref)
	if err == nil {
		return nil
	}
	rc, err := p.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("docker: pull %s: %w", ref, err)
	}
	defer rc.Close()
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return fmt.Errorf("docker: pull %s: %w", ref, err)
	}
	return nil
}

type dockerSandbox struct {
	cli         *client.Client
	containerID string

	mu      sync.Mutex
	started bool
}

func (s *dockerSandbox) ID() string { return s.containerID }

// Start starts the container. Errors if called twice.
func (s *dockerSandbox) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return fmt.Errorf("docker: sandbox %s already started", s.containerID)
	}
	if err := s.cli.ContainerStart(ctx, s.containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("docker: start: %w", err)
	}
	s.started = true
	return nil
}

// Wait blocks until the container exits and returns its exit code.
func (s *dockerSandbox) Wait(ctx context.Context) (int, error) {
	statusCh, errCh := s.cli.ContainerWait(ctx, s.containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return 0, fmt.Errorf("docker: wait: %w", err)
		}
	case status := <-statusCh:
		if status.Error != nil {
			return 0, fmt.Errorf("docker: wait: %s", status.Error.Message)
		}
		return int(status.StatusCode), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return 0, fmt.Errorf("docker: wait: no status")
}

// Logs streams combined stdout and stderr from container start.
func (s *dockerSandbox) Logs(ctx context.Context) (io.ReadCloser, error) {
	r, err := s.cli.ContainerLogs(ctx, s.containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("docker: logs: %w", err)
	}
	pr, pw := io.Pipe()
	go func() {
		_, err := stdcopy.StdCopy(pw, pw, r)
		r.Close()
		pw.CloseWithError(err)
	}()
	return pr, nil
}

// Destroy force-removes the container and its writable layer. Idempotent.
func (s *dockerSandbox) Destroy(ctx context.Context) error {
	err := s.cli.ContainerRemove(ctx, s.containerID, container.RemoveOptions{Force: true})
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("docker: destroy: %w", err)
	}
	return nil
}
