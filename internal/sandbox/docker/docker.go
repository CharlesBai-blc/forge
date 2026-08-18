// Package docker implements sandbox.Provider using the Docker Engine API.
package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"

	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

var (
	_ sandbox.Provider = (*Provider)(nil)
	_ sandbox.Sandbox  = (*dockerSandbox)(nil)
)

const jitPath = "jitconfig"

// Hardened profile constants (FR-15, tdd.md §4.5). The capability and
// seccomp baseline is documented in docs/design/threat-model.md and
// asserted by the FR-17 isolation suite.
const (
	// hardenedNetwork is a dedicated bridge with inter-container
	// communication disabled: jobs get egress (GitHub) but cannot
	// reach sibling sandboxes.
	hardenedNetwork = "forge-jobs"
	// hardenedUser is the actions/runner uid. Numeric so it holds on
	// images without a matching passwd entry.
	hardenedUser = "1001"
)

// Provider drives container lifecycle via the Docker Engine API.
type Provider struct {
	cli *client.Client
	Log *slog.Logger

	netMu    sync.Mutex
	netReady bool
}

// NewProvider connects to the local Docker daemon.
func NewProvider() (*Provider, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker: client: %w", err)
	}
	return &Provider{cli: cli}, nil
}

// Ping reports whether the Docker daemon is reachable (FR-18).
func (p *Provider) Ping(ctx context.Context) error {
	_, err := p.cli.Ping(ctx)
	return err
}

func (p *Provider) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

// Close closes the Docker client.
func (p *Provider) Close() error {
	return p.cli.Close()
}

// Create pulls spec.Image if needed and creates a fresh, not-yet-started
// container with spec's resource limits. It does not start the container.
// With spec.Hardened, the FR-15 profile applies: dedicated ICC-off bridge,
// no-new-privileges, all capabilities dropped, Docker's default seccomp
// profile, non-root user, and the disk quota when the storage driver
// supports it.
func (p *Provider) Create(ctx context.Context, spec sandbox.Spec) (sandbox.Sandbox, error) {
	if spec.Image == "" {
		return nil, fmt.Errorf("docker: image required")
	}
	if err := p.ensureImage(ctx, spec.Image); err != nil {
		return nil, err
	}

	cfg := &container.Config{
		Image: spec.Image,
		Cmd:   spec.Command,
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
	if spec.Hardened {
		if err := p.ensureNetwork(ctx); err != nil {
			return nil, err
		}
		host.NetworkMode = hardenedNetwork
		// no-new-privileges blocks setuid escalation; seccomp is left
		// unset so Docker's default profile applies (tdd.md §4.5).
		host.SecurityOpt = []string{"no-new-privileges"}
		// Baseline: none. The runner is non-root, and no capability is
		// required to run CI steps as uid 1001 (threat-model.md).
		host.CapDrop = []string{"ALL"}
		cfg.User = hardenedUser
		if spec.DiskBytes > 0 {
			host.StorageOpt = map[string]string{"size": strconv.FormatInt(spec.DiskBytes, 10)}
		}
	}

	resp, err := p.cli.ContainerCreate(ctx, cfg, host, nil, nil, "")
	if err != nil && host.StorageOpt != nil {
		// FR-14: disk quotas are storage-driver dependent (overlay2 on
		// xfs with pquota). Rather than guess from the daemon's error
		// text, just retry once without the quota: enforcing the rest
		// of the hardened profile beats refusing to run the job. If
		// the retry fails too, its error is the one that's returned.
		p.log().Warn("docker: create with disk limit failed, retrying without it", "err", err)
		host.StorageOpt = nil
		resp, err = p.cli.ContainerCreate(ctx, cfg, host, nil, nil, "")
	}
	if err != nil {
		return nil, fmt.Errorf("docker: create: %w", err)
	}
	return &dockerSandbox{cli: p.cli, containerID: resp.ID}, nil
}

// ensureNetwork creates the hardened bridge if it does not exist.
// Inter-container communication is disabled on it, so sandboxes cannot
// see each other while keeping outbound access to GitHub (FR-15).
func (p *Provider) ensureNetwork(ctx context.Context) error {
	p.netMu.Lock()
	defer p.netMu.Unlock()
	if p.netReady {
		return nil
	}
	_, err := p.cli.NetworkInspect(ctx, hardenedNetwork, network.InspectOptions{})
	if err == nil {
		p.netReady = true
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return fmt.Errorf("docker: network inspect: %w", err)
	}
	_, err = p.cli.NetworkCreate(ctx, hardenedNetwork, network.CreateOptions{
		Driver: "bridge",
		Options: map[string]string{
			"com.docker.network.bridge.enable_icc": "false",
		},
	})
	if err != nil && !errdefs.IsConflict(err) { // conflict: another agent process created it first
		return fmt.Errorf("docker: network create: %w", err)
	}
	p.netReady = true
	return nil
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
	warm    bool
}

var _ sandbox.Warmable = (*dockerSandbox)(nil)

func (s *dockerSandbox) ID() string { return s.containerID }

// WarmStart starts the container before a job attaches (FR-16). The
// command idles until /jitconfig appears, so a later Start only injects
// the credential. Errors if the sandbox already started.
func (s *dockerSandbox) WarmStart(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.warm {
		return fmt.Errorf("docker: sandbox %s already started", s.containerID)
	}
	if err := s.cli.ContainerStart(ctx, s.containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("docker: warm start: %w", err)
	}
	s.warm = true
	return nil
}

// Start begins the job. If jitEncoded is non-empty, it is copied in as
// /jitconfig (FR-4). A warm sandbox is already running its wait loop, so
// the copy alone releases it; a cold one is started after the copy.
// Errors if called twice (FR-13).
func (s *dockerSandbox) Start(ctx context.Context, jitEncoded string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return fmt.Errorf("docker: sandbox %s already started", s.containerID)
	}
	if jitEncoded != "" {
		if err := s.copyJIT(ctx, jitEncoded); err != nil {
			return err
		}
	}
	if !s.warm {
		if err := s.cli.ContainerStart(ctx, s.containerID, container.StartOptions{}); err != nil {
			return fmt.Errorf("docker: start: %w", err)
		}
	}
	s.started = true
	return nil
}

func (s *dockerSandbox) copyJIT(ctx context.Context, encoded string) error {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	b := []byte(encoded)
	if err := tw.WriteHeader(&tar.Header{
		Name: jitPath,
		Mode: 0o644, // actions/runner runs as uid 1001; 0600 would be unreadable
		Size: int64(len(b)),
	}); err != nil {
		return fmt.Errorf("docker: jit tar: %w", err)
	}
	if _, err := tw.Write(b); err != nil {
		return fmt.Errorf("docker: jit tar: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("docker: jit tar: %w", err)
	}
	if err := s.cli.CopyToContainer(ctx, s.containerID, "/", &buf, container.CopyToContainerOptions{}); err != nil {
		return fmt.Errorf("docker: copy jit: %w", err)
	}
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

// Logs streams combined stdout and stderr from container start. With
// follow set, the stream stays open until the container stops.
func (s *dockerSandbox) Logs(ctx context.Context, follow bool) (io.ReadCloser, error) {
	r, err := s.cli.ContainerLogs(ctx, s.containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
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
