package docker

// FR-17 isolation suite. Proves the hardened profile's claims
// (threat-model.md): no cross-job file persistence, no visibility into
// other containers, resource limits enforced, hardened options applied,
// sandboxes single-use. Requires a Docker daemon and is gated behind
// FORGE_ISOLATION_TESTS=1; CI runs it on main (fs.md FR-17, NFR-7):
//
//	FORGE_ISOLATION_TESTS=1 go test ./internal/sandbox/docker/ -run TestIsolation -v

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

func isolationGate(t *testing.T) *Provider {
	t.Helper()
	if os.Getenv("FORGE_ISOLATION_TESTS") != "1" {
		t.Skip("set FORGE_ISOLATION_TESTS=1 to run the FR-17 isolation suite")
	}
	return dockerProvider(t)
}

// hardenedSpec is the FR-15 profile with FR-14 limits scaled down so
// enforcement failures surface fast.
func hardenedSpec(cmd ...string) sandbox.Spec {
	return sandbox.Spec{
		Image:       testImage,
		Command:     cmd,
		CPU:         1,
		MemoryBytes: 64 << 20,
		PIDs:        16,
		DiskBytes:   1 << 30,
		Hardened:    true,
	}
}

// runHardened creates a hardened sandbox, runs cmd to completion, and
// returns its exit code and combined output.
func runHardened(t *testing.T, p *Provider, cmd ...string) (int, string) {
	t.Helper()
	return runInSandbox(t, p, hardenedSpec(cmd...))
}

func runInSandbox(t *testing.T, p *Provider, spec sandbox.Spec) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sb := createSandbox(t, p, spec)
	if err := sb.Start(ctx, ""); err != nil {
		t.Fatalf("Start: %v", err)
	}
	code, err := sb.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	rc, err := sb.Logs(ctx, false)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read logs: %v", err)
	}
	return code, string(b)
}

// TestIsolationNoCrossJobFilePersistence: a file written by one job is
// gone for the next job on the same image, because every job gets a
// fresh writable layer and the old one is destroyed (FR-13, FR-17).
func TestIsolationNoCrossJobFilePersistence(t *testing.T) {
	p := isolationGate(t)

	code, out := runHardened(t, p, "sh", "-c", "echo contaminated > /tmp/forge-marker && cat /tmp/forge-marker")
	if code != 0 || !strings.Contains(out, "contaminated") {
		t.Fatalf("writer job: code=%d out=%q", code, out)
	}

	code, out = runHardened(t, p, "sh", "-c", "test ! -e /tmp/forge-marker && echo clean")
	if code != 0 || !strings.Contains(out, "clean") {
		t.Fatalf("file persisted across jobs: code=%d out=%q", code, out)
	}
}

// TestIsolationNoCrossContainerVisibility: a hardened sandbox cannot
// reach a sibling sandbox over the network (ICC is disabled on the
// forge-jobs bridge), cannot see the Docker socket, and sees only its
// own processes (FR-15, FR-17).
func TestIsolationNoCrossContainerVisibility(t *testing.T) {
	p := isolationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Sibling: an idle TCP listener another sandbox will try to reach.
	listener := createSandbox(t, p, hardenedSpec("sh", "-c", "nc -l -p 9000; sleep 60"))
	if err := listener.Start(ctx, ""); err != nil {
		t.Fatalf("listener Start: %v", err)
	}
	var ip string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		info, err := p.cli.ContainerInspect(ctx, listener.ID())
		if err != nil {
			t.Fatalf("inspect listener: %v", err)
		}
		if net, ok := info.NetworkSettings.Networks[hardenedNetwork]; ok && net.IPAddress != "" && info.State.Running {
			ip = net.IPAddress
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ip == "" {
		t.Fatal("listener never came up on the forge-jobs network")
	}

	code, out := runHardened(t, p, "sh", "-c",
		fmt.Sprintf("nc -w 2 %s 9000 </dev/null && echo CONNECTED || echo BLOCKED", ip))
	if code != 0 || !strings.Contains(out, "BLOCKED") {
		t.Fatalf("sibling sandbox reachable over ICC-off bridge: code=%d out=%q", code, out)
	}

	code, out = runHardened(t, p, "sh", "-c", "test ! -e /var/run/docker.sock && echo nosock")
	if code != 0 || !strings.Contains(out, "nosock") {
		t.Fatalf("docker socket visible in sandbox: code=%d out=%q", code, out)
	}

	// PID namespace: with the listener still running on the host side,
	// the probe sees only its own process tree.
	code, out = runHardened(t, p, "sh", "-c", "ls -d /proc/[0-9]* | wc -l")
	if code != 0 {
		t.Fatalf("proc probe: code=%d out=%q", code, out)
	}
	n := parseTrailingInt(t, out)
	if n > 4 {
		t.Fatalf("sandbox sees %d processes, expected only its own shell tree", n)
	}
}

// TestIsolationResourceLimitsEnforced: the FR-14 limits do more than
// appear in the container config: exceeding memory kills the job and
// the PID quota bounds fork attempts (FR-17).
func TestIsolationResourceLimitsEnforced(t *testing.T) {
	p := isolationGate(t)

	// Memory: allocating past 64 MiB gets the process OOM-killed.
	code, out := runHardened(t, p, "sh", "-c", "tail /dev/zero")
	if code == 0 {
		t.Fatalf("allocation past the memory limit succeeded: out=%q", out)
	}
	if code != 137 {
		t.Logf("memory hog exit = %d (expected 137/SIGKILL from the OOM killer)", code)
	}

	// PIDs: 64 concurrent forks under a quota of 16 must fail. The
	// shell aborts when fork is denied (busybox treats it as fatal),
	// so enforcement shows up as a nonzero exit. The control run with
	// a roomy quota proves the workload itself is fine.
	forkBomb := []string{"sh", "-c", "for i in $(seq 1 64); do sleep 30 & done; echo all_forked"}
	code, out = runHardened(t, p, forkBomb...)
	if code == 0 && strings.Contains(out, "all_forked") {
		t.Fatal("64 concurrent forks succeeded under a PID limit of 16")
	}
	t.Logf("PID limit stopped the fork bomb: code=%d", code)

	control := hardenedSpec(forkBomb...)
	control.PIDs = 128
	code, out = runInSandbox(t, p, control)
	if code != 0 || !strings.Contains(out, "all_forked") {
		t.Fatalf("control run under PID limit 128 failed: code=%d out=%q", code, out)
	}
}

// TestIsolationDiskQuota: writing past DiskBytes fails when the storage
// driver supports per-container quotas; skipped otherwise (FR-14: disk
// limits are storage-driver dependent).
func TestIsolationDiskQuota(t *testing.T) {
	p := isolationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sb := createSandbox(t, p, hardenedSpec("sh", "-c",
		"dd if=/dev/zero of=/tmp/blob bs=1M count=2048 2>&1 && echo WROTE || echo DENIED"))
	info, err := p.cli.ContainerInspect(ctx, sb.ID())
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if _, ok := info.HostConfig.StorageOpt["size"]; !ok {
		t.Skip("storage driver does not support per-container disk quotas")
	}
	if err := sb.Start(ctx, ""); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := sb.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	rc, err := sb.Logs(ctx, false)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if !strings.Contains(string(b), "DENIED") {
		t.Fatalf("wrote 2 GiB under a 1 GiB quota: %q", b)
	}
}

// TestIsolationHardenedProfileApplied asserts the FR-15 options on the
// created container and that the job really runs as the non-root user.
func TestIsolationHardenedProfileApplied(t *testing.T) {
	p := isolationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sb := createSandbox(t, p, hardenedSpec("id", "-u"))
	info, err := p.cli.ContainerInspect(ctx, sb.ID())
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	hc := info.HostConfig
	if hc.Privileged {
		t.Error("container is privileged")
	}
	if len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" {
		t.Errorf("CapDrop = %v, want [ALL]", hc.CapDrop)
	}
	if len(hc.CapAdd) != 0 {
		t.Errorf("CapAdd = %v, want none (threat-model.md baseline)", hc.CapAdd)
	}
	found := false
	for _, opt := range hc.SecurityOpt {
		if opt == "no-new-privileges" {
			found = true
		}
		if strings.Contains(opt, "seccomp") && strings.Contains(opt, "unconfined") {
			t.Errorf("seccomp disabled: %v", hc.SecurityOpt)
		}
	}
	if !found {
		t.Errorf("SecurityOpt = %v, want no-new-privileges", hc.SecurityOpt)
	}
	if string(hc.NetworkMode) != hardenedNetwork {
		t.Errorf("NetworkMode = %s, want %s", hc.NetworkMode, hardenedNetwork)
	}
	if hc.NetworkMode.IsHost() {
		t.Error("container on host network")
	}
	if len(hc.Binds) != 0 || len(hc.Mounts) != 0 {
		t.Errorf("host mounts present: binds=%v mounts=%v", hc.Binds, hc.Mounts)
	}
	if info.Config.User != hardenedUser {
		t.Errorf("User = %q, want %q", info.Config.User, hardenedUser)
	}

	if err := sb.Start(ctx, ""); err != nil {
		t.Fatalf("Start: %v", err)
	}
	code, err := sb.Wait(ctx)
	if err != nil || code != 0 {
		t.Fatalf("Wait: code=%d err=%v", code, err)
	}
	rc, err := sb.Logs(ctx, false)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if got := strings.TrimSpace(stripLogNoise(string(b))); got != hardenedUser {
		t.Errorf("id -u = %q, want %q (non-root)", got, hardenedUser)
	}
}

// TestIsolationSingleUse: a sandbox runs exactly one job, a second Start
// is rejected, and no container ID is ever reused (FR-13, tdd.md §8).
func TestIsolationSingleUse(t *testing.T) {
	p := isolationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		sb := createSandbox(t, p, hardenedSpec("true"))
		id := sb.ID()
		if seen[id] {
			t.Fatalf("container ID %s reused", id)
		}
		seen[id] = true
		if err := sb.Start(ctx, ""); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := sb.Start(ctx, ""); err == nil {
			t.Fatal("second Start accepted")
		}
		if _, err := sb.Wait(ctx); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if err := sb.Destroy(ctx); err != nil {
			t.Fatalf("Destroy: %v", err)
		}
		if _, err := p.cli.ContainerInspect(ctx, id); err == nil {
			t.Fatalf("container %s still exists after Destroy", id)
		}
	}
}

func parseTrailingInt(t *testing.T, out string) int {
	t.Helper()
	fields := strings.Fields(out)
	if len(fields) == 0 {
		t.Fatalf("no output to parse: %q", out)
	}
	var n int
	if _, err := fmt.Sscanf(fields[len(fields)-1], "%d", &n); err != nil {
		t.Fatalf("parse %q: %v", out, err)
	}
	return n
}

// stripLogNoise drops non-printable multiplexing residue so exact-match
// assertions hold on demuxed output.
func stripLogNoise(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || (r >= 32 && r < 127) {
			return r
		}
		return -1
	}, s)
}
