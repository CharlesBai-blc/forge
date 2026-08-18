# Forge threat model

**Version:** 1.0 | **Date:** August 17, 2026 | **Owner:** Charles Bai
**Requirement:** FR-28. Claims map to the FR-17 isolation suite
(`internal/sandbox/docker/isolation_test.go`) and to tests named below.
This document also fixes the capability and seccomp baseline that
tdd.md §4.5 defers here.

## 1. What Forge runs

Every CI job executes arbitrary, attacker-controllable code: a malicious
pull request is the normal case, not the edge case (US-6). The job runs
inside a single-use Docker container created by `forge-agent` with the
hardened profile (FR-15). The container is destroyed after exactly one
job (FR-13).

## 2. Threats Forge defends against

### 2.1 Job-to-job contamination

A job must not read, write, or influence another job's workspace,
cache, credentials, or network services.

Mechanisms:

- Single-use sandboxes. Each job gets a freshly created container and
  writable layer; both are destroyed after the job. There is no reuse
  path in the type system: `sandbox.Provider` has only `Create`, and
  `Sandbox` has no Reset or Release. A second `Start` on the same
  sandbox errors. Warm-pool sandboxes (FR-16) are taken at most once
  and destroyed after their one job.
- Network isolation. Hardened containers join the dedicated
  `forge-jobs` bridge with inter-container communication disabled, so
  sandboxes have egress (GitHub) but cannot reach each other.
- Per-job JIT runner credentials, registered at claim time and never
  reused (FR-5). A job cannot replay another job's runner registration.

Tests: `TestIsolationNoCrossJobFilePersistence`,
`TestIsolationNoCrossContainerVisibility`, `TestIsolationSingleUse`,
`TestStartTwiceErrors`, `TestWarmStartThenAttach` (single-use holds on
the warm path), `TestPoolTakeIsSingleUseAndRefills`.

### 2.2 Host persistence

A job must not write to the host filesystem, see the Docker daemon, or
leave anything behind after its container is removed.

Mechanisms:

- No host mounts. The hardened profile mounts nothing from the host;
  the Docker socket is not present in the sandbox. Any future mount is
  read-only image content by policy (FR-15).
- Non-root execution. Hardened containers run as uid 1001 (the
  actions/runner user), enforced by container config, not by the image.
- `no-new-privileges` blocks privilege escalation through setuid
  binaries inside the container.
- Destroy removes the container and its writable layer on every exit
  path, including failure, cancellation, and panic.

Tests: `TestIsolationHardenedProfileApplied` (no binds, no mounts,
non-root, no-new-privileges), `TestIsolationNoCrossContainerVisibility`
(no docker.sock), `TestDestroyRemovesContainer`,
`TestIsolationSingleUse` (container gone after Destroy).

### 2.3 Runner reuse

A GitHub runner registration must serve exactly one job. A stolen or
leaked JIT credential from one attempt must be useless for another.

Mechanisms:

- JIT registration happens at claim time, one registration per
  execution attempt, never re-registered (FR-5, tdd.md §4.4).
- Registrations whose credential was never consumed are unregistered
  when the attempt is reclaimed.

Tests: JIT single-use integration tests (tdd.md §8), runner tests
asserting one `RegisterJIT` per attempt and unregistration of
unconsumed credentials (`internal/runner/runner_test.go`).

### 2.4 Resource abuse

A job must not starve the host or other jobs of CPU, memory, processes,
or disk.

Mechanisms (FR-14, enforced by the kernel via Docker):

- CPU quota (default 2.0 cores), memory limit (default 4 GiB), PID
  limit (default 4096).
- Writable-layer disk quota (default 20 GiB) where the storage driver
  supports per-container quotas (overlay2 on xfs with `pquota`, and
  drivers with native size support). Where the driver does not support
  it, the agent logs a warning and enforces the rest of the profile;
  the isolation suite skips the disk case on such hosts. Operators who
  need a hard disk guarantee must run a supporting storage driver.

Tests: `TestIsolationResourceLimitsEnforced` (memory OOM-kill, PID
quota functional, with a roomy-quota control run),
`TestIsolationDiskQuota`.

## 3. Capability and seccomp baseline (FR-15)

The hardened profile applies, in `internal/sandbox/docker`:

| Control | Setting |
|---|---|
| Capabilities | drop ALL, add back none |
| Seccomp | Docker's default profile (not unconfined) |
| no-new-privileges | on |
| Privileged | off |
| User | uid 1001, non-root |
| Network | `forge-jobs` bridge, inter-container communication off, never host network |
| Host mounts | none |
| Resource limits | FR-14 CPU, memory, PIDs, disk (driver permitting) |

The capability baseline is empty because the runner and every job step
execute as uid 1001; an unprivileged process gains nothing from ambient
capabilities, and CI workloads that genuinely need privileged
operations (Docker-in-Docker, package installation as root) are out of
scope for the hardened profile by design. Asserted by
`TestIsolationHardenedProfileApplied`.

## 4. What Forge does not defend against

- **Kernel-level container escapes.** A job that exploits a Linux
  kernel or container-runtime vulnerability can escape the sandbox.
  Containers share the host kernel; this is inherent to the Docker
  sandbox. Mitigation is deferred to the Firecracker microVM sandbox
  planned for v1.1 (charter, fs.md §5).
- **Malicious operators.** Anyone with control-plane admin credentials
  or shell access to a worker owns the fleet.
- **Egress abuse.** Jobs have outbound network access because CI needs
  it (GitHub, package registries). Forge does not filter egress in
  v1.0; a job can exfiltrate anything it can read inside its own
  sandbox, which is scoped by §2.1 and §2.2.
- **Denial of service by queue flooding.** Rate limiting beyond
  resource caps is out of scope for v1.0.

## 5. Secrets and control-plane surface

Covered in tdd.md §7 and tested where noted there: GitHub credentials
and enrollment tokens encrypted at rest (FR-27), per-machine revocable
agent tokens, webhook HMAC verification, admin session auth at M5.
Until then, the dashboard (`/v1/dashboard`, `/v1/jobs/*`) and the FR-25
`/metrics` endpoint are unauthenticated: anyone reaching the control
plane can read job metadata, logs, and queue/latency metrics. No
secrets are exposed by either surface. Deploy behind a network
boundary (VPN, firewall) until M5 admin auth lands.

*Revision log:* v1.0 initial, published at M4 per FR-28.
