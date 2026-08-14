# Forge - Functional Specification

**Version:** 1.1 | **Date:** August 13, 2026 | **Owner:** Charles Bai
**Scope authority:** Project Charter v1.0.
**Companion doc:** Technical Design Document (architecture, data model, API surface). This document defines what Forge does. The TDD defines how.

---

## 1. Product Overview

Forge is a self-hosted CI compute platform. A control plane (single Go binary, `forge`) receives jobs from GitHub Actions, queues them, and dispatches them to worker machines (`forge-agent`) that the user owns. Every job runs in a fresh, single-use sandbox that is destroyed after the job completes. When queue depth exceeds fleet capacity, Forge provisions temporary cloud workers via Terraform and retires them when the queue drains.

### 1.1 Design principles

1. **Single binary.** Control plane and dashboard ship as one binary, the agent as another. External dependencies are minimized and each is justified in the TDD.
2. **Single-use sandboxes.** No sandbox runs two jobs. This is a security invariant, covered by tests.
3. **Vertical slices.** Each milestone produces a working end-to-end system.
4. **Measured claims.** Performance and security statements in the README must trace to a reproducible test or benchmark.
5. **Interface seams.** CI source and sandbox implementations sit behind Go interfaces (`RunnerSource`, `Sandbox`), even while only one implementation of each ships in v1.0.

## 2. Users and Scenarios

**Persona A: solo developer.** Owns a capable desktop or VPS. Wants faster, free CI for personal repos. Success: install to first job in 10 minutes or less.

**Persona B: small team lead.** 3-10 engineers, heavy test suite, growing Actions bill. Wants shared runners on hardware the team controls, with overflow capacity. Success: a fleet of two or more machines shared across repos, with burst absorbing spikes.

### 2.1 User stories (v1.0)

- US-1: As a repo owner, I connect my GitHub repository to Forge so its Actions jobs run on my machines.
- US-2: As a machine owner, I run one bootstrap command on a Linux box to add it to my fleet.
- US-3: As a developer, I push code and my job starts on a warm runner within seconds.
- US-4: As an operator, I can see jobs, queue depth, and per-machine status on a dashboard, with live logs per job.
- US-5: As an operator, I drain a machine for maintenance and its queued work moves to the rest of the fleet.
- US-6: As an operator, I trust that a malicious PR's job cannot affect any other job or persist on the host.
- US-7: As an operator, I enable cloud burst so spikes beyond fleet capacity are absorbed by temporary cloud workers within a cost cap I set.

## 3. Functional Requirements

Requirements are numbered for traceability. Tests and PRs reference them. [Mx] marks the delivering milestone.

### 3.1 Installation and setup

- **FR-1 [M1]** `install.sh` installs the `forge` binary on Linux (x86_64, arm64). `forge install` registers a systemd service.
- **FR-2 [M1]** First run provides a setup flow (CLI at M1, web at M5) that captures admin credentials, GitHub credentials, and data directory. Config persists to a single file. Every option is settable via flags or env for scripted installs.
- **FR-3 [M3]** The dashboard provides a copy-paste bootstrap command that installs `forge-agent` on a new machine, authenticates it with a one-time enrollment token, and joins it to the fleet.

### 3.2 GitHub integration (RunnerSource: `github`)

- **FR-4 [M2]** Forge registers a just-in-time (JIT) ephemeral runner with GitHub for each incoming job, scoped to the configured repo or org. Jobs target Forge through standard `runs-on` labels with no workflow changes beyond the label.
- **FR-5 [M2]** Runner registrations are single-use. A JIT runner is created for one job and never re-registered.
- **FR-6 [M2]** Job status and logs report back through the standard runner protocol. The GitHub UI shows results exactly as with hosted runners.
- **FR-7 [M2]** Supports repo-level registration, one configurable label set, and org-level registration if credentials allow. Multiple repos may share one Forge instance.
- **FR-8 [M1 only]** An interim webhook-triggered execution mode exists to reduce M1 risk. It is deleted at M2 and is not documented.

### 3.3 Job lifecycle and queue

- **FR-9 [M1]** Jobs progress through explicit states: queued, assigned, running, then succeeded, failed, or lost. State history is persisted and queryable.
- **FR-10 [M3]** Jobs queue in Redis Streams. Workers claim jobs via consumer groups. At M1 an embedded in-process queue is acceptable. Redis is required from M3 and ships in the provided compose file.
- **FR-11 [M3]** Delivery is at-least-once with idempotent claiming. A job assigned to a dead worker is reclaimed after a visibility timeout and re-dispatched. A job is never executed by two workers concurrently.
- **FR-12 [M3]** A job that fails N times (default 2) moves to a dead-letter state, visible on the dashboard with logs, and reports to GitHub as failed.

### 3.4 Sandbox and isolation (Sandbox: `docker`)

- **FR-13 [M1, hardened M4]** Every job runs in a freshly created Docker container from a configured base image. The container and its writable layer are destroyed after exactly one job. No code path permits sandbox reuse.
- **FR-14 [M1]** Sandboxes run with configurable resource limits (CPU, memory, PIDs, disk) and safe defaults.
- **FR-15 [M4]** Hardened default profile: no host network, no privileged mode, read-only host mounts, non-root user, documented seccomp and capabilities baseline.
- **FR-16 [M4]** Warm pool: workers pre-create N ready sandboxes per label. A queued job attaches to a warm sandbox when available. Pool size is configurable. Warm sandboxes remain single-use.
- **FR-17 [M4]** An isolation test suite proves: no cross-job file persistence, no visibility into other containers, resource limits enforced. Runs in CI. The README security section references its pass state.

### 3.5 Fleet management

- **FR-18 [M3]** The control plane tracks each worker's identity, labels, capacity, liveness (heartbeat), and current jobs.
- **FR-19 [M3]** Operators can cordon (stop accepting jobs), drain (cordon plus requeue assigned-not-started work and let running jobs finish), and remove machines from the dashboard or CLI.
- **FR-20 [M3]** A worker that misses heartbeats is marked lost. Its in-flight jobs follow FR-11 reclamation. The worker can re-enroll cleanly.

### 3.6 Cloud burst

- **FR-21 [M5]** When queue depth exceeds fleet capacity for a sustained, configurable window, Forge provisions burst workers (AWS, bundled Terraform module). Burst workers enroll exactly like FR-3 machines.
- **FR-22 [M5]** Burst workers retire when the queue drains below a threshold for a sustained window. Retirement uses drain semantics and never kills running jobs. The instance terminates afterward.
- **FR-23 [M5]** Burst respects hard caps: max concurrent instances and max hours per day. Hitting a cap is visible on the dashboard. Defaults are conservative.

### 3.7 Dashboard and observability

- **FR-24 [M3 basic, M5 full]** Embedded web dashboard shows queue depth, job list with states and durations, live log streaming per job, per-machine status and utilization, and burst activity.
- **FR-25 [M4]** Control plane and agents expose Prometheus metrics: queue depth, job latency percentiles, sandbox start time, warm-pool hit rate. The benchmark consumes these.
- **FR-26 [M2]** Structured logs throughout. A failed job's full diagnostic trail is retrievable from the dashboard alone.

### 3.8 Security and secrets

- **FR-27 [M2]** GitHub credentials and enrollment tokens are encrypted at rest. Agents authenticate with per-machine tokens revocable from the dashboard.
- **FR-28 [M4]** A written threat model states what Forge defends against (job-to-job contamination, host persistence, runner reuse) and what it does not (kernel-level container escapes, deferred to the Firecracker sandbox). Claims map to FR-17 tests.

## 4. Non-Functional Requirements

- **NFR-1 Performance [M4]:** warm-pool job start under 2s p95 (queued to executing). Cold sandbox start under 5s p95 on the reference host.
- **NFR-2 Benchmark [M5]:** at least 2x wall-clock speedup vs GitHub hosted `ubuntu-latest` on the published reference workload. Methodology and scripts in `bench/`.
- **NFR-3 Reliability [M4]:** zero lost jobs across 50 or more forced worker-kill trials. Control-plane restart never loses accepted jobs.
- **NFR-4 Scale target [M5]:** 5 workers, 50 concurrent jobs, 1,000 jobs per day sustained.
- **NFR-5 Usability:** zero to first job in 10 minutes or less for Persona A, validated by at least one external tester.
- **NFR-6 Portability:** Linux x86_64 and arm64. No CGO in the control plane unless required.
- **NFR-7 Code quality:** CI runs build, vet, staticcheck, and tests with race detection on every PR. Failure-injection suite runs on main. Releases are tagged, versioned, and checksummed.

## 5. Out of Scope for v1.0

GitLab, Gitea, and Bitbucket sources. Firecracker and gVisor sandboxes (planned v1.1). Windows and macOS runners. Kubernetes-based execution. Multi-tenant accounts or RBAC beyond a single admin. Artifact storage beyond logs. GPU scheduling. Usage metering and billing. Autoscaling policies beyond queue depth. Preview-environment features (recorded in FUTURE.md).

## 6. Acceptance Criteria by Milestone

- **M1:** Push to a test repo triggers a job in a fresh container on one machine. Pass/fail visible in Forge logs. Test verifies the container is destroyed.
- **M2:** Interim path deleted. JIT runner registered per job. Results appear in GitHub's UI. FR-27 credential handling in place.
- **M3:** Two or more machines enrolled via bootstrap command. Redis Streams queue. Killing a worker mid-job results in reclamation and completion elsewhere. Drain works. Basic dashboard.
- **M4:** Warm pool meets NFR-1. Isolation suite (FR-17) and chaos suite (NFR-3) pass in CI. Threat model published.
- **M5:** Flooding the queue triggers burst instances that absorb load, drain, and terminate within caps. Benchmark meets NFR-2. Forge runs its own CI. v1.0 tagged.

## 7. Open Questions (resolved in the TDD)

1. GitHub App vs PAT as the primary credential mode.
2. Redis required from M3 for all installs vs an embedded alternative for single-node.
3. Log streaming transport (SSE vs WebSocket) and retention policy.
4. Burst instance sizing, AMI strategy, and whether spot instances are the default.
5. Org-level runner groups in v1.0 or v1.1.

## 8. Traceability

PRs reference the FR and NFR IDs they implement or test. README claims link to the requirement and the test or benchmark backing them. This file changes only through versioned revision with a log entry.

*Revision log:* v1.0 initial. v1.1 style cleanup, no scope changes.