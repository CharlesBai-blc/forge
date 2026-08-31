# Forge

Self-hosted CI compute platform. A control plane (`forge`) receives
GitHub Actions jobs and dispatches them to worker machines
(`forge-agent`) you own. Every job runs in a fresh, single-use Docker
sandbox. Goal, scope, and requirements are in `docs/`. This file
describes what exists in the repo.

## State

Milestone M5 (code complete, AWS validation pending): admin login and
web setup, fleet control from the dashboard, the burst controller and
its Terraform module, and the NFR-4 soak harness. Live AWS burst runs,
the NFR-2 benchmark, and the 24h soak are deferred to a credentialed
validation pass (`docs/m5-aws-handoff.md`). M4 delivered hardened
sandboxes, warm pools, metrics, isolation and chaos suites, and the
threat model; earlier milestones delivered JIT runner registration
(M2) and the durable multi-worker queue, fleet management, and basic
dashboard (M3).

- `cmd/forge` - control plane: webhook intake, Redis Streams queue,
  claim/status/logs API, sweeper, dashboard with admin auth, burst
  controller, `/metrics`.
- `cmd/forge-agent` - worker: enrollment, claim loop, Docker sandboxes,
  warm pool, `/metrics`.
- `internal/sandbox/docker` - the `Sandbox` implementation and the
  FR-15 hardened profile.
- `internal/burst` - queue-pressure scale up/down of AWS instances
  through Terraform (FR-21, FR-22, FR-23).
- `deploy/terraform/burst` - the burst worker module (spot
  `c7g.2xlarge`, Ubuntu 24.04 via SSM parameter, cloud-init bootstrap).
- `bench/` - benchmark methodology and scripts (NFR-1, NFR-2, NFR-4).
- `docs/foundation/` - project charter and business impact.
- `docs/specs/fs.md` - functional and non-functional requirements.
- `docs/design/tdd.md` - technical design.
- `docs/design/threat-model.md` - what Forge defends against (FR-28).

## Security

Every job runs as an unprivileged user in a freshly created container
that is destroyed after exactly one job. The hardened profile drops all
capabilities, applies Docker's default seccomp profile and
`no-new-privileges`, mounts nothing from the host, and isolates
sandboxes from each other on an ICC-off bridge. Details and non-goals
(kernel-level escapes are deferred to the v1.1 Firecracker sandbox) are
in `docs/design/threat-model.md`.

These claims are backed by the FR-17 isolation suite,
`internal/sandbox/docker/isolation_test.go`, which runs in CI (the
`isolation` job) and must pass on every change:

```bash
FORGE_ISOLATION_TESTS=1 go test ./internal/sandbox/docker/ -run TestIsolation -v
```

The dashboard and admin API require a login: a single admin account
(created at first-run `/setup` or seeded with `-admin-user` /
`-admin-password`), Argon2id password hash, and HttpOnly session
cookies whose tokens are stored only as SHA-256 hashes. GitHub
credentials are encrypted at rest, and worker tokens are revocable from
the dashboard or CLI (FR-2, FR-27). Backed by
`internal/api/admin_test.go` and `internal/secret/password_test.go`;
surface details in `docs/design/threat-model.md` §5.

## Reliability

Accepted jobs are not lost: jobs persist in SQLite before dispatch,
delivery is at-least-once with idempotent claiming, and jobs on dead
workers are reclaimed (FR-11). The NFR-3 chaos suite forces 50+ worker
kills and a control-plane restart and verifies every job reaches
exactly one terminal state. It runs in CI (the `chaos` job) with the
race detector:

```bash
go test ./internal/api/ -run TestChaos -race -v
```

## Performance

Warm pools pre-start sandboxes so a claim only injects the job
credential (FR-16). NFR-1 targets warm job start under 2s p95 and cold
sandbox start under 5s p95, measured from the FR-25 metrics by
`bench/start-latency.sh`; methodology in `bench/README.md`. Speedup
versus hosted runners (NFR-2) is measured by `bench/ci-speedup.sh`.
Numbers are only quotable with the host hardware and trial count per
`bench/README.md`.

Scale (NFR-4) is exercised by the `bench/soak` harness: 50 concurrent
simulated agents against a real control plane, failing on any lost,
failed, or duplicated job. A short smoke runs in CI (the `soak-smoke`
job); the 24h acceptance run is part of the AWS validation pass.

## Cloud burst

When queue depth exceeds idle fleet capacity for a sustained window,
the control plane provisions spot EC2 workers through the bundled
Terraform module, and drains and destroys them when the queue stays
quiet (FR-21, FR-22). Instance-count and daily instance-hours caps are
enforced before every apply, with cap hits and apply failures surfaced
as dashboard banners and metrics (FR-23). Off by default; enable with
`-burst-dir deploy/terraform/burst -burst-url https://<control-plane>
-burst-agent-url https://<artifact>/forge-agent-linux-arm64`.
The controller is tested against a stub terraform binary
(`internal/burst`); live AWS acceptance is pending per
`docs/m5-aws-handoff.md`.

## Build

```
go build ./...
```

## Test

```
go test ./...
```
