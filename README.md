# Forge

Self-hosted CI compute platform. A control plane (`forge`) receives
GitHub Actions jobs and dispatches them to worker machines
(`forge-agent`) you own. Every job runs in a fresh, single-use Docker
sandbox. Goal, scope, and requirements are in `docs/`. This file
describes what exists in the repo.

## State

Milestone M4: hardened sandboxes, warm pools, metrics, isolation and
chaos suites, threat model. Earlier milestones delivered JIT runner
registration (M2) and the durable multi-worker queue, fleet management,
and basic dashboard (M3). Cloud burst and the full dashboard are M5.

- `cmd/forge` - control plane: webhook intake, Redis Streams queue,
  claim/status/logs API, sweeper, dashboard, `/metrics`.
- `cmd/forge-agent` - worker: enrollment, claim loop, Docker sandboxes,
  warm pool, `/metrics`.
- `internal/sandbox/docker` - the `Sandbox` implementation and the
  FR-15 hardened profile.
- `bench/` - benchmark methodology and scripts (NFR-1, NFR-2).
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

## Build

```
go build ./...
```

## Test

```
go test ./...
```
