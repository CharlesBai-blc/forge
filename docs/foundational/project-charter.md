# Forge: Project Charter

Owner: Charles Bai
Date: August 2026
Status: v1.0

## Summary

Forge is a self-hosted CI compute platform. One binary turns machines you own into disposable GitHub Actions runners, with overflow to temporary cloud instances when the fleet is busy.

## Why

Teams pay GitHub per minute to run CI on slow 2-core machines while their own hardware sits idle. The official self-hosted route (actions-runner-controller) requires running Kubernetes. Depot, Blacksmith, Namespace, and BuildJet all sell faster CI compute, so the problem is worth money. Forge is the version you own: install one program, point a repo at it, CI runs on your hardware.

## Objectives

- O1: zero to working runner in under 10 minutes
- O2: 2x or faster vs GitHub hosted runners on a published benchmark
- O3: warm job start under 2 seconds (queued to executing)
- O4: zero lost jobs across 50+ forced worker kills
- O5: every job runs in a fresh sandbox, destroyed after one use, verified by test
- O6: Forge runs its own CI by v1.0

## Scope

v1.0: GitHub Actions only. Linux x86_64/arm64. Docker sandboxes. Multi-machine fleet with a queue. Warm pools. Queue-depth cloud burst via Terraform (AWS). Web dashboard. Install script, systemd service, versioned releases.

Deferred: GitLab/Gitea, Firecracker sandboxes (v1.1), Windows/macOS, Kubernetes executor, multi-tenant accounts, GPU jobs, billing.

Scope changes require editing this file first.

## Deliverables

1. Charter, business impact doc, functional spec
2. Technical design doc
3. forge and forge-agent binaries, versioned releases
4. Install script, systemd unit, machine bootstrap one-liner
5. Web dashboard
6. Terraform module for burst workers
7. Unit, integration, and failure-injection test suites in CI
8. README with architecture, comparison table, benchmark methodology, threat model

## Milestones

- M0: docs, repo, CI skeleton, Go spike
- M1: one job end to end: trigger, Docker sandbox, result
- M2: real GitHub integration via JIT ephemeral runners
- M3: fleet: Redis queue, multiple machines, bootstrap, drain
- M4: warm pools, isolation tests, chaos tests, threat model
- M5: cloud burst, dashboard, benchmark, v1.0

