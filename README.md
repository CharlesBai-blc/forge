# Forge

Self-hosted CI compute platform. Goal, scope, and requirements are in `docs/`. This file describes what exists in the repo.

## State

Milestone M0: docs, repo, CI skeleton, Go spike. No job execution, no GitHub integration, no sandboxing, no fleet, no dashboard.

The repo contains:

- `cmd/forge/main.go` - prints `forge v0.0.1` and exits.
- `cmd/forge-agent/main.go` - prints `forge-agent v0.0.1` and exits.
- `go.mod` - module `github.com/CharlesBai-blc/forge`, Go 1.26.6.
- `.github/workflows/ci.yml` - runs `go build ./...`, `go vet ./...`, `go test ./...` on `ubuntu-latest` for every push and pull request.
- `docs/foundational/project-charter.md` - scope, objectives, milestones.
- `docs/foundational/business-impact.md` - problem statement.
- `docs/specs/fs.md` - functional and non-functional requirements.

No test files exist for either binary.

## Build

```
go build ./...
```

## Test

```
go test ./...
```
