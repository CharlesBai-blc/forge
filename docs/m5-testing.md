# M5 testing

This walkthrough validates the M5 work that does not require AWS
credentials. Live EC2 validation, NFR-2, the 24h NFR-4 run, and the
v1.0 tag are deferred to `docs/m5-aws-handoff.md`.

## 1. Automated tests

From the repository root:

```bash
go test ./...
go vet ./...
go build ./...
go test ./internal/api/ -run 'Test(Login|Session|Logout|Setup|AdminWorker|DashboardShowsBurst)' -v
go test ./internal/burst/ -v
go test ./internal/store/ -run 'Test(MigrationUpgradeFromM4|Burst|CreateGetAdmin|Session)' -v
```

Expected: every command exits zero. The focused tests cover:

- Argon2id password hashing and session creation, expiry, logout, and
  negative authentication cases (FR-2, FR-27).
- One-shot first-run setup and credential persistence hooks (FR-2).
- Dashboard cordon, uncordon, drain, remove, and token revocation
  (FR-19, FR-27).
- Sustained burst windows, caps, apply failures, enrollment-token burst
  propagation, drain-before-destroy, and the Terraform command wrapper
  (FR-21, FR-22, FR-23).
- Upgrade of an M4-era database through migration 0005.

## 2. Terraform validation without AWS

Terraform 1.5 or newer is required. This initializes only the provider
schema and does not contact an AWS account:

```bash
terraform -chdir=deploy/terraform/burst fmt -check
terraform -chdir=deploy/terraform/burst init -backend=false
terraform -chdir=deploy/terraform/burst validate
```

Expected: `Success! The configuration is valid.` Do not run
`terraform apply` in this walkthrough.

The module resolves Ubuntu 24.04 arm64 through the public Canonical SSM
parameter, defaults to spot `c7g.2xlarge`, has an on-demand fallback
flag, allows egress but no inbound traffic, and bootstraps a supplied
linux/arm64 `forge-agent` binary.

## 3. First-run web setup

Start Redis:

```bash
docker compose up -d redis
```

Start Forge in terminal 1. GitHub owner/repository and the sandbox image
are install configuration; GitHub credentials can be entered in the
setup page:

```bash
go run ./cmd/forge \
  -addr :8080 \
  -data-dir /tmp/forge-m5 \
  -redis 127.0.0.1:6379 \
  -github-owner OWNER \
  -github-repo REPO \
  -image ghcr.io/actions/actions-runner:latest
```

Open `http://localhost:8080/`.

Expected:

1. The first page is **Forge setup**.
2. A password shorter than eight characters is rejected.
3. Submit an admin username/password, webhook secret, and GitHub token.
4. The browser is logged in and the dashboard appears.
5. Refreshing keeps the session.
6. A direct second `POST /setup` returns 404.
7. **Log out** returns to the login page; bad credentials are rejected.

For a scripted install, remove `/tmp/forge-m5` and start with:

```bash
FORGE_ADMIN_USER=admin \
FORGE_ADMIN_PASSWORD='replace-with-a-long-password' \
go run ./cmd/forge \
  -addr :8080 \
  -data-dir /tmp/forge-m5 \
  -redis 127.0.0.1:6379 \
  -github-owner OWNER \
  -github-repo REPO \
  -image ghcr.io/actions/actions-runner:latest
```

Expected: `/` serves the login page, not setup. Equivalent flags are
`-admin-user` and `-admin-password`.

Production note: terminate TLS at Forge or a trusted reverse proxy.
The session cookie is `Secure` when the request is HTTPS and always
`HttpOnly; SameSite=Strict`.

## 4. Fleet controls and revocation

With one or more enrolled workers, use the dashboard worker table:

1. **cordon** changes an active worker to cordoned.
2. **uncordon** returns it to active.
3. **drain** stops new claims; assigned work is requeued and running
   work finishes before the sweeper changes the worker to cordoned.
4. **remove** is available from cordoned or lost and asks for
   confirmation. It marks the worker removed and replaces its stored
   token hash.

Expected: a removed agent's next heartbeat or claim is unauthorized.
These buttons call
`POST /v1/admin/workers/{id}/{cordon|uncordon|drain|remove}` and require
an admin session.

## 5. Dashboard and metrics

Expected dashboard additions:

- Login and logout.
- A `burst` marker beside burst-provisioned workers.
- A burst panel when burst is configured: desired instances, instance
  cap, instance-hours today, daily cap, and cap/apply-failure banner.

The following metrics remain available at `/metrics`:

```text
forge_burst_instances
forge_burst_cap_hits_total
forge_burst_apply_failures_total
```

`/metrics` is intentionally not session-gated; see
`docs/design/threat-model.md` §5.

## 6. NFR-4 smoke

Run the same harness used by the CI `soak-smoke` job:

```bash
go run ./bench/soak -duration 45s -job 1s
```

Expected: 5 simulated machines x 10 sequential agent slots, 50 jobs in
flight, more than 1,000 jobs completed, and `soak: PASS`. The harness
fails on any failed, dead-lettered, or duplicated job.

The 24h acceptance command is:

```bash
go run ./bench/soak -duration 24h -redis 127.0.0.1:6379
```

Do not claim NFR-4 acceptance from the smoke. Record the full 24h output
in the later validation job.

## 7. Local burst controller test

No AWS account is used here:

```bash
go test ./internal/burst/ -run \
  'Test(ScaleUpAfterSustainedWindow|ScaleDownLifecycle|MaxInstancesCap|DailyHoursCap|ApplyFailureBannerAndRetry|CLIApplyInvocations)' \
  -v
```

Expected: the fake clock advances through the 120s/600s windows and the
stub Terraform executable records scale-up and scale-down applies.

## Stop point

The next meaningful acceptance step is a real Terraform apply. Stop
here and use `docs/m5-aws-handoff.md`; it lists the user decisions,
credentials, IAM policy, artifact URL, and evidence to collect.
