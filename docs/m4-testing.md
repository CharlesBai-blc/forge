# M4 manual test

Walk this top to bottom on a Mac with Docker Desktop. Do not open
`docs/m3-testing.md`. Everything you need is here.

Pass means the M4 acceptance criteria hold (fs.md §6): the warm pool
meets NFR-1, the isolation suite (FR-17) and chaos suite (NFR-3) pass,
and the threat model is published (FR-28). Also covers FR-13 hardened,
FR-14 disk, FR-15, FR-16, FR-25.

Use a repo you admin. A throwaway public or private repo is fine. Do
not use `ubuntu-latest`. Do not mount Docker or run `docker` inside the
workflow; Forge does not expose the host daemon.

`runs-on` must include a label no other runner has. `forge` is that
label. If another self-hosted runner has both `self-hosted` and
`forge`, GitHub may send the job there.

You need four terminals. Keep them labeled: **tunnel**, **forge**,
**agent**, **check**. Leave **check** open the whole walk; exports live
there.

Any failed Pass line is an M4 release blocker.

## 1. Tools

1. Open Docker Desktop. Wait until it says Docker is running.

In **check**:

```bash
go version
docker info >/dev/null && echo docker-ok
sqlite3 -version
jq --version
gh --version
```

Pass: all five print versions or `docker-ok`. Install anything missing:

In **check**:

```bash
brew install jq gh cloudflared
```

or install [ngrok](https://ngrok.com/download) instead of cloudflared.

In **check**, authenticate `gh` against the test-repo account if it is
not already:

```bash
gh auth status
```

In **check**, build both binaries:

```bash
cd ~/repos/forge
go build -o ./forge ./cmd/forge
go build -o ./forge-agent ./cmd/forge-agent
```

In **check**, start from empty test data:

```bash
rm -rf /tmp/forge-m4
mkdir -p /tmp/forge-m4
```

Do not substitute a data directory that contains jobs or credentials
you need.

## 2. Automated suites

These need Go and Docker only. Run them before the live stack so they
do not share containers with a running agent.

In **check**:

```bash
cd ~/repos/forge
go vet ./...
go test ./...
```

Pass: every package is `ok`.

In **check**, FR-17 isolation suite against your real Docker daemon:

```bash
cd ~/repos/forge
FORGE_ISOLATION_TESTS=1 go test ./internal/sandbox/docker/ -run TestIsolation -v
```

Pass: every `TestIsolation*` test passes.
`TestIsolationDiskQuota` may skip with `storage driver does not
support per-container disk quotas`. That is allowed (FR-14: disk
limits are storage-driver dependent) and is not a failure.

In **check**, NFR-3 chaos suite with the race detector:

```bash
cd ~/repos/forge
go test ./internal/api/ -run TestChaos -race -v
```

Pass: `TestChaosWorkerKillTrials` logs 50 or more forced kills with
zero lost jobs, and `TestChaosControlPlaneRestart` passes.

Both suites also run as the `isolation` and `chaos` jobs in
`.github/workflows/ci.yml`.

## 3. Threat model published (FR-28)

In **check**:

```bash
cd ~/repos/forge
test -f docs/design/threat-model.md && echo threat-model-ok
grep -q threat-model README.md && echo readme-ok
```

Pass: both lines print.

Open `docs/design/threat-model.md` and confirm it states:

- capabilities: drop ALL, add none
- seccomp: Docker's default profile, not unconfined
- kernel-level container escapes are out of scope (Firecracker, v1.1)

Those three must match what you inspect on a live container in
section 12.

## 4. Values

In **check**, set these and leave the shell open. Replace owner and
repo with the GitHub repo you admin:

```bash
cd ~/repos/forge
export FORGE_OWNER='YOUR_GITHUB_USER'
export FORGE_REPO='YOUR_TEST_REPO'
export FORGE_WEBHOOK_SECRET="$(openssl rand -hex 16)"
echo "webhook secret: $FORGE_WEBHOOK_SECRET"
```

Print the secret once and keep it. You paste it into GitHub in
section 7 and into Forge in section 10.

## 5. GitHub token

Forge calls `POST /repos/{owner}/{repo}/actions/runners/generate-jitconfig`.
You must be a repo admin.

1. Open https://github.com/settings/personal-access-tokens
2. Fine-grained tokens, Generate new token
3. Name: `forge-m4`
4. Resource owner: the account that owns `$FORGE_OWNER/$FORGE_REPO`
5. Repository access, Only select repositories, `$FORGE_REPO`
6. Permissions, Repository, Administration, Read and write
7. Generate token. Copy it.

In **check**:

```bash
export FORGE_TOKEN='paste-the-token'
```

Classic PAT alternative: scope `repo`, and you must still be a repo
admin.

## 6. Workflows

Add these three files to `$FORGE_OWNER/$FORGE_REPO` on the default
branch. That is the test repo, not necessarily this checkout.

`.github/workflows/forge-m4-fast.yml`:

```yaml
name: forge-m4-fast
on:
  workflow_dispatch:
jobs:
  ping:
    runs-on: [self-hosted, forge]
    steps:
      - run: echo hello from forge m4
      - run: uname -a
      - run: id
```

`.github/workflows/forge-m4-long.yml`:

```yaml
name: forge-m4-long
on:
  workflow_dispatch:
jobs:
  hold:
    runs-on: [self-hosted, forge]
    steps:
      - name: Hold for inspect
        run: |
          for n in $(seq 1 30); do
            echo "long tick=$n"
            sleep 2
          done
```

`.github/workflows/forge-latency.yml` (copy from this repo):

```yaml
name: forge-latency
on: workflow_dispatch
jobs:
  latency-forge:
    runs-on: [self-hosted, forge]
    steps:
      - run: echo latency-probe
```

Commit all three on the default branch.

In **check**, confirm GitHub sees them:

```bash
gh workflow list -R "$FORGE_OWNER/$FORGE_REPO"
```

Pass: `forge-m4-fast`, `forge-m4-long`, and `forge-latency` are listed
and active.

## 7. Redis

In **check**:

```bash
cd ~/repos/forge
docker compose up -d redis
docker compose exec -T redis redis-cli ping
```

Pass: `PONG`.

A leftover M3 run will already have the stream. Flush so Forge creates
it cleanly:

In **check**:

```bash
docker compose exec -T redis redis-cli FLUSHALL
docker compose exec -T redis redis-cli EXISTS forge:jobs
```

Pass: `OK` then `0`.

## 8. Tunnel and webhook

In **tunnel**:

```bash
cloudflared tunnel --url http://localhost:8080
```

or:

```bash
ngrok http 8080
```

Copy the `https://…` origin only. Leave this process running.

In **check**:

```bash
export FORGE_TUNNEL='https://YOUR-SUBDOMAIN.trycloudflare.com'
echo "$FORGE_TUNNEL/webhook/github"
```

In the test repo on GitHub:

1. Settings, Webhooks, Add webhook
2. Payload URL: the printed `$FORGE_TUNNEL/webhook/github` value
3. Content type: `application/json`
4. Secret: paste `$FORGE_WEBHOOK_SECRET`
5. Which events, Let me select individual events
6. Uncheck Pushes
7. Check Workflow jobs
8. Active: checked
9. Add webhook

If an older Forge webhook still points at the current tunnel and uses
the same secret, edit it instead of adding another. Do not leave two
active Forge webhooks on the repo.

## 9. Image

In **check**:

```bash
docker pull ghcr.io/actions/actions-runner:latest
```

On Apple Silicon this is `linux/arm64`.

## 10. Start Forge

In **forge**, export the same four values used in **check**, then start
the control plane. `-hardened` defaults to true and `-disk-mb` defaults
to 20480; pass them so the log of this walk matches the profile you
inspect later.

```bash
cd ~/repos/forge
export FORGE_OWNER='YOUR_GITHUB_USER'
export FORGE_REPO='YOUR_TEST_REPO'
export FORGE_WEBHOOK_SECRET='paste-the-webhook-secret'
export FORGE_TOKEN='paste-the-token'

./forge \
  -addr :8080 \
  -data-dir /tmp/forge-m4/control \
  -webhook-secret "$FORGE_WEBHOOK_SECRET" \
  -github-token "$FORGE_TOKEN" \
  -github-owner "$FORGE_OWNER" \
  -github-repo "$FORGE_REPO" \
  -redis 127.0.0.1:6379 \
  -image ghcr.io/actions/actions-runner:latest \
  -hardened \
  -disk-mb 20480
```

Do not pass `-command`.

Pass: a line `forge starting` with `addr=:8080` and
`image=ghcr.io/actions/actions-runner:latest`.

Leave this process running.

In **check**:

```bash
docker compose exec -T redis redis-cli XINFO GROUPS forge:jobs
```

Pass: one group named `forge`.

In **check**:

```bash
curl -sS http://localhost:8080/metrics | grep forge_queue_depth
curl -sS http://localhost:8080/v1/dashboard | jq '{queue_depth, jobs, workers}'
```

Pass: `forge_queue_depth` is present, `queue_depth` is `0`, and `jobs`
and `workers` are empty arrays.

## 11. Enroll one agent

In **check**, mint a one-time token. Tokens expire after one hour; mint
again if you pause the walk.

```bash
cd ~/repos/forge
(umask 077; ./forge enroll-token -data-dir /tmp/forge-m4/control > /tmp/forge-m4/token-a)
```

In **agent**, start the worker with the warm pool on. Default pool size
is 2 and default metrics address is `127.0.0.1:9091`; pass both
explicitly:

```bash
cd ~/repos/forge
./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m4/agent \
  -enroll-token "$(cat /tmp/forge-m4/token-a)" \
  -warm-pool 2 \
  -metrics-addr 127.0.0.1:9091
```

Pass in **agent**: `forge-agent starting` with a non-empty `id` and
`warm_pool=2`.

Leave this process running.

In **check**:

```bash
rm /tmp/forge-m4/token-a
export WORKER_A="$(jq -r .id /tmp/forge-m4/agent/worker.json)"
printf 'A=%s\n' "$WORKER_A"
./forge workers -data-dir /tmp/forge-m4/control
```

Pass: the ID is non-empty, and `forge workers` lists one `active`
worker.

Wait for the pool to fill. The agent fetches the sandbox spec from
Forge, then creates two idle containers.

In **check**:

```bash
sleep 15
docker ps --filter network=forge-jobs
docker network inspect forge-jobs --format '{{index .Options "com.docker.network.bridge.enable_icc"}}'
```

Pass: two containers from `ghcr.io/actions/actions-runner:latest` are
running idle, and the inspect prints `false`.

In **check**, record those IDs. You will compare them after the first
job:

```bash
export WARM_IDS="$(docker ps -q --filter network=forge-jobs | tr '\n' ' ')"
echo "warm pool: $WARM_IDS"
```

Pass: two IDs print.

In **check**:

```bash
curl -sS http://127.0.0.1:9091/metrics | grep forge_agent_warm_pool
```

Pass: `forge_agent_warm_pool_idle` is `2` and
`forge_agent_warm_pool_hits_total` is `0` (no job yet).

## 12. Hardened profile on a live job (FR-15)

The long job sleeps about 60 seconds. You inspect Docker while that
sleep is in progress. Sequence: dispatch, poll until `running`,
Ctrl-C the poll, inspect immediately. Do not wait for GitHub to go
green first.

In **check**, dispatch:

```bash
gh workflow run forge-m4-long.yml -R "$FORGE_OWNER/$FORGE_REPO"
```

In **check**, start this poll right after dispatch. It is how you wait.
There is no separate wait before it.

```bash
while true; do
  curl -sS http://localhost:8080/v1/dashboard |
    jq '{queue_depth, jobs: [.jobs[] | {id,state,attempt,worker_id}]}'
  sleep 2
done
```

The job will print `queued`, then `assigned`, then `running`. When you
see `"state": "running"`, Ctrl-C. The job is still running; Ctrl-C only
stops the poll. The sleep inside the container continues.

Then, still in **check**, while that job is in `running`:

```bash
docker ps --filter network=forge-jobs
```

Pass: three containers, or two if the pool has not yet refilled the
taken slot. One of them is a previously recorded `$WARM_IDS` ID (warm
hit) or a new ID (cold miss). Inspect every live sandbox:

In **check**:

```bash
for CID in $(docker ps -q --filter network=forge-jobs); do
  docker inspect "$CID" --format \
    'id={{.ID}} caps_drop={{.HostConfig.CapDrop}} caps_add={{.HostConfig.CapAdd}} sec={{.HostConfig.SecurityOpt}} user={{.Config.User}} net={{.HostConfig.NetworkMode}} priv={{.HostConfig.Privileged}} binds={{json .HostConfig.Binds}}'
done
```

Pass for the running job container (and for idle pool members):

- `caps_drop=[ALL]`
- `caps_add=[]`
- `sec` contains `no-new-privileges` and does not contain `unconfined`
- `user=1001`
- `net=forge-jobs`
- `priv=false`
- `binds` is empty (`[]` or `<no value>`)

In **check**, wait for GitHub to finish:

```bash
gh run list -R "$FORGE_OWNER/$FORGE_REPO" --workflow forge-m4-long.yml --limit 1
```

Pass: the run is `completed` / `success`. The hardened profile did not
break the runner. `id` in the job log is uid 1001 if the workflow
printed `id`.

## 13. Warm start and single-use (FR-16, FR-13)

The long job consumed one pooled sandbox. Wait for refill.

In **check**:

```bash
sleep 20
export WARM_IDS="$(docker ps -q --filter network=forge-jobs | tr '\n' ' ')"
echo "warm pool before fast job: $WARM_IDS"
docker ps --filter network=forge-jobs
```

Pass: two idle containers. Copy the two IDs.

In **check**, dispatch the short job:

```bash
gh workflow run forge-m4-fast.yml -R "$FORGE_OWNER/$FORGE_REPO"
```

In **check**, start this poll right after dispatch. Stop it with Ctrl-C
when the latest job shows `"state": "succeeded"`.

```bash
while true; do
  curl -sS http://localhost:8080/v1/dashboard |
    jq '{queue_depth, jobs: [.jobs[] | {id,state,attempt,worker_id}]}'
  sleep 2
done
```

Then, in **check**:

```bash
echo "warm pool before: $WARM_IDS"
docker ps --filter network=forge-jobs
docker ps -a --filter network=forge-jobs
```

Pass:

- the job ran in one of the IDs from `warm pool before`
- that ID is gone from `docker ps` and from `docker ps -a`
- the pool is two idle containers again within about 30 seconds, with
  at least one new ID

In **check**:

```bash
curl -sS http://127.0.0.1:9091/metrics | grep forge_agent_warm_pool
```

Pass: `forge_agent_warm_pool_hits_total` is at least `1`, and
`forge_agent_warm_pool_idle` is `2`.

In **check**:

```bash
gh run list -R "$FORGE_OWNER/$FORGE_REPO" --workflow forge-m4-fast.yml --limit 1
```

Pass: the run is `completed` / `success`.

## 14. Metrics (FR-25)

In **check**:

```bash
curl -sS http://127.0.0.1:8080/metrics | grep -E 'forge_queue_depth|forge_job_latency_seconds'
curl -sS http://127.0.0.1:9091/metrics | grep -E 'forge_agent_sandbox_start_seconds|forge_agent_warm_pool'
```

Pass:

- control plane serves `forge_queue_depth` and
  `forge_job_latency_seconds` with phases `queued_to_running` and
  `total`
- agent serves `forge_agent_sandbox_start_seconds` (at least `mode="warm"`
  after the jobs above) and the warm-pool series

## 15. NFR-1 latency, warm scenario

Histograms accumulate for the process lifetime. Restart Forge and the
agent so this scenario starts from zero.

In **forge**, Ctrl-C.

In **agent**, Ctrl-C.

In **forge**, start again against the same data directory. Credentials
are already stored, so omit the webhook secret and GitHub token:

```bash
cd ~/repos/forge
./forge \
  -addr :8080 \
  -data-dir /tmp/forge-m4/control \
  -github-owner "$FORGE_OWNER" \
  -github-repo "$FORGE_REPO" \
  -redis 127.0.0.1:6379 \
  -image ghcr.io/actions/actions-runner:latest \
  -hardened \
  -disk-mb 20480
```

If `$FORGE_OWNER` and `$FORGE_REPO` are not set in **forge**, paste
owner and repo directly.

Pass: `forge starting`. Leave this process running.

In **agent**, restart without an enrollment token. The existing
`worker.json` is reused. Keep the warm pool on:

```bash
cd ~/repos/forge
./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m4/agent \
  -warm-pool 2 \
  -metrics-addr 127.0.0.1:9091
```

Pass: `forge-agent starting` with the same `id` as before. Leave this
process running.

In **check**, wait for the pool:

```bash
sleep 15
docker ps --filter network=forge-jobs
curl -sS http://127.0.0.1:9091/metrics | grep forge_agent_warm_pool_idle
```

Pass: two idle containers, idle gauge `2`.

In **check**, record the host so the number is quotable:

```bash
uname -a
sysctl -n hw.ncpu
```

In **check**, dispatch 20 sequential latency jobs and print p95 from
the FR-25 metrics:

```bash
cd ~/repos/forge/bench
BENCH_REPO="$FORGE_OWNER/$FORGE_REPO" TRIALS=20 ./start-latency.sh
```

Pass:

- 20 trials complete
- queued-to-running p95 is under 2s
- warm-pool hits is 20 (or 19 if the first trial raced refill; re-run
  the scenario from the restart if hits are far below 20)
- record the p95, the hit count, and the host from `uname`

## 16. NFR-1 latency, cold scenario

Restart again so the cold histogram is not mixed with warm samples.

In **forge**, Ctrl-C.

In **agent**, Ctrl-C.

In **forge**:

```bash
cd ~/repos/forge
./forge \
  -addr :8080 \
  -data-dir /tmp/forge-m4/control \
  -github-owner "$FORGE_OWNER" \
  -github-repo "$FORGE_REPO" \
  -redis 127.0.0.1:6379 \
  -image ghcr.io/actions/actions-runner:latest \
  -hardened \
  -disk-mb 20480
```

Pass: `forge starting`. Leave this process running.

In **agent**, start with the pool off:

```bash
cd ~/repos/forge
./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m4/agent \
  -warm-pool 0 \
  -metrics-addr 127.0.0.1:9091
```

Pass: `forge-agent starting` with `warm_pool=0`. Leave this process
running.

In **check**, confirm there is no warm pool:

```bash
sleep 5
docker ps --filter network=forge-jobs
```

Pass: no idle runner containers.

In **check**:

```bash
cd ~/repos/forge/bench
BENCH_REPO="$FORGE_OWNER/$FORGE_REPO" TRIALS=20 ./start-latency.sh
```

Pass:

- 20 trials complete
- sandbox start (cold) p95 is under 5s
- warm-pool hits is 0, misses is 20
- record the p95 and the host from `uname`

## 17. Result

All Pass lines green means M4's acceptance criteria hold on this
machine.

## 18. Failures

- **Forge exits with `stream: ping 127.0.0.1:6379`.** Redis is not
  running. In **check**: `docker compose up -d redis`.
- **`EXISTS forge:jobs` prints `1` before Forge starts.** Leftover
  Redis state. In **check**: `docker compose exec -T redis redis-cli
  FLUSHALL`, then confirm `EXISTS` is `0`.
- **Webhook Recent Deliveries show 401.** The HMAC secret does not
  match `$FORGE_WEBHOOK_SECRET`.
- **Agent enrollment returns 401.** The enrollment token is invalid or
  expired (TTL is one hour). Mint a new token against
  `/tmp/forge-m4/control`.
- **Warm pool never appears.** The agent could not fetch spec, or
  Docker cannot pull the image. Check the **agent** log for `pool
  create` errors and `warm pool: spec fetch failed`.
- **`docker network inspect forge-jobs` fails.** The agent has not
  created a hardened sandbox yet. Wait, or check that `-hardened` is
  on (it is the default).
- **Inspected container is privileged or on host network.** Hardened
  profile is not applied. Confirm the claim spec: in **check**,
  `curl -sS -H "Authorization: Bearer $(jq -r .token /tmp/forge-m4/agent/worker.json)" http://127.0.0.1:8080/v1/agents/"$WORKER_A"/spec | jq`.
  `hardened` must be `true`.
- **Fast job created a new container ID that was not in `$WARM_IDS`.**
  Cold miss. Wait for `forge_agent_warm_pool_idle` to be `2` before
  dispatching.
- **`start-latency.sh` cannot reach agent metrics.** The agent is not
  listening on `127.0.0.1:9091`. Restart it with
  `-metrics-addr 127.0.0.1:9091`.
- **Warm p95 is over 2s, or cold p95 is over 5s.** NFR-1 failed on
  this host. Record the number and the hardware; do not quote a pass.
- **GitHub job runs on another machine.** Another self-hosted runner
  matched both labels. Keep `runs-on: [self-hosted, forge]` and remove
  that label combination elsewhere.
- **GitHub JIT registration returns 401 or 403.** The token lacks
  Administration write, or the token owner is not a repo admin.
- **`gh workflow run` cannot find the workflow.** The file is not on
  the default branch of `$FORGE_OWNER/$FORGE_REPO`.

## 19. Stop

Ctrl-C in **forge**, **agent**, and **tunnel**. Delete the GitHub
webhook. Revoke the GitHub token. Remove the three test workflows if
the repo does not need them.

In **check**:

```bash
cd ~/repos/forge
docker compose down
docker rm -f $(docker ps -aq --filter network=forge-jobs) 2>/dev/null || true
docker network rm forge-jobs 2>/dev/null || true
rm -rf /tmp/forge-m4
rm -f ./forge ./forge-agent
```
