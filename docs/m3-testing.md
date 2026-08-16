# M3 manual test

Walk this top to bottom on a Mac with Docker Desktop. Pass means two independently enrolled agents share the Redis queue, fleet operations work, worker loss is accounted for, queued work survives a Redis and control-plane restart, and the dashboard exposes jobs, workers, transitions, and logs (FR-3, FR-10, FR-11, FR-12, FR-18, FR-19, FR-20, FR-24, FR-26, FR-27).

This local test uses two agent processes and two data directories on one Docker host. For the literal two-machine M3 acceptance test, run the two agent commands on separate Linux machines and replace `127.0.0.1` with the control-plane address. The behavior and checks are otherwise the same.

Use a repo you admin. A throwaway public or private repo is fine. Do not use `ubuntu-latest`. Do not mount Docker or run `docker` inside the workflow; Forge does not expose the host daemon.

You need five terminals. Keep them labeled: **tunnel**, **forge**, **agent-a**, **agent-b**, **check**.

Any failed Pass line is an M3 release blocker.

## 1. Tools

1. Open Docker Desktop. Wait until it says Docker is running.
2. Confirm Go, Docker, SQLite, and jq:

```bash
go version
docker info >/dev/null && echo docker-ok
sqlite3 -version
jq --version
```

Install jq if needed:

```bash
brew install jq
```

3. Install a public HTTPS tunnel if the M2 tunnel is gone. Pick one:

```bash
brew install cloudflared
```

or install [ngrok](https://ngrok.com/download).

4. Build both binaries. In **check**:

```bash
cd ~/repos/forge
go build -o ./forge ./cmd/forge
go build -o ./forge-agent ./cmd/forge-agent
```

5. Start from empty test data:

```bash
rm -rf /tmp/forge-m3
mkdir -p /tmp/forge-m3
```

Do not substitute a data directory that contains jobs or credentials you need.

## 2. Values

In **check**, set these and leave the shell open. Replace owner and repo.

```bash
cd ~/repos/forge
export FORGE_OWNER='YOUR_GITHUB_USER'
export FORGE_REPO='YOUR_TEST_REPO'
export FORGE_WEBHOOK_SECRET="$(openssl rand -hex 16)"
echo "webhook secret: $FORGE_WEBHOOK_SECRET"
```

Print the secret once and keep it. You paste it into GitHub in step 6 and into Forge in step 8.

## 3. GitHub token

Forge calls `POST /repos/{owner}/{repo}/actions/runners/generate-jitconfig`. You must be a repo admin.

1. Open https://github.com/settings/personal-access-tokens
2. Fine-grained tokens → Generate new token
3. Name: `forge-m3`
4. Resource owner: the account that owns `$FORGE_OWNER/$FORGE_REPO`
5. Repository access → Only select repositories → `$FORGE_REPO`
6. Permissions → Repository → Administration → Read and write
7. Generate token. Copy it.

In **check**:

```bash
export FORGE_TOKEN='paste-the-token'
```

Classic PAT alternative: scope `repo`, and you must still be a repo admin.

## 4. Workflows

Add these workflows to `$FORGE_OWNER/$FORGE_REPO`, not necessarily this checkout.

`.github/workflows/forge-m3-fast.yml`:

```yaml
name: forge-m3-fast
on:
  workflow_dispatch:
jobs:
  ping:
    runs-on: [self-hosted, forge]
    steps:
      - run: echo hello from forge m3
      - run: uname -a
      - run: id
```

`.github/workflows/forge-m3-fleet.yml`:

```yaml
name: forge-m3-fleet
on:
  workflow_dispatch:
jobs:
  fleet:
    strategy:
      max-parallel: 2
      matrix:
        slot: [a, b]
    runs-on: [self-hosted, forge]
    steps:
      - name: Hold both workers
        run: |
          for n in $(seq 1 20); do
            echo "slot=${{ matrix.slot }} tick=$n"
            sleep 2
          done
```

`.github/workflows/forge-m3-long.yml`:

```yaml
name: forge-m3-long
on:
  workflow_dispatch:
jobs:
  hold:
    runs-on: [self-hosted, forge]
    steps:
      - name: Run past the visibility timeout
        run: |
          for n in $(seq 1 45); do
            echo "long tick=$n"
            sleep 2
          done
```

Commit all three on the default branch.

`runs-on` must include a label no other runner has. `forge` is that label. If another self-hosted runner has both labels, GitHub may send the job there.

## 5. Redis

In **check**:

```bash
cd ~/repos/forge
docker compose up -d redis
docker compose exec -T redis redis-cli ping
```

Pass: `PONG`.

Confirm the stream does not exist yet:

```bash
docker compose exec -T redis redis-cli EXISTS forge:jobs
```

Expect `0`. Forge creates the stream and consumer group at startup.

## 6. Tunnel and webhook

In **tunnel**:

```bash
cloudflared tunnel --url http://localhost:8080
```

or:

```bash
ngrok http 8080
```

Copy the `https://…` origin only. In **check**:

```bash
export FORGE_TUNNEL='https://YOUR-SUBDOMAIN.trycloudflare.com'
echo "$FORGE_TUNNEL/webhook/github"
```

In the test repo on GitHub:

1. Settings → Webhooks → Add webhook
2. Payload URL: the printed `$FORGE_TUNNEL/webhook/github` value
3. Content type: `application/json`
4. Secret: paste `$FORGE_WEBHOOK_SECRET`
5. Which events → Let me select individual events
6. Uncheck **Pushes**
7. Check **Workflow jobs**
8. Active: checked
9. Add webhook

If the M2 webhook still points at the current tunnel and uses the same secret, edit it instead of adding another. Do not leave two active Forge webhooks on the repo.

## 7. Image

In **check**, pull once:

```bash
docker pull ghcr.io/actions/actions-runner:latest
```

On Apple Silicon this is `linux/arm64`.

## 8. Start Forge

In **forge**, export the same four values used in **check**, then start the control plane:

```bash
cd ~/repos/forge
export FORGE_OWNER='YOUR_GITHUB_USER'
export FORGE_REPO='YOUR_TEST_REPO'
export FORGE_WEBHOOK_SECRET='paste-the-webhook-secret'
export FORGE_TOKEN='paste-the-token'

./forge \
  -addr :8080 \
  -data-dir /tmp/forge-m3/control \
  -webhook-secret "$FORGE_WEBHOOK_SECRET" \
  -github-token "$FORGE_TOKEN" \
  -github-owner "$FORGE_OWNER" \
  -github-repo "$FORGE_REPO" \
  -redis 127.0.0.1:6379 \
  -image ghcr.io/actions/actions-runner:latest
```

Do not pass `-command`. Limit flags (`-cpu`, `-memory-mb`, `-pids`) are optional; defaults are 2 CPU, 4 GiB, 4096 PIDs.

Pass: a line `forge starting` with `addr=:8080` and `image=ghcr.io/actions/actions-runner:latest`.

Leave this process running.

In **check**:

```bash
docker compose exec -T redis redis-cli XINFO GROUPS forge:jobs
```

Pass: one group named `forge`.

## 9. Dashboard before enrollment

Open http://localhost:8080/.

Pass:

- The page title is `Forge`.
- Queue depth is `0`.
- Jobs and Workers are empty.
- The bootstrap section shows `forge enroll-token` followed by `forge-agent`.

Check the JSON endpoint:

```bash
curl -sS http://localhost:8080/v1/dashboard | jq
```

Pass: `queue_depth` is `0`, and `jobs` and `workers` are empty arrays.

The M3 dashboard has no admin login. Bind Forge to localhost or a trusted network while testing.

The displayed bootstrap assumes `forge-agent` is already installed. Building it in step 1 makes this source-tree test work, but it does not satisfy FR-3's fresh-machine installation requirement by itself.

## 10. Enroll two agents

In **check**, mint two one-time tokens. Tokens expire after one hour; mint again if you pause the walk.

```bash
(umask 077; ./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/token-a)
(umask 077; ./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/token-b)
```

In **agent-a**:

```bash
cd ~/repos/forge
./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m3/agent-a \
  -enroll-token "$(cat /tmp/forge-m3/token-a)" &
echo $! > /tmp/forge-m3/agent-a.pid
wait
```

In **agent-b**:

```bash
cd ~/repos/forge
./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m3/agent-b \
  -enroll-token "$(cat /tmp/forge-m3/token-b)" &
echo $! > /tmp/forge-m3/agent-b.pid
wait
```

Pass in each agent terminal: `forge-agent starting` with a non-empty `id`.

In **check**:

```bash
rm /tmp/forge-m3/token-a /tmp/forge-m3/token-b
export WORKER_A="$(jq -r .id /tmp/forge-m3/agent-a/worker.json)"
export WORKER_B="$(jq -r .id /tmp/forge-m3/agent-b/worker.json)"
printf 'A=%s\nB=%s\n' "$WORKER_A" "$WORKER_B"
./forge workers -data-dir /tmp/forge-m3/control
```

Pass:

- The IDs are non-empty and different.
- `forge workers` lists two `active` workers.
- The dashboard lists two healthy workers with capacity `1`.
- `curl -sS http://localhost:8080/v1/dashboard | jq '.workers'` shows both IDs, architecture, last-seen time, and zero running jobs.

## 11. Enrollment and agent credentials

Confirm the one-time tokens were consumed and only hashes are stored:

```bash
sqlite3 /tmp/forge-m3/control/forge.db \
  'SELECT length(token_hash), used_at IS NOT NULL FROM enrollment_tokens;'
sqlite3 /tmp/forge-m3/control/forge.db \
  'SELECT id, state, capacity, healthy, length(token_hash), arch, version FROM workers;'
stat -f '%Lp %N' \
  /tmp/forge-m3/agent-a/worker.json \
  /tmp/forge-m3/agent-b/worker.json
```

Expect:

- two enrollment rows with 32-byte hashes and `used_at=1`
- two worker rows with 32-byte token hashes, `active`, capacity `1`, and healthy `1`
- both `worker.json` files mode `600`

Test one-time use with a disposable enrollment:

```bash
(umask 077; ./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/token-once)
export ONCE_TOKEN="$(cat /tmp/forge-m3/token-once)"

curl -sS -o /tmp/forge-m3/once.json -w '%{http_code}\n' \
  -X POST http://localhost:8080/v1/agents/enroll \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$ONCE_TOKEN" \
    '{token:$token,name:"token-once",arch:"amd64",version:"manual"}')"

curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8080/v1/agents/enroll \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$ONCE_TOKEN" \
    '{token:$token,name:"token-reuse",arch:"amd64",version:"manual"}')"
```

Pass: the first response is `201`; the second is `409`.

Save the disposable worker credentials for the removal test:

```bash
export ONCE_ID="$(jq -r .worker_id /tmp/forge-m3/once.json)"
export ONCE_MACHINE_TOKEN="$(jq -r .token /tmp/forge-m3/once.json)"
rm /tmp/forge-m3/token-once
```

Test agent authentication:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  http://localhost:8080/v1/agents/"$WORKER_A"/claim
```

Pass: `401`.

Test Docker health reporting with a disposable third agent:

```bash
(umask 077; ./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/token-health)
DOCKER_HOST=unix:///tmp/forge-m3/no-docker.sock \
  ./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m3/agent-health \
  -enroll-token "$(cat /tmp/forge-m3/token-health)" \
  > /tmp/forge-m3/agent-health.log 2>&1 &
echo $! > /tmp/forge-m3/agent-health.pid
rm /tmp/forge-m3/token-health
sleep 35

export HEALTH_ID="$(jq -r .id /tmp/forge-m3/agent-health/worker.json)"
curl -sS http://localhost:8080/v1/dashboard |
  jq --arg id "$HEALTH_ID" '.workers[] | select(.id == $id)'
```

Pass: the disposable worker is active with `healthy=false`, and it does not claim work. Unhealthy workers get an immediate 204 on claim; they do not hold a 30-second poll.

Stop and remove it:

```bash
kill "$(cat /tmp/forge-m3/agent-health.pid)"
./forge cordon -data-dir /tmp/forge-m3/control "$HEALTH_ID"
./forge remove -data-dir /tmp/forge-m3/control "$HEALTH_ID"
```

## 12. Two-worker dispatch and dashboard

In GitHub, Actions → `forge-m3-fleet` → Run workflow.

While it runs, in **check**:

```bash
while true; do
  curl -sS http://localhost:8080/v1/dashboard |
    jq '{queue_depth, jobs: [.jobs[] | {id,state,attempt,worker_id,duration_ms}], workers: [.workers[] | {id,state,healthy,running,capacity,utilization}]}'
  sleep 2
done
```

Stop the loop with Ctrl-C after both jobs finish.

Pass:

- two jobs reach `running` at the same time
- the jobs have different `worker_id` values
- both workers show `running=1`, `capacity=1`, and `utilization=1`
- duration increases while each job is non-terminal
- both jobs finish `succeeded` with attempt `1`
- queue depth returns to `0`
- both GitHub matrix jobs are green

While both jobs are running, verify worker fencing. Send a terminal report for worker A's job using worker B's token:

```bash
export JOB_ON_A="$(
  curl -sS http://localhost:8080/v1/dashboard |
    jq -r --arg id "$WORKER_A" \
      '.jobs[] | select(.state == "running" and .worker_id == $id) | .id' |
    head -n 1
)"
export TOKEN_A="$(jq -r .token /tmp/forge-m3/agent-a/worker.json)"
export TOKEN_B="$(jq -r .token /tmp/forge-m3/agent-b/worker.json)"

curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST \
  -H "Authorization: Bearer $TOKEN_B" \
  -H 'Content-Type: application/json' \
  -d '{"state":"succeeded"}' \
  http://localhost:8080/v1/jobs/"$JOB_ON_A"/attempts/1/status
```

Pass: `403`, and worker A's job continues. A worker cannot report status for another worker's assignment.

Open one running job in the dashboard before it finishes.

Pass:

- transitions show `queued → assigned → running`
- the Logs pane adds `slot=… tick=…` lines while the job is still running
- after completion, transitions add `succeeded`
- refreshing the page preserves the complete log

Test the SSE endpoint after completion. Copy the selected job ID from the dashboard:

```bash
export JOB_ID='paste-job-id'
curl -sS -D /tmp/forge-m3/sse.headers -N \
  http://localhost:8080/v1/jobs/"$JOB_ID"/logs/stream |
  tee /tmp/forge-m3/sse.log
grep -i '^Content-Type: text/event-stream' /tmp/forge-m3/sse.headers
```

Pass: `Content-Type` is `text/event-stream`, output contains an `id:` byte offset, the workflow log, and `event: end`.

Reconnect from the final offset:

```bash
export LOG_OFFSET="$(awk '/^id:/{value=$2} END{print value}' /tmp/forge-m3/sse.log)"
curl -sS -N \
  -H "Last-Event-ID: $LOG_OFFSET" \
  http://localhost:8080/v1/jobs/"$JOB_ID"/logs/stream
```

Pass: the prior log bytes are not repeated; the terminal `end` event is returned.

After both jobs are terminal, replay the assigned worker's succeeded report and check the agent outbox:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST \
  -H "Authorization: Bearer $TOKEN_A" \
  -H 'Content-Type: application/json' \
  -d '{"state":"succeeded"}' \
  http://localhost:8080/v1/jobs/"$JOB_ON_A"/attempts/1/status

stat -f '%Lp %N' \
  /tmp/forge-m3/agent-a/status.json \
  /tmp/forge-m3/agent-b/status.json
cat /tmp/forge-m3/agent-a/status.json
```

Pass: `204`. Duplicate `running` / `succeeded` / `failed` reports from the assigned worker are idempotent. Both `status.json` files are mode `600` and contain `[]`.

## 13. Cordon and uncordon

Cordon worker A:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_A"
./forge workers -data-dir /tmp/forge-m3/control
```

Pass: worker A is `cordoned` in the CLI and dashboard.

Wait 35 seconds for any claim request that started before the cordon to finish:

```bash
sleep 35
```

Dispatch `forge-m3-fast`.

Pass:

- worker A stays idle
- worker B runs the job
- the job finishes green

Uncordon worker A:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_A"
```

Pass: worker A returns to `active` and healthy.

## 14. Drain with running and queued work

Cordon worker B and wait out its open claim:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_B"
sleep 35
```

Dispatch `forge-m3-fleet`. Wait until worker A runs one matrix job and queue depth is `1`, then:

```bash
./forge drain -data-dir /tmp/forge-m3/control "$WORKER_A"
```

Pass while the first job runs:

- worker A is `draining`
- its running job continues
- the second matrix job stays queued
- worker A does not claim the second job

After the first job finishes, allow one 10-second sweep:

```bash
sleep 12
./forge workers -data-dir /tmp/forge-m3/control
```

Pass: worker A is `cordoned`.

Release worker B:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_B"
```

Pass: worker B claims the queued matrix job and both GitHub jobs finish green.

## 15. Drain assigned work before it starts

This uses a disposable API client to hold a job in `assigned` without starting a sandbox.

Cordon both real workers and wait out their open claims:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_A"
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_B"
sleep 35
```

Enroll the disposable worker:

```bash
(umask 077; ./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/token-drain)
export DRAIN_ENROLL="$(cat /tmp/forge-m3/token-drain)"

curl -sS \
  -X POST http://localhost:8080/v1/agents/enroll \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$DRAIN_ENROLL" \
    '{token:$token,name:"drain-client",arch:"amd64",version:"manual"}')" \
  > /tmp/forge-m3/drain-worker.json

export DRAIN_ID="$(jq -r .worker_id /tmp/forge-m3/drain-worker.json)"
export DRAIN_TOKEN="$(jq -r .token /tmp/forge-m3/drain-worker.json)"
rm /tmp/forge-m3/token-drain
```

Dispatch `forge-m3-fast`, then claim it without reporting `running`:

```bash
curl -sS \
  -H "Authorization: Bearer $DRAIN_TOKEN" \
  http://localhost:8080/v1/agents/"$DRAIN_ID"/claim \
  > /tmp/forge-m3/drain-claim.json
jq '{job_id,attempt}' /tmp/forge-m3/drain-claim.json

./forge drain -data-dir /tmp/forge-m3/control "$DRAIN_ID"
sleep 12
```

Pass:

- the claim reports attempt `1`
- the job returns to `queued`
- its assigned worker is cleared
- the drain transition has reason `drain`
- the drained attempt is not consumed
- the disposable worker settles in `cordoned`

Release worker B:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_B"
```

Pass: worker B claims the job as attempt `1`, not attempt `2`, and it finishes green.

## 16. Durable queue and startup reconciliation

Uncordon worker A, cordon both workers, and wait:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_A"
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_A"
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_B"
sleep 35
```

Dispatch `forge-m3-fast`.

Pass before restart:

```bash
curl -sS http://localhost:8080/v1/dashboard | jq '.queue_depth'
sqlite3 /tmp/forge-m3/control/forge.db \
  "SELECT id, state, attempt FROM jobs WHERE state='queued';"
docker compose exec -T redis redis-cli XPENDING forge:jobs forge
```

Expect queue depth `1`, one SQLite `queued` row, and one pending or undelivered Redis entry.

In **forge**, Ctrl-C.

In **check**, delete the Redis container and start a fresh one:

```bash
docker compose down
docker compose up -d redis
docker compose exec -T redis redis-cli ping
```

Pass: `PONG`. The Redis stream is empty because this compose file has no Redis volume.

Restart Forge in **forge** with the same data directory. Omit the webhook secret and GitHub token to also test encrypted credential reload:

```bash
cd ~/repos/forge
./forge \
  -addr :8080 \
  -data-dir /tmp/forge-m3/control \
  -github-owner "$FORGE_OWNER" \
  -github-repo "$FORGE_REPO" \
  -redis 127.0.0.1:6379 \
  -image ghcr.io/actions/actions-runner:latest
```

If the variables are not set in **forge**, paste owner and repo directly.

Pass after restart:

```bash
curl -sS http://localhost:8080/v1/dashboard | jq '.queue_depth'
docker compose exec -T redis redis-cli XLEN forge:jobs
```

Expect queue depth `1` and a non-zero stream length. Forge rebuilt delivery state from SQLite. Restart against this data directory also applies schema v4 (`prev_state` on workers, `runner_id` on jobs). A migration line in the startup log is expected.

Release worker B:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_B"
```

Pass: the queued job finishes green without a new webhook delivery.

Do not expect Redis `XLEN` to return to zero. `XACK` clears the consumer-group pending entry; it does not delete stream history. Check pending work instead:

```bash
docker compose exec -T redis redis-cli XPENDING forge:jobs forge
```

Pass: pending count is `0`.

## 17. Reclaim an abandoned assigned job

Keep worker A cordoned. Cordon worker B and wait:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_B"
sleep 35
```

Enroll a disposable crash client:

```bash
(umask 077; ./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/token-crash)
export CRASH_ENROLL="$(cat /tmp/forge-m3/token-crash)"

curl -sS \
  -X POST http://localhost:8080/v1/agents/enroll \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$CRASH_ENROLL" \
    '{token:$token,name:"crash-client",arch:"amd64",version:"manual"}')" \
  > /tmp/forge-m3/crash-worker.json

export CRASH_ID="$(jq -r .worker_id /tmp/forge-m3/crash-worker.json)"
export CRASH_TOKEN="$(jq -r .token /tmp/forge-m3/crash-worker.json)"
rm /tmp/forge-m3/token-crash
```

Dispatch `forge-m3-fast`, then abandon the claim:

```bash
curl -sS \
  -H "Authorization: Bearer $CRASH_TOKEN" \
  http://localhost:8080/v1/agents/"$CRASH_ID"/claim \
  > /tmp/forge-m3/crash-claim.json
jq '{job_id,attempt}' /tmp/forge-m3/crash-claim.json

./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_B"
```

Do not send a heartbeat or status from the crash client. Wait up to 75 seconds.

Pass:

- crash client becomes `lost` after about 30 to 40 seconds (from enrollment; claim does not refresh `last_seen`)
- its Redis entry is reclaimed after about 60 to 70 seconds from claim
- transitions show `assigned → lost` with reason `visibility_timeout`, then `lost → queued`
- worker B receives attempt `2`
- only worker B runs user code
- the GitHub job finishes green

The attempt number increases because the first JIT assignment was abandoned. This is the pre-acquisition worker-loss window.

Same worker claiming again is a different path: the agent restarted and still holds the pending Redis entry. Cordon worker B, wait, enroll a restart client, and dispatch `forge-m3-fast`:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_B"
sleep 35

(umask 077; ./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/token-restart)
export RESTART_ENROLL="$(cat /tmp/forge-m3/token-restart)"

curl -sS \
  -X POST http://localhost:8080/v1/agents/enroll \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$RESTART_ENROLL" \
    '{token:$token,name:"restart-client",arch:"amd64",version:"manual"}')" \
  > /tmp/forge-m3/restart-worker.json

export RESTART_ID="$(jq -r .worker_id /tmp/forge-m3/restart-worker.json)"
export RESTART_TOKEN="$(jq -r .token /tmp/forge-m3/restart-worker.json)"
rm /tmp/forge-m3/token-restart
```

Claim once, then claim again immediately with the same token:

```bash
curl -sS \
  -H "Authorization: Bearer $RESTART_TOKEN" \
  http://localhost:8080/v1/agents/"$RESTART_ID"/claim \
  > /tmp/forge-m3/restart-claim-1.json
jq '{job_id,attempt}' /tmp/forge-m3/restart-claim-1.json

curl -sS \
  -H "Authorization: Bearer $RESTART_TOKEN" \
  http://localhost:8080/v1/agents/"$RESTART_ID"/claim \
  > /tmp/forge-m3/restart-claim-2.json
jq '{job_id,attempt}' /tmp/forge-m3/restart-claim-2.json

export RESTART_JOB="$(jq -r .job_id /tmp/forge-m3/restart-claim-2.json)"
sqlite3 /tmp/forge-m3/control/forge.db \
  "SELECT from_state, to_state, reason FROM transitions WHERE job_id='$RESTART_JOB' AND reason='worker_restart';"
```

Pass:

- first claim is attempt `1`
- second claim is the same `job_id` at attempt `2`
- transitions include `assigned → lost` with reason `worker_restart`

Drain the restart client so the job is not stuck on a curl-only worker, then release worker B:

```bash
./forge drain -data-dir /tmp/forge-m3/control "$RESTART_ID"
sleep 12
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_B"
```

Pass: worker B runs the job and it finishes green. Drain requeued the assigned-not-started attempt without leaving it on the restart client.

## 18. Healthy job past the visibility timeout

Uncordon worker A and leave both real workers active:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_A"
```

Dispatch `forge-m3-long`. Watch it in GitHub and the dashboard for its full 90 seconds.

Pass:

- the same worker owns the job for the full run
- regular heartbeats keep the worker active and healthy
- the job stays `running` beyond 60 seconds
- no `lost` transition appears
- the job finishes `succeeded` on attempt `1`
- GitHub and Forge agree on the terminal result

This is the visibility-timeout safety check. A healthy long-running job must not be reclaimed.

## 19. Kill a worker after GitHub acquisition

Cordon worker B and wait:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_B"
sleep 35
```

Dispatch `forge-m3-long`. Wait until the dashboard shows the job `running` on worker A. Then uncordon worker B and simulate loss of worker A:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_B"
kill -KILL "$(cat /tmp/forge-m3/agent-a.pid)"
```

Because both local agents share Docker Desktop, also stop worker A's running sandbox to simulate loss of the machine, not only loss of the agent process. Confirm only one Forge runner container is running before this command:

```bash
docker ps --filter ancestor=ghcr.io/actions/actions-runner:latest
export LOST_CONTAINER="$(
  docker ps -q --filter ancestor=ghcr.io/actions/actions-runner:latest
)"
docker kill "$LOST_CONTAINER"
```

Wait up to 75 seconds.

Pass:

- worker A becomes `lost`
- the job transitions `running → lost → failed`
- reason is `worker_lost`
- the job is not dispatched to worker B after GitHub acquired it
- GitHub reports the job failed
- no second worker runs the same attempt

GitHub binds an acquired workflow job to one runner. Re-dispatch is safe only before acquisition, as tested in step 17.

Remove the stopped local container. On a lost physical worker its Docker host would be gone with it:

```bash
docker rm -f "$LOST_CONTAINER"
```

Restart worker A in **agent-a** without an enrollment token:

```bash
cd ~/repos/forge
./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m3/agent-a &
echo $! > /tmp/forge-m3/agent-a.pid
wait
```

Pass: the existing `worker.json` credential is reused and the next heartbeat restores worker A to `active`. A stale `running` report in `status.json` may log status 409 and is dropped; that is expected after `worker_lost`.

Cordon worker A, kill the agent, wait for `lost`, then restart it from the same data directory:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_A"
kill -KILL "$(cat /tmp/forge-m3/agent-a.pid)"
sleep 35
./forge workers -data-dir /tmp/forge-m3/control

cd ~/repos/forge
./forge-agent \
  -addr http://127.0.0.1:8080 \
  -data-dir /tmp/forge-m3/agent-a &
echo $! > /tmp/forge-m3/agent-a.pid
wait
sleep 12
./forge workers -data-dir /tmp/forge-m3/control
```

Pass: worker A is `lost` after the kill, then `cordoned` after the heartbeat, not `active`. Cordon intent survives loss.

Uncordon so step 20 can cordon both workers from a known state:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_A"
```

Pass: worker A is `active`.

## 20. Dead-letter after two abandoned assignments

Cordon both real workers and wait:

```bash
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_A"
./forge cordon -data-dir /tmp/forge-m3/control "$WORKER_B"
sleep 35
```

Create the first disposable client:

```bash
./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/dead-1.enroll
token="$(cat /tmp/forge-m3/dead-1.enroll)"
curl -sS \
  -X POST http://localhost:8080/v1/agents/enroll \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$token" \
    '{token:$token,name:"dead-1",arch:"amd64",version:"manual"}')" \
  > /tmp/forge-m3/dead-1.json
rm /tmp/forge-m3/dead-1.enroll
```

Dispatch `forge-m3-fast`. Claim attempt 1 and abandon it:

```bash
export DEAD1_ID="$(jq -r .worker_id /tmp/forge-m3/dead-1.json)"
export DEAD1_TOKEN="$(jq -r .token /tmp/forge-m3/dead-1.json)"
curl -sS \
  -H "Authorization: Bearer $DEAD1_TOKEN" \
  http://localhost:8080/v1/agents/"$DEAD1_ID"/claim \
  > /tmp/forge-m3/dead-1-claim.json
```

Wait 65 to 75 seconds for the job to return to `queued`. Enroll the second client now so it is still active when it claims attempt 2:

```bash
./forge enroll-token -data-dir /tmp/forge-m3/control > /tmp/forge-m3/dead-2.enroll
token="$(cat /tmp/forge-m3/dead-2.enroll)"
curl -sS \
  -X POST http://localhost:8080/v1/agents/enroll \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$token" \
    '{token:$token,name:"dead-2",arch:"amd64",version:"manual"}')" \
  > /tmp/forge-m3/dead-2.json
rm /tmp/forge-m3/dead-2.enroll

export DEAD2_ID="$(jq -r .worker_id /tmp/forge-m3/dead-2.json)"
export DEAD2_TOKEN="$(jq -r .token /tmp/forge-m3/dead-2.json)"
curl -sS \
  -H "Authorization: Bearer $DEAD2_TOKEN" \
  http://localhost:8080/v1/agents/"$DEAD2_ID"/claim \
  > /tmp/forge-m3/dead-2-claim.json
```

Wait another 65 to 75 seconds.

Pass:

- the job ends `failed`
- `dead_lettered` is true
- reason is `max_attempts`
- transitions show both attempts and both `visibility_timeout` losses
- the dashboard labels the job `failed (dead letter)`
- selecting the job shows its full transition trail and Logs pane
- the GitHub workflow job stays queued: no attempt acquired it, and GitHub times it out on its own (FR-12)
- the Redis pending count returns to zero

Inspect the record directly:

```bash
export DEAD_JOB_ID="$(jq -r .job_id /tmp/forge-m3/dead-2-claim.json)"
curl -sS http://localhost:8080/v1/jobs/"$DEAD_JOB_ID" | jq
sqlite3 /tmp/forge-m3/control/forge.db \
  "SELECT id, state, attempt, dead_lettered, reason FROM jobs WHERE id='$DEAD_JOB_ID';"
```

## 21. Remove a worker and revoke its token

The disposable worker from step 11 has not heartbeated and should now be `lost`:

```bash
./forge workers -data-dir /tmp/forge-m3/control
./forge remove -data-dir /tmp/forge-m3/control "$ONCE_ID"
```

Pass: the worker becomes `removed`.

Try its saved machine token:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -H "Authorization: Bearer $ONCE_MACHINE_TOKEN" \
  http://localhost:8080/v1/agents/"$ONCE_ID"/claim
```

Pass: `401`. Removal replaced the stored token hash.

The dashboard and `forge workers` retain the removed worker as history. It cannot heartbeat or claim work.

## 22. Final state

Uncordon the two real workers:

```bash
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_A"
./forge uncordon -data-dir /tmp/forge-m3/control "$WORKER_B"
```

Dispatch `forge-m3-fast` one final time.

Pass:

- the job finishes green
- both real workers are active and healthy
- queue depth is zero
- no Redis entries are pending
- no runner containers remain

Run the final checks:

```bash
curl -sS http://localhost:8080/v1/dashboard |
  jq '{queue_depth, workers: [.workers[] | {id,state,healthy,running}], latest_job: .jobs[0]}'

docker compose exec -T redis redis-cli XPENDING forge:jobs forge
docker ps -a --filter ancestor=ghcr.io/actions/actions-runner:latest

sqlite3 /tmp/forge-m3/control/forge.db \
  'SELECT state, count(*) FROM jobs GROUP BY state ORDER BY state;'
sqlite3 /tmp/forge-m3/control/forge.db \
  'SELECT state, count(*) FROM workers GROUP BY state ORDER BY state;'
sqlite3 /tmp/forge-m3/control/forge.db \
  'SELECT job_id, attempt, from_state, to_state, reason FROM transitions ORDER BY id;'
```

Expect:

- queue depth `0`
- two real workers `active`, healthy, and idle
- Redis pending count `0`
- no stopped runner containers left behind
- succeeded, failed worker-loss, and dead-lettered jobs represented in SQLite
- transition history matches the dashboard

## 23. Failures

- **Forge exits with `stream: ping 127.0.0.1:6379`.** Redis is not running. Run `docker compose up -d redis`.
- **Queued webhook Recent Deliveries show 202.** Expected. Ping is 204. 401 means the HMAC secret does not match.
- **Agent enrollment returns 401.** The enrollment token is invalid or expired (TTL is one hour). Mint a new token against the same control-plane data directory.
- **Agent enrollment returns 409.** The one-time token was already consumed. Mint a different token.
- **Agent repeatedly logs claim status 403.** The worker is cordoned or draining. Use `forge uncordon` when it should accept work.
- **Agent repeatedly logs claim status 401.** Its machine token was revoked, or its `worker.json` belongs to another control plane. Remove the local agent data and enroll as a new worker only if revocation was intentional.
- **Duplicate terminal status from the assigned worker returns 409.** Idempotent retries must return 204. 403 means the wrong worker; that is still the fencing check in step 12.
- **Dashboard shows a worker lost while its agent is running.** Check agent heartbeat errors and control-plane reachability. A healthy agent heartbeats every 10 seconds; lost is set after 30 seconds.
- **A cordoned worker receives one more job.** The cordon raced a claim request that was already open. Wait 35 seconds after cordoning before dispatching the verification job.
- **Queue depth is zero while Redis `XLEN` is non-zero.** Expected. Queue depth comes from SQLite queued jobs; Redis retains acknowledged stream history.
- **Queued work disappears after the Redis restart.** Forge did not restart against the same SQLite data directory, or startup reconciliation failed. Check `/tmp/forge-m3/control/forge.db` and the Forge startup log.
- **A healthy job becomes `worker_lost` around 60 seconds.** The sweeper reclaimed a live worker's entry. Heartbeats carry running job IDs and the sweeper returns entries whose worker still reports the job; check for heartbeat errors in the agent log. If heartbeats were flowing, this fails FR-11 and step 18.
- **Second claim after an abandoned assignment returns 204 and the job stays `assigned`.** The restarted worker acked its pending entry without resolving the attempt. This fails FR-11 and the restart check in step 17.
- **A cordoned worker returns `active` after an agent restart.** Heartbeat revival dropped cordon intent. This fails FR-19 and the restart check in step 19.
- **A dead-lettered job remains queued in GitHub.** Expected when no attempt acquired the GitHub job. GitHub has no API to fail a queued workflow job; it times the job out on its own (FR-12).
- **The dashboard bootstrap fails on a fresh machine.** It invokes `forge-agent` but does not install it. This fails the installation part of FR-3.
- **A killed local agent leaves its container running.** Killing an agent process is not the same as killing its host. Stop the container as directed in step 19.
- **Stopping an agent kills its running job.** Ctrl-C or SIGKILL on the agent destroys the sandbox and the attempt reports failed. Drain the worker and wait for running work to finish before stopping an agent.
- **Remove fails with an invalid worker transition.** Active and draining workers cannot be removed. Cordon an active worker first, or wait for a draining worker to settle at cordoned.
- **GitHub job runs on another machine.** Another self-hosted runner matched both labels. Keep `runs-on: [self-hosted, forge]` and remove that label combination elsewhere.
- **GitHub JIT registration returns 401 or 403.** The token lacks Administration write, or the token owner is not a repo admin.

Startup reconciliation restores queued jobs already stored in SQLite. It does not discover GitHub jobs whose webhook arrived while Forge was down. Worker labels are empty at enrollment, and the current agent reports fixed capacity `1`.

Do not test cloud burst, warm pools, metrics, TLS setup, admin login, or dashboard fleet controls here. Those are M4 or M5 work.

## 24. Stop

Ctrl-C in **forge**, **agent-a**, **agent-b**, and **tunnel**. Delete the GitHub webhook. Revoke the GitHub token. Remove the three test workflows if the repo does not need them.

In **check**:

```bash
kill "$(cat /tmp/forge-m3/agent-a.pid)" 2>/dev/null || true
kill "$(cat /tmp/forge-m3/agent-b.pid)" 2>/dev/null || true
docker compose down
rm -rf /tmp/forge-m3
rm -f ./forge ./forge-agent
```
