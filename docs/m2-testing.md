# M2 manual test

Walk this top to bottom on a Mac with Docker Desktop. Pass means a GitHub Actions job runs on your machine and the result shows in GitHub's UI (FR-4, FR-5, FR-6, FR-13, FR-26, FR-27).

Use a repo you admin. A throwaway public or private repo is fine. Do not use `ubuntu-latest`. Do not mount Docker or run `docker` inside the workflow; Forge does not expose the host daemon.

You need three terminals. Keep them labeled: **tunnel**, **forge**, **check**.

## 1. Tools

1. Open Docker Desktop. Wait until it says Docker is running.
2. Confirm Go and Docker:

```bash
go version
docker info >/dev/null && echo docker-ok
```

3. Install a public HTTPS tunnel (GitHub will not POST to `localhost`). Pick one:

```bash
brew install cloudflared
```

or install [ngrok](https://ngrok.com/download).

4. Confirm `sqlite3` (macOS ships it):

```bash
sqlite3 -version
```

## 2. Values

In **check**, set these and leave the shell open. Replace owner and repo.

```bash
cd ~/repos/forge
export FORGE_OWNER='YOUR_GITHUB_USER'
export FORGE_REPO='YOUR_TEST_REPO'
export FORGE_WEBHOOK_SECRET="$(openssl rand -hex 16)"
echo "webhook secret: $FORGE_WEBHOOK_SECRET"
```

Print the secret once and keep it. You paste it into GitHub in step 5 and into Forge in step 7.

## 3. GitHub token

Forge calls `POST /repos/{owner}/{repo}/actions/runners/generate-jitconfig`. You must be a repo admin.

1. Open https://github.com/settings/personal-access-tokens
2. Fine-grained tokens → Generate new token
3. Name: `forge-m2`
4. Resource owner: the account that owns `$FORGE_OWNER/$FORGE_REPO`
5. Repository access → Only select repositories → `$FORGE_REPO`
6. Permissions → Repository → Administration → Read and write
7. Generate token. Copy it.

In **check**:

```bash
export FORGE_TOKEN='paste-the-token'
```

Classic PAT alternative: scope `repo`, and you must still be a repo admin.

## 4. Workflow

In `$FORGE_OWNER/$FORGE_REPO` (not necessarily this checkout), add `.github/workflows/forge-m2.yml`:

```yaml
name: forge-m2
on:
  workflow_dispatch:
jobs:
  ping:
    runs-on: [self-hosted, forge]
    steps:
      - run: echo hello from forge
      - run: uname -a
      - run: id
```

Commit on the default branch.

`runs-on` must include a label no other runner has. `forge` is that label. If you already have a machine registered as `self-hosted`, GitHub may give the job to it unless `forge` is required too.

## 5. Tunnel

In **tunnel**:

```bash
cloudflared tunnel --url http://localhost:8080
```

or:

```bash
ngrok http 8080
```

Copy the `https://…` origin only (no path). In **check**:

```bash
export FORGE_TUNNEL='https://YOUR-SUBDOMAIN.trycloudflare.com'
echo "$FORGE_TUNNEL/webhook/github"
```

Leave the tunnel process running. If it restarts, the URL changes and you must edit the webhook.

## 6. Webhook

In the test repo on GitHub:

1. Settings → Webhooks → Add webhook
2. Payload URL: the printed `$FORGE_TUNNEL/webhook/github` value
3. Content type: `application/json`
4. Secret: paste `$FORGE_WEBHOOK_SECRET`
5. Which events → Let me select individual events
6. Uncheck **Pushes** (it is on by default)
7. Check **Workflow jobs**
8. Active: checked
9. Add webhook

Recent Deliveries will show a `ping`. That ping fails until Forge is listening (step 8). That is expected.

## 7. Image

In **check**, pull once:

```bash
docker pull ghcr.io/actions/actions-runner:latest
```

On Apple Silicon this is `linux/arm64`.

## 8. Start Forge

In **forge**, from the forge checkout that defaults an empty `-command` to `./run.sh --jitconfig "$(cat /jitconfig)"` and copies `/jitconfig` as mode `0644`:

```bash
cd ~/repos/forge
go run ./cmd/forge \
  -addr :8080 \
  -data-dir ./data \
  -webhook-secret "$FORGE_WEBHOOK_SECRET" \
  -github-token "$FORGE_TOKEN" \
  -github-owner "$FORGE_OWNER" \
  -github-repo "$FORGE_REPO" \
  -image ghcr.io/actions/actions-runner:latest
```

Do not pass `-command`.

Pass: a line `forge starting` with `addr=:8080` and `image=ghcr.io/actions/actions-runner:latest`.

Fail: `forge: -webhook-secret is required` or `forge: -github-token is required` means the env vars were empty in this shell. Export them here or paste the values on the flags.

Leave this process running.

## 9. Confirm intake

In **check**:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8080/webhook/github \
  -H 'X-GitHub-Event: workflow_job' \
  -H 'X-Hub-Signature-256: sha256=deadbeef' \
  -d '{}'
```

Pass: `401`.

On GitHub, open the webhook → Recent Deliveries → the `ping` row → Redeliver.

Pass: response `204`.

## 10. Dispatch the job

1. Test repo → Actions → `forge-m2` → Run workflow → Run workflow
2. Open the run → `ping`

Pass (GitHub):

- Job leaves Queued (first sandbox start is slower than later ones)
- Log lines `hello from forge`, `uname -a`, and `id` (uid 1001, user `runner`)
- Job is green
- Settings → Actions → Runners: a `forge-…` runner may appear during the job and is gone after it

Pass (forge terminal): no `register jit` error. You should see the process stay up.

Pass (check terminal):

```bash
docker ps -a
sqlite3 ./data/forge.db 'SELECT id, state, repo, labels FROM jobs;'
sqlite3 ./data/forge.db 'SELECT job_id, from_state, to_state, reason FROM transitions ORDER BY id;'
ls -l ./data/logs/
stat -f '%Lp' ./data/secret.key
sqlite3 ./data/forge.db "SELECT name, length(ciphertext) FROM secrets;"
```

Expect:

- `docker ps -a` does not still list the runner container
- one jobs row, `state=succeeded`, `labels` containing `self-hosted` and `forge`
- transitions `queued → assigned → running → succeeded`
- `./data/logs/<job-id>-0.log` exists and is non-empty
- `secret.key` mode `600`
- secrets rows `webhook_secret` and `github_token` with non-zero ciphertext lengths. The `github_token` blob must not equal `$FORGE_TOKEN`.

## 11. Encrypted creds on restart

In **forge**: Ctrl-C.

Start again with the same data dir and **no** secret or token flags:

```bash
cd ~/repos/forge
go run ./cmd/forge \
  -addr :8080 \
  -data-dir ./data \
  -github-owner "$FORGE_OWNER" \
  -github-repo "$FORGE_REPO" \
  -image ghcr.io/actions/actions-runner:latest
```

Dispatch `forge-m2` again.

Pass: second job green in GitHub, second `succeeded` row in `jobs`. Forge loaded creds from `./data/forge.db`.

## 12. Failures

| What you see | Cause | Fix |
| --- | --- | --- |
| Webhook ping stays failed after step 8 | Tunnel URL changed, or payload path is not `/webhook/github` | Copy the current tunnel origin, edit the webhook, redeliver ping |
| GitHub job stuck Queued, forge silent | Event is not **Workflow jobs**, or HMAC secret mismatch | Webhook Recent Deliveries: 401 means secret; 204 on `queued` means Forge ignored a non-queued action |
| Forge log `register jit` status 401/403 | Token lacks Administration write, or you are not a repo admin | New token, step 3 |
| Forge log `register jit` status 404 | Wrong owner/repo | Match `$FORGE_OWNER` / `$FORGE_REPO` to the repo that has the workflow |
| Job runs on another machine | Another self-hosted runner matched `self-hosted` | Keep `runs-on: [self-hosted, forge]` |
| `permission denied` reading `/jitconfig` | Tree still copies the file as `0600` | Use the tree where the tar mode is `0644` |
| `./run.sh: not found` | You passed `-command`, or `-image` is not the official runner | Drop `-command`, use `ghcr.io/actions/actions-runner:latest` |
| Workflow step cannot run `docker` | Host Docker is not in the sandbox | Use the workflow in step 4 |
| Webhook redelivery of the same job returns 500 | `jobs` is unique on `(source, external_id)` | Dispatch a new run, do not Redeliver a consumed `queued` delivery |

Do not test org registration (`-github-org`) here. Do not expect missed webhooks to be recovered if Forge was down (`ListQueued` is not called on startup). Dashboard, fleet, and enrollment tokens are M3+.

## 13. Stop

Ctrl-C in **forge** and **tunnel**. Delete the GitHub webhook. Revoke the token. Optional:

```bash
rm -rf ~/repos/forge/data
```
