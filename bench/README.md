# bench

Benchmark methodology and scripts (NFR-1, NFR-2, NFR-4). This
directory holds the hosted vs Forge CI speedup, the start-latency
bench, and the NFR-4 soak harness. Reclaim latency and idempotent
claiming (FR-11) live next to the code they measure:
`internal/api/reclaim_bench_test.go`.

## Start latency (NFR-1)

`start-latency.sh` reports p95 job-start latency from the FR-25 metrics
(the benchmark consumes the metrics; it does not time anything itself):

- **queued-to-running p95** from the control plane's
  `forge_job_latency_seconds{phase="queued_to_running"}` histogram.
  NFR-1 target: under 2s with the warm pool on.
- **sandbox start p95 per mode** from the agent's
  `forge_agent_sandbox_start_seconds{mode="warm"|"cold"}` histogram,
  plus warm-pool hit and miss counts. NFR-1 target: cold under 5s.

p95 is interpolated from histogram buckets, the same estimate
Prometheus's `histogram_quantile()` gives.

### Run it

1. Copy `forge-latency.yml` to `.github/workflows/forge-latency.yml` in
   a repo connected to Forge. The job body is one `echo`, so the
   measured window is start overhead, not workload.
2. Histograms accumulate for the process lifetime, so restart the
   control plane and agent before each scenario to start from clean
   counters:
   - **warm scenario:** agent with the default `-warm-pool 2`.
   - **cold scenario:** agent with `-warm-pool 0`.
3. From this directory, with `gh` authenticated against that repo:

```bash
BENCH_REPO=owner/repo TRIALS=20 ./start-latency.sh
```

`TRIALS=0` (the default) skips dispatching and only reads the current
metrics, for fleets already under load. `FORGE_URL` and
`AGENT_METRICS_URL` override the default local endpoints.

Dispatches are sequential, so with a single worker and pool size 2 the
pool refills between jobs and every trial in the warm scenario is a
warm start; confirm with the reported hit count. Quote the number with
the host hardware, the scenario (warm or cold), and the trial count.

## CI speedup vs hosted runners

`ci-speedup.sh` dispatches `forge-bench.yml` N times. Each run executes an
identical job on `ubuntu-latest` and on a Forge worker
(`runs-on: [self-hosted, forge]`), sequentially across trials so a
single-worker fleet never queues one benchmark job behind another.

### What is measured

All timing comes from GitHub's Jobs API, never from Forge's own
bookkeeping:

- **execution** = `completed_at - started_at`. Runner picked up the job to
  job finished. This is the number for a speedup claim.
- **queued** = `started_at - created_at`. Reported separately, not folded
  into the speedup, because a single-worker fleet and GitHub's own
  scheduling both sit in it.
- **total** = `completed_at - created_at`. End-to-end, reported alongside.

The script prints the median of N trials for each variant and the
hosted/forge ratio for execution and total.

### Workload

Pinned Go 1.23.4 toolchain, then:

1. `go build -a std cmd`: compile the standard library and the entire
   Go toolchain (compiler, linker, go command) from scratch. This is
   the bulk of the workload, minutes of pure compute.
2. `go test -count=1 -short` over sixteen stdlib trees (`archive`,
   `bufio`, `bytes`, `compress`, `container`, `encoding`, `fmt`,
   `hash`, `image`, `index`, `math`, `regexp`, `sort`, `strconv`,
   `strings`, `text`, `unicode`). `crypto/...` is omitted: Go 1.23.4's
   `crypto/tls` testdata certificates expired on 2025-01-01, so those
   tests fail on today's clock on every runner.

The workload must dwarf fixed per-job overhead (sandbox creation and
the in-container toolchain download on Forge, both ~20s) or the ratio
measures overhead, not compute.

Chosen because it is CPU-bound, has zero third-party module downloads (the
toolchain ships the source), and is byte-identical on both runners. The
speedup measures compute, not a network lottery.

### Sandbox image

`setup-go` downloads the toolchain into a fresh container on every Forge
job; a hosted runner has it preinstalled. That download is fixed
per-job overhead, not compute, and it is large enough relative to a
short job to distort the ratio. `Dockerfile` builds a sandbox image
that pre-populates `setup-go`'s own tool cache with Go 1.23.4, so the
same, unmodified `setup-go` step in `forge-bench.yml` finds a cache hit
on Forge instead of downloading. This does not touch the workflow: it
only removes overhead that a real installed toolchain would not pay.

Build it once, on the machine running `forge-agent`:

```bash
docker build -t forge-bench:go1.23.4 -f Dockerfile .
```

Start (or restart) the control plane with `-image forge-bench:go1.23.4`
instead of the bare `ghcr.io/actions/actions-runner:latest`.

### Run it

1. Copy `forge-bench.yml` to `.github/workflows/forge-bench.yml` in a
   repo connected to Forge and commit it to the default branch. Do not
   put it in this repo's `.github/workflows/`: Forge's own CI stays on
   `ubuntu-latest`.
2. Have the M3 stack running: control plane (using the image above), at
   least one enrolled worker, webhook delivering to Forge. The control
   plane queues only `self-hosted` jobs, so the hosted matrix leg does
   not occupy a worker.
3. From this directory, with `gh` authenticated against that repo:

```bash
BENCH_REPO=owner/repo TRIALS=5 ./ci-speedup.sh
```

Raw per-job JSON is written to `results/<timestamp>.json` (gitignored).
A failed trial aborts the benchmark; partial results are not reported.

### Honest reporting

A number from this script is quotable only with all of the following:

- **The worker hardware.** The speedup is mostly your machine vs GitHub's
  2-core hosted runner. Say what the machine is (the `Record host` step
  logs `uname -a` and the CPU count in every job).
- **The workload.** "Go stdlib build and test suite", not "CI".
- **Median of N trials**, with N stated. Never a best-of.
- **Cold sandboxes.** At M3 there is no warm pool; every Forge job pays
  container creation inside the measured window, which a hosted runner's
  always-on VM does not. This asymmetry favors the hosted runner, which
  makes the measured speedup conservative. Re-run after M4 warm pools
  before citing a warm-start number.
- **Sandbox limits.** The Forge job runs under the configured sandbox CPU
  and memory limits. State them if they are below the host's capacity.

Do not cite queued or total medians as a speedup without noting fleet size:
with one worker, Forge queue wait is a property of the test setup, not the
scheduler.

## Scale soak (NFR-4)

`soak/` is a Go harness that runs a real control plane in-process
(SQLite store, Redis stream, claim, sweep) and drives it with simulated
agents. The generator keeps the queue topped up to the concurrency
target; each agent enrolls, heartbeats, and runs a sequential
claim-report loop exactly like `internal/runner`, with the sandbox
replaced by a timed sleep. NFR-4 targets scheduler and state-machine
endurance; sandbox behavior is covered by FR-17 and NFR-1.

Topology: 5 simulated machines x 10 agent slots = 50 concurrent jobs.
An agent process runs one job at a time by design (tdd.md §4.6: a
worker consumer re-reading its own pending entries signals a restart),
so a machine with N slots runs N agents; the harness enrolls each slot
as its own worker.

The run fails on any failed, dead-lettered, or duplicated job, or if
nothing completes. It reports totals and queued-to-running p50/p95
measured client-side from job creation.

```bash
# CI smoke (also in .github/workflows/ci.yml, job soak-smoke):
go run ./bench/soak -duration 45s -job 1s

# NFR-4 acceptance run: 24h against a real Redis
docker compose up -d redis   # or any Redis
go run ./bench/soak -duration 24h -redis 127.0.0.1:6379
```

`-redis mini` (the default) uses an embedded miniredis, fine for smokes;
use a real Redis for the 24h run so stream memory is bounded by Redis,
not the harness process. The 24h acceptance run is deferred to the AWS
validation job (docs/m5-aws-handoff.md) so it can share the burst-week
schedule; a 20s local smoke completed 1,021 jobs at 50 concurrent with
zero failures or duplicates, which already exceeds NFR-4's 1,000
jobs-per-day floor by three orders of magnitude on throughput.

## Reclaim (FR-11)

Package tests in `internal/api/reclaim_bench_test.go`. Not a hosted-vs-Forge
comparison; they measure the control plane's reclaim path.

```bash
go test ./internal/api/ -run '^$' -bench BenchmarkReclaimSweep -benchtime 300x
go test ./internal/api/ -run TestReclaimIsIdempotentUnderConcurrentClaims -race -v
```
