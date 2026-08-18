#!/usr/bin/env bash
# NFR-1 start-latency benchmark. Optionally dispatches forge-latency.yml
# TRIALS times, then reads the FR-25 metrics and reports p95s:
#
#   - queued-to-running p95 from the control plane's
#     forge_job_latency_seconds{phase="queued_to_running"} histogram.
#     NFR-1 target: under 2s with the warm pool on.
#   - sandbox start p95 per mode from the agent's
#     forge_agent_sandbox_start_seconds{mode="warm"|"cold"} histogram.
#     NFR-1 target: cold under 5s.
#
# p95 is computed from histogram buckets by linear interpolation, the
# same estimate Prometheus's histogram_quantile() would give.
# See bench/README.md for the warm vs cold methodology.
set -euo pipefail

FORGE_URL="${FORGE_URL:-http://127.0.0.1:8080}"
AGENT_METRICS_URL="${AGENT_METRICS_URL:-http://127.0.0.1:9091}"
TRIALS="${TRIALS:-0}"
REPO="${BENCH_REPO:-}"
WORKFLOW="forge-latency.yml"

command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }

if [ "$TRIALS" -gt 0 ]; then
  [ -n "$REPO" ] || { echo "set BENCH_REPO=owner/repo to dispatch trials, or TRIALS=0 to only read metrics" >&2; exit 1; }
  command -v gh >/dev/null || { echo "gh is required to dispatch trials" >&2; exit 1; }
  for i in $(seq 1 "$TRIALS"); do
    before="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    gh workflow run "$WORKFLOW" -R "$REPO"
    run_id=""
    for _ in $(seq 1 30); do
      sleep 2
      run_id="$(gh run list -R "$REPO" --workflow "$WORKFLOW" \
        --json databaseId,createdAt \
        --jq "[.[] | select(.createdAt >= \"$before\")][0].databaseId // empty")"
      [ -n "$run_id" ] && break
    done
    [ -n "$run_id" ] || { echo "trial $i: dispatched run not found" >&2; exit 1; }
    echo "trial $i/$TRIALS: run $run_id (waiting)"
    gh run watch "$run_id" -R "$REPO" --exit-status >/dev/null || {
      echo "trial $i: run $run_id failed; a failed trial invalidates the benchmark" >&2
      exit 1
    }
  done
fi

# p95 <file> <metric> <label-regex>: interpolated 95th percentile from
# an ascending-bucket Prometheus histogram.
p95() {
  awk -v metric="$2" -v labels="$3" '
    $1 ~ "^"metric"_bucket" && $0 ~ labels {
      match($0, /le="[^"]*"/)
      le = substr($0, RSTART + 4, RLENGTH - 5)
      n++; les[n] = le; counts[n] = $NF
    }
    END {
      if (n == 0 || counts[n] == 0) { print "n/a"; exit }
      target = 0.95 * counts[n]
      prev_le = 0; prev_c = 0
      for (i = 1; i <= n; i++) {
        if (counts[i] >= target) {
          if (les[i] == "+Inf") { printf ">%s\n", prev_le; exit }
          if (counts[i] == prev_c) { print les[i]; exit }
          printf "%.3f\n", prev_le + (les[i] - prev_le) * (target - prev_c) / (counts[i] - prev_c)
          exit
        }
        prev_le = les[i]; prev_c = counts[i]
      }
    }' "$1"
}

samples() {
  awk -v metric="$2" -v labels="$3" '
    $1 ~ "^"metric"_count" && $0 ~ labels { print $NF; found = 1 }
    END { if (!found) print 0 }' "$1"
}

counter() {
  awk -v metric="$2" '$1 == metric { print $NF; found = 1 } END { if (!found) print 0 }' "$1"
}

seconds() {
  if [ "$1" = "n/a" ]; then
    printf 'n/a'
  else
    printf '%ss' "$1"
  fi
}

cp_metrics="$(mktemp)"
trap 'rm -f "$cp_metrics" "${agent_metrics:-}"' EXIT
curl -fsS "$FORGE_URL/metrics" > "$cp_metrics"

echo
echo "control plane ($FORGE_URL/metrics)"
q2r_p95="$(p95 "$cp_metrics" forge_job_latency_seconds 'phase="queued_to_running"')"
q2r_n="$(samples "$cp_metrics" forge_job_latency_seconds 'phase="queued_to_running"')"
echo "  queued-to-running p95: $(seconds "$q2r_p95") over $q2r_n jobs (NFR-1 warm target: <2s)"

agent_metrics="$(mktemp)"
if curl -fsS "$AGENT_METRICS_URL/metrics" > "$agent_metrics" 2>/dev/null; then
  echo
  echo "agent ($AGENT_METRICS_URL/metrics)"
  for mode in warm cold; do
    m_p95="$(p95 "$agent_metrics" forge_agent_sandbox_start_seconds "mode=\"$mode\"")"
    m_n="$(samples "$agent_metrics" forge_agent_sandbox_start_seconds "mode=\"$mode\"")"
    echo "  sandbox start ($mode) p95: $(seconds "$m_p95") over $m_n jobs"
  done
  hits="$(counter "$agent_metrics" forge_agent_warm_pool_hits_total)"
  misses="$(counter "$agent_metrics" forge_agent_warm_pool_misses_total)"
  echo "  warm-pool hits: $hits  misses: $misses"
  echo "  (NFR-1 cold target: sandbox start <5s)"
else
  echo
  echo "agent metrics not reachable at $AGENT_METRICS_URL (set AGENT_METRICS_URL or start forge-agent with -metrics-addr)"
fi
