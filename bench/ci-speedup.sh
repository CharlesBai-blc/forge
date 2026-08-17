#!/usr/bin/env bash
# CI speedup benchmark (NFR-2 methodology). Dispatches forge-bench.yml
# TRIALS times against BENCH_REPO, waits for each run, and reports
# hosted vs forge timings from GitHub's job timestamps.
# See bench/README.md for what the numbers mean.
set -euo pipefail

REPO="${BENCH_REPO:?set BENCH_REPO=owner/repo (the repo with forge-bench.yml)}"
TRIALS="${TRIALS:-5}"
WORKFLOW="forge-bench.yml"

command -v gh >/dev/null || { echo "gh is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

OUT_DIR="$(cd "$(dirname "$0")" && pwd)/results"
mkdir -p "$OUT_DIR"
RAW="$OUT_DIR/$(date -u +%Y%m%dT%H%M%SZ).json"

echo "repo=$REPO trials=$TRIALS workflow=$WORKFLOW"
echo "raw results: $RAW"

run_ids=()
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
  if ! gh run watch "$run_id" -R "$REPO" --exit-status >/dev/null; then
    echo "trial $i: run $run_id failed; a failed trial invalidates the benchmark" >&2
    echo "inspect: gh run view $run_id -R $REPO" >&2
    exit 1
  fi
  run_ids+=("$run_id")
done

for id in "${run_ids[@]}"; do
  gh api "repos/$REPO/actions/runs/$id/jobs" --jq '.jobs[]'
done | jq -s '
  [ .[]
    | select(.name | test("^bench-"))
    | { run_id,
        name,
        runner: .runner_name,
        variant: (if (.name | test("forge")) then "forge" else "hosted" end),
        queued_s: ((.started_at  | fromdateiso8601) - (.created_at | fromdateiso8601)),
        exec_s:   ((.completed_at | fromdateiso8601) - (.started_at | fromdateiso8601)),
        total_s:  ((.completed_at | fromdateiso8601) - (.created_at | fromdateiso8601)) } ]
' > "$RAW"

echo
echo "per-trial results (seconds):"
jq -r '
  ["run_id","variant","runner","queued_s","exec_s","total_s"],
  (.[] | [.run_id, .variant, .runner, .queued_s, .exec_s, .total_s])
  | @tsv
' "$RAW" | column -t -s "$(printf '\t')"

echo
jq -r '
  def median: sort
    | if length % 2 == 1 then .[length/2|floor]
      else (.[length/2 - 1] + .[length/2]) / 2 end;
  def r2: . * 100 | round / 100;
  group_by(.variant)
  | map({ variant: .[0].variant, n: length,
          exec:   ([.[].exec_s]   | median),
          total:  ([.[].total_s]  | median),
          queued: ([.[].queued_s] | median) })
  | ( .[] | "\(.variant): n=\(.n) exec_median=\(.exec)s total_median=\(.total)s queued_median=\(.queued)s" ),
    ( (map({(.variant): .}) | add) as $v
      | if ($v.hosted and $v.forge) then
          "speedup, execution (hosted/forge):  \(($v.hosted.exec  / $v.forge.exec)  | r2)x",
          "speedup, end-to-end (hosted/forge): \(($v.hosted.total / $v.forge.total) | r2)x"
        else
          "missing a variant; no speedup computed"
        end )
' "$RAW"
