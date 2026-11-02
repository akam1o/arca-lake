#!/usr/bin/env bash
# Performance regression check (Phase 2.3 §8): a fixed workload on the
# two-host layout, three repetitions in shuffled order, reduced to the
# indicators in perf_summary.py and compared with the baseline recorded
# for this environment label. Not a CI job: it needs the validation hosts
# and its thresholds hold only for runs of the same label.
#   HOST_A=.. HOST_B=.. benchmarks/linux/regress.sh <env-label> [out] [--record]
set -euo pipefail
cd "$(dirname "$0")/../.."
ENV_LABEL="${1:?environment label (names the hosts and layout, e.g. lab-2host)}"
OUT="${2:-benchmarks/linux/results/regress-$(date +%Y%m%d-%H%M%S)}"
RECORD="${3:-}"
CONFIGS="durable:4 none:4" PASSES=3 bash benchmarks/linux/repeat.sh "$OUT" 3 2>&1 | grep -E '^==|done'
python3 -I benchmarks/linux/perf_summary.py "$OUT" > "$OUT/summary.json"
cat "$OUT/summary.json"
python3 -I benchmarks/linux/compare.py "$OUT/summary.json" --env "$ENV_LABEL" ${RECORD:+--record} | tee "$OUT/compare.md"
