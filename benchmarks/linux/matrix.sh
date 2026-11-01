#!/usr/bin/env bash
# The RFC-009 two-host measurement matrix, on top of two-host.sh.
# HOST_A=user@a HOST_B=user@b benchmarks/linux/matrix.sh [results-dir]
set -euo pipefail
cd "$(dirname "$0")/../.."
export OUT="${1:-benchmarks/linux/results}"
TH=benchmarks/linux/two-host.sh
MIB=$((1 << 20))
export OBJECTS="${OBJECTS:-96}" SIZE="${SIZE:-$((16 * MIB))}"

run() { # <label> <conc> <passes> [range]
  CONC="$2" PASSES="$3" RANGE_BYTES="${4:-0}" bash "$TH" run "$1" | grep -E '^\{' | cut -c1-200
}

echo "== durable only (4 MiB chunks)"
CHUNK_BYTES=$((4 * MIB)) bash "$TH" setup durable >/dev/null
run durable-c1 1 2
run durable-c4 4 2
run durable-c8 8 2

for mode in none repl ec; do
  echo "== cache $mode (4 MiB chunks, unlimited capacity)"
  CHUNK_BYTES=$((4 * MIB)) bash "$TH" setup "$mode" >/dev/null
  run "$mode-c1" 1 3
  run "$mode-c4" 4 3
  run "$mode-c8" 8 3
  run "$mode-c4-range4m" 4 2 $((4 * MIB))
done

for mode in none ec; do
  echo "== cache $mode, capacity 800 MiB (data 1.5 GiB)"
  CACHE_CAPACITY=$((800 * MIB)) CHUNK_BYTES=$((4 * MIB)) bash "$TH" setup "$mode" >/dev/null
  run "$mode-cap800m-c4" 4 3
done

echo "== 1 MiB chunks"
CHUNK_BYTES=$MIB bash "$TH" setup durable >/dev/null
run durable-1m-c4 4 2
CHUNK_BYTES=$MIB bash "$TH" setup ec >/dev/null
run ec-1m-c4 4 3

bash "$TH" teardown
echo "matrix done: $OUT"
