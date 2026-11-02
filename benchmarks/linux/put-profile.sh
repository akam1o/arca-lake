#!/usr/bin/env bash
# R-48: profile PUT at concurrency 4 over several repetitions with the
# per-second samplers on every host (two-host.sh PROFILE=1). Each
# repetition writes a fresh key space; the cluster is set up once per
# repetition so that every run starts from an empty store, as repeat.sh
# does.
#   HOST_A=.. HOST_B=.. benchmarks/linux/put-profile.sh <out> [reps=3] [mode=durable]
# Environment: OBJECTS (96) SIZE (16 MiB) CONC (4) CHUNK_BYTES (4 MiB)
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?results dir}"; REPS="${2:-3}"; MODE="${3:-durable}"
TH=benchmarks/linux/two-host.sh
export OUT CHUNK_BYTES="${CHUNK_BYTES:-$((4 << 20))}"
mkdir -p "$OUT"
for r in $(seq 1 "$REPS"); do
  bash "$TH" setup "$MODE" >/dev/null 2>&1
  echo "== repetition $r"
  PROFILE=1 PASSES=0 OBJECTS="${OBJECTS:-96}" SIZE="${SIZE:-$((16 << 20))}" CONC="${CONC:-4}" bash "$TH" run "put-r$r" 2>&1 | grep -E '^\{' | cut -c1-120
  bash "$TH" teardown >/dev/null 2>&1 || true
done
bash benchmarks/linux/sanitize.sh "$OUT" >/dev/null
echo "done: $OUT"
