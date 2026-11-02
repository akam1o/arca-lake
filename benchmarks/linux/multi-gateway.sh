#!/usr/bin/env bash
# Aggregate throughput with 1, 2 (and 3 with HOST_C) gateways, durable
# only and with an unprotected cache tier (Phase 2.3 §7). Each gateway is
# driven by the load generator on its own host; PUT then two GET passes.
#   HOST_A=.. HOST_B=.. [HOST_C=..] benchmarks/linux/multi-gateway.sh <out>
# Environment: OBJECTS (96, per gateway) SIZE (16 MiB) CONC (4, per gateway)
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?results dir}"
TH=benchmarks/linux/two-host.sh
export OUT CHUNK_BYTES="${CHUNK_BYTES:-$((4 << 20))}"
max=2; [ -n "${HOST_C:-}" ] && max=3
mkdir -p "$OUT"
for mode in durable none; do
  for g in $(seq 1 $max); do
    echo "== $mode, $g gateway(s)"
    if ! GATEWAYS=$g bash "$TH" setup "$mode" > "$OUT/setup-$mode-gw$g.log" 2>&1; then
      echo "setup failed (see $OUT/setup-$mode-gw$g.log)"; bash "$TH" teardown >/dev/null 2>&1 || true; continue
    fi
    GATEWAYS=$g OBJECTS="${OBJECTS:-96}" SIZE="${SIZE:-$((16 << 20))}" CONC="${CONC:-4}" PASSES=2 bash "$TH" run "$mode-gw$g" > "$OUT/run-$mode-gw$g.log" 2>&1 || echo "run failed (see $OUT/run-$mode-gw$g.log)"
    grep -E '^\{' "$OUT/run-$mode-gw$g.log" | cut -c1-120 || true
    bash "$TH" teardown >/dev/null 2>&1 || true
  done
done
bash benchmarks/linux/sanitize.sh "$OUT" >/dev/null
echo "done: $OUT"
