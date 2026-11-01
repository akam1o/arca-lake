#!/usr/bin/env bash
# Run the shard transfer matrix on loopback and collect JSON lines.
#
# Usage: benchmarks/transfer/run.sh [out.jsonl]
# Environment:
#   DURATION      seconds per client run (default 5)
#   SHARD_BYTES   shard size (default 1048576)
#   FRAME_BYTES   frame size (default 65536)
#   CONCURRENCY   space-separated list (default "1 8")
#   SERVER_DELAY_US  artificial per-frame server delay for the backpressure case (default 0)

set -euo pipefail
OUT="${1:-/tmp/arca-transfer-results.jsonl}"
DURATION="${DURATION:-5}"
SHARD_BYTES="${SHARD_BYTES:-1048576}"
FRAME_BYTES="${FRAME_BYTES:-65536}"
CONCURRENCY="${CONCURRENCY:-1 8}"
SERVER_DELAY_US="${SERVER_DELAY_US:-0}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/target/release/transfer-bench"
TMP="$(mktemp -d)"

cargo build -q --release -p arca-lake-transfer-bench
: > "$OUT"

for transport in grpc raw; do
  for tls in false true; do
    for op in put get; do
      for conc in $CONCURRENCY; do
        tls_flag=""; [[ "$tls" == "true" ]] && tls_flag="--tls"
        "$BIN" server --transport "$transport" $tls_flag --listen 127.0.0.1:0 \
          --duration $((DURATION + 4)) --frame-bytes "$FRAME_BYTES" \
          --server-delay-us "$SERVER_DELAY_US" \
          --cert-out "$TMP/cert.pem" --addr-out "$TMP/addr" > "$TMP/server.out" 2>/dev/null &
        spid=$!
        for _ in $(seq 1 50); do [[ -s "$TMP/addr" ]] && break; sleep 0.1; done
        addr="$(cat "$TMP/addr")"
        sleep 0.3
        label="$transport/tls=$tls/$op/c=$conc"
        "$BIN" client --transport "$transport" $tls_flag --addr "$addr" --op "$op" \
          --shard-bytes "$SHARD_BYTES" --frame-bytes "$FRAME_BYTES" \
          --concurrency "$conc" --duration "$DURATION" --ca-cert "$TMP/cert.pem" \
          --label "$label" >> "$OUT"
        wait "$spid" || true
        cat "$TMP/server.out" >> "$OUT"
        rm -f "$TMP/addr"
        echo "done: $label"
      done
    done
  done
done
echo "results: $OUT"
