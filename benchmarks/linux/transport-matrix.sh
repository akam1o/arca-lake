#!/usr/bin/env bash
# gRPC versus raw framed TCP shard-transfer matrix between two hosts
# (RFC-009, Phase 2.2 §4): transports × TLS × connections × concurrent
# streams × frame size, each for `--duration` seconds, client on HOST_B,
# server on HOST_A. Output: one JSON line per client run (the bench's own
# format, which includes CPU seconds and RSS) into <out>/transport.jsonl.
#
#   HOST_A=user@a HOST_B=user@b benchmarks/linux/transport-matrix.sh <out>
# Environment: DURATION (6) SHARD_BYTES (1 MiB) CONNECTIONS ("1 2 4")
#   CONCURRENCY ("1 4 8") FRAMES ("65536 262144 1048576") TLS ("0 1")
#   TRANSPORTS ("grpc raw") OPS ("get put")
set -euo pipefail
: "${HOST_A:?}" "${HOST_B:?}"
OUT="${1:?out dir}"; mkdir -p "$OUT"
DURATION="${DURATION:-6}"; SHARD_BYTES="${SHARD_BYTES:-1048576}"
REMOTE_REPO="${REMOTE_REPO:-\$HOME/arca-lake}"
TB="$REMOTE_REPO/target/release/transfer-bench"
addr_of() { ssh -o BatchMode=yes "$1" "hostname -I | awk '{print \$1}'"; }
ADDR_A="$(addr_of "$HOST_A")"
port=5600
: > "$OUT/transport.jsonl"
for transport in ${TRANSPORTS:-grpc raw}; do
for tls in ${TLS:-0 1}; do
for conns in ${CONNECTIONS:-1 2 4}; do
for conc in ${CONCURRENCY:-1 4 8}; do
for frame in ${FRAMES:-65536 262144 1048576}; do
for op in ${OPS:-get put}; do
  [ "$conns" -gt "$conc" ] && continue
  port=$((port + 1)); tlsflag=""; [ "$tls" = 1 ] && tlsflag="--tls"
  ssh -o BatchMode=yes "$HOST_A" "nohup $TB server --transport $transport --listen $ADDR_A:$port $tlsflag --cert-out /tmp/tb-cert.pem --addr-out /tmp/tb-addr --frame-bytes $frame --duration $((DURATION + 8)) --threads 4 > /tmp/tb-server-$port.log 2>&1 < /dev/null &"
  sleep 1.5
  if [ "$tls" = 1 ]; then scp -q "$HOST_A:/tmp/tb-cert.pem" /tmp/tb-cert.pem; scp -q /tmp/tb-cert.pem "$HOST_B:/tmp/tb-cert.pem"; fi
  label="$transport-tls$tls-conn$conns-conc$conc-frame$frame-$op"
  ssh -o BatchMode=yes "$HOST_B" "$TB client --transport $transport --addr $ADDR_A:$port --op $op $tlsflag --ca-cert /tmp/tb-cert.pem --shard-bytes $SHARD_BYTES --frame-bytes $frame --concurrency $conc --connections $conns --duration $DURATION --threads 4 --label $label" 2>/dev/null | grep -E '^\{' >> "$OUT/transport.jsonl" || echo "failed: $label" >&2
  # Server-side CPU from its own summary line.
  sleep $((DURATION + 9))
  ssh -o BatchMode=yes "$HOST_A" "grep -E '^\{' /tmp/tb-server-$port.log | sed 's/\"label\":\"\"/\"label\":\"server-$label\"/'" >> "$OUT/transport.jsonl" || true
done; done; done; done; done; done
sed -i.bak -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(:[0-9]+)?/<addr>/g' "$OUT/transport.jsonl" && rm -f "$OUT/transport.jsonl.bak"
echo "done: $OUT/transport.jsonl"
