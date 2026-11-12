#!/usr/bin/env bash
# Deferred reclamation under load with real binaries on real FoundationDB
# (Phase 3.13, R-106, ADR-0030). One host, all on 127.0.0.1: a private
# fdbserver (the system FoundationDB is never touched), one controller,
# three storage nodes (one 2+1 group) with a short retention, a gateway
# and the load generator. For WINDOW minutes, iterations of: PUT N objects,
# GET (verified) the previous iteration's, DELETE the one before. Every
# SAMPLE seconds: the controller's reclaim counters, each node's CPU time,
# RSS, pending-reclaim file sizes and capacity metrics. Run it with the
# binaries of two releases for a comparison (same s3load for both).
#
#   benchmarks/linux/reclaim-load.sh <out-dir>
# Environment: BIN_DIR (arca-lake), S3LOAD (s3load binary), WINDOW (20),
#   N (32), SIZE (1 MiB), RETENTION (120 s), FDB_PORT (4710), RUN
#   (/mnt/arca-fdb/p313-reclaim), SAMPLE (15), PACE (seconds slept between
#   iterations, 0: none; a pace keeps a retention's deletes within the
#   nodes' capacity, so that admission does not refuse writes).
set -euo pipefail
OUT="${1:?output directory}"
mkdir -p "$OUT"
BIN_DIR="${BIN_DIR:?}"; S3LOAD="${S3LOAD:?}"
WINDOW="${WINDOW:-20}"; N="${N:-32}"; SIZE="${SIZE:-$((1 << 20))}"
RETENTION="${RETENTION:-120}"; FDB_PORT="${FDB_PORT:-4710}"
RUN="${RUN:-/mnt/arca-fdb/p313-reclaim}"; SAMPLE="${SAMPLE:-15}"; PACE="${PACE:-0}"
export ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1
A="$BIN_DIR/arca-lake"; CF="$RUN/fdb.cluster"; ADMIN="$A admin --cluster-file $CF"
EP="http://127.0.0.1:9400"
start() { local n="$1"; shift; nohup "$@" > "$RUN/logs/$n.log" 2>&1 < /dev/null & echo $! > "$RUN/$n.pid"; }
cleanup() {
  touch "$OUT/.stop"
  for p in "$RUN"/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true; done
  sleep 2; for p in "$RUN"/*.pid; do [ -f "$p" ] && kill -9 "$(cat "$p")" 2>/dev/null || true; done
  mkdir -p "$OUT/logs"
  for f in "$RUN"/logs/*.log; do [ -f "$f" ] && gzip -c "$f" > "$OUT/logs/$(basename "$f").gz"; done
  rm -rf "$RUN"
}
trap cleanup EXIT
rm -rf "$RUN"; mkdir -p "$RUN/logs" "$RUN/fdb-data" "$RUN/fdb-log"
AT=@  # cluster file: description:id, at, coordinators
echo "p313:reclaim${AT}127.0.0.1:$FDB_PORT" > "$CF"
start fdb fdbserver -p "127.0.0.1:$FDB_PORT" -C "$CF" -d "$RUN/fdb-data" -L "$RUN/fdb-log"
sleep 2; fdbcli -C "$CF" --exec 'configure new single ssd' > /dev/null
for _ in $(seq 1 30); do fdbcli -C "$CF" --exec 'status minimal' | grep -q 'is available' && break; sleep 1; done
for i in 0 1 2; do
  start "d$i" "$A" storage-node --listen "127.0.0.1:$((9300 + i))" --dir "$RUN/d$i" \
    --metrics-listen "127.0.0.1:$((9930 + i))" --capacity-bytes $((6 << 30)) --reclaim-retention-secs "$RETENTION" --reclaim-skew-secs 5
done
sleep 2
for i in 0 1 2; do kill -0 "$(cat "$RUN/d$i.pid")" 2>/dev/null || { echo "node d$i did not start: $(tail -3 "$RUN/logs/d$i.log")" >&2; exit 1; }; done
$ADMIN init > /dev/null 2>&1 || true
for i in 0 1 2; do $ADMIN register-node --address "127.0.0.1:$((9300 + i))" --label "node=d$i" --label device_class=hdd > /dev/null; done
$ADMIN create-groups --tier default --device-class hdd --failure-domain node --count 1 --k 2 --m 1 \
  --chunk-bytes $((1 << 20)) --small-threshold 0 > /dev/null
start ctl "$A" controller --id ctl --http 127.0.0.1:9501 --cluster-file "$CF" --allow-shared-filesystem \
  --failure-domain node --gc-interval-secs 5 --gc-grace-secs 10 --reconcile-interval-secs 30 --reclaim-interval-secs 10
start gateway "$A" gateway --backend cluster --cluster-file "$CF" --allow-anonymous --listen 127.0.0.1:9400 \
  --metrics-listen 127.0.0.1:9401 --k 2 --m 1 --chunk-bytes $((1 << 20)) --small-threshold 0 --failure-domain node
for _ in $(seq 1 60); do curl -s -o /dev/null "$EP/" && break; sleep 1; done
"$S3LOAD" --endpoint "$EP" --bucket rload --op create-bucket > /dev/null

sampler() {
  set +e +o pipefail
  printf 'time\tsource\tmetric\tvalue\n' > "$OUT/samples.tsv"
  while [ ! -f "$OUT/.stop" ]; do
    t=$(date +%s)
    {
      curl -s http://127.0.0.1:9501/metrics | grep -E '^arca_controller_(reclaimed_total|reclaimed_bytes_total|reclaim_refused_total|reclaim_errors_total|gc_reclaimed_bytes_total) ' | awk '{print "ctl", $1, $2}'
      for i in 0 1 2; do
        pid=$(cat "$RUN/d$i.pid")
        read -r utime stime < <(awk '{print $14, $15}' "/proc/$pid/stat")
        echo "d$i cpu_ticks $((utime + stime))"
        echo "d$i rss_kib $(awk '/VmRSS/ {print $2}' "/proc/$pid/status")"
        echo "d$i pending_files_bytes $(cat "$RUN/d$i"/pending-reclaim.* 2>/dev/null | wc -c)"
        curl -s "http://127.0.0.1:$((9930 + i))/metrics" | grep -E '^arca_node_capacity_bytes\{kind="(total|available|reserved)"' | awk -v n="d$i" '{print n, $1, $2}'
      done
    } | awk -v t="$t" '{print t "\t" $1 "\t" $2 "\t" $3}' >> "$OUT/samples.tsv"
    sleep "$SAMPLE"
  done
}
rm -f "$OUT/.stop"; sampler & SAMPLER=$!
s3() { "$S3LOAD" --endpoint "$EP" --bucket rload --size "$SIZE" --label "$1" "${@:2}" 2>>"$OUT/s3load-stderr.log" >> "$OUT/s3load.jsonl" || true; }
end=$(( $(date +%s) + WINDOW * 60 )); it=0
while [ "$(date +%s)" -lt "$end" ]; do
  it=$((it + 1))
  s3 put --op put --prefix "i$it" --objects "$N" --concurrency 8
  [ "$it" -le 1 ] || s3 get --op get --prefix "i$((it - 1))" --objects "$N" --concurrency 8 --verify
  [ "$it" -le 2 ] || s3 delete --op delete --prefix "i$((it - 2))" --objects "$N" --concurrency 8
  [ "$PACE" = 0 ] || sleep "$PACE"
done
# Drain: no load for two retentions plus reclaim passes.
sleep $(( RETENTION * 2 + 60 ))
touch "$OUT/.stop"; wait "$SAMPLER" 2>/dev/null || true
for i in 0 1 2; do $ADMIN node status --node $((i + 1)) 2>/dev/null | grep -iE 'reclaim' | sed "s/^/d$i /"; done > "$OUT/final-health.txt" || true
grep -h panicked "$RUN"/logs/*.log > "$OUT/panics.txt" 2>/dev/null || true
echo "iterations=$it window_min=$WINDOW n=$N size=$SIZE retention=$RETENTION" > "$OUT/result.txt"
