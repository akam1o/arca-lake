#!/usr/bin/env bash
# Two-host measurement driver for arca-lake (RFC-009, Phase 2.1).
#
# Host A runs FoundationDB (a private fdbserver owned by the test user, not
# the system service), the controller, two durable storage nodes and one or
# two cache nodes; host B runs one durable node, one or two cache nodes, the
# gateway, and the load generator. Both hosts need `~/arca-lake` built with
# `cargo build --release --features fdb -p arca-lake -p arca-lake-s3load`
# and `foundationdb-clients` installed; host A also needs `fdbserver` on the
# PATH (foundationdb-server package).
#
# With HOST_C set, a three-host layout is used instead: one durable node and
# one cache node per host (host = failure domain), FoundationDB and the
# controller on A, the gateway and the load generator on C. Every durable
# shard and every cache shard then crosses a host boundary except the one on
# the gateway's host.
#
# Host addresses come only from the environment. Nothing in this script, in
# its output directory, or in the summaries it produces records them.
#
#   HOST_A=user@a HOST_B=user@b [HOST_C=user@c] benchmarks/linux/two-host.sh setup <mode>
#   HOST_A=... HOST_B=... benchmarks/linux/two-host.sh run <label> [opts]
#   HOST_A=... HOST_B=... benchmarks/linux/two-host.sh teardown
#
# mode: durable | none | repl | ec   (cache tier protection; durable = no cache)
# run options (environment): OBJECTS (32) SIZE (16 MiB) CONC (4) PASSES (3)
#   RANGE_BYTES (0) OUT (benchmarks/linux/results)
# setup options (environment): CHUNK_BYTES (4 MiB) CACHE_CAPACITY (0 = unlimited)
#   CACHE_MAX_POPULATIONS (8) GW_EXTRA_ARGS (extra gateway flags, e.g. --read-parallelism 1)
set -euo pipefail

: "${HOST_A:?ssh target of host A (user@host)}"
: "${HOST_B:?ssh target of host B (user@host)}"
REMOTE_REPO="${REMOTE_REPO:-\$HOME/arca-lake}"
RUN_DIR="${RUN_DIR:-\$HOME/arca-run}"
FDB_PORT="${FDB_PORT:-4510}"
CHUNK_BYTES="${CHUNK_BYTES:-$((4 << 20))}"
CACHE_CAPACITY="${CACHE_CAPACITY:-0}"
OUT="${OUT:-benchmarks/linux/results}"

HOST_C="${HOST_C:-}"
sa() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_A" "$@"; }
sb() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_B" "$@"; }
sc() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_C" "$@"; }
addr_of() { ssh -o BatchMode=yes "$1" "hostname -I | awk '{print \$1}'"; }

ADDR_A="$(addr_of "$HOST_A")"
ADDR_B="$(addr_of "$HOST_B")"
ADDR_C=""; [ -n "$HOST_C" ] && ADDR_C="$(addr_of "$HOST_C")"
# The gateway host: C in the three-host layout, B otherwise.
if [ -n "$HOST_C" ]; then sg() { sc "$@"; }; ADDR_G="$ADDR_C"; else sg() { sb "$@"; }; ADDR_G="$ADDR_B"; fi
HOSTS_FNS="sa sb"; [ -n "$HOST_C" ] && HOSTS_FNS="sa sb sc"
BIN="$REMOTE_REPO/target/release/arca-lake"
S3LOAD="$REMOTE_REPO/target/release/s3load"
CLUSTER_FILE="$RUN_DIR/fdb.cluster"
ADMIN="$BIN admin --cluster-file $CLUSTER_FILE"

# start <ssh-fn> <name> <command...>: nohup a process on a host, pid file.
start() {
  local fn="$1" name="$2"; shift 2
  "$fn" "mkdir -p $RUN_DIR/logs && cd $RUN_DIR && { nohup $* > $RUN_DIR/logs/$name.log 2>&1 < /dev/null & echo \$! > $RUN_DIR/$name.pid; }"
}
stop_all() {
  local fn="$1"
  "$fn" "for p in $RUN_DIR/*.pid; do [ -f \"\$p\" ] && kill \"\$(cat \"\$p\")\" 2>/dev/null || true; done; sleep 1; for p in $RUN_DIR/*.pid; do [ -f \"\$p\" ] && kill -9 \"\$(cat \"\$p\")\" 2>/dev/null || true; done; rm -rf $RUN_DIR" 2>/dev/null || true
}

register() { # <ssh-fn> <addr> <name> <class>
  sa "$ADMIN register-node --address $2 --label node=$3 --label device_class=$4"
}

cmd_setup() {
  local mode="${1:?mode: durable|none|repl|ec}"
  for fn in $HOSTS_FNS; do stop_all "$fn"; done
  sa "mkdir -p $RUN_DIR/fdb/data $RUN_DIR/fdb/log $RUN_DIR/logs && echo 'arca:bench@$ADDR_A:$FDB_PORT' > $CLUSTER_FILE"
  start sa fdb "fdbserver -p $ADDR_A:$FDB_PORT -C $CLUSTER_FILE -d $RUN_DIR/fdb/data -L $RUN_DIR/fdb/log"
  sleep 2
  sa "fdbcli -C $CLUSTER_FILE --exec 'configure new single ssd' >/dev/null; for i in \$(seq 1 30); do fdbcli -C $CLUSTER_FILE --exec 'status minimal' | grep -q 'is available' && break; sleep 1; done; fdbcli -C $CLUSTER_FILE --exec 'status minimal'"
  sb "mkdir -p $RUN_DIR/logs && echo 'arca:bench@$ADDR_A:$FDB_PORT' > $CLUSTER_FILE"
  [ -n "$HOST_C" ] && sc "mkdir -p $RUN_DIR/logs && echo 'arca:bench@$ADDR_A:$FDB_PORT' > $CLUSTER_FILE"

  if [ -n "$HOST_C" ]; then
    # Three hosts: one durable node and one cache node per host.
    start sa d0 "$BIN storage-node --listen $ADDR_A:9100 --dir $RUN_DIR/d0 --metrics-listen 127.0.0.1:9910"
    start sb d1 "$BIN storage-node --listen $ADDR_B:9100 --dir $RUN_DIR/d1 --metrics-listen 127.0.0.1:9910"
    start sc d2 "$BIN storage-node --listen $ADDR_C:9100 --dir $RUN_DIR/d2 --metrics-listen 127.0.0.1:9910"
    if [ "$mode" != durable ]; then
      start sa c0 "$BIN storage-node --listen $ADDR_A:9110 --dir $RUN_DIR/c0 --metrics-listen 127.0.0.1:9920"
      start sb c1 "$BIN storage-node --listen $ADDR_B:9110 --dir $RUN_DIR/c1 --metrics-listen 127.0.0.1:9920"
      start sc c2 "$BIN storage-node --listen $ADDR_C:9110 --dir $RUN_DIR/c2 --metrics-listen 127.0.0.1:9920"
    fi
    sleep 2
    register sa "$ADDR_A:9100" a-d0 hdd
    register sa "$ADDR_B:9100" b-d1 hdd
    register sa "$ADDR_C:9100" c-d2 hdd
    if [ "$mode" != durable ]; then
      register sa "$ADDR_A:9110" a-c0 nvme
      register sa "$ADDR_B:9110" b-c1 nvme
      register sa "$ADDR_C:9110" c-c2 nvme
    fi
  else
    # Two hosts: durable nodes two on A, one on B; cache c0 on A, c2 on B,
    # and c3 on B for the coded mode.
    start sa d0 "$BIN storage-node --listen $ADDR_A:9100 --dir $RUN_DIR/d0 --metrics-listen 127.0.0.1:9910"
    start sa d1 "$BIN storage-node --listen $ADDR_A:9101 --dir $RUN_DIR/d1 --metrics-listen 127.0.0.1:9911"
    start sb d2 "$BIN storage-node --listen $ADDR_B:9100 --dir $RUN_DIR/d2 --metrics-listen 127.0.0.1:9910"
    if [ "$mode" != durable ]; then
      start sa c0 "$BIN storage-node --listen $ADDR_A:9110 --dir $RUN_DIR/c0 --metrics-listen 127.0.0.1:9920"
      start sb c2 "$BIN storage-node --listen $ADDR_B:9110 --dir $RUN_DIR/c2 --metrics-listen 127.0.0.1:9920"
      if [ "$mode" = ec ]; then
        start sb c3 "$BIN storage-node --listen $ADDR_B:9111 --dir $RUN_DIR/c3 --metrics-listen 127.0.0.1:9921"
      fi
    fi
    sleep 2
    register sa "$ADDR_A:9100" a-d0 hdd
    register sa "$ADDR_A:9101" a-d1 hdd
    register sa "$ADDR_B:9100" b-d2 hdd
    if [ "$mode" != durable ]; then
      register sa "$ADDR_A:9110" a-c0 nvme
      register sa "$ADDR_B:9110" b-c2 nvme
      [ "$mode" = ec ] && register sa "$ADDR_B:9111" b-c3 nvme
    fi
  fi
  sa "$ADMIN create-groups --tier default --device-class hdd --failure-domain node --count 1 --k 2 --m 1 --chunk-bytes $CHUNK_BYTES --small-threshold 0"
  local cache_args=""
  case "$mode" in
    durable) ;;
    none) sa "$ADMIN create-cache-groups --tier fast --device-class nvme --protection none" ;;
    repl) sa "$ADMIN create-cache-groups --tier fast --device-class nvme --protection replication --replicas 2 --failure-domain node --count 1" ;;
    ec) sa "$ADMIN create-cache-groups --tier fast --device-class nvme --protection ec --data-shards 2 --parity-shards 1 --failure-domain node --count 1" ;;
    *) echo "unknown mode $mode" >&2; exit 2 ;;
  esac
  if [ "$mode" != durable ]; then
    cache_args="--cache-tier fast --cache-capacity-bytes $CACHE_CAPACITY --cache-high-watermark 0.9 --cache-low-watermark 0.8 --cache-max-populations ${CACHE_MAX_POPULATIONS:-8}"
  fi
  # CONTROLLER_EXTRA_ARGS: e.g. continuous scrubbing for the Phase 3.2
  # "GET/PUT during scrub" measurement.
  start sa controller "$BIN controller --cluster-file $CLUSTER_FILE --id ctl-a --http 127.0.0.1:9901 --failure-domain node --evict-interval-secs 2 --gc-interval-secs 10 --gc-grace-secs 30 $cache_args ${CONTROLLER_EXTRA_ARGS:-}"
  start sg gateway "$BIN gateway --backend cluster --cluster-file $CLUSTER_FILE --allow-anonymous --listen $ADDR_G:9000 --metrics-listen 127.0.0.1:9900 --k 2 --m 1 --chunk-bytes $CHUNK_BYTES --small-threshold 0 --failure-domain node $cache_args ${GW_EXTRA_ARGS:-}"
  # GATEWAYS=2|3: more gateways on the other hosts (multi-gateway runs,
  # Phase 2.3 §7); the load generator on each host drives its own.
  if [ "${GATEWAYS:-1}" -ge 2 ]; then
    start sa gateway2 "$BIN gateway --backend cluster --cluster-file $CLUSTER_FILE --allow-anonymous --listen $ADDR_A:9000 --metrics-listen 127.0.0.1:9900 --k 2 --m 1 --chunk-bytes $CHUNK_BYTES --small-threshold 0 --failure-domain node $cache_args ${GW_EXTRA_ARGS:-}"
  fi
  if [ "${GATEWAYS:-1}" -ge 3 ]; then
    [ -n "$HOST_C" ] || { echo "GATEWAYS=3 needs HOST_C" >&2; exit 2; }
    start sb gateway3 "$BIN gateway --backend cluster --cluster-file $CLUSTER_FILE --allow-anonymous --listen $ADDR_B:9000 --metrics-listen 127.0.0.1:9900 --k 2 --m 1 --chunk-bytes $CHUNK_BYTES --small-threshold 0 --failure-domain node $cache_args ${GW_EXTRA_ARGS:-}"
  fi
  sleep 2
  sa "$ADMIN status" | sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g'
  echo "mode=$mode chunk_bytes=$CHUNK_BYTES cache_capacity=$CACHE_CAPACITY hosts=$( [ -n "$HOST_C" ] && echo 3 || echo 2 ) gateways=${GATEWAYS:-1}" > /tmp/arca-two-host.mode
}

# Per-second samplers on every host for the duration of one command
# (benchmarks/linux/sampler.sh: sar, pidstat, TCP counters, every metrics
# endpoint, FoundationDB status). Used when PROFILE=1.
with_profile() { # <dir> <command...>
  local dir="$1"; shift
  mkdir -p "$dir"
  local urls_a="http://127.0.0.1:9910/metrics http://127.0.0.1:9911/metrics http://127.0.0.1:9920/metrics"
  local urls_b="http://127.0.0.1:9900/metrics http://127.0.0.1:9910/metrics http://127.0.0.1:9920/metrics http://127.0.0.1:9921/metrics"
  local urls_c="http://127.0.0.1:9900/metrics http://127.0.0.1:9910/metrics http://127.0.0.1:9920/metrics"
  sa "FDB_CLUSTER_FILE=$CLUSTER_FILE bash $REMOTE_REPO/benchmarks/linux/sampler.sh start $RUN_DIR/profile $urls_a"
  sb "bash $REMOTE_REPO/benchmarks/linux/sampler.sh start $RUN_DIR/profile $urls_b"
  [ -n "$HOST_C" ] && sc "bash $REMOTE_REPO/benchmarks/linux/sampler.sh start $RUN_DIR/profile $urls_c"
  local rc=0
  "$@" || rc=$?
  for fn in $HOSTS_FNS; do
    "$fn" "bash $REMOTE_REPO/benchmarks/linux/sampler.sh stop $RUN_DIR/profile"
    local suffix="${fn#s}"
    mkdir -p "$dir/profile-$suffix"
    for f in sar.txt pidstat.txt snmp.txt metrics.txt fdb.txt; do
      "$fn" "cat $RUN_DIR/profile/$f 2>/dev/null" > "$dir/profile-$suffix/$f" || true
    done
    "$fn" "rm -rf $RUN_DIR/profile"
  done
  return $rc
}

# sar collectors on both hosts for the duration of one command.
with_sar() { # <dir> <command...>
  local dir="$1"; shift
  mkdir -p "$dir"
  for fn in $HOSTS_FNS; do
    "$fn" "mkdir -p $RUN_DIR/sar && (sar -u 1 > $RUN_DIR/sar/cpu.txt 2>&1 < /dev/null & echo \$! > $RUN_DIR/sar/cpu.pid) && (sar -n DEV 1 > $RUN_DIR/sar/net.txt 2>&1 < /dev/null & echo \$! > $RUN_DIR/sar/net.pid) && (sar -b 1 > $RUN_DIR/sar/disk.txt 2>&1 < /dev/null & echo \$! > $RUN_DIR/sar/disk.pid)"
  done
  local rc=0
  "$@" || rc=$?
  for fn in $HOSTS_FNS; do
    "$fn" "kill \$(cat $RUN_DIR/sar/cpu.pid) \$(cat $RUN_DIR/sar/net.pid) \$(cat $RUN_DIR/sar/disk.pid) 2>/dev/null; sleep 0.2; true"
    local suffix="${fn#s}"
    "$fn" "cat $RUN_DIR/sar/cpu.txt" > "$dir/sar-cpu-$suffix.txt"
    "$fn" "cat $RUN_DIR/sar/net.txt" > "$dir/sar-net-$suffix.txt"
    "$fn" "cat $RUN_DIR/sar/disk.txt" > "$dir/sar-disk-$suffix.txt"
  done
  return $rc
}

metrics() { sg "curl -s http://127.0.0.1:9900/metrics" 2>/dev/null || true; }
cache_usage() { sa "$ADMIN cache-usage --tier fast" 2>/dev/null || echo '{"usage_bytes":0}'; }
rss() { # memory of arca-lake processes per host, kB
  for fn in $HOSTS_FNS; do
    echo "${fn#s}: $("$fn" "ps -o rss=,comm=,args= -C arca-lake,fdbserver,s3load | awk '{printf \"%s=%d \", \$2, \$1}'")"
  done
}

wait_populations() { # <log>
  local last=-1 stable=0 i u
  : > "$1"
  for i in $(seq 1 180); do
    u="$(cache_usage | sed -E 's/.*"usage_bytes":([0-9]+).*/\1/')"
    echo "$(date +%s) $u" >> "$1"
    if [ "$u" = "$last" ]; then stable=$((stable + 1)); else stable=0; fi
    [ "$stable" -ge 3 ] && return 0
    last="$u"
    sleep 1
  done
}

# Multi-gateway fan-out: gateway i is driven by the load generator on its
# own host (1: the gateway host, 2: host A, 3: host B) with its own key
# prefix; the phases run concurrently and each writes <phase>-g<i>.jsonl.
gw_fn() { case "$1" in 1) echo sg ;; 2) echo sa ;; 3) echo sb ;; esac; }
gw_addr() { case "$1" in 1) echo "$ADDR_G" ;; 2) echo "$ADDR_A" ;; 3) echo "$ADDR_B" ;; esac; }
fanout() { # <dir> <phase> <s3load args without --endpoint/--prefix/--label>
  local dir="$1" phase="$2"; shift 2
  local n="${GATEWAYS:-1}" i
  for i in $(seq 1 "$n"); do
    ( "$(gw_fn "$i")" "$S3LOAD --endpoint http://$(gw_addr "$i"):9000 --bucket bench --prefix ${PREFIX:-obj}-g$i --label $phase-g$i $*" > "$dir/$phase-g$i.jsonl" 2>/dev/null || true ) &
  done
  wait
  cat "$dir/$phase"-g*.jsonl
}

cmd_run() {
  local label="${1:?label}"
  local objects="${OBJECTS:-32}" size="${SIZE:-$((16 << 20))}" conc="${CONC:-4}" passes="${PASSES:-3}" range="${RANGE_BYTES:-0}"
  local dir="$OUT/$label"
  mkdir -p "$dir"
  cp /tmp/arca-two-host.mode "$dir/mode.txt" 2>/dev/null || true
  echo "objects=$objects size=$size concurrency=$conc passes=$passes range_bytes=$range" > "$dir/params.txt"
  local ep="http://$ADDR_G:9000"
  # PROFILE=1: run the per-second samplers (sampler.sh) on every host during
  # the PUT and record s3load --progress (put-progress.jsonl);
  # PREFIX: key prefix for the PUT and GET phases (phases that must not
  # overwrite another phase's objects use different prefixes);
  # S3LOAD_EXTRA: extra s3load flags for the GET passes (e.g. --order random);
  # SKIP_PUT=1: reuse the objects of a previous run with the same key space;
  # PUT_OP=multipart-put [PUT_EXTRA="--part-size N"]: upload the objects as
  # multipart uploads instead of single PUTs (RFC-007 §5.3 regression check);
  # MIXED_PUT_OBJECTS=N: during every GET pass, PUT N objects under another
  # prefix concurrently (recorded as mixedput<p>.jsonl).
  local extra="${S3LOAD_EXTRA:-}"
  sg "$S3LOAD --endpoint $ep --bucket bench --op create-bucket" >/dev/null
  if [ "${GATEWAYS:-1}" -ge 2 ]; then
    metrics > "$dir/metrics-before-put.txt"
    [ "${SKIP_PUT:-0}" = 1 ] || with_sar "$dir/put" fanout "$dir" put --op put --objects $objects --size $size --concurrency $conc > "$dir/put.jsonl"
    metrics > "$dir/metrics-after-put.txt"
    for p in $(seq 1 "$passes" 2>/dev/null | awk -v n="$passes" '$1 <= n'); do
      metrics > "$dir/metrics-before-get$p.txt"
      with_sar "$dir/get$p" fanout "$dir" get$p --op get --objects $objects --size $size --concurrency $conc --range-bytes $range --verify $extra > "$dir/get$p.jsonl"
      wait_populations "$dir/population-after-get$p.txt"
      metrics > "$dir/metrics-after-get$p.txt"
    done
    rss > "$dir/rss.txt"
    echo "results in $dir"
    return
  fi
  metrics > "$dir/metrics-before-put.txt"
  if [ "${SKIP_PUT:-0}" != 1 ]; then
    if [ "${PROFILE:-0}" = 1 ]; then
      # Samplers on every host plus s3load's per-second progress on stderr.
      with_profile "$dir/put" sg "$S3LOAD --endpoint $ep --bucket bench --op ${PUT_OP:-put} ${PUT_EXTRA:-} ${PREFIX:+--prefix $PREFIX} --objects $objects --size $size --concurrency $conc --progress --label $label-put" > "$dir/put.jsonl" 2> "$dir/put-progress.jsonl"
    else
      with_sar "$dir/put" sg "$S3LOAD --endpoint $ep --bucket bench --op ${PUT_OP:-put} ${PUT_EXTRA:-} ${PREFIX:+--prefix $PREFIX} --objects $objects --size $size --concurrency $conc --label $label-put" > "$dir/put.jsonl"
    fi
    cat "$dir/put.jsonl"
    sleep 2
  fi
  metrics > "$dir/metrics-after-put.txt"
  cache_usage > "$dir/cache-usage-after-put.json"
  # (not `seq 1 $passes`: BSD seq counts down from 1 to 0)
  for p in $(seq 1 "$passes" 2>/dev/null | awk -v n="$passes" '$1 <= n'); do
    metrics > "$dir/metrics-before-get$p.txt"
    if [ -n "${MIXED_PUT_OBJECTS:-}" ]; then
      (sg "$S3LOAD --endpoint $ep --bucket bench --op put --prefix mixed$p --objects $MIXED_PUT_OBJECTS --size $size --concurrency $conc --label $label-mixedput$p" > "$dir/mixedput$p.jsonl" 2>/dev/null || true) &
    fi
    with_sar "$dir/get$p" sg "$S3LOAD --endpoint $ep --bucket bench --op get ${PREFIX:+--prefix $PREFIX} --objects $objects --size $size --concurrency $conc --range-bytes $range --verify --label $label-get$p $extra" > "$dir/get$p.jsonl"
    wait
    cat "$dir/get$p.jsonl"
    [ -f "$dir/mixedput$p.jsonl" ] && cat "$dir/mixedput$p.jsonl"
    # Let asynchronous populations finish before the next pass: poll the
    # tier usage until it has not changed for three consecutive seconds
    # (or 180 s), logging the series so that population throughput can be
    # derived from it.
    wait_populations "$dir/population-after-get$p.txt"
    metrics > "$dir/metrics-after-get$p.txt"
    cache_usage > "$dir/cache-usage-after-get$p.json"
  done
  rss > "$dir/rss.txt"
  echo "results in $dir"
}

cmd_teardown() { for fn in $HOSTS_FNS; do stop_all "$fn"; done; echo "stopped and removed $RUN_DIR on every host"; }
cmd_status() { sa "$ADMIN status" | sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g'; metrics; }

case "${1:-}" in
  setup) shift; cmd_setup "$@" ;;
  run) shift; cmd_run "$@" ;;
  teardown) cmd_teardown ;;
  status) cmd_status ;;
  *) echo "usage: $0 setup <mode> | run <label> | teardown | status" >&2; exit 2 ;;
esac
