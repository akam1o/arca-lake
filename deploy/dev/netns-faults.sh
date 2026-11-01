#!/usr/bin/env bash
# Namespace-pair fault injection for arca-lake on one Linux host
# (docs/verification/network-faults.md). Runs ON the test host.
#
# Creates two network namespaces joined by a veth pair, runs a private
# FoundationDB server, the controller and three storage nodes plus cache
# nodes split across the namespaces, and a gateway in the second namespace.
# Every impairment (netem, tbf, link down, socket destruction) is applied to
# the veth ends *inside* the test namespaces; no interface, qdisc, firewall
# rule or route of the host itself is touched, so the host's SSH path is
# never affected. `teardown` deletes the namespaces (the veth pair goes with
# them) and the run directory. `setup` refuses to run if the namespaces
# already exist.
#
#   bash netns-faults.sh setup [none|repl|ec]      # default ec
#   bash netns-faults.sh scenario <name>           # see cmd_scenario
#   bash netns-faults.sh all                       # every scenario in order
#   bash netns-faults.sh teardown
#
# Requires: passwordless sudo for `ip netns`, `ip link`, `tc`, `ss -K`;
# ~/arca-lake built with --release --features fdb (arca-lake, s3load);
# fdbserver and fdbcli on the PATH. Results: $RUN/results/<scenario>/.
set -euo pipefail

NS1=arca1 NS2=arca2 V1=arca-v1 V2=arca-v2
IP1=10.77.0.1 IP2=10.77.0.2
REPO="${REPO:-$HOME/arca-lake}"
RUN="${RUN:-$HOME/arca-ns}"
BIN="$REPO/target/release/arca-lake"
S3LOAD="$REPO/target/release/s3load"
CF="$RUN/fdb.cluster"
ADMIN="$BIN admin --cluster-file $CF"
CHUNK_BYTES="${CHUNK_BYTES:-$((4 << 20))}"
EP="http://$IP2:9000"
ME="$(id -un)"

# Run a command as this user inside a namespace.
in1() { sudo -n ip netns exec "$NS1" sudo -n -u "$ME" env HOME="$HOME" PATH="$PATH" "$@"; }
in2() { sudo -n ip netns exec "$NS2" sudo -n -u "$ME" env HOME="$HOME" PATH="$PATH" "$@"; }
root1() { sudo -n ip netns exec "$NS1" "$@"; }
root2() { sudo -n ip netns exec "$NS2" "$@"; }

# start <1|2> <name> <command...>
start() {
  local ns="$1" name="$2"; shift 2
  "in$ns" bash -c "mkdir -p $RUN/logs && cd $RUN && { nohup $* >> $RUN/logs/$name.log 2>&1 < /dev/null & echo \$! > $RUN/$name.pid; }"
}
kill_name() { # <name> [signal]
  local pid; pid="$(cat "$RUN/$1.pid" 2>/dev/null || true)"
  [ -n "$pid" ] && kill "-${2:-TERM}" "$pid" 2>/dev/null || true
}
wait_gone() { # <name>
  local pid; pid="$(cat "$RUN/$1.pid" 2>/dev/null || true)"
  [ -z "$pid" ] && return 0
  for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.1; done
  kill -9 "$pid" 2>/dev/null || true
}

node_cmd() { # <ns-ip> <port> <dir>
  echo "$BIN storage-node --listen $1:$2 --dir $RUN/$3"
}
start_d2() { start 2 d2 "$(node_cmd $IP2 9100 d2)"; }
start_c2() { start 2 c2 "$(node_cmd $IP2 9110 c2)"; }

cmd_setup() {
  local mode="${1:-ec}"
  if ip netns list 2>/dev/null | grep -qE "^($NS1|$NS2)( |$)"; then
    echo "namespaces $NS1/$NS2 already exist; run teardown first" >&2; exit 1
  fi
  [ -x "$BIN" ] && [ -x "$S3LOAD" ] || { echo "build $BIN and $S3LOAD first" >&2; exit 1; }
  command -v fdbserver >/dev/null || { echo "fdbserver not on PATH" >&2; exit 1; }
  rm -rf "$RUN"; mkdir -p "$RUN/logs" "$RUN/fdb/data" "$RUN/fdb/log" "$RUN/results"
  sudo -n ip netns add "$NS1"; sudo -n ip netns add "$NS2"
  sudo -n ip link add "$V1" type veth peer name "$V2"
  sudo -n ip link set "$V1" netns "$NS1"; sudo -n ip link set "$V2" netns "$NS2"
  root1 ip addr add "$IP1/24" dev "$V1"; root1 ip link set "$V1" up; root1 ip link set lo up
  root2 ip addr add "$IP2/24" dev "$V2"; root2 ip link set "$V2" up; root2 ip link set lo up
  root2 ping -c 1 -W 1 "$IP1" >/dev/null

  echo "arca:ns@$IP1:4510" > "$CF"
  start 1 fdb "fdbserver -p $IP1:4510 -C $CF -d $RUN/fdb/data -L $RUN/fdb/log"
  sleep 1.5
  in1 fdbcli -C "$CF" --exec 'configure new single memory' >/dev/null
  for _ in $(seq 1 30); do in1 fdbcli -C "$CF" --exec 'status minimal' | grep -q 'is available' && break; sleep 1; done
  in1 fdbcli -C "$CF" --exec 'status minimal'

  start 1 d0 "$(node_cmd $IP1 9100 d0)"
  start 1 d1 "$(node_cmd $IP1 9101 d1)"
  start_d2
  local cache_args=""
  if [ "$mode" != durable ]; then
    start 1 c0 "$(node_cmd $IP1 9110 c0)"
    start_c2
    [ "$mode" = ec ] && start 2 c3 "$(node_cmd $IP2 9111 c3)"
    cache_args="--cache-tier fast"
  fi
  sleep 1.5
  reg() { in1 $ADMIN register-node --address "$1" --label "node=$2" --label "device_class=$3" >/dev/null; }
  reg "$IP1:9100" d0 hdd; reg "$IP1:9101" d1 hdd; reg "$IP2:9100" d2 hdd
  if [ "$mode" != durable ]; then
    reg "$IP1:9110" c0 nvme; reg "$IP2:9110" c2 nvme
    [ "$mode" = ec ] && reg "$IP2:9111" c3 nvme
  fi
  in1 $ADMIN create-groups --tier default --device-class hdd --failure-domain node --count 1 --k 2 --m 1 --chunk-bytes "$CHUNK_BYTES" --small-threshold 0 >/dev/null
  case "$mode" in
    durable) ;;
    none) in1 $ADMIN create-cache-groups --tier fast --device-class nvme --protection none >/dev/null ;;
    repl) in1 $ADMIN create-cache-groups --tier fast --device-class nvme --protection replication --replicas 2 --failure-domain node >/dev/null ;;
    ec) in1 $ADMIN create-cache-groups --tier fast --device-class nvme --protection ec --data-shards 2 --parity-shards 1 --failure-domain node >/dev/null ;;
    *) echo "unknown mode $mode" >&2; exit 2 ;;
  esac
  start_controller "$cache_args"
  start 2 gateway "$BIN gateway --backend cluster --cluster-file $CF --allow-anonymous --listen $IP2:9000 --metrics-listen 127.0.0.1:9900 --k 2 --m 1 --chunk-bytes $CHUNK_BYTES --small-threshold 0 --failure-domain node --lease-secs 20 $cache_args"
  echo "$mode" > "$RUN/mode"
  sleep 1.5
  in2 $S3LOAD --endpoint "$EP" --bucket faults --op create-bucket
  in1 $ADMIN status | sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:/<addr>:/g'
}

start_controller() {
  start 1 controller "$BIN controller --cluster-file $CF --id ctl --http 127.0.0.1:9901 --failure-domain node --lease-secs 4 --auto-repair --unhealthy-after-secs 6 --health-interval-secs 2 --repair-interval-secs 2 --gc-interval-secs 3 --gc-grace-secs 3 --min-orphan-age-secs 40 --reconcile-interval-secs 10 --evict-interval-secs 5 --cache-repair-chunks-per-pass 64 $*"
}

# ---- impairments (veth ends inside the namespaces only) ----
impair() { # <netem spec>, both directions
  root1 tc qdisc replace dev "$V1" root netem $*
  root2 tc qdisc replace dev "$V2" root netem $*
}
bandwidth() { # <rate>
  root1 tc qdisc replace dev "$V1" root tbf rate "$1" burst 64kb latency 400ms
  root2 tc qdisc replace dev "$V2" root tbf rate "$1" burst 64kb latency 400ms
}
clear_impairment() {
  root1 tc qdisc del dev "$V1" root 2>/dev/null || true
  root2 tc qdisc del dev "$V2" root 2>/dev/null || true
}
partition() { # on|off
  if [ "$1" = on ]; then root2 ip link set "$V2" down; else root2 ip link set "$V2" up; fi
}
# Destroy every established socket from namespace 2 to durable node d0;
# records the matching sockets before and after in $1.
reset_to_d0() {
  local log="$1"
  { echo "before:"; root2 ss -tn state established "dst $IP1 dport = :9100" 2>&1; } >> "$log"
  root2 ss -K -tn state established "dst $IP1 dport = :9100" >> "$log" 2>&1 || echo "ss -K failed: $?" >> "$log"
  { echo "after:"; root2 ss -tn state established "dst $IP1 dport = :9100" 2>&1; } >> "$log"
}

# ---- measurement helpers ----
gw_metrics() { in2 curl -s http://127.0.0.1:9900/metrics || true; }
ctl_metrics() { in1 curl -s http://127.0.0.1:9901/metrics || true; }
load() { # <out-file> <s3load args...>
  local out="$1"; shift
  in2 $S3LOAD --endpoint "$EP" --bucket faults "$@" > "$out" 2> "${out%.jsonl}.err" || true
  cat "$out"
}
verify_all() { # <out> <prefix> <objects> <size>
  load "$1" --op get --prefix "$2" --objects "$3" --size "$4" --concurrency 4 --verify --allow-missing --label "verify-$2"
}
snapshot() { # <dir> <tag>
  gw_metrics > "$1/gw-$2.txt"; ctl_metrics > "$1/ctl-$2.txt"
  in1 $ADMIN status 2>/dev/null | sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:/<addr>:/g' > "$1/status-$2.txt" || true
}

cmd_scenario() {
  local name="${1:?scenario}"
  local dir="$RUN/results/$name"; mkdir -p "$dir"
  local size=$((8 << 20)) n=12
  snapshot "$dir" before
  case "$name" in
    baseline)
      load "$dir/put.jsonl" --op put --prefix base --objects $n --size $size --concurrency 4 --label base-put
      load "$dir/get.jsonl" --op get --prefix base --objects $n --size $size --concurrency 4 --passes 2 --verify --label base-get
      ;;
    loss2|loss10)
      local pct="${name#loss}"
      impair loss "${pct}%"
      load "$dir/put.jsonl" --op put --prefix "$name" --objects $n --size $size --concurrency 4 --label "$name-put"
      load "$dir/get.jsonl" --op get --prefix "$name" --objects $n --size $size --concurrency 4 --passes 2 --verify --label "$name-get"
      clear_impairment
      verify_all "$dir/verify.jsonl" "$name" $n $size
      ;;
    delay-nojitter)
      impair delay 50ms
      load "$dir/put.jsonl" --op put --prefix dnj --objects $n --size $size --concurrency 4 --label dnj-put
      load "$dir/get.jsonl" --op get --prefix dnj --objects $n --size $size --concurrency 4 --passes 2 --verify --label dnj-get
      clear_impairment
      verify_all "$dir/verify.jsonl" dnj $n $size
      ;;
    delay)
      impair delay 50ms 10ms
      load "$dir/put.jsonl" --op put --prefix delay --objects $n --size $size --concurrency 4 --label delay-put
      load "$dir/get.jsonl" --op get --prefix delay --objects $n --size $size --concurrency 4 --passes 2 --verify --label delay-get
      clear_impairment
      ;;
    bandwidth)
      bandwidth 100mbit
      load "$dir/put.jsonl" --op put --prefix bw --objects 4 --size $size --concurrency 2 --label bw-put
      load "$dir/get.jsonl" --op get --prefix bw --objects 4 --size $size --concurrency 2 --passes 2 --verify --label bw-get
      clear_impairment
      ;;
    reset)
      load "$dir/put.jsonl" --op put --prefix rst --objects 96 --size $size --concurrency 4 --label rst-put
      (in2 $S3LOAD --endpoint "$EP" --bucket faults --op get --prefix rst --objects 96 --size $size --concurrency 4 --passes 2 --label rst-get > "$dir/get.jsonl" 2> "$dir/get.err" || true) &
      sleep 1.0; reset_to_d0 "$dir/sockets.txt"; sleep 1.0; reset_to_d0 "$dir/sockets.txt"
      wait
      cat "$dir/get.jsonl"
      verify_all "$dir/verify.jsonl" rst 96 $size
      ;;
    partition)
      (in2 $S3LOAD --endpoint "$EP" --bucket faults --op put --prefix part --objects 160 --size $size --concurrency 2 --label part-put > "$dir/put.jsonl" 2> "$dir/put.err" || true) &
      sleep 1.0; partition on; sleep 10; partition off
      wait
      cat "$dir/put.jsonl"
      load "$dir/get-during.jsonl" --op get --prefix base --objects 4 --size $size --concurrency 2 --verify --allow-missing --label part-get-after
      verify_all "$dir/verify.jsonl" part 160 $size
      # pending generations of failed PUTs expire with the lease (20 s) and are collected.
      sleep 28; snapshot "$dir" after-lease
      ;;
    node-restart)
      load "$dir/put.jsonl" --op put --prefix nr --objects $n --size $size --concurrency 4 --label nr-put
      load "$dir/warm.jsonl" --op get --prefix nr --objects $n --size $size --concurrency 4 --verify --label nr-warm
      (in2 $S3LOAD --endpoint "$EP" --bucket faults --op get --prefix nr --objects $n --size $size --concurrency 4 --passes 40 --verify --label nr-get > "$dir/get.jsonl" 2> "$dir/get.err" || true) &
      sleep 1; kill_name d2 KILL; wait_gone d2
      { echo "listeners in ns2 after killing d2:"; root2 ss -ltn; } > "$dir/listeners-while-down.txt" 2>&1
      load "$dir/put-while-down.jsonl" --op put --prefix nrdown --objects 2 --size $size --concurrency 2 --label nr-put-down
      sleep 1; start_d2; sleep 1
      kill_name c2 KILL; wait_gone c2; sleep 1.5; start_c2
      wait
      cat "$dir/get.jsonl"
      load "$dir/put-after.jsonl" --op put --prefix nrup --objects 2 --size $size --concurrency 2 --label nr-put-up
      verify_all "$dir/verify.jsonl" nr $n $size
      verify_all "$dir/verify-down.jsonl" nrdown 2 $size
      ;;
    controller-restart)
      (in2 $S3LOAD --endpoint "$EP" --bucket faults --op put --prefix cr --objects 96 --size $size --concurrency 4 --label cr-put > "$dir/put.jsonl" 2> "$dir/put.err" || true) &
      sleep 1; kill_name controller KILL; wait_gone controller; sleep 2
      start_controller "$( [ "$(cat "$RUN/mode")" != durable ] && echo --cache-tier fast )"
      wait
      cat "$dir/put.jsonl"
      load "$dir/get.jsonl" --op get --prefix cr --objects 96 --size $size --concurrency 4 --passes 2 --verify --label cr-get
      sleep 8
      ;;
    inflight)
      (in2 $S3LOAD --endpoint "$EP" --bucket faults --op put --prefix inf --objects 48 --size $((16 << 20)) --concurrency 4 --label inf-put > "$dir/put.jsonl" 2> "$dir/put.err" || true) &
      sleep 1.0; partition on; sleep 3; partition off
      wait
      cat "$dir/put.jsonl"
      verify_all "$dir/verify.jsonl" inf 48 $((16 << 20))
      (in2 $S3LOAD --endpoint "$EP" --bucket faults --op get --prefix rst --objects 96 --size $size --concurrency 4 --passes 3 --verify --label inf-get > "$dir/get.jsonl" 2> "$dir/get.err" || true) &
      sleep 1.0; partition on; sleep 3; partition off
      wait
      cat "$dir/get.jsonl"
      verify_all "$dir/verify-get.jsonl" rst 96 $size
      ;;
    repair-under-loss)
      load "$dir/put.jsonl" --op put --prefix rep --objects $n --size $size --concurrency 4 --label rep-put
      impair loss 2%
      start 2 d3 "$(node_cmd $IP2 9102 d3)"; sleep 1
      in1 $ADMIN register-node --address "$IP2:9102" --label node=d3 --label device_class=hdd >/dev/null
      kill_name d2 KILL; wait_gone d2
      # auto repair: d2 unhealthy after 6 s, replaced by d3, reconstructed, verified.
      for _ in $(seq 1 90); do
        in1 $ADMIN status 2>/dev/null | grep -q 'Active' && ! in1 $ADMIN status 2>/dev/null | grep -q 'Repairing' && in1 $ADMIN status 2>/dev/null | grep -qE "members \[.*\]" && sleep 1
        ctl_metrics | grep -q '^arca_controller_repairs_completed_total [1-9]' && break
        sleep 1
      done
      clear_impairment
      snapshot "$dir" after-repair
      verify_all "$dir/verify.jsonl" rep $n $size
      verify_all "$dir/verify-base.jsonl" base $n $size
      ;;
    gc-under-delay)
      load "$dir/put.jsonl" --op put --prefix gc --objects $n --size $size --concurrency 4 --label gc-put
      load "$dir/get.jsonl" --op get --prefix gc --objects $n --size $size --concurrency 4 --verify --label gc-get
      impair delay 30ms 5ms
      # Overwrite everything: old generations retire, GC collects them (and their cache copies) under delay.
      load "$dir/put2.jsonl" --op put --prefix gc --objects $n --size $size --concurrency 4 --seed 2 --label gc-put2
      sleep 12
      clear_impairment
      snapshot "$dir" after-gc
      load "$dir/verify.jsonl" --op get --prefix gc --objects $n --size $size --concurrency 4 --verify --seed 2 --label gc-verify
      ;;
    *) echo "unknown scenario $name" >&2; exit 2 ;;
  esac
  snapshot "$dir" after
  echo "scenario $name done: $dir"
}

cmd_all() {
  for s in baseline loss2 loss10 delay delay-nojitter bandwidth reset partition node-restart controller-restart inflight gc-under-delay repair-under-loss; do
    cmd_scenario "$s"
  done
}

cmd_teardown() {
  clear_impairment 2>/dev/null || true
  for p in "$RUN"/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true; done
  sleep 1
  for p in "$RUN"/*.pid; do [ -f "$p" ] && kill -9 "$(cat "$p")" 2>/dev/null || true; done
  sudo -n ip netns del "$NS1" 2>/dev/null || true
  sudo -n ip netns del "$NS2" 2>/dev/null || true
  echo "namespaces removed; results kept in $RUN/results"
}

case "${1:-}" in
  setup) shift; cmd_setup "$@" ;;
  scenario) shift; cmd_scenario "$@" ;;
  all) cmd_all ;;
  teardown) cmd_teardown ;;
  *) echo "usage: $0 setup [mode] | scenario <name> | all | teardown" >&2; exit 2 ;;
esac
