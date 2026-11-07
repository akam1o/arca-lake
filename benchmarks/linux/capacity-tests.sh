#!/usr/bin/env bash
# Phase 3.6 capacity and backpressure tests on one host, on the soak
# cluster layout (soak24.sh: 4 durable + 2 cache nodes, 2 gateways, 2
# controllers, 3 FoundationDB processes, TLS everywhere). Each test sets up
# its own cluster, runs, checks and tears it down; a sampler records the
# leader controller's admission and GC gauges, the gateways' admission
# counters and the nodes' capacity every 10 s (OUT/<test>/samples.jsonl).
#
#   A  GC saturation: 256 KiB overwrites stepped up past what GC reclaims,
#      then lowered; every acknowledged key is read back.
#   B  Multipart saturation: two-part uploads of 1 MiB parts (create,
#      upload, complete; abort on failure), stepped up, then lowered;
#      every acknowledged object is read back.
#   C  Capacity exhaustion: each durable node on its own small file system
#      (CAP_DIR/<node>, prepared by the caller: a mount point per node, or
#      plain directories with NODE_CAP_BYTES as the limit). Writes until
#      refused, reads continue, the reserve holds; the data is deleted and
#      writes come back on their own.
#   D  Failure injection: the soak workload (acknowledged-write ledger) with
#      soak24.sh's fault schedule every FAULT_GAP_SECS on capacity-capped
#      nodes, then soak24.sh's final checks.
#
#   RUN=~/arca-p36 OUT=~/arca-p36-out NODE_DATA_DIR=/data/p36 capacity-tests.sh A B C D
#
# Environment: RUN (cluster state; removed between tests), OUT, NODE_DATA_DIR (the nodes'
# directories for A, B and D), NODE_CAP_BYTES (per durable node for A, B, D; 4 GiB), CAP_DIR
# (C), FDB_DIR (passed to soak24.sh), STEP_SECS (300), D_SECS (2400), FAULT_GAP_SECS (120).
set -uo pipefail
REPO="${REPO:-$(cd "$(dirname "$0")/../.." && pwd)}"
RUN="${RUN:?}"; OUT="${OUT:?}"
SOAK="$REPO/benchmarks/linux/soak24.sh"
GCLOAD="$REPO/target/release/gcload"
STEP_SECS="${STEP_SECS:-300}"
CAP="${NODE_CAP_BYTES:-$((4 << 30))}"
AK="soak-ak"; SK="soak-secret-key-not-a-real-credential"
GW="http://127.0.0.1:9500"
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "$(ts) $*" | tee -a "$OUT/progress.log"; }

# GC every 10 s with a 60 s grace, admission dwell 60 s: dynamics within
# minutes instead of hours.
export GC_INTERVAL_SECS=10 GC_GRACE_SECS=60
export CONTROLLER_EXTRA_ARGS="--admission-dwell-secs 60 --admission-max-chunk-bytes $((1 << 20))"

metric() { awk -v n="$1" '$1==n {print $2; exit}'; }
leader_metrics() {
  for p in 9550 9551; do
    m="$(curl -s -m 5 "http://127.0.0.1:$p/metrics")"
    if echo "$m" | grep -q '^arca_controller_leader 1'; then echo "$m"; return; fi
  done
}
sampler() { # dir
  while [ ! -f "$1/stop" ]; do
    local m line t; m="$(leader_metrics)"; t="$(date +%s)"
    line="{\"t\":$t"
    for n in arca_controller_backpressure_state arca_controller_gc_backlog arca_controller_gc_backlog_bytes \
      arca_controller_gc_backlog_physical_bytes arca_controller_gc_backlog_oldest_age_seconds \
      arca_controller_gc_collected_total arca_controller_gc_reclaimed_bytes_total \
      arca_controller_gc_reclaim_bytes_per_second arca_controller_admission_excluded_groups; do
      line+=",\"${n#arca_controller_}\":$(echo "$m" | metric "$n" | sed 's/^$/null/')"
    done
    for k in headroom usable reserved reclaimable inflight; do
      line+=",\"cap_$k\":$(echo "$m" | metric "arca_controller_capacity_bytes{kind=\"$k\"}" | sed 's/^$/null/')"
    done
    for p in 9510 9511; do
      local g; g="$(curl -s -m 5 "http://127.0.0.1:$p/metrics")"
      line+=",\"gw$p\":{"
      local first=1
      for n in arca_gateway_admission_state arca_gateway_admission_throttled_total arca_gateway_admission_rejected_total \
        arca_gateway_node_capacity_refusals_total arca_gateway_placement_capacity_failures_total arca_gateway_put_in_flight; do
        [ $first -eq 1 ] || line+=","; first=0
        line+="\"${n#arca_gateway_}\":$(echo "$g" | metric "$n" | sed 's/^$/null/')"
      done
      line+="}"
    done
    for p in 9700 9701 9702 9703; do
      local nm; nm="$(curl -s -m 5 "http://127.0.0.1:$p/metrics")"
      line+=",\"n$p\":{"
      local first=1
      for k in total available reserved inflight allocated live reclaimable; do
        [ $first -eq 1 ] || line+=","; first=0
        line+="\"$k\":$(echo "$nm" | metric "arca_node_capacity_bytes{kind=\"$k\"}" | sed 's/^$/null/')"
      done
      line+=",\"refused_client\":$(echo "$nm" | metric 'arca_node_admission_refused_total{class="client"}' | sed 's/^$/null/')"
      line+=",\"anon\":$(echo "$nm" | metric 'arca_node_process_memory_bytes{kind="anon"}' | sed 's/^$/null/')}"
    done
    echo "$line}" >> "$1/samples.jsonl"
    sleep 10
  done
}

gcload() { "$GCLOAD" --endpoint "$GW" --access-key "$AK" --secret-key "$SK" "$@"; }

# A fresh cluster: no state of the previous test (its nodes' directories
# emptied, mount points kept).
clean() {
  rm -rf "$RUN"
  [ -n "${FDB_DIR:-}" ] && rm -rf "$FDB_DIR"
  local base="${NODE_DATA_DIR:-}"
  if [ -n "$base" ]; then
    for n in d0 d1 d2 d3 c0 c1; do
      [ -d "$base/$n" ] && find "$base/$n" -mindepth 1 -delete
    done
  fi
}
begin() { # test dir
  clean
  bash "$SOAK" setup > "$1/setup.log" 2>&1 || { say "setup failed"; return 1; }
  rm -f "$1/stop"; sampler "$1" & SAMPLER=$!
}
end() { # test dir
  touch "$1/stop"; wait "$SAMPLER" 2>/dev/null
  cp "$RUN"/logs/ctl-*.log "$1/" 2>/dev/null
  grep -h 'write admission state changed' "$RUN"/logs/ctl-*.log > "$1/admission-transitions.log" 2>/dev/null
  bash "$SOAK" teardown > "$1/teardown.log" 2>&1
}
# Wait until the leader's backlog is drained (or `max` seconds). The
# gauge is refreshed by GC passes: what was retired just now is in it only
# after the grace and a pass.
drain_wait() { # dir max
  local t0; t0="$(date +%s)"
  sleep $((GC_GRACE_SECS + 3 * GC_INTERVAL_SECS))
  while :; do
    local b; b="$(leader_metrics | metric arca_controller_gc_backlog)"
    [ "${b:-1}" = 0 ] && break
    [ $(( $(date +%s) - t0 )) -ge "$2" ] && break
    sleep 10
  done
  echo "{\"drain_s\":$(( $(date +%s) - t0 )),\"backlog\":${b:-null}}" >> "$1/backlog-drain.jsonl"
}

test_A() {
  local d="$OUT/A"; mkdir -p "$d"
  NODE_EXTRA_ARGS="--capacity-bytes $CAP" begin "$d" || return
  say "A: GC saturation"
  gcload --bucket gca --mode overwrite --size $((256 << 10)) --keys 4000 --workers 64 \
    --rates 100,200,400,800,1600 --step-secs "$STEP_SECS" --get-pct 10 > "$d/load-up.jsonl" 2> "$d/load-up.err"
  gcload --bucket gca --mode overwrite --size $((256 << 10)) --keys 4000 --workers 64 \
    --rates 50 --step-secs $((2 * STEP_SECS)) --get-pct 10 --verify > "$d/load-down.jsonl" 2> "$d/load-down.err"
  say "A: verify exit $?"
  drain_wait "$d" 1200
  end "$d"
}

test_B() {
  local d="$OUT/B"; mkdir -p "$d"
  NODE_EXTRA_ARGS="--capacity-bytes $CAP" begin "$d" || return
  say "B: multipart saturation"
  gcload --bucket gcb --mode multipart --size $((1 << 20)) --keys 1000 --workers 64 \
    --rates 10,20,40,80,160 --step-secs "$STEP_SECS" --get-pct 10 > "$d/load-up.jsonl" 2> "$d/load-up.err"
  gcload --bucket gcb --mode multipart --size $((1 << 20)) --keys 1000 --workers 32 \
    --rates 10 --step-secs $((2 * STEP_SECS)) --get-pct 10 --verify > "$d/load-down.jsonl" 2> "$d/load-down.err"
  say "B: verify exit $?"
  drain_wait "$d" 1200
  end "$d"
}

test_C() {
  local d="$OUT/C"; mkdir -p "$d"
  [ -n "${CAP_DIR:-}" ] || { say "C: CAP_DIR not set"; return; }
  # The cache nodes stay on NODE_DATA_DIR (not under test).
  for n in c0 c1; do mkdir -p "$CAP_DIR/$n"; done
  NODE_DATA_DIR="$CAP_DIR" NODE_EXTRA_ARGS="${C_NODE_ARGS:-}" begin "$d" || return
  df -B1 "$CAP_DIR"/d* > "$d/df-start.txt"
  say "C: fill"
  # Fill well past the capacity; GETs go on throughout.
  gcload --bucket gcc --prefix fill/ --mode put-only --size $((1 << 20)) --workers 32 \
    --rates 40,40,40,40 --step-secs 120 --get-pct 20 --verify > "$d/fill.jsonl" 2> "$d/fill.err"
  # (The read-back of every acknowledged object ran while the cluster
  # was full.)
  say "C: fill verify exit $?"
  df -B1 "$CAP_DIR"/d* > "$d/df-full.txt"
  say "C: drain"
  gcload --bucket gcc --prefix fill/ --mode drain > "$d/drain.jsonl" 2> "$d/drain.err"
  drain_wait "$d" 900
  # Admission lifts one step per dwell; writes come back on their own.
  say "C: recovery"
  gcload --bucket gcc --prefix again/ --mode put-only --size $((1 << 20)) --workers 16 \
    --rates 10,10,10,10,10 --step-secs 120 --verify > "$d/recovery.jsonl" 2> "$d/recovery.err"
  say "C: recovery verify exit $?"
  df -B1 "$CAP_DIR"/d* > "$d/df-end.txt"
  NODE_DATA_DIR="$CAP_DIR" end "$d"
}

test_D() {
  local d="$OUT/D"; mkdir -p "$d"
  clean
  NODE_EXTRA_ARGS="--capacity-bytes $CAP" bash "$SOAK" setup > "$d/setup.log" 2>&1 || { say "D: setup failed"; return; }
  rm -f "$d/stop"; sampler "$d" & SAMPLER=$!
  say "D: soak workload with faults"
  DURATION_SECS="${D_SECS:-2400}" FAULT_GAP_SECS="${FAULT_GAP_SECS:-120}" FAULT_START_SECS=120 \
    NODE_EXTRA_ARGS="--capacity-bytes $CAP" bash "$SOAK" start > "$d/start.log" 2>&1
  sleep "${D_SECS:-2400}"
  NODE_EXTRA_ARGS="--capacity-bytes $CAP" bash "$SOAK" finish > "$d/finish.log" 2>&1
  cp "$RUN/summary.json" "$RUN/faults.log" "$RUN"/final-*.txt "$d/" 2>/dev/null
  grep -h '"violation":' "$RUN/logs/soakload.log" > "$d/violations.log" 2>/dev/null
  grep -h '"t_s"' "$RUN/logs/soakload.log" | tail -n 3 > "$d/soakload-last.jsonl" 2>/dev/null
  NODE_EXTRA_ARGS="--capacity-bytes $CAP" end "$d"
}

mkdir -p "$OUT"
for t in "$@"; do
  say "test $t start"
  "test_$t"
  say "test $t done"
done
say finished
