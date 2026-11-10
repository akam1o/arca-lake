#!/usr/bin/env bash
# Capacity rebalancing on real FoundationDB under load (Phase 3.9, S11).
#
# Two hosts: FoundationDB (a private fdbserver, not the system service),
# controller ctl-a and four durable nodes on A; controller ctl-b, two
# durable nodes, the gateway and the load generator on B. Group 1 (2+1
# erasure coding, d0 d1 d2) is filled; group 2 (d3 d4 d5, same shape, same
# tier) is added, as in a capacity expansion. Every node advertises
# CAP_BYTES, so that the groups' rooms differ although nodes share disks.
#
# 1. baseline: WINDOW minutes of PUT (verified bodies), GET --verify and
#    DELETE with rebalancing paused (GC and reconciliation on);
# 2. rebalance: the same with rebalancing resumed; the controller leader is
#    killed at a third of the window (restarted 30 s later) and node d1 is
#    stopped for 20 s at two thirds;
# 3. converge: no load; wait until rebalancing stops and orphans are
#    reconciled; then every object that must exist is read back verified.
#
# The run fails on any body mismatch, on a missing object that was
# acknowledged, or on a verify error. Host addresses come only from the
# environment and are stripped from the output.
#
#   HOST_A=user@a HOST_B=user@b benchmarks/linux/rebalance-load.sh <out-dir>
# Environment: WINDOW (minutes, 12), CAP_BYTES (3 GiB), BASE (objects, 320),
#   FDB_PORT (4510), FDB_DATA (/mnt/arca-fdb/p39-rebalance), RUN_DIR,
#   REMOTE_REPO (the checkout whose release binaries run; the load
#   generator always comes from LOAD_REPO, default $HOME/arca-lake),
#   REBALANCE (on; off keeps rebalancing paused in the second window too:
#   the same load and faults, for comparison), SAMPLE_SECS (15: admission
#   state, GC backlog and per-node capacity into samples.tsv).
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?output directory}"
mkdir -p "$OUT"
: "${HOST_A:?}" "${HOST_B:?}"
WINDOW="${WINDOW:-12}"
CAP="${CAP_BYTES:-$((3 << 30))}"
BASE="${BASE:-320}"
SIZE=$((8 << 20))
FDB_PORT="${FDB_PORT:-4510}"
FDB_DATA="${FDB_DATA:-/mnt/arca-fdb/p39-rebalance}"
REMOTE_REPO="${REMOTE_REPO:-\$HOME/arca-lake}"
RUN_DIR="${RUN_DIR:-\$HOME/arca-p39-rebalance}"
sa() { ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 "$HOST_A" "$@"; }
sb() { ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 "$HOST_B" "$@"; }
addr_of() { ssh -o BatchMode=yes "$1" "hostname -I | awk '{print \$1}'"; }
ADDR_A="$(addr_of "$HOST_A")"
ADDR_B="$(addr_of "$HOST_B")"
strip() { sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g'; }
ARCA_ENV="${ARCA_ENV:-ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1}"
BIN="env $ARCA_ENV $REMOTE_REPO/target/release/arca-lake"
S3LOAD="${LOAD_REPO:-\$HOME/arca-lake}/target/release/s3load"
REBALANCE="${REBALANCE:-on}"
SAMPLE_SECS="${SAMPLE_SECS:-15}"
CF="$RUN_DIR/fdb.cluster"
ADMIN="$BIN admin --cluster-file $CF"
EP="http://$ADDR_B:9000"
log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$OUT/run.log"; }
start() { # <ssh-fn> <name> <command...>
  local fn="$1" name="$2"; shift 2
  "$fn" "mkdir -p $RUN_DIR/logs && cd $RUN_DIR && { nohup $* > $RUN_DIR/logs/$name.log 2>&1 < /dev/null & echo \$! > $RUN_DIR/$name.pid; }"
}
kill_proc() { "$1" "kill \$(cat $RUN_DIR/$2.pid) 2>/dev/null || true"; }
stop_all() { "$1" "for p in $RUN_DIR/*.pid; do [ -f \"\$p\" ] && kill \"\$(cat \"\$p\")\" 2>/dev/null || true; done; sleep 2; for p in $RUN_DIR/*.pid; do [ -f \"\$p\" ] && kill -9 \"\$(cat \"\$p\")\" 2>/dev/null || true; done; rm -f $RUN_DIR/*.pid"; }

CTL_ARGS="--cluster-file $CF --allow-shared-filesystem --failure-domain node --gc-interval-secs 10 --gc-grace-secs 30 --reconcile-interval-secs 30 --min-orphan-age-secs 60 --rebalance --rebalance-interval-secs 10 --rebalance-mib-per-pass 128 --rebalance-start-imbalance 0.15 --rebalance-stop-imbalance 0.05"
node_cmd() { # <addr:port> <dir> <metrics port>
  echo "$BIN storage-node --listen $1 --dir $RUN_DIR/$2 --metrics-listen 127.0.0.1:$3 --capacity-bytes $CAP"
}
node() { # <ssh-fn> <name> <addr> <port> <metrics port>
  start "$1" "$2" "$(node_cmd "$3:$4" "$2" "$5")"
}

# ---- setup (only this run's directories and processes)
stop_all sa; stop_all sb
sa "rm -rf $RUN_DIR $FDB_DATA; mkdir -p $RUN_DIR/logs $RUN_DIR/fdb-log $FDB_DATA && echo 'arca:p39@$ADDR_A:$FDB_PORT' > $CF"
sb "rm -rf $RUN_DIR; mkdir -p $RUN_DIR/logs && echo 'arca:p39@$ADDR_A:$FDB_PORT' > $CF"
start sa fdb "fdbserver -p $ADDR_A:$FDB_PORT -C $CF -d $FDB_DATA -L $RUN_DIR/fdb-log"
sleep 2
sa "fdbcli -C $CF --exec 'configure new single ssd' >/dev/null; for i in \$(seq 1 30); do fdbcli -C $CF --exec 'status minimal' | grep -q 'is available' && break; sleep 1; done; fdbcli -C $CF --exec 'status minimal'" | tee -a "$OUT/run.log"
node sa d0 "$ADDR_A" 9100 9910
node sa d1 "$ADDR_A" 9101 9911
node sb d2 "$ADDR_B" 9100 9910
sleep 2
sa "$ADMIN init" > /dev/null 2>&1 || true
sa "$ADMIN register-node --address $ADDR_A:9100 --label node=a-d0 --label device_class=hdd" > /dev/null
sa "$ADMIN register-node --address $ADDR_A:9101 --label node=a-d1 --label device_class=hdd" > /dev/null
sa "$ADMIN register-node --address $ADDR_B:9100 --label node=b-d2 --label device_class=hdd" > /dev/null
sa "$ADMIN create-groups --tier default --device-class hdd --failure-domain node --count 1 --k 2 --m 1 --chunk-bytes $((4 << 20)) --small-threshold 0" > /dev/null
start sa controller "$BIN controller --id ctl-a --http 127.0.0.1:9901 $CTL_ARGS"
start sb controller "$BIN controller --id ctl-b --http 127.0.0.1:9901 $CTL_ARGS"
start sb gateway "$BIN gateway --backend cluster --cluster-file $CF --allow-anonymous --listen $ADDR_B:9000 --metrics-listen 127.0.0.1:9900 --k 2 --m 1 --chunk-bytes $((4 << 20)) --small-threshold 0 --failure-domain node"
for i in $(seq 1 60); do sb "curl -s -o /dev/null $EP/" && break; sleep 1; done
sb "$S3LOAD --endpoint $EP --bucket rebal --op create-bucket" > /dev/null
sa "$ADMIN rebalance pause" > /dev/null

failures=0
s3() { # <label> <args...>: one s3load run on B, recorded
  local label="$1"; shift
  local line
  line=$(sb "$S3LOAD --endpoint $EP --bucket rebal --label $label --size $SIZE $*" 2>>"$OUT/s3load-stderr.log" || true)
  [ -n "$line" ] || line="{\"label\":\"$label\",\"errors\":-1}"
  echo "$line" >> "$OUT/s3load.jsonl"
  local m
  m=$(printf '%s' "$line" | python3 -I -c 'import json,sys
d=json.loads(sys.stdin.read())
print(int(d.get("mismatches",0) or 0))')
  [ "$m" = 0 ] || { log "BODY MISMATCH in $label"; failures=$((failures+1)); }
  printf '%s' "$line"
}
errors_of() { python3 -I -c 'import json,sys; print(int(json.loads(sys.stdin.read()).get("errors",0)))'; }

leader_host() { # prints a or b
  if sa "curl -s http://127.0.0.1:9901/metrics" | grep -q '^arca_controller_leader 1'; then echo a; else echo b; fi
}
snapshot() { # <label>
  {
    echo "== $1 $(date -u +%H:%M:%S)"
    sa "$ADMIN rebalance status" 2>&1
    sa "$ADMIN delete-fence status" 2>&1
    for p in 9910 9911 9912 9913; do sa "curl -s http://127.0.0.1:$p/metrics" | grep -E '^arca_node_(capacity_bytes|delete_fence)' | sed "s/^/a:$p /"; done
    for p in 9910 9911; do sb "curl -s http://127.0.0.1:$p/metrics" | grep -E '^arca_node_(capacity_bytes|delete_fence)' | sed "s/^/b:$p /"; done
    for h in sa sb; do $h "curl -s http://127.0.0.1:9901/metrics" | grep -E '^arca_controller_(leader |rebalance|gc_backlog|shards_deleted|delete_fence|groups_repairing)' | sed "s/^/ctl-${h#s} /"; done
  } 2>&1 | strip >> "$OUT/snapshots.txt"
}

# Every SAMPLE_SECS: gateway admission state, the leader's GC backlog and
# rebalance counters, and each node's capacity (total, available,
# reserved): write_rejected time, capacity spread, reserve violations.
sampler() {
  # A probe that finds nothing (a node not started yet) must not end it.
  set +e +o pipefail
  printf 'time\tsource\tmetric\tvalue\n' > "$OUT/samples.tsv"
  while [ ! -f "$OUT/.stop-sampler" ]; do
    local t; t=$(date -u +%s)
    {
      sb "curl -s http://127.0.0.1:9900/metrics" | grep -E '^arca_gateway_admission_state' | sed "s/^/gw /"
      for h in sa sb; do $h "curl -s http://127.0.0.1:9901/metrics" | grep -E '^arca_controller_(leader |gc_backlog_bytes|rebalance_moved_bytes_total|rebalance_capacity_stops_total)' | sed "s/^/ctl-${h#s} /"; done
      for p in 9910 9911 9912 9913; do sa "curl -s http://127.0.0.1:$p/metrics" | grep -E '^arca_node_capacity_bytes\{kind="(total|available|reserved)"' | sed "s/^/a:$p /"; done
      for p in 9910 9911; do sb "curl -s http://127.0.0.1:$p/metrics" | grep -E '^arca_node_capacity_bytes\{kind="(total|available|reserved)"' | sed "s/^/b:$p /"; done
    } 2>/dev/null | awk -v t="$t" '{print t "\t" $1 "\t" $2 "\t" $3}' >> "$OUT/samples.tsv"
    sleep "$SAMPLE_SECS"
  done
}
rm -f "$OUT/.stop-sampler"
sampler &
SAMPLER=$!

# ---- fill group 1
log "fill: $BASE objects of $SIZE bytes"
line=$(s3 fill --op put --prefix base --objects "$BASE" --concurrency 8)
[ "$(printf '%s' "$line" | errors_of)" = 0 ] || { log "fill had errors: $line"; failures=$((failures+1)); }
# ---- expand: group 2
node sa d3 "$ADDR_A" 9102 9912
node sa d4 "$ADDR_A" 9103 9913
node sb d5 "$ADDR_B" 9101 9911
sleep 2
sa "$ADMIN register-node --address $ADDR_A:9102 --label node=a-d3 --label device_class=hdd2" > /dev/null
sa "$ADMIN register-node --address $ADDR_A:9103 --label node=a-d4 --label device_class=hdd2" > /dev/null
sa "$ADMIN register-node --address $ADDR_B:9101 --label node=b-d5 --label device_class=hdd2" > /dev/null
sa "$ADMIN create-groups --tier default --device-class hdd2 --failure-domain node --count 1 --k 2 --m 1 --chunk-bytes $((4 << 20)) --small-threshold 0" > /dev/null
sleep 20
snapshot expanded

# Live set of the load: iteration i writes 8 objects under l<i>; the
# objects of iteration i-2 are deleted. A PUT run with errors marks its
# iteration "uncertain" (a failed PUT leaves no object).
state=()
iter=0
window() { # <label> <minutes> [faults]
  local label="$1" minutes="$2" faults="${3:-}"
  local t0 end third
  t0=$(date +%s); end=$((t0 + minutes * 60)); third=$((minutes * 20))
  local killed=0 stopped=0 restart=""
  while [ "$(date +%s)" -lt "$end" ]; do
    iter=$((iter + 1))
    line=$(s3 "$label-put" --op put --prefix "l$iter" --objects 8 --concurrency 4)
    if [ "$(printf '%s' "$line" | errors_of)" = 0 ]; then state[$iter]=ok; else state[$iter]=uncertain; fi
    s3 "$label-get" --op get --prefix base --objects 64 --first $(( (iter * 64) % (BASE - 64) )) --concurrency 4 --verify > /dev/null
    local d=$((iter - 2))
    if [ "$d" -ge 1 ]; then
      sb "for i in \$(seq 0 7); do curl -s -o /dev/null -w '%{http_code}\n' -X DELETE \"$EP/rebal/l$d/\$(printf '%08d' \$i)\"; done" | sort | uniq -c | sed "s/^/$label-delete l$d /" >> "$OUT/deletes.txt"
      state[$d]=deleted
    fi
    local now=$(( $(date +%s) - t0 ))
    if [ -n "$faults" ] && [ "$killed" = 0 ] && [ "$now" -ge "$third" ]; then
      local h; h=$(leader_host)
      log "fault: kill the controller leader (ctl-$h)"
      snapshot "before-leader-kill"
      kill_proc "s$h" controller; killed=1
      ( sleep 30; start "s$h" controller "$BIN controller --id ctl-$h --http 127.0.0.1:9901 $CTL_ARGS"; ) &
      restart=$!
    fi
    if [ -n "$faults" ] && [ "$stopped" = 0 ] && [ "$now" -ge $((third * 2)) ]; then
      log "fault: stop node d1 for 20 s"
      kill_proc sa d1; stopped=1
      sleep 20
      node sa d1 "$ADDR_A" 9101 9911
    fi
  done
  # Only the controller restart (the sampler runs on).
  [ -z "$restart" ] || wait "$restart" || true
}

log "baseline window: $WINDOW min, rebalancing paused"
window baseline "$WINDOW"
snapshot after-baseline
if [ "$REBALANCE" = on ]; then
  sa "$ADMIN rebalance resume" > /dev/null
fi
log "rebalance window ($REBALANCE): $WINDOW min, leader kill and node stop"
window rebalance "$WINDOW" faults
snapshot after-rebalance

# ---- converge
log "converge: no load"
converge_start=$(date +%s)
deadline=$(( $(date +%s) + 25 * 60 ))
[ "$REBALANCE" = on ] || deadline=$(date +%s)
while [ "$(date +%s)" -lt "$deadline" ]; do
  st=$(sa "$ADMIN rebalance status" 2>&1 || true)
  echo "$st" | strip | head -6 >> "$OUT/converge.txt"
  if echo "$st" | grep -q 'last_reason balanced'; then
    log "rebalancing settled after $(( $(date +%s) - converge_start )) s of converge"; break
  fi
  sleep 30
done
# Reconciliation of the moved sources (min orphan age 60 s, every 30 s).
sleep 180
snapshot converged

# ---- verify everything that must exist
log "verify"
line=$(s3 verify-base --op get --prefix base --objects "$BASE" --concurrency 8 --verify)
[ "$(printf '%s' "$line" | errors_of)" = 0 ] || { log "verify-base errors: $line"; failures=$((failures+1)); }
for i in "${!state[@]}"; do
  case "${state[$i]}" in
    ok) line=$(s3 "verify-l$i" --op get --prefix "l$i" --objects 8 --concurrency 4 --verify)
        [ "$(printf '%s' "$line" | errors_of)" = 0 ] || { log "verify l$i errors: $line"; failures=$((failures+1)); } ;;
    uncertain) s3 "verify-l$i" --op get --prefix "l$i" --objects 8 --concurrency 4 --verify --allow-missing > /dev/null ;;
  esac
done
for h in sa sb; do $h "grep -h -E 'ERROR|panicked' $RUN_DIR/logs/*.log | tail -50"; done 2>&1 | strip > "$OUT/errors-in-logs.txt" || true
log "iterations $iter, failures $failures"
touch "$OUT/.stop-sampler"; wait "$SAMPLER" 2>/dev/null || true; rm -f "$OUT/.stop-sampler"
echo "failures=$failures iterations=$iter window_min=$WINDOW base=$BASE cap=$CAP rebalance=$REBALANCE" > "$OUT/result.txt"
stop_all sa; stop_all sb
[ "$failures" = 0 ]
