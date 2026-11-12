#!/usr/bin/env bash
# Member repair under faults with real binaries on real FoundationDB
# (Phase 3.13, R-104, ADR-0029). One host: a private fdbserver (its own
# port and data directory; the system FoundationDB is never touched), two
# controllers (auto repair on), eight storage nodes, a gateway and the
# load generator, all on 127.0.0.1. Two 2+1 groups are filled, then, with
# PUT/GET load running:
#
#   S1 a member of group A is stopped; once its slot is being rebuilt, the
#      new target is stopped too: the slot must be rebuilt onto a third
#      node (retarget) and the group become Active;
#   S2 a member of group B is stopped; once rebuilding, the controller
#      leader is killed: the other controller completes it;
#   S3 the stopped nodes return (stale shards; nothing must break);
#   S4 a member of group A is stopped, and while its slot is rebuilt a
#      second member: the group must wait for nodes (awaiting-nodes), not
#      replace the second member, and stay Repairing; meanwhile
#   S5 a node holding no shard of group A is drained: the drain completes;
#      rebalancing does not report group A as holding it; then the second
#      member returns and group A becomes Active;
#   end: every acknowledged object reads back verified.
#
#   benchmarks/linux/repair-faults.sh <out-dir>
# Environment: BIN_DIR (release binaries: arca-lake, s3load), FDB_PORT
#   (4710), RUN (/mnt/arca-fdb/p313-repair), OBJECTS (96), SIZE (8 MiB).
set -euo pipefail
OUT="${1:?output directory}"
mkdir -p "$OUT"
BIN_DIR="${BIN_DIR:?release binaries}"
FDB_PORT="${FDB_PORT:-4710}"
RUN="${RUN:-/mnt/arca-fdb/p313-repair}"
OBJECTS="${OBJECTS:-96}"
SIZE="${SIZE:-$((8 << 20))}"
export ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1
A="$BIN_DIR/arca-lake"
S3LOAD="$BIN_DIR/s3load"
CF="$RUN/fdb.cluster"
ADMIN="$A admin --cluster-file $CF"
EP="http://127.0.0.1:9400"
T0=$(date +%s)
log() { echo "$(( $(date +%s) - T0 ))s $*" | tee -a "$OUT/run.log"; }
start() { # <name> <command...>
  local name="$1"; shift
  nohup "$@" > "$RUN/logs/$name.log" 2>&1 < /dev/null &
  echo $! > "$RUN/$name.pid"
}
stop() { kill "$(cat "$RUN/$1.pid")" 2>/dev/null || true; rm -f "$RUN/$1.pid"; }
cleanup() {
  touch "$OUT/.stop"
  for p in "$RUN"/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true; done
  sleep 2
  for p in "$RUN"/*.pid; do [ -f "$p" ] && kill -9 "$(cat "$p")" 2>/dev/null || true; done
  mkdir -p "$OUT/logs"
  for f in "$RUN"/logs/*.log; do [ -f "$f" ] && gzip -c "$f" > "$OUT/logs/$(basename "$f").gz"; done
  rm -rf "$RUN"
}
trap cleanup EXIT

rm -rf "$RUN"; mkdir -p "$RUN/logs" "$RUN/fdb-data" "$RUN/fdb-log"
AT=@  # cluster file: description:id, at, coordinators
echo "p313:repair${AT}127.0.0.1:$FDB_PORT" > "$CF"
start fdb fdbserver -p "127.0.0.1:$FDB_PORT" -C "$CF" -d "$RUN/fdb-data" -L "$RUN/fdb-log"
sleep 2
fdbcli -C "$CF" --exec 'configure new single ssd' > /dev/null
for _ in $(seq 1 30); do fdbcli -C "$CF" --exec 'status minimal' | grep -q 'is available' && break; sleep 1; done
node_start() { # <i>
  start "d$1" "$A" storage-node --listen "127.0.0.1:$((9300 + $1))" --dir "$RUN/d$1" \
    --metrics-listen "127.0.0.1:$((9930 + $1))" --capacity-bytes $((4 << 30)) --reclaim-retention-secs 300
}
for i in 0 1 2 3 4 5 6 7; do node_start "$i"; done
sleep 2
$ADMIN init > /dev/null 2>&1 || true
for i in 0 1 2 3 4 5 6 7; do
  $ADMIN register-node --address "127.0.0.1:$((9300 + i))" --label "node=d$i" --label device_class=hdd > /dev/null
done
$ADMIN create-groups --tier default --device-class hdd --failure-domain node --count 2 --k 2 --m 1 \
  --chunk-bytes $((4 << 20)) --small-threshold 0 > /dev/null
CTL="--cluster-file $CF --allow-shared-filesystem --failure-domain node --auto-repair --unhealthy-after-secs 15 \
  --repair-interval-secs 3 --health-interval-secs 2 --failure-repair-chunks-per-pass 64 --gc-interval-secs 10 \
  --reconcile-interval-secs 30 --reclaim-interval-secs 30 --rebalance --rebalance-interval-secs 10"
ctl_start() { start "ctl-$1" "$A" controller --id "ctl-$1" --http "127.0.0.1:$2" $CTL; }
ctl_start a 9501
ctl_start b 9502
start gateway "$A" gateway --backend cluster --cluster-file "$CF" --allow-anonymous --listen 127.0.0.1:9400 \
  --metrics-listen 127.0.0.1:9401 --k 2 --m 1 --chunk-bytes $((4 << 20)) --small-threshold 0 --failure-domain node
for _ in $(seq 1 60); do curl -s -o /dev/null "$EP/" && break; sleep 1; done
"$S3LOAD" --endpoint "$EP" --bucket rfaults --op create-bucket > /dev/null

failures=0
fail() { log "FAIL: $*"; failures=$((failures + 1)); }
s3() { # <label> <args...>
  local line
  line=$("$S3LOAD" --endpoint "$EP" --bucket rfaults --size "${S3SIZE:-$SIZE}" --label "$1" "${@:2}" 2>>"$OUT/s3load-stderr.log" || true)
  [ -n "$line" ] || line="{\"label\":\"$1\",\"errors\":-1}"
  echo "$line" >> "$OUT/s3load.jsonl"
  printf '%s' "$line"
}
field() { python3 -I -c "import json,sys; print(json.loads(sys.stdin.read()).get('$1',0))"; }

# Group facts from `admin status`: "group ID ... epoch E STATE members [a, b, c]".
groups() { $ADMIN status 2>/dev/null | grep '^group '; }
g_state() { groups | awk -v g="$1" '$2 == g {for (i=1;i<=NF;i++) if ($i=="epoch") print $(i+2)}'; }
g_members() { groups | awk -v g="$1" '$2 == g' | sed -E 's/.*members \[(.*)\]/\1/; s/,//g'; }
g_status() { $ADMIN status 2>/dev/null | grep "^repair group $1 " || true; }
node_index() { echo $(( $1 - 1 )); }  # node ids are registered 1..8 for d0..d7
wait_for() { # <seconds> <what> <cmd...>
  local deadline=$(( $(date +%s) + $1 )) what="$2"; shift 2
  while ! "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || { fail "timed out: $what"; return 1; }
    sleep 2
  done
  log "ok: $what"
}
leader_port() {
  for p in 9501 9502; do curl -s "http://127.0.0.1:$p/metrics" | grep -q '^arca_controller_leader 1' && { echo "$p"; return; }; done
  echo none
}
sample() { # <label>: leader metrics, node RSS/CPU
  {
    echo "== $1 $(( $(date +%s) - T0 ))s"
    p=$(leader_port); [ "$p" = none ] || curl -s "http://127.0.0.1:$p/metrics" | grep -E '^arca_controller_(groups_repairing|groups_repair_waiting|repair_retargets_total|repair_group_errors_total|repairs_(started|completed)_total|chunks_reconstructed_total) '
    $ADMIN status 2>/dev/null | grep -E '^(group|repair group|node .* lifecycle)'
    $ADMIN rebalance status 2>/dev/null | grep -E 'last_reason' || true
  } >> "$OUT/samples.txt" 2>&1
}

log "fill: $OBJECTS objects of $SIZE bytes"
line=$(s3 fill --op put --prefix base --objects "$OBJECTS" --concurrency 8)
[ "$(printf '%s' "$line" | field errors)" = 0 ] || fail "fill errors: $line"
GA=$(groups | awk 'NR==1 {print $2}'); GB=$(groups | awk 'NR==2 {print $2}')
log "groups A=$GA [$(g_members "$GA")] B=$GB [$(g_members "$GB")]"

# Background load: PUT/GET/DELETE in iterations until told to stop.
load() {
  local it=0
  while [ ! -f "$OUT/.stop" ]; do
    it=$((it + 1))
    S3SIZE=$((1 << 20)) s3 "load-put" --op put --prefix "l$it" --objects 4 --concurrency 4 > /dev/null
    s3 "load-get" --op get --prefix base --objects 16 --first $(( (it * 16) % (OBJECTS - 16) )) --concurrency 4 --verify > /dev/null
    [ "$it" -le 2 ] || S3SIZE=$((1 << 20)) s3 "load-delete" --op delete --prefix "l$((it - 2))" --objects 4 --concurrency 4 > /dev/null
  done
}
rm -f "$OUT/.stop"
load &
LOAD=$!

# ---- S1: target failure
read -r -a MA <<< "$(g_members "$GA")"
first=${MA[0]}
log "S1: stop node $first (group $GA)"; stop "d$(node_index "$first")"
t1=$(date +%s)
target=""
is_rebuilding() { local m; m=($(g_members "$GA")); [ "$(g_state "$GA")" = Repairing ] && [ "${m[0]}" != "$first" ] && target=${m[0]}; }
wait_for 120 "S1 group $GA rebuilding" is_rebuilding || true
log "S1: target $target; stop it"; [ -n "$target" ] && stop "d$(node_index "$target")"
sample S1-target-stopped
retargeted() { local m; m=($(g_members "$GA")); [ "$(g_state "$GA")" = Active ] && [ "${m[0]}" != "$first" ] && [ "${m[0]}" != "$target" ]; }
wait_for 600 "S1 group $GA Active on a third node" retargeted && log "S1: $(( $(date +%s) - t1 ))s from the first stop; members [$(g_members "$GA")]"
sample S1-done

# ---- S2: leader kill during a repair
read -r -a MB <<< "$(g_members "$GB")"
b0=${MB[0]}
alive() { [ -f "$RUN/d$(node_index "$1").pid" ]; }
for m in "${MB[@]}"; do alive "$m" && { b0=$m; break; }; done
log "S2: stop node $b0 (group $GB)"; stop "d$(node_index "$b0")"
t2=$(date +%s)
b_rebuilding() { [ "$(g_state "$GB")" = Repairing ]; }
wait_for 120 "S2 group $GB rebuilding" b_rebuilding || true
lp=$(leader_port); lname=$([ "$lp" = 9501 ] && echo a || echo b)
log "S2: kill the leader ctl-$lname"; stop "ctl-$lname"
sample S2-leader-killed
b_active() { [ "$(g_state "$GB")" = Active ] && ! g_members "$GB" | grep -qw "$b0"; }
wait_for 600 "S2 group $GB Active under the other leader" b_active && log "S2: $(( $(date +%s) - t2 ))s"
ctl_start "$lname" "$lp"
sample S2-done

# ---- S3: stopped nodes return
for n in "$first" "$target" "$b0"; do [ -n "$n" ] && node_start "$(node_index "$n")"; done
log "S3: nodes $first $target $b0 back"
sleep 30
sample S3

# ---- S4: double failure in group A (2+1: the second member is needed)
read -r -a MA <<< "$(g_members "$GA")"
a0=${MA[0]}; a1=${MA[1]}
log "S4: stop node $a0 (group $GA)"; stop "d$(node_index "$a0")"
a_rebuilding() { [ "$(g_state "$GA")" = Repairing ] && ! g_members "$GA" | grep -qw "$a0"; }
wait_for 120 "S4 group $GA rebuilding" a_rebuilding || true
log "S4: stop the second member $a1"; stop "d$(node_index "$a1")"
waiting() { g_status "$GA" | grep -q 'status awaiting-nodes'; }
# Each attempt deep-verifies the target's shards first (minutes on a large
# group): the first attempt after the second stop takes that long.
wait_for 900 "S4 group $GA awaiting nodes" waiting || true
sample S4-waiting

# ---- S5: drain and rebalancing while group A waits
spare=""
for id in 1 2 3 4 5 6 7 8; do
  if ! g_members "$GA" | grep -qw "$id" && alive "$id" && [ "$id" != "$a0" ] && [ "$id" != "$a1" ]; then
    # A node with no shard of group A (not one of its earlier targets).
    if [ "$(ls "$RUN/d$(node_index "$id")" 2>/dev/null | wc -l)" -ge 0 ] && ! grep -qw "$id" <<< "$first $target"; then spare=$id; break; fi
  fi
done
log "S5: drain node $spare"
$ADMIN node decommission --node "$spare" --yes > /dev/null 2>&1 || fail "drain refused"
drained() { $ADMIN status 2>/dev/null | grep -E "^node $spare lifecycle" | grep -q Decommissioned; }
wait_for 600 "S5 node $spare decommissioned while group $GA waits" drained || true
$ADMIN rebalance status 2>/dev/null | grep last_reason | tee -a "$OUT/run.log" | grep -q "group $GA is Repairing (awaiting" && fail "rebalancing held by the waiting group"
read -r -a now <<< "$(g_members "$GA")"
[ "${now[1]}" = "$a1" ] || fail "S4: the needed member $a1 was replaced: [${now[*]}]"
[ "$(g_state "$GA")" = Repairing ] || fail "S4: group $GA left Repairing while a member it needs is down"
sample S5
log "S4: second member $a1 back"; node_start "$(node_index "$a1")"
a_done() { [ "$(g_state "$GA")" = Active ]; }
wait_for 900 "S4 group $GA Active after the member returned" a_done || true
node_start "$(node_index "$a0")"
sample S4-done

# ---- end
touch "$OUT/.stop"; wait "$LOAD" 2>/dev/null || true
log "verify every base object"
line=$(s3 verify --op get --prefix base --objects "$OBJECTS" --concurrency 8 --verify)
[ "$(printf '%s' "$line" | field errors)" = 0 ] && [ "$(printf '%s' "$line" | field mismatches)" = 0 ] || fail "verify: $line"
python3 -I - "$OUT/s3load.jsonl" > "$OUT/load-summary.txt" <<'EOF'
import json, sys, statistics
by = {}
for l in open(sys.argv[1]):
    try: d = json.loads(l)
    except ValueError: continue
    by.setdefault(d.get("label"), []).append(d)
for label, rs in sorted(by.items()):
    p99 = [r.get("lat_ms_p99", 0) for r in rs]; ops = [r.get("ops_per_s", 0) for r in rs]
    print(f"{label}: runs={len(rs)} failed_runs={sum(1 for r in rs if r.get('errors',0) < 0)} errors={sum(max(r.get('errors',0),0) for r in rs)} mismatches={sum(r.get('mismatches',0) for r in rs)} "
          f"ops/s median={statistics.median(ops):.2f} p99 ms median={statistics.median(p99):.1f} max={max(p99):.1f}")
EOF
cat "$OUT/load-summary.txt" | tee -a "$OUT/run.log"
grep -h -E 'panicked' "$RUN"/logs/*.log > "$OUT/panics.txt" 2>/dev/null || true
[ -s "$OUT/panics.txt" ] && fail "panics in logs"
echo "failures=$failures objects=$OBJECTS seconds=$(( $(date +%s) - T0 ))" | tee "$OUT/result.txt"
[ "$failures" = 0 ]
