#!/usr/bin/env bash
# Phase 4.0 scalability harness: a two-host cluster on a private
# FoundationDB, and `arcabench` runs with per-second samplers on both
# hosts (benchmarks/linux/sampler.sh). Host addresses come only from the
# environment and are stripped from everything written locally.
#
#   HOST_A=user@a HOST_B=user@b bench-cluster.sh setup
#   HOST_A=... HOST_B=... bench-cluster.sh run <label> <arcabench args...>
#   HOST_A=... HOST_B=... bench-cluster.sh restart-gateways
#   HOST_A=... HOST_B=... bench-cluster.sh restart-controller
#   HOST_A=... HOST_B=... bench-cluster.sh teardown
# CLIENTS (run): generator processes [1] (client ids from CLIENT_BASE [0]:
# key choices differ, bodies do not); NO_SAMPLER=1: no samplers (to
# measure their overhead); QUIESCE=1: wait for earlier runs' background
# work first (`quiesce`).
#
# Layout (environment, defaults in brackets):
#   FoundationDB, the controller and NODES_A durable nodes on A [4];
#   NODES_B durable nodes on B [0]; GATEWAYS gateways on GW_HOST [1, b],
#   or one gateway per entry of GW_HOSTS (for example "b c", with HOST_C
#   set: a third host for gateways only); the load generator on LOAD_HOST
#   [a].
# Cluster: K, M [2, 1], CHUNK [4 MiB], SMALL (small-object threshold,
#   replicated below it) [0], GROUP_COUNT [1], NODE_CAP (allotment per node)
#   [6 GiB], RETENTION (--reclaim-retention-secs; empty = the release's
#   default) [], CACHE (none | repl: two cache nodes and a cache tier) [none],
#   FDB_PROCS [1], GW_BIN (another arca-lake binary for the gateways, for
#   A/B runs of a gateway change), CTL_BIN (likewise for the controller,
#   applied by setup and restart-controller),
#   CONTROLLER_EXTRA_ARGS, GW_EXTRA_ARGS, NODE_EXTRA_ARGS.
# Paths: REMOTE_REPO [/mnt/arca-fdb/p40-src] (release build of arca-lake
#   with --features fdb and arcabench), RUN_DIR [/mnt/arca-fdb/p40-run],
#   FDB_DATA [/mnt/arca-fdb/p40-fdb], FDB_PORT [4730], OUT [results dir].
set -euo pipefail
cmd="${1:?setup|run|restart-gateways|teardown}"; shift
: "${HOST_A:?}" "${HOST_B:?}"
NODES_A="${NODES_A:-4}"; NODES_B="${NODES_B:-0}"; GATEWAYS="${GATEWAYS:-1}"
GW_HOST="${GW_HOST:-b}"; LOAD_HOST="${LOAD_HOST:-a}"
K="${K:-2}"; M="${M:-1}"; CHUNK="${CHUNK:-$((4 << 20))}"; SMALL="${SMALL:-0}"; GROUP_COUNT="${GROUP_COUNT:-1}"
NODE_CAP="${NODE_CAP:-$((6 << 30))}"; RETENTION="${RETENTION:-}"; CACHE="${CACHE:-none}"
REMOTE_REPO="${REMOTE_REPO:-/mnt/arca-fdb/p40-src}"; RUN_DIR="${RUN_DIR:-/mnt/arca-fdb/p40-run}"
FDB_DATA="${FDB_DATA:-/mnt/arca-fdb/p40-fdb}"; FDB_PORT="${FDB_PORT:-4730}"; FDB_PROCS="${FDB_PROCS:-1}"
OUT="${OUT:-benchmarks/linux/results/phase40}"
# ssh, retried when the connection itself fails (exit 255): the driver
# survives a network outage of its own host of up to about SSH_RETRIES
# minutes [40]. A command whose connection dropped mid-way runs again.
retry_ssh() {
  local t="$1"; shift
  local i rc
  for i in $(seq 1 "${SSH_RETRIES:-40}"); do
    rc=0; ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 "$t" "$@" || rc=$?
    [ "$rc" != 255 ] && return "$rc"
    sleep $(( i * 10 < 60 ? i * 10 : 60 ))
  done
  return 255
}
sa() { retry_ssh "$HOST_A" "$@"; }
sb() { retry_ssh "$HOST_B" "$@"; }
sc() { retry_ssh "${HOST_C:?HOST_C is not set}" "$@"; }
on() { case "$1" in a) shift; sa "$@" ;; c) shift; sc "$@" ;; *) shift; sb "$@" ;; esac; }
# A command that must not run twice when a retried ssh repeats it (the
# connection can drop after the remote side started it): it runs only if
# its marker directory did not exist yet; its exit status is kept in the
# marker, and a repeat waits for it and returns it (a failure stays one).
once() { # <host> <marker> <command>
  local h="$1" m="$2"; shift 2
  on "$h" "mkdir -p \$(dirname $m); if mkdir $m 2>/dev/null; then ( $* ); echo \$? > $m/rc; fi; for i in \$(seq 1 600); do [ -f $m/rc ] && exit \$(cat $m/rc); sleep 1; done; exit 1"
}
addr_of() { ssh -o BatchMode=yes "$1" "hostname -I | awk '{print \$1}'"; }
ADDR_A="$(addr_of "$HOST_A")"; ADDR_B="$(addr_of "$HOST_B")"; ADDR_C=""
[ -n "${HOST_C:-}" ] && ADDR_C="$(addr_of "$HOST_C")"
addr() { case "$1" in a) echo "$ADDR_A" ;; c) echo "$ADDR_C" ;; *) echo "$ADDR_B" ;; esac; }
# Gateways: one per entry of GW_HOSTS when set, else GATEWAYS on GW_HOST.
if [ -n "${GW_HOSTS:-}" ]; then read -r -a GW_LIST <<< "$GW_HOSTS"; GATEWAYS=${#GW_LIST[@]}; fi
gw_host() { if [ -n "${GW_HOSTS:-}" ]; then echo "${GW_LIST[$1]}"; else echo "$GW_HOST"; fi; }
HOSTS="a b"; [ -n "${HOST_C:-}" ] && HOSTS="a b c"
strip() { sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g'; }
BIN="env ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1 $REMOTE_REPO/target/release/arca-lake"
BENCH="$REMOTE_REPO/target/release/arcabench"
CF="$RUN_DIR/fdb.cluster"
ADMIN="$BIN admin --cluster-file $CF"
start() { # <host> <name> <command...>
  local h="$1" n="$2"; shift 2
  on "$h" "mkdir -p $RUN_DIR/logs $RUN_DIR/.started && cd $RUN_DIR && { mkdir $RUN_DIR/.started/$n 2>/dev/null || exit 0; ulimit -n 65536 2>/dev/null || ulimit -n \$(ulimit -Hn); nohup $* > $RUN_DIR/logs/$n.log 2>&1 < /dev/null & echo \$! > $RUN_DIR/$n.pid; }"
}
stop_host() { # <host>: only this harness's processes (pid files)
  on "$1" "for p in $RUN_DIR/*.pid; do [ -f \"\$p\" ] && kill \"\$(cat \"\$p\")\" 2>/dev/null || true; done; sleep 2; for p in $RUN_DIR/*.pid; do [ -f \"\$p\" ] && kill -9 \"\$(cat \"\$p\")\" 2>/dev/null || true; done; rm -f $RUN_DIR/*.pid; rm -rf $RUN_DIR/.started" || true
}
gateway_cmd() { # <i>
  local cache=""; [ "$CACHE" = repl ] && cache="--cache-tier fast --cache-capacity-bytes 0 --cache-high-watermark 0.9 --cache-low-watermark 0.8"
  local bin="$BIN"; [ -n "${GW_BIN:-}" ] && bin="env ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1 $GW_BIN"
  echo "$bin gateway --backend cluster --cluster-file $CF --allow-anonymous --listen $(addr "$(gw_host "$1")"):$((9000 + $1)) --metrics-listen 127.0.0.1:$((9500 + $1)) --k $K --m $M --chunk-bytes $CHUNK --small-threshold $SMALL --failure-domain node $cache ${GW_EXTRA_ARGS:-}"
}
controller_cmd() {
  local cache=""; [ "$CACHE" = repl ] && cache="--cache-tier fast --cache-capacity-bytes 0 --cache-high-watermark 0.9 --cache-low-watermark 0.8"
  local bin="$BIN"; [ -n "${CTL_BIN:-}" ] && bin="env ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1 $CTL_BIN"
  echo "$bin controller --allow-shared-filesystem --cluster-file $CF --id ctl-a --http 127.0.0.1:9901 --failure-domain node $cache ${CONTROLLER_EXTRA_ARGS:-}"
}
endpoints() { local e=""; for i in $(seq 0 $((GATEWAYS - 1))); do e="$e --endpoint http://$(addr "$(gw_host "$i")"):$((9000 + i))"; done; echo "$e"; }

cmd_setup() {
  stop_host a; stop_host b
  sa "rm -rf $RUN_DIR $FDB_DATA; mkdir -p $RUN_DIR/logs $FDB_DATA"
  sb "rm -rf $RUN_DIR; mkdir -p $RUN_DIR/logs"
  local coords=""
  for i in $(seq 0 $((FDB_PROCS - 1))); do coords="$coords,$ADDR_A:$((FDB_PORT + i))"; done
  for h in a b; do on "$h" "echo 'arca:p40@${coords#,}' > $CF"; done
  sa "mkdir -p $RUN_DIR"
  for i in $(seq 0 $((FDB_PROCS - 1))); do
    sa "mkdir -p $FDB_DATA/d$i $FDB_DATA/l$i"
    start a "fdb$i" "fdbserver -p $ADDR_A:$((FDB_PORT + i)) -C $CF -d $FDB_DATA/d$i -L $FDB_DATA/l$i"
  done
  sleep 2
  sa "fdbcli -C $CF --exec 'configure new single ssd' >/dev/null; for i in \$(seq 1 30); do fdbcli -C $CF --exec 'status minimal' | grep -q 'is available' && break; sleep 1; done"
  local reclaim=""; [ -n "$RETENTION" ] && reclaim="--reclaim-retention-secs $RETENTION --reclaim-skew-secs $(( RETENTION > 120 ? 60 : (RETENTION > 2 ? RETENTION / 3 : 0) ))"
  local i=0
  for h in a b; do
    local n; n=$([ "$h" = a ] && echo "$NODES_A" || echo "$NODES_B")
    for j in $(seq 0 $((n - 1))); do
      [ "$n" -gt 0 ] || break
      start "$h" "d$i" "$BIN storage-node --listen $(addr "$h"):$((9100 + i)) --dir $RUN_DIR/d$i --metrics-listen 127.0.0.1:$((9700 + i)) --capacity-bytes $NODE_CAP $reclaim ${NODE_EXTRA_ARGS:-}"
      i=$((i + 1))
    done
  done
  local total=$i
  if [ "$CACHE" = repl ]; then
    for c in 0 1; do start a "c$c" "$BIN storage-node --listen $ADDR_A:$((9200 + c)) --dir $RUN_DIR/c$c --metrics-listen 127.0.0.1:$((9800 + c)) --capacity-bytes $((2 << 30))"; done
  fi
  sleep 3
  sa "$ADMIN init" > /dev/null 2>&1 || true
  i=0
  for h in a b; do
    local n; n=$([ "$h" = a ] && echo "$NODES_A" || echo "$NODES_B")
    for j in $(seq 0 $((n - 1))); do
      [ "$n" -gt 0 ] || break
      once a "$RUN_DIR/.admin/register-$i" "$ADMIN register-node --address $(addr "$h"):$((9100 + i)) --label node=n$i --label device_class=hdd" > /dev/null
      i=$((i + 1))
    done
  done
  once a "$RUN_DIR/.admin/groups" "$ADMIN create-groups --tier default --device-class hdd --failure-domain node --count $GROUP_COUNT --k $K --m $M --chunk-bytes $CHUNK --small-threshold $SMALL" > /dev/null
  if [ "$CACHE" = repl ]; then
    for c in 0 1; do once a "$RUN_DIR/.admin/register-c$c" "$ADMIN register-node --address $ADDR_A:$((9200 + c)) --label node=c$c --label device_class=nvme" > /dev/null; done
    once a "$RUN_DIR/.admin/cache-groups" "$ADMIN create-cache-groups --tier fast --device-class nvme --protection replication --replicas 2 --failure-domain node --count 1" > /dev/null
  fi
  start a controller "$(controller_cmd)"
  for g in $(seq 0 $((GATEWAYS - 1))); do start "$(gw_host "$g")" "gw$g" "$(gateway_cmd "$g")"; done
  sleep 4
  for g in $(seq 0 $((GATEWAYS - 1))); do on "$LOAD_HOST" "for i in \$(seq 1 60); do curl -s -o /dev/null http://$(addr "$(gw_host "$g")"):$((9000 + g))/ && break; sleep 1; done"; done
  {
    echo "nodes_a=$NODES_A nodes_b=$NODES_B total_nodes=$total gateways=$GATEWAYS gw_host=$GW_HOST load_host=$LOAD_HOST"
    echo "k=$K m=$M chunk=$CHUNK small=$SMALL groups=$GROUP_COUNT node_cap=$NODE_CAP retention=${RETENTION:-default} cache=$CACHE fdb_procs=$FDB_PROCS"
    echo "controller_extra=${CONTROLLER_EXTRA_ARGS:-} gw_extra=${GW_EXTRA_ARGS:-} node_extra=${NODE_EXTRA_ARGS:-}"
  } | sa "cat > $RUN_DIR/layout.txt"
  sa "$ADMIN status" 2>/dev/null | strip | head -20
}

cmd_restart_gateways() {
  local cf; cf="$(sa "cat $CF")"
  for g in $(seq 0 $((GATEWAYS - 1))); do
    local h; h="$(gw_host "$g")"
    # A gateway-only host gets the cluster file from A.
    on "$h" "mkdir -p $RUN_DIR/logs; [ -f $CF ] || echo '$cf' > $CF"
    on "$h" "p=\$(cat $RUN_DIR/gw$g.pid 2>/dev/null); [ -n \"\$p\" ] && kill \$p 2>/dev/null; for i in \$(seq 1 60); do [ -n \"\$p\" ] && kill -0 \$p 2>/dev/null || break; sleep 1; done; rm -rf $RUN_DIR/.started/gw$g"
    start "$h" "gw$g" "$(gateway_cmd "$g")"
  done
  sleep 4
}

# Stop the controller (waiting for it to exit) and start it again, with
# CTL_BIN and CONTROLLER_EXTRA_ARGS as they are now.
cmd_restart_controller() {
  sa "p=\$(cat $RUN_DIR/controller.pid 2>/dev/null); [ -n \"\$p\" ] && kill \$p 2>/dev/null; for i in \$(seq 1 60); do [ -n \"\$p\" ] && kill -0 \$p 2>/dev/null || break; sleep 1; done; rm -rf $RUN_DIR/.started/controller"
  start a controller "$(controller_cmd)"
  sleep 2
}

metrics_urls() { # <host>
  local u="" i=0
  for h in a b; do
    local n; n=$([ "$h" = a ] && echo "$NODES_A" || echo "$NODES_B")
    for j in $(seq 0 $((n - 1))); do [ "$n" -gt 0 ] || break; [ "$h" = "$1" ] && u="$u http://127.0.0.1:$((9700 + i))/metrics"; i=$((i + 1)); done
  done
  [ "$1" = a ] && u="$u http://127.0.0.1:9901/metrics"
  for g in $(seq 0 $((GATEWAYS - 1))); do [ "$(gw_host "$g")" = "$1" ] && u="$u http://127.0.0.1:$((9500 + g))/metrics"; done
  echo "$u"
}

# Wait until background work of earlier runs is done (validity rule 8):
# the leader's GC backlog 0, every node's compaction backlog below 64 MiB
# and no admitted write in flight, and the nodes' disks below 20 %
# utilisation, in three checks in a row (about 4 s apart: each check
# includes a 1 s iostat interval and a 3 s pause); at most QUIESCE_MAX
# seconds [600].
# Prints the seconds waited (or "timeout").
cmd_quiesce() {
  local t0; t0=$(date +%s); local ok=0
  while [ $(( $(date +%s) - t0 )) -lt "${QUIESCE_MAX:-600}" ]; do
    local gc; gc=$(sa "curl -s 127.0.0.1:9901/metrics | awk '/^arca_controller_gc_backlog_bytes /{print \$2}'")
    local busy=0
    for h in a b; do
      local urls; urls=$(metrics_urls "$h" | tr ' ' '\n' | { grep -E ':97[0-9][0-9]/' || true; } | tr '\n' ' ')
      [ -n "$urls" ] || continue
      local b; b=$(on "$h" "for u in $urls; do curl -s \$u; done | awk '/^arca_node_compaction_backlog_bytes /{seen++; if (\$2 > 67108864) n++} /^arca_node_capacity_bytes.kind=.inflight./{if (\$2 > 0) n++} END{print n+0, seen+0}'; iostat -dx 1 2 | awk '/^(sd|vd|nvme)/{if (\$NF > u) u=\$NF} END{print u+0}'" | tr '\n' ' ')
      set -- $b
      # A node not answering counts as busy (fail closed).
      [ "${1:-0}" -gt 0 ] && busy=1
      [ "${2:-0}" -lt "$(echo $urls | wc -w)" ] && busy=1
      awk -v u="${3:-0}" 'BEGIN{exit !(u > 20)}' && busy=1
    done
    if [ "${gc:-1}" = 0 ] && [ "$busy" = 0 ]; then ok=$((ok + 1)); else ok=0; fi
    [ "$ok" -ge 3 ] && { echo $(( $(date +%s) - t0 )); return 0; }
    sleep 3
  done
  echo timeout
}

cmd_run() {
  local label="${1:?label}"; shift
  local dir="$OUT/$label"; mkdir -p "$dir"
  if [ -n "${QUIESCE:-}" ]; then echo "quiesce_s=$(cmd_quiesce)" > "$dir/quiesce.txt"; fi
  local rdir="$RUN_DIR/runs/$label"
  for h in a b; do
    on "$h" "rm -rf $rdir; mkdir -p $rdir/profile"
    [ -n "${NO_SAMPLER:-}" ] || once "$h" "$rdir/.sampler" "FDB_CLUSTER_FILE=$([ $h = a ] && echo $CF) SAMPLE_METRICS_EVERY=5 bash $REMOTE_REPO/benchmarks/linux/sampler.sh start $rdir/profile $(metrics_urls $h)"
  done
  # Samplers never outlive the run, whatever ends it.
  trap "for h in a b; do on \$h 'bash $REMOTE_REPO/benchmarks/linux/sampler.sh stop $rdir/profile' > /dev/null 2>&1 || true; done" EXIT
  # The generators run detached on their host; the driver polls for
  # them. CLIENTS > 1 (read workloads only): more processes with the same
  # arguments and their own --client-id, summaries summary-<i>.json.
  local clients="${CLIENTS:-1}"
  for c in $(seq 0 $((clients - 1))); do
    local suf=""; [ "$c" -gt 0 ] && suf="-$c"
    on "$LOAD_HOST" "cd $rdir && { mkdir $rdir/.launched$suf 2>/dev/null || exit 0; ulimit -n 65536 2>/dev/null || ulimit -n \$(ulimit -Hn); nohup sh -c '$BENCH $(endpoints) --client-id $((c + ${CLIENT_BASE:-0})) --timeline $rdir/timeline$suf.jsonl $* > $rdir/summary$suf.json 2> $rdir/stderr$suf.log; echo \$? > $rdir/rc$suf' < /dev/null > /dev/null 2>&1 & }"
  done
  local want=""; local c3=0
  while [ "$c3" -lt "$clients" ]; do
    local suf3=""; [ "$c3" -gt 0 ] && suf3="-$c3"
    want="$want && test -f $rdir/rc$suf3"; c3=$((c3 + 1))
  done
  # At most RUN_MAX_WAIT seconds [21600] for the generators.
  local w0; w0=$(date +%s)
  while ! on "$LOAD_HOST" "true $want"; do
    [ $(( $(date +%s) - w0 )) -gt "${RUN_MAX_WAIT:-21600}" ] && { echo "generator wait timed out" >&2; break; }
    sleep 5
  done
  local rc; rc=$(on "$LOAD_HOST" "cat $rdir/rc* 2>/dev/null" | sort -n | tail -1 || true); rc="${rc:-1}"
  local c2=1
  while [ "$c2" -lt "$clients" ]; do on "$LOAD_HOST" "cat $rdir/summary-$c2.json" | strip > "$dir/summary-$c2.json" || true; c2=$((c2 + 1)); done
  for h in a b; do
    on "$h" "bash $REMOTE_REPO/benchmarks/linux/sampler.sh stop $rdir/profile"
    mkdir -p "$dir/profile-$h"
    for f in sar.txt pidstat.txt fdb.txt fds.txt metrics.txt; do on "$h" "cat $rdir/profile/$f 2>/dev/null" | strip > "$dir/profile-$h/$f" || true; done
    gzip -f "$dir/profile-$h/metrics.txt" "$dir/profile-$h/sar.txt" "$dir/profile-$h/pidstat.txt" 2>/dev/null || true
  done
  on "$LOAD_HOST" "cat $rdir/summary.json" | strip > "$dir/summary.json"
  on "$LOAD_HOST" "cat $rdir/timeline.jsonl" | strip | gzip > "$dir/timeline.jsonl.gz"
  on "$LOAD_HOST" "tail -c 20000 $rdir/stderr.log" | strip > "$dir/stderr.log"
  sa "cat $RUN_DIR/layout.txt" > "$dir/layout.txt"
  echo "arcabench $*" | strip > "$dir/command.txt"
  echo "rc=$rc" >> "$dir/command.txt"
  cat "$dir/summary.json"
  return "$rc"
}

# Every metrics endpoint once, admin status, FoundationDB status json and
# the nodes' directory sizes.
cmd_snapshot() {
  local label="${1:?label}" dir="$OUT/$1"; mkdir -p "$dir"
  sa "$ADMIN status 2>/dev/null" | strip > "$dir/admin-status.txt"
  sa "fdbcli -C $CF --exec 'status json' 2>/dev/null" | strip > "$dir/fdb-status.json"
  for h in $HOSTS; do
    on "$h" "for u in $(metrics_urls $h); do echo \"--- \$u\"; curl -s --max-time 5 \$u; done" | strip | gzip > "$dir/metrics-$h.txt.gz"
    on "$h" "du -sb $RUN_DIR/d* $FDB_DATA 2>/dev/null || true" | strip > "$dir/du-$h.txt"
  done
}

# A scrub pass of every group, unpaced, timed (admin integrity scrub --wait).
cmd_scrub() {
  local label="${1:?label}" dir="$OUT/$1"; mkdir -p "$dir"
  local groups; groups=$(sa "$ADMIN status 2>/dev/null" | awk '/^group /{print $2}')
  for g in $groups; do
    # Detached on A and timed there: a dropped connection neither stops
    # it nor starts a second one.
    # A directory per call (a label scrubbed again is timed again).
    local r="$RUN_DIR/scrub/$label-$g-$(date +%s)"
    once a "$r" "cd $r && nohup sh -c 't0=\$(date +%s); $ADMIN integrity scrub --group $g --wait --page-mib 64 > out.txt 2>/dev/null; echo \$? > scrub-rc; echo \$((\$(date +%s) - t0)) > seconds' < /dev/null > /dev/null 2>&1 &"
    while ! sa "test -f $r/seconds"; do sleep 10; done
    sa "cat $r/out.txt" | strip > "$dir/scrub-$g.txt"
    echo "group $g seconds $(sa "cat $r/seconds") rc $(sa "cat $r/scrub-rc")" >> "$dir/scrub-times.txt"
  done
  cat "$dir/scrub-times.txt"
}

cmd_teardown() { stop_host a; stop_host b; sa "rm -rf $RUN_DIR $FDB_DATA"; sb "rm -rf $RUN_DIR"; }

case "$cmd" in
  setup) cmd_setup ;;
  run) cmd_run "$@" ;;
  restart-gateways) cmd_restart_gateways ;;
  restart-controller) cmd_restart_controller ;;
  snapshot) cmd_snapshot "$@" ;;
  quiesce) cmd_quiesce ;;
  scrub) cmd_scrub "$@" ;;
  teardown) cmd_teardown ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
