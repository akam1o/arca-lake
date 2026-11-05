#!/usr/bin/env bash
# Long-duration soak test on one host (Phase 3.4 §7): a complete arca-lake
# cluster with mutual TLS on every node RPC and FoundationDB over TLS, a
# mixed S3 workload with an acknowledged-write ledger (soakload), planned
# faults, and per-minute resource samples. Runs on the test host itself
# (start it under nohup or tmux); nothing outside RUN is touched, and every
# process is stopped by its PID file after its executable is checked.
#
#   soak24.sh setup            # FoundationDB, PKI, nodes, controllers, gateways
#   soak24.sh start            # workload + fault schedule + sampler
#   soak24.sh status
#   soak24.sh finish           # stop faults, settle, final checks, summary
#   soak24.sh teardown         # stop every process of RUN (keeps RUN)
#
# Environment: RUN (default ~/arca-soak; on a real disk, not a tmpfs), DURATION_SECS (86400),
# FAULT_GAP_SECS (900), FAULT_START_SECS (600), WORKERS (8), REPO (this checkout; release build
# with `--features fdb,fault-injection` and the soakload binaries).
#
# Faults (round robin, one every FAULT_GAP_SECS): storage node SIGTERM
# restart, storage node SIGKILL, gateway kill, controller leader kill,
# FoundationDB process kill/restart, network delay / blackhole / reset on
# one node (userspace proxy), shard corruption on a stopped node, transient
# read and write EIO (test-only control file), cache node kill, certificate
# rotation (gateway, node, controller; same CA, atomic symlink swap).
set -euo pipefail
REPO="${REPO:-$(cd "$(dirname "$0")/../.." && pwd)}"
RUN="${RUN:-$HOME/arca-soak}"
DURATION_SECS="${DURATION_SECS:-86400}"
FAULT_GAP_SECS="${FAULT_GAP_SECS:-900}"
WORKERS="${WORKERS:-8}"
BIN="$REPO/target/release/arca-lake"
SOAKLOAD="$REPO/target/release/soakload"
FAULTPROXY="$REPO/target/release/faultproxy"
FDBSH="$REPO/deploy/dev/fdb-tls-cluster.sh"
TD="soak.arca-lake.invalid"
FDB_TD="fdb-test.arca-lake.invalid"
P="$RUN/pki"
F="$RUN/fdb"
CF="$F/fdb.cluster"
CHUNK=$((1 << 20))
SMALL=$((64 << 10))
AK="soak-ak"
SK="soak-secret-key-not-a-real-credential"

# name -> listen port / metrics port
declare -A LISTEN=([d0]=9600 [d1]=9601 [d2]=9602 [d3]=9603 [c0]=9620 [c1]=9621)
declare -A METRICS=([d0]=9700 [d1]=9701 [d2]=9702 [d3]=9703 [c0]=9720 [c1]=9721 [gw-a]=9510 [gw-b]=9511 [ctl-a]=9550 [ctl-b]=9551)
declare -A GWPORT=([gw-a]=9500 [gw-b]=9501)
PROXIED=d3
PROXY_PORT=9613
NODES="d0 d1 d2 d3 c0 c1"
DURABLE="d0 d1 d2 d3"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "$(ts) $*" | tee -a "$RUN/soak.log" >&2; }

fdb_env() { # role (admin|gateway|controller)
  echo "ARCA_FDB_TLS_CA=$F/pki/ca/ca.pem ARCA_FDB_TLS_CERT=$F/pki/$1/cert.pem ARCA_FDB_TLS_KEY=$F/pki/$1/key.pem ARCA_FDB_TLS_VERIFY_PEERS=Check.Valid=1,S.subjectAltName>=URI:spiffe://$FDB_TD/fdb/"
}
node_env() { # name (directory under pki with a `current` symlink)
  echo "ARCA_TLS_CA=$P/ca/ca.pem ARCA_TLS_CERT=$P/$1/current/cert.pem ARCA_TLS_KEY=$P/$1/current/key.pem ARCA_TRUST_DOMAIN=$TD"
}
admin() { env $(fdb_env admin) $(node_env admin) "$BIN" admin --cluster-file "$CF" "$@"; }

# Issue role/name into pki/<dir>/<serial> and swap pki/<dir>/current to it
# atomically (a certificate agent would do the same).
issue() { # dir role name
  local d="$P/$1" serial; serial="$(date +%s%N | cut -c1-16)"
  mkdir -p "$d"
  "$BIN" tls issue --ca-dir "$P/ca" --trust-domain "$TD" --role "$2" --name "$3" \
    --serial "$serial" --days 7 --out "$d/$serial" > /dev/null
  ln -sfn "$serial" "$d/current.new" && mv -T "$d/current.new" "$d/current"
}

# ---- processes ----------------------------------------------------------

pid_ok() { # name -> true if the PID file names a live process of ours
  local f="$RUN/pids/$1" pid exe
  [ -f "$f" ] || return 1
  pid="$(cat "$f")"
  exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
  case "$exe" in "$BIN"|"$SOAKLOAD"|"$FAULTPROXY"|"${FDBSERVER:-/usr/sbin/fdbserver}"|/usr/bin/bash|/bin/bash) ;; *) return 1 ;; esac
  tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q -- "$RUN"
}
launch() { # name env... -- command...
  local name="$1"; shift
  local envs=()
  while [ "$1" != -- ]; do envs+=("$1"); shift; done; shift
  mkdir -p "$RUN/logs" "$RUN/pids"
  nohup env "${envs[@]}" "$@" >> "$RUN/logs/$name.log" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" > "$RUN/pids/$name"
  # The PID is nohup's, then env's, until the command execs: wait for that,
  # or an immediate liveness check would see a foreign executable and
  # start a second copy.
  local want; want="$(basename "$1")"
  for _ in $(seq 1 50); do
    case "$(readlink "/proc/$pid/exe" 2>/dev/null)" in */"$want") break ;; esac
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
}
stop() { # name [signal]
  local sig="${2:-TERM}"
  if pid_ok "$1"; then
    local pid; pid="$(cat "$RUN/pids/$1")"
    kill "-$sig" "$pid" 2>/dev/null || true
    for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$RUN/pids/$1"
}

node_id() { cat "$RUN/ids/$1"; }
start_node() { # name
  local n="$1"
  launch "$n" $(node_env "$n") -- "$BIN" storage-node --listen "127.0.0.1:${LISTEN[$n]}" \
    --dir "$RUN/$n" --metrics-listen "127.0.0.1:${METRICS[$n]}" --tls-reload-secs 10 \
    --segment-max-bytes $((256 << 20)) --compaction-interval-secs 30
}
CACHE_ARGS="--cache-tier fast --cache-capacity-bytes $((256 << 20)) --cache-high-watermark 0.9 --cache-low-watermark 0.7 --cache-max-populations 4"
start_controller() { # name
  local n="$1"
  launch "$n" $(fdb_env controller) $(node_env "$n") -- "$BIN" controller --cluster-file "$CF" --id "$n" \
    --http "127.0.0.1:${METRICS[$n]}" --failure-domain node --lease-secs 10 --tls-reload-secs 10 \
    --auto-repair --unhealthy-after-secs 120 --repair-interval-secs 60 --health-interval-secs 10 \
    --gc-interval-secs 60 --gc-grace-secs 300 --min-orphan-age-secs 600 --reconcile-interval-secs 900 \
    --scrub-interval-secs 3600 --scrub-rate-mib 16 --stripe-repair-interval-secs 30 \
    --tiering-tier cold --tiering-tier default --tiering-promote-reads 3 --tiering-promote-window-secs 900 \
    --tiering-demote-after-secs 3600 --tiering-interval-secs 300 --evict-interval-secs 10 \
    $CACHE_ARGS
}
start_gateway() { # name
  local n="$1"
  launch "$n" $(fdb_env gateway) $(node_env "$n") -- "$BIN" gateway --backend cluster --cluster-file "$CF" \
    --listen "127.0.0.1:${GWPORT[$n]}" --metrics-listen "127.0.0.1:${METRICS[$n]}" --tls-reload-secs 10 \
    --access-key "$AK" --secret-key "$SK" --k 2 --m 1 --chunk-bytes "$CHUNK" --small-threshold "$SMALL" \
    --failure-domain node --multipart-min-part-bytes $((1 << 20)) $CACHE_ARGS
}
start_proxy() {
  launch proxy -- "$FAULTPROXY" --listen "127.0.0.1:$PROXY_PORT" --target "127.0.0.1:${LISTEN[$PROXIED]}" --control "$RUN/proxy.ctl"
}

cmd_setup() {
  [ -e "$RUN" ] && { echo "$RUN exists; teardown and remove it first" >&2; exit 2; }
  for b in "$BIN" "$SOAKLOAD" "$FAULTPROXY"; do [ -x "$b" ] || { echo "missing $b" >&2; exit 2; }; done
  mkdir -p "$RUN/ids" "$RUN/pids" "$RUN/logs"
  log "setup: FoundationDB (3 processes, TLS)"
  # Bounded memory: three processes share the host with the cluster.
  ARCA_BIN="$BIN" TRUST_DOMAIN="$FDB_TD" FDB_EXTRA_ARGS="--memory 2GiB --cache-memory 256MiB" bash "$FDBSH" start "$F" 4800 tls > "$RUN/logs/fdb-setup.log" 2>&1
  for n in 1 2 3; do cp "$F/pid-$n" "$RUN/pids/fdb-$n"; done
  log "setup: PKI"
  "$BIN" tls init-ca --dir "$P/ca" --name "soak CA" --days 30 > /dev/null
  issue admin admin op
  for g in gw-a gw-b; do issue "$g" gateway "$g"; done
  for c in ctl-a ctl-b; do issue "$c" controller "$c"; done
  admin init > "$RUN/logs/init.log" 2>&1
  log "setup: nodes"
  for n in $NODES; do
    local class=hdd addr="127.0.0.1:${LISTEN[$n]}"
    case "$n" in c*) class=nvme ;; esac
    [ "$n" = "$PROXIED" ] && addr="127.0.0.1:$PROXY_PORT"
    admin register-node --address "$addr" --label "node=$n" --label "device_class=$class" \
      | sed -nE 's/^node ([0-9]+) registered.*/\1/p' > "$RUN/ids/$n"
    issue "$n" node "$(node_id "$n")"
    start_node "$n"
  done
  : > "$RUN/proxy.ctl"; start_proxy
  sleep 3
  admin create-groups --tier default --device-class hdd --count 2 --k 2 --m 1 --chunk-bytes "$CHUNK" --small-threshold "$SMALL" --failure-domain node > "$RUN/logs/groups.log"
  admin create-groups --tier cold --device-class hdd --count 1 --k 2 --m 1 --chunk-bytes "$CHUNK" --small-threshold "$SMALL" --failure-domain node >> "$RUN/logs/groups.log"
  admin create-cache-groups --tier fast --device-class nvme --protection replication --replicas 2 --failure-domain node --count 1 >> "$RUN/logs/groups.log"
  log "setup: controllers and gateways"
  for c in ctl-a ctl-b; do start_controller "$c"; done
  for g in gw-a gw-b; do start_gateway "$g"; done
  sleep 5
  admin status > "$RUN/logs/status-setup.log" 2>&1
  for p in "$RUN"/pids/*; do pid_ok "$(basename "$p")" || { log "setup: $(basename "$p") is not running"; exit 1; }; done
  log "setup: done"
}

# ---- sampler --------------------------------------------------------------

sample_once() {
  local t; t="$(date +%s)"
  for f in "$RUN"/pids/*; do
    local n pid; n="$(basename "$f")"; pid="$(cat "$f")"
    [ -r "/proc/$pid/status" ] || continue
    local rss thr fds
    rss="$(awk '/^VmRSS/{print $2}' "/proc/$pid/status")"
    thr="$(awk '/^Threads/{print $2}' "/proc/$pid/status")"
    fds="$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)"
    echo "{\"t\":$t,\"proc\":\"$n\",\"pid\":$pid,\"rss_kib\":${rss:-0},\"threads\":${thr:-0},\"fds\":$fds}"
  done >> "$RUN/samples.jsonl"
  for n in "${!METRICS[@]}"; do
    curl -s -m 5 "http://127.0.0.1:${METRICS[$n]}/metrics" 2>/dev/null | grep -v '^#' | sed "s/^/$t $n /"
  done | gzip -c >> "$RUN/metrics.log.gz"
  for n in $NODES; do echo "$t $n $(du -sb "$RUN/$n" 2>/dev/null | cut -f1)"; done >> "$RUN/disk.log"
  # FoundationDB counters only (no topology or identifiers kept).
  fdbcli -C "$CF" --tls-certificate-file "$F/pki/admin/cert.pem" --tls-key-file "$F/pki/admin/key.pem" \
    --tls-ca-file "$F/pki/ca/ca.pem" --timeout 10 --exec 'status json' 2>/dev/null \
    | python3 -I -c '
import json,sys,time
try: s=json.load(sys.stdin)["cluster"]
except Exception: print(json.dumps({"t":int(time.time()),"error":"no status"})); sys.exit()
w=s.get("workload",{}); tx=w.get("transactions",{})
g=lambda d,k: d.get(k,{}).get("counter")
print(json.dumps({"t":int(time.time()),"healthy":s.get("data",{}).get("state",{}).get("healthy"),
 "committed":g(tx,"committed"),"conflicted":g(tx,"conflicted"),"started":g(tx,"started"),
 "rejected":g(tx,"rejected_for_queued_too_long"),"kv_bytes":s.get("data",{}).get("total_kv_size_bytes"),
 "processes":len(s.get("processes",{})),"recovery":s.get("recovery_state",{}).get("name")}))' >> "$RUN/fdb.jsonl"
}
cmd__sampler() {
  # A sample must never end the loop: processes come and go under faults.
  set +e +o pipefail
  while [ ! -f "$RUN/finished" ]; do sample_once; sleep 60; done
}

# ---- faults ---------------------------------------------------------------

wait_s() { local end=$(( $(date +%s) + $1 )); while [ "$(date +%s)" -lt "$end" ] && [ ! -f "$RUN/stop-faults" ]; do sleep 2; done; }
fault() { echo "$(ts) $*" >> "$RUN/faults.log"; }
leader() { admin status 2>/dev/null | sed -nE 's/^leader ([^ ]+) token.*/\1/p'; }

do_fault() { # index
  local k="$1" i=$(( $1 / 14 ))
  local dn; dn="$(echo $DURABLE | cut -d' ' -f$(( i % 4 + 1 )))"
  case $(( k % 14 )) in
    0) fault "node-restart $dn"; stop "$dn" TERM; wait_s 20; start_node "$dn" ;;
    1) local g=gw-a; [ $(( i % 2 )) -eq 1 ] && g=gw-b
       fault "gateway-kill $g"; stop "$g" KILL; wait_s 5; start_gateway "$g" ;;
    2) local l; l="$(leader)"
       if [ -n "$l" ] && [ -f "$RUN/pids/$l" ]; then fault "leader-kill $l"; stop "$l" KILL; wait_s 60; start_controller "$l"
       else fault "leader-kill skipped (leader '$l')"; fi ;;
    3) local n=$(( i % 3 + 1 ))
       fault "fdb-process-restart $n"; bash "$FDBSH" kill "$F" "$n"; wait_s 30
       bash "$FDBSH" restart "$F" "$n"; cp "$F/pid-$n" "$RUN/pids/fdb-$n" ;;
    4) fault "net-delay $PROXIED 300ms 120s"; echo "delay 300" > "$RUN/proxy.ctl"; wait_s 120; : > "$RUN/proxy.ctl" ;;
    5) fault "node-kill $dn"; stop "$dn" KILL; wait_s 30; start_node "$dn" ;;
    6) local c=c0; [ $(( i % 2 )) -eq 1 ] && c=c1
       fault "cache-node-kill $c"; stop "$c" KILL; wait_s 20; start_node "$c" ;;
    7) fault "net-blackhole $PROXIED 45s"; echo blackhole > "$RUN/proxy.ctl"; wait_s 45; : > "$RUN/proxy.ctl" ;;
    8) fault "corrupt-shard $dn (node stopped)"; stop "$dn" TERM
       fault "corrupt-shard-detail $dn $("$BIN" storage-node corrupt-shard --dir "$RUN/$dn" --pick "$RANDOM$RANDOM" 2>&1 | tr '\n' ' ')"
       start_node "$dn" ;;
    9) fault "eio-read $dn 20"; echo "eio-read 20" > "$RUN/$dn/FAULT"; wait_s 90; : > "$RUN/$dn/FAULT" ;;
    10) fault "net-reset $PROXIED 10s"; echo reset > "$RUN/proxy.ctl"; wait_s 10; : > "$RUN/proxy.ctl" ;;
    11) local who
        case $(( i % 3 )) in 0) who=gw-a; issue gw-a gateway gw-a ;; 1) who="$dn"; issue "$dn" node "$(node_id "$dn")" ;; *) who=ctl-a; issue ctl-a controller ctl-a ;; esac
        fault "cert-rotate $who" ;;
    12) fault "eio-write $dn 5"; echo "eio-write 5" > "$RUN/$dn/FAULT"; wait_s 90; : > "$RUN/$dn/FAULT" ;;
    13) local o=ctl-a; [ "$(leader)" = ctl-a ] && o=ctl-b
        fault "controller-restart (standby) $o"; stop "$o" TERM; wait_s 10; start_controller "$o" ;;
  esac
}
cmd__faults() {
  # One fault failing (e.g. no leader at that moment) is logged, not fatal.
  set +e +o pipefail
  local k=0
  wait_s "${FAULT_START_SECS:-600}"
  while [ ! -f "$RUN/stop-faults" ]; do
    do_fault "$k" || fault "fault $k failed"
    k=$(( k + 1 ))
    # Restart anything a fault left down (e.g. a node that failed to start).
    for n in $NODES; do pid_ok "$n" || { fault "restart-missing $n"; start_node "$n"; }; done
    for g in gw-a gw-b; do pid_ok "$g" || { fault "restart-missing $g"; start_gateway "$g"; }; done
    for c in ctl-a ctl-b; do pid_ok "$c" || { fault "restart-missing $c"; start_controller "$c"; }; done
    wait_s "$FAULT_GAP_SECS"
  done
  : > "$RUN/proxy.ctl"; for n in $NODES; do : > "$RUN/$n/FAULT"; done
  fault "faults stopped"
}

cmd_start() {
  date +%s > "$RUN/started"
  launch soakload -- "$SOAKLOAD" --endpoint "http://127.0.0.1:${GWPORT[gw-a]}" --endpoint "http://127.0.0.1:${GWPORT[gw-b]}" \
    --access-key "$AK" --secret-key "$SK" --workers "$WORKERS" --duration-secs "$DURATION_SECS" \
    --report-secs 60 --verify-every-secs 7200 --pause-ms "${PAUSE_MS:-100}" --timeout-secs 60 --bucket-prefix soak \
    --size-scale-pct "${SIZE_SCALE_PCT:-40}" --part-mib 1 \
    --run-label "$RUN"
  launch sampler -- bash "$0" _sampler "$RUN"
  launch faults -- bash "$0" _faults "$RUN"
  log "started: duration ${DURATION_SECS}s, fault gap ${FAULT_GAP_SECS}s"
}

cmd_status() {
  for f in "$RUN"/pids/*; do n="$(basename "$f")"; printf '%-10s %s\n' "$n" "$(pid_ok "$n" && echo up || echo DOWN)"; done
  tail -n 3 "$RUN/faults.log" 2>/dev/null || true
  grep -h '"t_s"' "$RUN/logs/soakload.log" 2>/dev/null | tail -n 1 | cut -c1-400
  echo "violations: $(grep -c '"violation":' "$RUN/logs/soakload.log" 2>/dev/null || true)"
}

cmd_finish() {
  touch "$RUN/stop-faults"
  log "finish: waiting for the workload's final verification"
  while pid_ok soakload; do sleep 10; done
  while pid_ok faults; do sleep 2; done
  : > "$RUN/proxy.ctl"; for n in $NODES; do : > "$RUN/$n/FAULT"; done
  log "finish: settle 600 s (GC, repair, reconcile)"
  sleep 600
  sample_once
  admin integrity status > "$RUN/final-integrity.txt" 2>&1 || true
  admin integrity verify-manifests > "$RUN/final-manifests.txt" 2>&1 || true
  admin status > "$RUN/final-status.txt" 2>&1 || true
  touch "$RUN/finished"
  stop sampler
  python3 -I "$REPO/benchmarks/linux/soak-summary.py" "$RUN" > "$RUN/summary.json" || true
  log "finish: summary in $RUN/summary.json"
}

cmd_teardown() {
  touch "$RUN/stop-faults" "$RUN/finished"
  # The fault loop first, so that it cannot start anything again.
  stop faults TERM
  for n in sampler soakload gw-a gw-b ctl-a ctl-b $NODES proxy; do stop "$n" TERM; done
  bash "$FDBSH" stop "$F" || true
  rm -f "$RUN"/pids/fdb-*
  # Anything left (a process started while stopping): ours by owner, by
  # this run's directory on its command line, and by executable.
  local me; me="$(id -u)"
  for d in /proc/[0-9]*; do
    [ "$(stat -c %u "$d" 2>/dev/null)" = "$me" ] || continue
    tr '\0' ' ' < "$d/cmdline" 2>/dev/null | grep -q -- "$RUN/" || continue
    case "$(readlink "$d/exe" 2>/dev/null)" in
      "$BIN"|"$SOAKLOAD"|"$FAULTPROXY"|"${FDBSERVER:-/usr/sbin/fdbserver}")
        log "teardown: stopping leftover pid ${d#/proc/}"
        kill -9 "${d#/proc/}" 2>/dev/null || true ;;
    esac
  done
  log "teardown: done"
}

# Shutdown reliability (Phase 3.5, R-68), on a set-up cluster without the
# workload: gateway and controller restarted as children of this shell so
# that their exit status is seen; a gateway is signalled while gcload drives
# PUTs and DELETEs through it, every fifth round right after a FoundationDB
# process restart. One line per round: process, signal, exit status, seconds
# to exit. 0 is a clean exit; 139/134 a crash.
cmd__shutdown_test() {
  local rounds="${ROUNDS:-40}" out="$RUN/shutdown.txt" gcload="$REPO/target/release/gcload"
  stop gw-a TERM; stop ctl-a TERM
  for i in $(seq 1 "$rounds"); do
    local sig=TERM; [ $((i % 2)) -eq 0 ] && sig=INT
    for who in gw-a ctl-a; do
      if [ "$who" = gw-a ]; then
        env $(fdb_env gateway) $(node_env gw-a) "$BIN" gateway --backend cluster --cluster-file "$CF" \
          --listen "127.0.0.1:${GWPORT[gw-a]}" --metrics-listen "127.0.0.1:${METRICS[gw-a]}" --tls-reload-secs 10 \
          --access-key "$AK" --secret-key "$SK" --k 2 --m 1 --chunk-bytes "$CHUNK" --small-threshold "$SMALL" \
          --failure-domain node --multipart-min-part-bytes $((1 << 20)) $CACHE_ARGS >> "$RUN/logs/shutdown-$who.log" 2>&1 &
      else
        env $(fdb_env controller) $(node_env ctl-a) "$BIN" controller --cluster-file "$CF" --id ctl-a \
          --http "127.0.0.1:${METRICS[ctl-a]}" --failure-domain node --lease-secs 10 --gc-interval-secs 5 --gc-grace-secs 0 \
          --auto-repair --repair-interval-secs 5 --health-interval-secs 2 $CACHE_ARGS >> "$RUN/logs/shutdown-$who.log" 2>&1 &
      fi
      local pid=$! load=""
      sleep 4
      if [ "$who" = gw-a ] && [ -x "$gcload" ]; then
        "$gcload" --endpoint "http://127.0.0.1:${GWPORT[gw-a]}" --access-key "$AK" --secret-key "$SK" \
          --bucket shutdown --mode put-delete --rates 40 --step-secs 6 --keys 50 --size 65536 > /dev/null 2>&1 &
        load=$!
        sleep 2
      fi
      [ $((i % 5)) -eq 0 ] && bash "$FDBSH" restart "$F" "$(( (i / 5) % 3 + 1 ))" > /dev/null 2>&1
      local t0; t0="$(date +%s.%N)"
      kill "-$sig" "$pid"
      wait "$pid"; local rc=$?
      local secs; secs="$(echo "$(date +%s.%N) - $t0" | bc)"
      [ -n "$load" ] && wait "$load" 2>/dev/null
      echo "$i $who $sig rc=$rc secs=$secs" >> "$out"
    done
  done
  log "shutdown test: $(grep -c 'rc=0 ' "$out") clean of $(wc -l < "$out")"
}

cmd="${1:?setup|start|status|finish|teardown}"
case "$cmd" in
  setup) cmd_setup ;;
  start) cmd_start ;;
  status) cmd_status ;;
  finish) cmd_finish ;;
  teardown) cmd_teardown ;;
  _sampler) cmd__sampler ;;
  _faults) cmd__faults ;;
  _shutdown_test) cmd__shutdown_test ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
