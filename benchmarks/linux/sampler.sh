#!/usr/bin/env bash
# Per-second sampler run on one host for the duration of a measurement
# phase (R-48 PUT profiling). Writes into <out>:
#   sar.txt      sar -u -d -n DEV -w -r 1   (CPU, disks, network, context switches, memory)
#   pidstat.txt  pidstat -h -u -w -r 1 for arca-lake / fdbserver / s3load processes
#   snmp.txt     /proc/net/snmp and /proc/net/netstat every second (TCP retransmissions)
#   metrics.txt  every metrics endpoint given as an argument, every second
#   fdb.txt      FoundationDB status json digest every 2 s when FDB_CLUSTER_FILE is set
# Usage: sampler.sh start <out> [metrics-url...] | sampler.sh stop <out>
set -u
cmd="${1:?start|stop}"; out="${2:?out dir}"; shift 2
case "$cmd" in
  start)
    mkdir -p "$out"; : > "$out/pids"
    sar -u -d -n DEV -w -r 1 > "$out/sar.txt" 2>&1 < /dev/null & echo $! >> "$out/pids"
    pidstat -h -u -w -r -C 'arca-lake|fdbserver|s3load' 1 > "$out/pidstat.txt" 2>&1 < /dev/null & echo $! >> "$out/pids"
    ( while true; do echo "--- $(date +%s.%N)"; grep -E '^Tcp:|^TcpExt:' /proc/net/snmp /proc/net/netstat; sleep 1; done ) > "$out/snmp.txt" 2>&1 < /dev/null & echo $! >> "$out/pids"
    if [ $# -gt 0 ]; then
      ( while true; do ts=$(date +%s.%N); for u in "$@"; do echo "--- $ts $u"; curl -s --max-time 0.8 "$u" || echo "unreachable"; done; sleep 1; done ) > "$out/metrics.txt" 2>&1 < /dev/null & echo $! >> "$out/pids"
    fi
    if [ -n "${FDB_CLUSTER_FILE:-}" ]; then
      ( while true; do
          ts=$(date +%s.%N)
          fdbcli -C "$FDB_CLUSTER_FILE" --timeout 2 --exec 'status json' 2>/dev/null | python3 -I -c '
import json,sys
try:
    s=json.load(sys.stdin)["cluster"]
except Exception:
    print(json.dumps({"ts":float(sys.argv[1]),"error":"no status"})); sys.exit(0)
lp=s.get("latency_probe",{}); qos=s.get("qos",{}); wl=s.get("workload",{})
procs=s.get("processes",{}).values()
disk_busy=max([p.get("disk",{}).get("busy",0) for p in procs] or [0])
disk_w=sum(p.get("disk",{}).get("writes",{}).get("hz",0) for p in procs)
cpu=sum(p.get("cpu",{}).get("usage_cores",0) for p in procs)
print(json.dumps({"ts":float(sys.argv[1]),"commit_s":lp.get("commit_seconds"),"read_s":lp.get("read_seconds"),"txn_start_s":lp.get("transaction_start_seconds"),
 "worst_log_queue":qos.get("worst_queue_bytes_log_server"),"worst_storage_queue":qos.get("worst_queue_bytes_storage_server"),
 "released_hz":qos.get("released_transactions_per_second"),"limiting":qos.get("performance_limited_by",{}).get("name"),
 "commits_hz":wl.get("transactions",{}).get("committed",{}).get("hz"),"disk_busy":disk_busy,"disk_writes_hz":disk_w,"cpu_cores":cpu}))
' "$ts"
          sleep 2
        done ) > "$out/fdb.txt" 2>&1 < /dev/null & echo $! >> "$out/pids"
    fi
    ;;
  stop)
    # Children first (sar's data collector), then the recorded processes;
    # only PIDs this sampler started, never a pattern match.
    if [ -f "$out/pids" ]; then
      while read -r p; do pkill -P "$p" 2>/dev/null || true; kill "$p" 2>/dev/null || true; done < "$out/pids"
      sleep 0.3
      while read -r p; do pkill -9 -P "$p" 2>/dev/null || true; kill -9 "$p" 2>/dev/null || true; done < "$out/pids"
    fi
    ;;
  *) echo "usage: $0 start <out> [url...] | stop <out>" >&2; exit 2 ;;
esac
