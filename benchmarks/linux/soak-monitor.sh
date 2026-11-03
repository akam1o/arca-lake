#!/usr/bin/env bash
# Read-only monitor of a running soak (benchmarks/linux/soak24.sh), Phase
# 3.4: what the soak's own sampler does not record, and the safe-stop
# conditions. Runs from a copy outside the checkout the soak uses, never
# touches its binaries or its tree, and only reads -- except when a stop
# condition is met, when it saves evidence first and then stops the fault
# loop and pauses the workload (SIGSTOP), leaving the cluster up for
# investigation.
#
#   RUN=~/arca-soak MON=~/arca-soak-monitor soak-monitor.sh run
#
# Every minute (MON/host.jsonl, MON/procs.jsonl, MON/segments.jsonl): host
# CPU (user, system, iowait, idle), block device throughput, utilisation
# and queue depth, loopback network bytes, available memory, free disk;
# per soak process CPU seconds, RSS, FDs, threads, I/O bytes; per node
# directory the segment files (count, bytes, segments under 64 MiB, the
# highest segment id: a churn indicator). Every 5 minutes (MON/meta.jsonl):
# metadata queues counted read-only in FoundationDB -- GC backlog
# (`("al","gc",…)`, with the oldest entry's age), pending generations,
# stripe repair tasks, open multipart uploads.
#
# Safe stop (MON/alerts.log, MON/evidence-<time>/): free disk under
# STOP_FREE_GIB (8), available memory under 400 MiB in three consecutive
# samples, or a workload violation. Alert only: a process over
# ALERT_RSS_MIB (2048) RSS or 10000 FDs, GC backlog growing for two hours.
set -uo pipefail
RUN="${RUN:?run directory}"
MON="${MON:?monitor directory}"
STOP_FREE_GIB="${STOP_FREE_GIB:-8}"
ALERT_RSS_MIB="${ALERT_RSS_MIB:-2048}"
DEV="${DEV:-$(basename "$(df --output=source "$RUN" | tail -n 1)")}"
F="$RUN/fdb"
mkdir -p "$MON"
cat > "$MON/meta.py" <<'PY'
import subprocess, sys, json, os
t = int(sys.argv[1])
F = os.environ["F"]
def keys(prefix_hex):
    begin = prefix_hex
    end = prefix_hex[:-4] + "\\x01"  # tuple string terminator \x00 -> \x01
    cmd = ["timeout", "120", "fdbcli", "-C", F + "/fdb.cluster",
           "--tls-certificate-file", F + "/pki/admin/cert.pem",
           "--tls-key-file", F + "/pki/admin/key.pem",
           "--tls-ca-file", F + "/pki/ca/ca.pem", "--timeout", "60",
           "--exec", f"getrangekeys {begin} {end} 1000000"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=150)
    except Exception:
        return None
    if r.returncode != 0:
        return None
    return [l for l in r.stdout.splitlines() if l.startswith("`")]
def unescape(s):
    out, i = bytearray(), 0
    while i < len(s):
        if s[i] == "\\" and s[i+1:i+2] == "x":
            out.append(int(s[i+2:i+4], 16)); i += 4
        elif s[i] == "\\" and s[i+1:i+2] == "\\":
            out.append(0x5c); i += 2
        else:
            out.append(ord(s[i])); i += 1
    return bytes(out)
def tuple_int(b, i):
    code = b[i]
    if code == 0x14: return 0, i + 1
    if 0x14 < code <= 0x1c:
        n = code - 0x14
        return int.from_bytes(b[i+1:i+1+n], "big"), i + 1 + n
    return None, i
res = {"t": t}
P = "\\x02al\\x00\\x02{}\\x00"
for name in ("gc", "pend", "repair", "mpu"):
    ks = keys(P.format(name))
    res[name] = None if ks is None else len(ks)
    if name == "gc" and ks:
        b = unescape(ks[0][1:ks[0].rindex("'")])
        prefix = len(b"\x02al\x00\x02gc\x00")
        v, _ = tuple_int(b, prefix)
        if v is not None:
            res["gc_oldest_age_s"] = max(0, t - v // 1000)
print(json.dumps(res))
PY

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
alert() { echo "$(ts) $*" | tee -a "$MON/alerts.log" >&2; }

fdb() {
  timeout 60 fdbcli -C "$F/fdb.cluster" --tls-certificate-file "$F/pki/admin/cert.pem" \
    --tls-key-file "$F/pki/admin/key.pem" --tls-ca-file "$F/pki/ca/ca.pem" --timeout 30 --exec "$1" 2>/dev/null
}

host_sample() {
  local t; t="$(date +%s)"
  local cpu; cpu="$(awk '/^cpu /{print $2+$3, $4, $6, $5}' /proc/stat)"
  local disk; disk="$(awk -v d="$DEV" '$3==d {print $6, $10, $13, $14}' /proc/diskstats)"
  local net; net="$(awk -F'[: ]+' '$2=="lo" {print $3, $11}' /proc/net/dev)"
  local mem; mem="$(awk '/^MemAvailable/{print $2}' /proc/meminfo)"
  local free; free="$(df --output=avail -B1 "$RUN" | tail -n 1)"
  read -r u s w i <<< "$cpu"; read -r rs ws io wt <<< "$disk"; read -r nr nt <<< "$net"
  echo "{\"t\":$t,\"cpu_user\":$u,\"cpu_sys\":$s,\"cpu_iowait\":$w,\"cpu_idle\":$i,\"rd_sectors\":${rs:-0},\"wr_sectors\":${ws:-0},\"io_ms\":${io:-0},\"weighted_ms\":${wt:-0},\"lo_rx\":${nr:-0},\"lo_tx\":${nt:-0},\"mem_avail_kib\":$mem,\"free_bytes\":$free}" >> "$MON/host.jsonl"
  echo "$free $mem"
}

proc_sample() {
  local t; t="$(date +%s)"
  for f in "$RUN"/pids/*; do
    local n pid; n="$(basename "$f")"; pid="$(cat "$f" 2>/dev/null)"
    [ -r "/proc/$pid/stat" ] || continue
    # utime + stime (fields 14, 15) after the command name.
    local ticks; ticks="$(sed 's/.*) //' "/proc/$pid/stat" | awk '{print $12 + $13}')"
    local rss thr fds rb wb
    rss="$(awk '/^VmRSS/{print $2}' "/proc/$pid/status")"
    thr="$(awk '/^Threads/{print $2}' "/proc/$pid/status")"
    fds="$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)"
    rb="$(awk '/^read_bytes/{print $2}' "/proc/$pid/io" 2>/dev/null)"
    wb="$(awk '/^write_bytes/{print $2}' "/proc/$pid/io" 2>/dev/null)"
    echo "{\"t\":$t,\"proc\":\"$n\",\"pid\":$pid,\"cpu_ticks\":${ticks:-0},\"rss_kib\":${rss:-0},\"threads\":${thr:-0},\"fds\":$fds,\"read_bytes\":${rb:-0},\"write_bytes\":${wb:-0}}"
    if [ "${rss:-0}" -gt $((ALERT_RSS_MIB * 1024)) ]; then alert "rss $n ${rss} KiB"; fi
    if [ "$fds" -gt 10000 ]; then alert "fds $n $fds"; fi
  done >> "$MON/procs.jsonl"
}

segment_sample() {
  python3 -I - "$RUN" "$(date +%s)" >> "$MON/segments.jsonl" <<'PY'
import os, sys, json, glob
run, t = sys.argv[1], int(sys.argv[2])
for d in sorted(glob.glob(run + "/d?") + glob.glob(run + "/c?")):
    c = b = sc = sb = mx = 0
    for e in os.scandir(d):
        if not (e.name.startswith("seg-") and e.name.endswith(".arca")):
            continue
        try:
            size = e.stat().st_size
        except OSError:
            continue
        c += 1; b += size
        if size < 64 << 20:
            sc += 1; sb += size
        mx = max(mx, int(e.name[4:20], 16))
    print(json.dumps({"t": t, "node": os.path.basename(d), "segments": c, "bytes": b,
                      "small_segments": sc, "small_bytes": sb, "max_segment_id": mx}))
PY
}

meta_sample() {
  F="$F" python3 -I "$MON/meta.py" "$(date +%s)" >> "$MON/meta.jsonl" || true
}

violations() {
  grep -c '"violation":' "$RUN/logs/soakload.log" 2>/dev/null || true
}

evidence() {
  local e; e="$MON/evidence-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$e"
  cp "$RUN"/faults.log "$RUN"/soak.log "$RUN"/disk.log "$RUN"/samples.jsonl "$RUN"/fdb.jsonl "$e"/ 2>/dev/null
  tail -n 2000 "$RUN/logs/soakload.log" > "$e/soakload-tail.log" 2>/dev/null
  for l in "$RUN"/logs/*.log; do tail -n 500 "$l" > "$e/$(basename "$l" .log)-tail.log"; done
  for p in 9510 9511 9550 9551 9700 9701 9702 9703 9720 9721; do
    curl -s -m 5 "http://127.0.0.1:$p/metrics" > "$e/metrics-$p.txt"
  done
  ls -l "$RUN"/d? "$RUN"/c? > "$e/segments.txt" 2>&1
  df -B1 "$RUN" > "$e/df.txt"; cat /proc/meminfo > "$e/meminfo.txt"
  echo "$e"
}

safe_stop() { # reason
  local e; e="$(evidence)"
  alert "SAFE STOP: $1 (evidence $e)"
  touch "$RUN/stop-faults"
  local p; p="$(cat "$RUN/pids/soakload" 2>/dev/null)"
  if [ -n "$p" ] && [ "$(readlink "/proc/$p/exe")" = "$(readlink -f "${SOAKLOAD:-$HOME/arca-lake/target/release/soakload}")" ]; then
    kill -STOP "$p" && alert "workload paused (SIGSTOP $p); cluster left running"
  fi
  touch "$MON/stopped"
}

cmd_run() {
  echo $$ > "$MON/pid"
  local i=0 lowmem=0 gc_hist=()
  while [ ! -f "$RUN/finished" ] && [ ! -f "$MON/stopped" ] && [ ! -f "$MON/quit" ]; do
    read -r free mem <<< "$(host_sample)"
    proc_sample
    segment_sample
    if [ $((i % 5)) -eq 0 ]; then
      meta_sample
      local g; g="$(tail -n 1 "$MON/meta.jsonl" 2>/dev/null | python3 -I -c 'import json,sys; print(json.loads(sys.stdin.read()).get("gc") or 0)' 2>/dev/null || echo 0)"
      gc_hist+=("$g")
      # Growing for two hours (24 samples, every one higher than 24 earlier).
      if [ "${#gc_hist[@]}" -gt 24 ]; then
        local old="${gc_hist[$((${#gc_hist[@]} - 25))]}"
        [ "$g" -gt $((old * 2 + 1000)) ] && alert "GC backlog grew from $old to $g in two hours"
      fi
    fi
    if [ "${free:-0}" -lt $((STOP_FREE_GIB << 30)) ]; then safe_stop "free disk $free bytes"; break; fi
    if [ "${mem:-0}" -lt 409600 ]; then lowmem=$((lowmem + 1)); else lowmem=0; fi
    if [ "$lowmem" -ge 3 ]; then safe_stop "available memory ${mem} KiB for 3 samples"; break; fi
    local v; v="$(violations)"
    if [ "${v:-0}" -gt 0 ]; then safe_stop "$v workload violations"; break; fi
    i=$((i + 1))
    sleep 60
  done
}

case "${1:-}" in
  run) cmd_run ;;
  once) host_sample > /dev/null; proc_sample; segment_sample; meta_sample ;;
  *) echo "usage: RUN=… MON=… $0 run" >&2; exit 2 ;;
esac
