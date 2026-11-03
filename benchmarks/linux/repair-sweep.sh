#!/usr/bin/env bash
# Parallel stripe repair sweep (Phase 3.3 §4): repair after a wiped device
# with 1/2/4/8 workers (unpaced) and with the shipped defaults, each under
# concurrent GET and PUT load, plus a no-repair load baseline. Per run:
# repair MiB/s and duration, controller and node CPU-s per GiB, disk and
# network bytes per repaired byte, client GET p50/p95/p99 and PUT MiB/s
# during the repair, peak controller RSS and peak queue depth.
#
#   HOST_A=.. HOST_B=.. benchmarks/linux/repair-sweep.sh <out-dir>
#
# Environment: OBJECTS (128) SIZE (16 MiB) WORKERS ("1 2 4 8")
# HOT_OBJECTS (16) HOT_SIZE (4 MiB) LOAD_CONC (4). NOLOAD=1 runs the repairs
# without client load (clean disk/network amplification; no baseline).
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?output directory}"
mkdir -p "$OUT"
: "${HOST_A:?}" "${HOST_B:?}"
REMOTE_REPO="${REMOTE_REPO:-\$HOME/arca-lake}"
RUN_DIR="${RUN_DIR:-\$HOME/arca-run}"
OBJECTS="${OBJECTS:-128}"
SIZE="${SIZE:-$((16 << 20))}"
WORKERS="${WORKERS:-1 2 4 8}"
HOT_OBJECTS="${HOT_OBJECTS:-16}"
HOT_SIZE="${HOT_SIZE:-$((4 << 20))}"
LOAD_CONC="${LOAD_CONC:-4}"
TH=benchmarks/linux/two-host.sh
sa() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_A" "$@"; }
sb() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_B" "$@"; }
addr_of() { ssh -o BatchMode=yes "$1" "hostname -I | awk '{print \$1}'"; }
ADDR_B="$(addr_of "$HOST_B")"
# Transport of node RPCs and metadata (ADR-0017): explicit plaintext unless
# ARCA_ENV says otherwise (for example the TLS flags as environment).
ARCA_ENV="${ARCA_ENV:-ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1}"
BIN="env $ARCA_ENV $REMOTE_REPO/target/release/arca-lake"
S3LOAD="$REMOTE_REPO/target/release/s3load"
ADMIN="$BIN admin --cluster-file $RUN_DIR/fdb.cluster"
EP="http://$ADDR_B:9000"

ctl_metrics() { sa "curl -s http://127.0.0.1:9901/metrics"; }
metric() { printf '%s\n' "$1" | awk -v n="$2" '$1 == n {print $2}' | head -1; }
snapshot() { # <ssh-fn> <file>
  "$1" "awk '{r+=\$6*512; w+=\$10*512} END{print \"disk_read_bytes \" r; print \"disk_write_bytes \" w}' /proc/diskstats; awk 'NR>2 && \$1 !~ /^lo:/ {split(\$1,a,\":\"); rx+=\$2; tx+=\$10} END{print \"net_rx_bytes \" rx; print \"net_tx_bytes \" tx}' /proc/net/dev; for n in controller d0 d1 d2; do p=$RUN_DIR/\$n.pid; [ -f \$p ] && echo \"cpu_\$n \$(ps -o cputimes= -p \$(cat \$p) 2>/dev/null | tr -d ' ')\"; done" > "$2" 2>/dev/null || true
}

# Client load on host B until a stop file appears: GET passes over the hot
# objects and PUT passes of new objects; each pass appends one line
# `{"t":<end time>,"x":<s3load summary>}`.
install_load() {
  sb "mkdir -p $RUN_DIR && cat > $RUN_DIR/load.sh" <<'EOF'
#!/usr/bin/env bash
# load.sh <get|put> <s3load> <endpoint> <tag> <objects> <size> <concurrency> <out>
op="$1"; s3load="$2"; ep="$3"; tag="$4"; n="$5"; size="$6"; conc="$7"; out="$8"
dir="$(dirname "$out")"; i=0
while [ ! -f "$dir/load-stop" ]; do
  if [ "$op" = get ]; then
    r=$("$s3load" --endpoint "$ep" --bucket bench --op get --prefix hot --objects "$n" --size "$size" --concurrency "$conc" --label "get-$tag")
  else
    r=$("$s3load" --endpoint "$ep" --bucket bench --op put --prefix "lp-$tag" --first $((i * n)) --objects "$n" --size "$size" --concurrency "$conc" --label "put-$tag")
    i=$((i + 1))
  fi
  echo "{\"t\":$(date +%s.%N),\"x\":$r}" >> "$out"
done
EOF
  sb "chmod +x $RUN_DIR/load.sh"
}
start_load() { # <tag>
  [ -n "${NOLOAD:-}" ] && return 0
  sb "rm -f $RUN_DIR/load-stop $RUN_DIR/load-$1.jsonl; cd $RUN_DIR && { nohup ./load.sh get $S3LOAD $EP $1 $HOT_OBJECTS $HOT_SIZE $LOAD_CONC $RUN_DIR/load-$1.jsonl > /dev/null 2>&1 & echo \$! > load-get.pid; nohup ./load.sh put $S3LOAD $EP $1 8 $HOT_SIZE 2 $RUN_DIR/load-$1.jsonl > /dev/null 2>&1 & echo \$! > load-put.pid; }"
}
stop_load() { # <tag>
  [ -n "${NOLOAD:-}" ] && { : > "$OUT/load-$1.jsonl"; return 0; }
  sb "touch $RUN_DIR/load-stop; for p in load-get load-put; do while kill -0 \$(cat $RUN_DIR/\$p.pid) 2>/dev/null; do sleep 0.5; done; done; cat $RUN_DIR/load-$1.jsonl" > "$OUT/load-$1.jsonl"
}

run_one() { # <tag> <workers> <rate-mib>
  local tag="$1" w="$2" rate="$3" dir="$OUT/$1"
  mkdir -p "$dir"
  export CONTROLLER_EXTRA_ARGS="--scrub-interval-secs 0 --stripe-repair-interval-secs 2 --stripe-repair-tasks-per-pass 4096 --stripe-repair-workers $w --stripe-repair-rate-mib $rate"
  bash "$TH" setup durable > "$dir/setup.log" 2>&1
  install_load
  sb "$S3LOAD --endpoint $EP --bucket bench --op create-bucket" > /dev/null
  sb "$S3LOAD --endpoint $EP --bucket bench --op put --objects $OBJECTS --size $SIZE --concurrency 4 --label put" > "$dir/put.jsonl"
  sb "$S3LOAD --endpoint $EP --bucket bench --op put --prefix hot --objects $HOT_OBJECTS --size $HOT_SIZE --concurrency 4 --label hot" > /dev/null
  if [ "$tag" = baseline ]; then
    start_load "$tag"; sleep 60; stop_load "$tag"
    echo "REPAIR_T0=0" > "$dir/env.txt"; echo "REPAIR_T1=0" >> "$dir/env.txt"
    bash "$TH" teardown > /dev/null 2>&1 || true
    return
  fi
  sb "kill \$(cat $RUN_DIR/d2.pid); sleep 1; rm -rf $RUN_DIR/d2; mkdir -p $RUN_DIR/d2"
  sb "cd $RUN_DIR && { nohup $BIN storage-node --listen $ADDR_B:9100 --dir $RUN_DIR/d2 --metrics-listen 127.0.0.1:9910 > $RUN_DIR/logs/d2.log 2>&1 < /dev/null & echo \$! > $RUN_DIR/d2.pid; }"
  sleep 2
  start_load "$tag"
  [ -n "${NOLOAD:-}" ] || sleep 10
  snapshot sa "$dir/a-before.txt"; snapshot sb "$dir/b-before.txt"
  local m0 r0 b0 t0 t1 depth r peak_rss=0 peak_depth=0 rss
  m0="$(ctl_metrics)"
  r0=$(metric "$m0" arca_controller_stripe_repairs_succeeded_total)
  b0=$(metric "$m0" arca_controller_repair_bytes_total)
  t0=$(date +%s.%N)
  sa "$ADMIN integrity scrub --group 1 --wait --rate-mib 0 --page 256 --page-mib 0" > "$dir/scrub-after-wipe.txt" 2>&1
  : > "$dir/timeline.txt"
  for _ in $(seq 1 1800); do
    depth=$(sa "$ADMIN integrity status" | awk '/repair queue depth/ {print $4}')
    rss=$(sa "ps -o rss= -p \$(cat $RUN_DIR/controller.pid)" | tr -d ' ')
    r=$(metric "$(ctl_metrics)" arca_controller_stripe_repairs_succeeded_total)
    echo "$(date +%s.%N) depth ${depth:-?} rss_kib ${rss:-0} repaired $((r - r0))" >> "$dir/timeline.txt"
    [ "${rss:-0}" -gt "$peak_rss" ] && peak_rss=$rss
    [ "${depth:-0}" -gt "$peak_depth" ] 2>/dev/null && peak_depth=$depth
    if [ "${depth:-1}" = 0 ] && [ $((r - r0)) -ge 1 ]; then break; fi
    sleep 1
  done
  t1=$(date +%s.%N)
  local m1; m1="$(ctl_metrics)"
  snapshot sa "$dir/a-after.txt"; snapshot sb "$dir/b-after.txt"
  stop_load "$tag"
  cat > "$dir/env.txt" <<EOF
WORKERS=$w
RATE_MIB=$rate
REPAIR_T0=$t0
REPAIR_T1=$t1
REPAIRED=$(( $(metric "$m1" arca_controller_stripe_repairs_succeeded_total) - r0 ))
REPAIR_BYTES=$(( $(metric "$m1" arca_controller_repair_bytes_total) - b0 ))
FAILED=$(metric "$m1" arca_controller_stripe_repairs_failed_total)
PEAK_RSS_KIB=$peak_rss
PEAK_DEPTH=$peak_depth
EOF
  sb "$S3LOAD --endpoint $EP --bucket bench --op get --objects $OBJECTS --size $SIZE --concurrency 4 --verify --label verify" > "$dir/verify-get.jsonl"
  bash "$TH" teardown > /dev/null 2>&1 || true
}

[ -n "${NOLOAD:-}" ] || run_one baseline 1 0
for w in $WORKERS; do run_one "w$w" "$w" 0; done
run_one default 4 64

python3 -I - "$OUT" <<'PYEOF'
import json, sys, pathlib, statistics
out = pathlib.Path(sys.argv[1])
gib, mib = 1 << 30, 1 << 20
def kv(p):
    d = {}
    if not p.exists(): return d
    for line in p.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            try: d[parts[0]] = float(parts[1])
            except ValueError: pass
    return d
def env(d):
    return dict(x.split('=', 1) for x in (d / 'env.txt').read_text().splitlines() if '=' in x)
def delta(d, side, k): return kv(d / f'{side}-after.txt').get(k, 0) - kv(d / f'{side}-before.txt').get(k, 0)
def load(tag, t0, t1):
    lines = []
    p = out / f'load-{tag}.jsonl'
    if not p.exists(): return {}
    for l in p.read_text().splitlines():
        try: r = json.loads(l)
        except ValueError: continue
        end = r['t']; x = r['x']
        if t1 and not (t0 <= end - x['duration_s'] and end <= t1): continue
        lines.append(x)
    gets = [x for x in lines if x['op'] == 'get' and x['errors'] == 0]
    puts = [x for x in lines if x['op'] == 'put']
    res = {'get_passes': len(gets), 'put_passes': len(puts),
           'errors': sum(x['errors'] for x in lines)}
    if gets:
        for q in ('p50', 'p95', 'p99'):
            res[f'get_{q}_ms'] = round(statistics.median(x[f'lat_ms_{q}'] for x in gets), 1)
        res['get_p99_ms_worst_pass'] = round(max(x['lat_ms_p99'] for x in gets), 1)
        res['get_mib_s'] = round(statistics.median(x['mib_per_s'] for x in gets), 1)
    if puts:
        res['put_mib_s'] = round(statistics.median(x['mib_per_s'] for x in puts), 1)
    return res
rows = {}
for d in sorted(p for p in out.iterdir() if p.is_dir()):
    e = env(d)
    t0, t1 = float(e.get('REPAIR_T0', 0)), float(e.get('REPAIR_T1', 0))
    row = {'load': load(d.name, t0, t1)}
    if t1:
        secs = t1 - t0; b = int(e['REPAIR_BYTES'])
        ctl = delta(d, 'a', 'cpu_controller')
        node = delta(d, 'a', 'cpu_d0') + delta(d, 'a', 'cpu_d1') + delta(d, 'b', 'cpu_d2')
        dr = delta(d, 'a', 'disk_read_bytes') + delta(d, 'b', 'disk_read_bytes')
        dw = delta(d, 'a', 'disk_write_bytes') + delta(d, 'b', 'disk_write_bytes')
        nt = delta(d, 'a', 'net_tx_bytes') + delta(d, 'b', 'net_tx_bytes')
        row.update({
            'workers': int(e['WORKERS']), 'rate_mib': int(e['RATE_MIB']),
            'repaired': int(e['REPAIRED']), 'failed': int(e.get('FAILED') or 0),
            'repair_bytes': b, 'seconds': round(secs, 1),
            'repair_mib_s': round(b / secs / mib, 1) if secs else None,
            'controller_cpu_s_per_gib': round(ctl / (b / gib), 2) if b else None,
            'node_cpu_s_per_gib': round(node / (b / gib), 2) if b else None,
            'disk_read_per_repaired_byte': round(dr / b, 2) if b else None,
            'disk_write_per_repaired_byte': round(dw / b, 2) if b else None,
            'net_tx_per_repaired_byte': round(nt / b, 2) if b else None,
            'peak_controller_rss_mib': round(int(e['PEAK_RSS_KIB']) / 1024, 1),
            'peak_queue_depth': int(e['PEAK_DEPTH']),
        })
    rows[d.name] = row
(out / 'summary.json').write_text(json.dumps(rows, indent=2) + '\n')
print(json.dumps(rows, indent=2))
PYEOF
