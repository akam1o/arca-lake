#!/usr/bin/env bash
# Scrub with a working set larger than the page cache (R-59, Phase 3.3
# §7; adaptive budget Phase 3.4 §5): windows of WINDOW seconds under the
# same client load (random 1 MiB range GETs over every object, plus PUTs),
# one per mode in MODES (none = no scrub, a number = fixed MiB/s, 0 =
# unpaced, adaptive = ADR-0018 starting at 32 MiB/s), REPS times with the
# order rotated each repetition. Per window: device read MiB/s, average
# queue depth and utilisation of the data device(s), CPU busy and iowait,
# effective scrub rate and full-pass time, GET throughput and p50/p95/p99,
# PUT MiB/s.
#
#   HOST_A=.. HOST_B=.. benchmarks/linux/scrub-diskbound.sh <out-dir>
#
# Environment: OBJECTS (1024) SIZE (16 MiB) WINDOW (120) MODES ("none 32
# adaptive") REPS (1). Check first that
# OBJECTS * SIZE * 1.5 * 2/3 (the shard bytes on host A) exceeds its RAM.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?output directory}"
mkdir -p "$OUT"
: "${HOST_A:?}" "${HOST_B:?}"
REMOTE_REPO="${REMOTE_REPO:-\$HOME/arca-lake}"
RUN_DIR="${RUN_DIR:-\$HOME/arca-run}"
OBJECTS="${OBJECTS:-1024}"
SIZE="${SIZE:-$((16 << 20))}"
WINDOW="${WINDOW:-120}"
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

# Device counters of the device holding the run directory, CPU iowait
# ticks, node scrub bytes.
snapshot() { # <ssh-fn> <file>
  "$1" "dev=\$(basename \$(df --output=source $RUN_DIR | tail -1)); dev=\${dev%%[0-9]*}; [ -e /sys/block/\$dev ] || dev=\$(lsblk -no pkname \$(df --output=source $RUN_DIR | tail -1) | head -1); awk -v d=\$dev '\$3==d {print \"rd_sectors \" \$6; print \"io_ms \" \$13; print \"weighted_ms \" \$14}' /proc/diskstats; awk '/^cpu / {print \"iowait_ticks \" \$6; print \"idle_ticks \" \$5; t=0; for(i=2;i<=NF;i++) t+=\$i; print \"total_ticks \" t}' /proc/stat; echo \"t_ms \$(date +%s%3N)\"; echo \"mem_total_kib \$(awk '/MemTotal/ {print \$2}' /proc/meminfo)\"; s=0; for p in 9910 9911; do v=\$(curl -s http://127.0.0.1:\$p/metrics | awk '\$1==\"arca_node_scrub_bytes_total\"{print \$2}'); s=\$((s + \${v:-0})); done; echo \"scrub_bytes \$s\"" > "$2" 2>/dev/null || true
}

install_load() {
  sb "mkdir -p $RUN_DIR && cat > $RUN_DIR/load.sh" <<'EOF'
#!/usr/bin/env bash
# load.sh <get|put> <s3load> <endpoint> <tag> <objects> <size> <out>
op="$1"; s3load="$2"; ep="$3"; tag="$4"; n="$5"; size="$6"; out="$7"
dir="$(dirname "$out")"; i=0
while [ ! -f "$dir/load-stop" ]; do
  if [ "$op" = get ]; then
    r=$("$s3load" --endpoint "$ep" --bucket bench --op get --objects "$n" --size "$size" --range-bytes $((1 << 20)) --order random --seed $((i + 7)) --concurrency 4 --label "get-$tag")
  else
    r=$("$s3load" --endpoint "$ep" --bucket bench --op put --prefix "lp-$tag" --first $((i * 8)) --objects 8 --size $((4 << 20)) --concurrency 2 --label "put-$tag")
  fi
  i=$((i + 1))
  echo "{\"t\":$(date +%s.%N),\"x\":$r}" >> "$out"
done
EOF
  sb "chmod +x $RUN_DIR/load.sh"
}
start_load() { # <tag>
  sb "rm -f $RUN_DIR/load-stop $RUN_DIR/load-$1.jsonl; cd $RUN_DIR && { nohup ./load.sh get $S3LOAD $EP $1 $OBJECTS $SIZE $RUN_DIR/load-$1.jsonl > /dev/null 2>&1 & echo \$! > load-get.pid; nohup ./load.sh put $S3LOAD $EP $1 0 0 $RUN_DIR/load-$1.jsonl > /dev/null 2>&1 & echo \$! > load-put.pid; }"
}
stop_load() { # <tag>
  sb "touch $RUN_DIR/load-stop; for p in load-get load-put; do while kill -0 \$(cat $RUN_DIR/\$p.pid) 2>/dev/null; do sleep 0.5; done; done; cat $RUN_DIR/load-$1.jsonl" > "$OUT/load-$1.jsonl"
}

window() { # <tag> <scrub rate MiB/s, or "none">
  local tag="$1" rate="$2"
  start_load "$tag"
  sleep 5
  if [ "$rate" != none ]; then
    local how="--rate-mib $rate"
    [ "$rate" = adaptive ] && how="--rate-mib 32 --adaptive"
    sa "cd $RUN_DIR && { nohup $ADMIN integrity scrub --group 1 --wait $how --page 256 --page-mib 8 > scrub-$tag.log 2>&1 & echo \$! > scrub.pid; }"
    sleep 2
  fi
  snapshot sa "$OUT/a-$tag-before.txt"; snapshot sb "$OUT/b-$tag-before.txt"
  echo "$tag start $(date +%s.%N)" >> "$OUT/windows.txt"
  sleep "$WINDOW"
  echo "$tag end $(date +%s.%N)" >> "$OUT/windows.txt"
  snapshot sa "$OUT/a-$tag-after.txt"; snapshot sb "$OUT/b-$tag-after.txt"
  if [ "$rate" != none ]; then sa "kill \$(cat $RUN_DIR/scrub.pid) 2>/dev/null || true"; fi
  stop_load "$tag"
}

export CONTROLLER_EXTRA_ARGS="--scrub-interval-secs 0 --stripe-repair-interval-secs 3600"
bash "$TH" setup durable > "$OUT/setup.log" 2>&1
install_load
sb "$S3LOAD --endpoint $EP --bucket bench --op create-bucket" > /dev/null
sb "$S3LOAD --endpoint $EP --bucket bench --op put --objects $OBJECTS --size $SIZE --concurrency 8 --label fill" > "$OUT/fill.jsonl"
MODES="${MODES:-none 32 adaptive}"
REPS="${REPS:-1}"
echo "objects=$OBJECTS size=$SIZE window=$WINDOW reps=$REPS" > "$OUT/params.txt"
: > "$OUT/windows.txt"
read -r -a modes <<< "$MODES"
for r in $(seq 1 "$REPS"); do
  # Rotate the order so that drift of shared storage favours no mode.
  n=${#modes[@]}
  for i in $(seq 0 $((n - 1))); do
    m="${modes[$(( (i + r - 1) % n ))]}"
    window "$m-r$r" "$m"
  done
done
bash "$TH" teardown > /dev/null 2>&1 || true

python3 -I - "$OUT" <<'PYEOF'
import json, sys, pathlib, statistics
out = pathlib.Path(sys.argv[1])
mib = 1 << 20
def kv(p):
    d = {}
    for line in p.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            try: d[parts[0]] = float(parts[1])
            except ValueError: pass
    return d
params = dict(x.split('=') for x in (out / 'params.txt').read_text().split())
win = {}
for l in (out / 'windows.txt').read_text().splitlines():
    tag, what, t = l.split()
    win.setdefault(tag, {})[what] = float(t)
shard_bytes = int(params['objects']) * int(params['size']) * 3 // 2
res = {'objects': int(params['objects']), 'size': int(params['size']),
       'shard_bytes_all_nodes': shard_bytes, 'windows': {}, 'modes': {}}
for tag in win:
    row = {}
    scrub = 0.0
    for side in ('a', 'b'):
        b, a = kv(out / f'{side}-{tag}-before.txt'), kv(out / f'{side}-{tag}-after.txt')
        dt = (a['t_ms'] - b['t_ms'])
        d = lambda k: a.get(k, 0) - b.get(k, 0)
        tot = max(d('total_ticks'), 1)
        row[f'host_{side}'] = {
            'disk_read_mib_s': round(d('rd_sectors') * 512 / mib / (dt / 1000), 1),
            'disk_util': round(d('io_ms') / dt, 2),
            'avg_queue_depth': round(d('weighted_ms') / dt, 2),
            'cpu_busy_pct': round(100 * (tot - d('idle_ticks') - d('iowait_ticks')) / tot, 1),
            'cpu_iowait_pct': round(100 * d('iowait_ticks') / tot, 1),
            'scrub_mib_s': round(d('scrub_bytes') / mib / (dt / 1000), 1),
            'ram_gib': round(a.get('mem_total_kib', 0) / (1 << 20), 1),
        }
        scrub += d('scrub_bytes') / mib / (dt / 1000)
    row['scrub_mib_s'] = round(scrub, 1)
    row['full_pass_hours'] = round(shard_bytes / mib / scrub / 3600, 2) if scrub > 0 else None
    t0, t1 = win[tag]['start'], win[tag]['end']
    lines = []
    for l in (out / f'load-{tag}.jsonl').read_text().splitlines():
        try: r = json.loads(l)
        except ValueError: continue
        x = r['x']
        if r['t'] >= t0 and r['t'] - x['duration_s'] <= t1:
            lines.append(x)
    gets = [x for x in lines if x['op'] == 'get']
    puts = [x for x in lines if x['op'] == 'put']
    if gets:
        for q in ('p50', 'p95', 'p99'):
            row[f'get_{q}_ms'] = round(statistics.median(x[f'lat_ms_{q}'] for x in gets), 1)
        row['get_mib_s'] = round(statistics.median(x['mib_per_s'] for x in gets), 1)
        row['get_passes'] = len(gets)
    if puts:
        row['put_mib_s'] = round(statistics.median(x['mib_per_s'] for x in puts), 1)
        row['put_p99_ms'] = round(statistics.median(x['lat_ms_p99'] for x in puts), 1)
    row['errors'] = sum(x['errors'] for x in lines)
    res['windows'][tag] = row
modes = {}
for tag, row in res['windows'].items():
    modes.setdefault(tag.rsplit('-r', 1)[0], []).append(row)
for m, rows in modes.items():
    agg = {'reps': len(rows)}
    for k in ('get_mib_s', 'get_p50_ms', 'get_p95_ms', 'get_p99_ms', 'put_mib_s', 'put_p99_ms', 'scrub_mib_s', 'full_pass_hours'):
        v = [r[k] for r in rows if r.get(k) is not None]
        if v:
            agg[k] = {'median': round(statistics.median(v), 2), 'min': min(v), 'max': max(v)}
    for side in ('a', 'b'):
        for k in ('cpu_busy_pct', 'cpu_iowait_pct', 'avg_queue_depth', 'disk_util'):
            v = [r[f'host_{side}'][k] for r in rows]
            agg[f'host_{side}_{k}'] = round(statistics.median(v), 2)
    res['modes'][m] = agg
(out / 'summary.json').write_text(json.dumps(res, indent=2) + '\n')
print(json.dumps(res['modes'], indent=2))
PYEOF
