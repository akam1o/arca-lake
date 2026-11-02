#!/usr/bin/env bash
# Scrub and stripe-repair throughput on the two-host layout (RFC-008 §8,
# Phase 3.2 §12): scrub rate (unpaced and at the default budget), repair
# throughput after a wiped device, CPU seconds per GiB, and disk/network
# bytes per byte verified or repaired. Uses two-host.sh for the cluster.
#
#   HOST_A=.. HOST_B=.. benchmarks/linux/integrity-perf.sh <out-dir>
#
# Environment: OBJECTS (64) SIZE (16 MiB) CHUNK_BYTES (4 MiB; two-host.sh).
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?output directory}"
mkdir -p "$OUT"
: "${HOST_A:?}" "${HOST_B:?}"
REMOTE_REPO="${REMOTE_REPO:-\$HOME/arca-lake}"
RUN_DIR="${RUN_DIR:-\$HOME/arca-run}"
OBJECTS="${OBJECTS:-64}"
SIZE="${SIZE:-$((16 << 20))}"
TH=benchmarks/linux/two-host.sh
sa() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_A" "$@"; }
sb() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_B" "$@"; }
addr_of() { ssh -o BatchMode=yes "$1" "hostname -I | awk '{print \$1}'"; }
ADDR_B="$(addr_of "$HOST_B")"
BIN="$REMOTE_REPO/target/release/arca-lake"
S3LOAD="$REMOTE_REPO/target/release/s3load"
CLUSTER_FILE="$RUN_DIR/fdb.cluster"
ADMIN="$BIN admin --cluster-file $CLUSTER_FILE"
EP="http://$ADDR_B:9000"

ctl_metrics() { sa "curl -s http://127.0.0.1:9901/metrics"; }
metric() { # <text> <name>
  printf '%s\n' "$1" | awk -v n="$2" '$1 == n {print $2}' | head -1
}
node_scrub_bytes() { # sum over the three durable nodes
  local a b
  a=$(sa "curl -s http://127.0.0.1:9910/metrics; curl -s http://127.0.0.1:9911/metrics" | awk '$1=="arca_node_scrub_bytes_total"{s+=$2} END{print s+0}')
  b=$(sb "curl -s http://127.0.0.1:9910/metrics" | awk '$1=="arca_node_scrub_bytes_total"{s+=$2} END{print s+0}')
  echo $((a + b))
}
# Host counters: bytes read/written on every block device, bytes
# received/sent on every non-loopback interface, controller CPU seconds.
snapshot() { # <ssh-fn> <file>
  "$1" "awk '{r+=\$6*512; w+=\$10*512} END{print \"disk_read_bytes \" r; print \"disk_write_bytes \" w}' /proc/diskstats; awk 'NR>2 && \$1 !~ /^lo:/ {split(\$1,a,\":\"); rx+=\$2; tx+=\$10} END{print \"net_rx_bytes \" rx; print \"net_tx_bytes \" tx}' /proc/net/dev; for n in controller d0 d1 d2; do p=$RUN_DIR/\$n.pid; [ -f \$p ] && echo \"cpu_\$n \$(ps -o cputimes= -p \$(cat \$p) 2>/dev/null | tr -d ' ')\"; done" > "$2" 2>/dev/null || true
}
delta() { # <before> <after> <name>
  local b a
  b=$(awk -v n="$3" '$1==n{print $2}' "$1"); a=$(awk -v n="$3" '$1==n{print $2}' "$2")
  echo $(( ${a:-0} - ${b:-0} ))
}

export CONTROLLER_EXTRA_ARGS="--scrub-interval-secs 0 --stripe-repair-interval-secs 2 --stripe-repair-tasks-per-pass 1024"
bash "$TH" setup durable > "$OUT/setup.log" 2>&1
sb "$S3LOAD --endpoint $EP --bucket bench --op create-bucket" > /dev/null
sb "$S3LOAD --endpoint $EP --bucket bench --op put --objects $OBJECTS --size $SIZE --concurrency 4 --label put" > "$OUT/put.jsonl"
DATA_BYTES=$((OBJECTS * SIZE))
SHARD_BYTES=$((DATA_BYTES * 3 / 2))
echo "objects=$OBJECTS size=$SIZE data_bytes=$DATA_BYTES shard_bytes=$SHARD_BYTES" > "$OUT/params.txt"

# 1. Scrub, unpaced: wall time and node bytes.
snapshot sa "$OUT/a-before-scrub.txt"; snapshot sb "$OUT/b-before-scrub.txt"
nb0=$(node_scrub_bytes)
t0=$(date +%s.%N)
sa "/usr/bin/time -f 'admin_cpu_user %U admin_cpu_sys %S' $ADMIN integrity scrub --group 1 --wait --rate-mib 0 --page 256 --page-mib 0" > "$OUT/scrub-unpaced.txt" 2>&1
t1=$(date +%s.%N)
nb1=$(node_scrub_bytes)
snapshot sa "$OUT/a-after-scrub.txt"; snapshot sb "$OUT/b-after-scrub.txt"
SCRUB_SECS=$(echo "$t1 - $t0" | bc)
SCRUB_BYTES=$((nb1 - nb0))

# 2. Scrub at the default budget (32 MiB/s, 8 MiB pages): the pass must
#    take about shard_bytes / 32 MiB seconds; CPU and disk are sampled
#    around this pass (long enough for whole-second CPU accounting).
snapshot sa "$OUT/a-before-paced.txt"; snapshot sb "$OUT/b-before-paced.txt"
t0=$(date +%s.%N)
sa "$ADMIN integrity scrub --group 1 --wait --rate-mib 32 --page 256 --page-mib 8" > "$OUT/scrub-paced.txt" 2>&1
t1=$(date +%s.%N)
snapshot sa "$OUT/a-after-paced.txt"; snapshot sb "$OUT/b-after-paced.txt"
PACED_SECS=$(echo "$t1 - $t0" | bc)

# 3. Repair after a wiped device: stop d2 on B, wipe it, restart it, let
#    the scrub's presence phase queue every stripe, and time the worker.
sb "kill \$(cat $RUN_DIR/d2.pid); sleep 1; rm -rf $RUN_DIR/d2; mkdir -p $RUN_DIR/d2"
sb "cd $RUN_DIR && { nohup $BIN storage-node --listen $ADDR_B:9100 --dir $RUN_DIR/d2 --metrics-listen 127.0.0.1:9910 > $RUN_DIR/logs/d2.log 2>&1 < /dev/null & echo \$! > $RUN_DIR/d2.pid; }"
sleep 2
# The worker starts on the tasks as soon as the scrub queues them, so the
# window opens before the scrub and closes when the queue is empty.
snapshot sa "$OUT/a-before-repair.txt"; snapshot sb "$OUT/b-before-repair.txt"
m0="$(ctl_metrics)"
r0=$(metric "$m0" arca_controller_stripe_repairs_succeeded_total)
b0=$(metric "$m0" arca_controller_repair_bytes_total)
t0=$(date +%s.%N)
sa "$ADMIN integrity scrub --group 1 --wait --rate-mib 0 --page 256 --page-mib 0" > "$OUT/scrub-after-wipe.txt" 2>&1
QUEUED=$(awk '/enqueued/ {for (i=1;i<=NF;i++) if ($i=="enqueued") print $(i+1)}' "$OUT/scrub-after-wipe.txt" | head -1)
# Queue depth from metadata: the controller gauge is refreshed only at
# the end of a worker pass and is stale while a long pass runs.
for _ in $(seq 1 900); do
  depth=$(sa "$ADMIN integrity status" | awk '/repair queue depth/ {print $4}')
  m="$(ctl_metrics)"
  r=$(metric "$m" arca_controller_stripe_repairs_succeeded_total)
  if [ "${depth:-1}" = 0 ] && [ $((r - r0)) -ge 1 ]; then break; fi
  sleep 1
done
t1=$(date +%s.%N)
m1="$(ctl_metrics)"
snapshot sa "$OUT/a-after-repair.txt"; snapshot sb "$OUT/b-after-repair.txt"
REPAIR_SECS=$(echo "$t1 - $t0" | bc)
REPAIRED=$(( $(metric "$m1" arca_controller_stripe_repairs_succeeded_total) - r0 ))
REPAIR_BYTES=$(( $(metric "$m1" arca_controller_repair_bytes_total) - b0 ))
FAILED=$(metric "$m1" arca_controller_stripe_repairs_failed_total)
printf '%s\n' "$m1" | grep -E '^arca_controller_(scrub|integrity|repair|stripe|shards_rewritten|data_integrity|manifest)' > "$OUT/controller-metrics-after.txt"

# 4. Verify: every object reads back, a final scrub is clean.
sb "$S3LOAD --endpoint $EP --bucket bench --op get --objects $OBJECTS --size $SIZE --concurrency 4 --verify --label verify" > "$OUT/verify-get.jsonl"
sa "$ADMIN integrity scrub --group 1 --wait --rate-mib 0 --page 256 --page-mib 0" > "$OUT/scrub-final.txt" 2>&1
sa "$ADMIN integrity status" | sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g' > "$OUT/status-final.txt"
mkdir -p "$OUT/logs"
sa "cd $RUN_DIR/logs && tar czf - controller.log d0.log d1.log 2>/dev/null" | tar xzf - -C "$OUT/logs" 2>/dev/null || true
sb "cd $RUN_DIR/logs && tar czf - d2.log gateway.log 2>/dev/null" | tar xzf - -C "$OUT/logs" 2>/dev/null || true
sed -i -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g' "$OUT"/logs/*.log 2>/dev/null || true
bash "$TH" teardown > /dev/null 2>&1 || true

cat > "$OUT/env.txt" <<EOF
SCRUB_SECS=$SCRUB_SECS
SCRUB_BYTES=$SCRUB_BYTES
PACED_SECS=$PACED_SECS
QUEUED=${QUEUED:-0}
REPAIR_SECS=$REPAIR_SECS
REPAIRED=$REPAIRED
REPAIR_BYTES=$REPAIR_BYTES
FAILED=${FAILED:-0}
EOF
python3 -I - "$OUT" <<'PYEOF'
import json, sys, re, pathlib
out = pathlib.Path(sys.argv[1])
def kv(p):
    d = {}
    for line in (out / p).read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            try: d[parts[0]] = float(parts[1])
            except ValueError: pass
    return d
def delta(a, b, k): return kv(b).get(k, 0) - kv(a).get(k, 0)
params = dict(x.split('=') for x in (out / 'params.txt').read_text().split())
env = dict(x.split('=', 1) for x in (out / 'env.txt').read_text().split('\n') if '=' in x)
shard_bytes = int(params['shard_bytes'])
gib = 1 << 30
scrub_secs = float(env['SCRUB_SECS']); scrub_bytes = int(env['SCRUB_BYTES'])
repair_secs = float(env['REPAIR_SECS']); repair_bytes = int(env['REPAIR_BYTES'])
admin = (out / 'scrub-unpaced.txt').read_text()
m = re.search(r'admin_cpu_user ([0-9.]+) admin_cpu_sys ([0-9.]+)', admin)
admin_cpu = (float(m.group(1)) + float(m.group(2))) if m else None
scrub_disk = delta('a-before-scrub.txt', 'a-after-scrub.txt', 'disk_read_bytes') + delta('b-before-scrub.txt', 'b-after-scrub.txt', 'disk_read_bytes')
scrub_net = delta('a-before-scrub.txt', 'a-after-scrub.txt', 'net_tx_bytes') + delta('b-before-scrub.txt', 'b-after-scrub.txt', 'net_tx_bytes')
node_cpu_scrub = sum(delta('a-before-scrub.txt', 'a-after-scrub.txt', f'cpu_{n}') for n in ('d0', 'd1')) + delta('b-before-scrub.txt', 'b-after-scrub.txt', 'cpu_d2')
node_cpu_paced = sum(delta('a-before-paced.txt', 'a-after-paced.txt', f'cpu_{n}') for n in ('d0', 'd1')) + delta('b-before-paced.txt', 'b-after-paced.txt', 'cpu_d2')
paced_disk = delta('a-before-paced.txt', 'a-after-paced.txt', 'disk_read_bytes') + delta('b-before-paced.txt', 'b-after-paced.txt', 'disk_read_bytes')
rep_disk_r = delta('a-before-repair.txt', 'a-after-repair.txt', 'disk_read_bytes') + delta('b-before-repair.txt', 'b-after-repair.txt', 'disk_read_bytes')
rep_disk_w = delta('a-before-repair.txt', 'a-after-repair.txt', 'disk_write_bytes') + delta('b-before-repair.txt', 'b-after-repair.txt', 'disk_write_bytes')
rep_net = delta('a-before-repair.txt', 'a-after-repair.txt', 'net_tx_bytes') + delta('b-before-repair.txt', 'b-after-repair.txt', 'net_tx_bytes')
ctl_cpu = delta('a-before-repair.txt', 'a-after-repair.txt', 'cpu_controller')
node_cpu_rep = sum(delta('a-before-repair.txt', 'a-after-repair.txt', f'cpu_{n}') for n in ('d0', 'd1')) + delta('b-before-repair.txt', 'b-after-repair.txt', 'cpu_d2')
summary = {
    'objects': int(params['objects']), 'size': int(params['size']), 'shard_bytes': shard_bytes,
    'scrub': {
        'bytes': scrub_bytes, 'seconds': round(scrub_secs, 2),
        'mib_s': round(scrub_bytes / scrub_secs / (1 << 20), 1) if scrub_secs else None,
        'admin_cpu_s_per_gib': round(admin_cpu / (scrub_bytes / gib), 2) if admin_cpu and scrub_bytes else None,
        'node_cpu_s_per_gib': round(node_cpu_scrub / (scrub_bytes / gib), 2) if scrub_bytes else None,
        'disk_read_amplification': round(scrub_disk / scrub_bytes, 3) if scrub_bytes else None,
        'net_tx_bytes_per_byte': round(scrub_net / scrub_bytes, 3) if scrub_bytes else None,
        'paced_32mib_seconds': round(float(env['PACED_SECS']), 2),
        'paced_expected_seconds': round(shard_bytes / (32 << 20), 2),
        'paced_node_cpu_s_per_gib': round(node_cpu_paced / (shard_bytes / gib), 2),
        'paced_disk_read_amplification': round(paced_disk / shard_bytes, 3),
    },
    'repair': {
        'queued': int(env['QUEUED'] or 0), 'repaired': int(env['REPAIRED']), 'failed': int(env['FAILED'] or 0),
        'bytes_written': repair_bytes, 'seconds': round(repair_secs, 2),
        'mib_s': round(repair_bytes / repair_secs / (1 << 20), 1) if repair_secs else None,
        'controller_cpu_s_per_gib': round(ctl_cpu / (repair_bytes / gib), 2) if repair_bytes else None,
        'node_cpu_s_per_gib': round(node_cpu_rep / (repair_bytes / gib), 2) if repair_bytes else None,
        'disk_read_bytes_per_repaired_byte': round(rep_disk_r / repair_bytes, 3) if repair_bytes else None,
        'disk_write_bytes_per_repaired_byte': round(rep_disk_w / repair_bytes, 3) if repair_bytes else None,
        'net_tx_bytes_per_repaired_byte': round(rep_net / repair_bytes, 3) if repair_bytes else None,
    },
}
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
PYEOF
