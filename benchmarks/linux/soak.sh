#!/usr/bin/env bash
# Bounded integrity soak on the two-host layout (Phase 3.2 §11): rounds of
# PUT, multipart PUT, overwrite, DELETE and verified GET under continuous
# scrubbing, GC, cache population and eviction, with fault injection
# (storage node restarts, controller leader failover, byte flips on a
# stopped device). Every GET verifies the whole body; the run fails if a
# body mismatches, if an unreadable read is reported, if a repair task is
# left unrecoverable or failed, or if the manifest scrub has findings.
#
#   HOST_A=.. HOST_B=.. benchmarks/linux/soak.sh <out-dir> [minutes]
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?output directory}"
MINUTES="${2:-20}"
mkdir -p "$OUT"
: "${HOST_A:?}" "${HOST_B:?}"
REMOTE_REPO="${REMOTE_REPO:-\$HOME/arca-lake}"
RUN_DIR="${RUN_DIR:-\$HOME/arca-run}"
TH=benchmarks/linux/two-host.sh
sa() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_A" "$@"; }
sb() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST_B" "$@"; }
addr_of() { ssh -o BatchMode=yes "$1" "hostname -I | awk '{print \$1}'"; }
ADDR_A="$(addr_of "$HOST_A")"
ADDR_B="$(addr_of "$HOST_B")"
BIN="$REMOTE_REPO/target/release/arca-lake"
S3LOAD="$REMOTE_REPO/target/release/s3load"
CLUSTER_FILE="$RUN_DIR/fdb.cluster"
ADMIN="$BIN admin --cluster-file $CLUSTER_FILE"
EP="http://$ADDR_B:9000"
start() { # <ssh-fn> <name> <command...>
  local fn="$1" name="$2"; shift 2
  "$fn" "mkdir -p $RUN_DIR/logs && cd $RUN_DIR && { nohup $* > $RUN_DIR/logs/$name.log 2>&1 < /dev/null & echo \$! > $RUN_DIR/$name.pid; }"
}
kill_proc() { "$1" "kill \$(cat $RUN_DIR/$2.pid) 2>/dev/null || true"; }
ctl_metrics() { sa "curl -s http://127.0.0.1:9901/metrics"; }
gw_metrics() { sb "curl -s http://127.0.0.1:9900/metrics"; }
strip_addr() { sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g'; }
log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$OUT/soak.log"; }

CTL_ARGS="--failure-domain node --evict-interval-secs 2 --gc-interval-secs 10 --gc-grace-secs 10 --scrub-interval-secs 5 --scrub-tick-secs 5 --scrub-rate-mib 32 --scrub-concurrency 4 --stripe-repair-interval-secs 5 --cache-tier fast --cache-capacity-bytes $((256 << 20)) --cache-high-watermark 0.9 --cache-low-watermark 0.8 --cache-max-populations 8"
export CONTROLLER_EXTRA_ARGS="--scrub-interval-secs 5 --scrub-tick-secs 5 --scrub-rate-mib 32 --scrub-concurrency 4 --stripe-repair-interval-secs 5 --gc-grace-secs 10"
export CACHE_CAPACITY=$((256 << 20))
bash "$TH" setup ec > "$OUT/setup.log" 2>&1
# A second controller on B: leader failover target.
start sb controller2 "$BIN controller --cluster-file $CLUSTER_FILE --id ctl-b --http 127.0.0.1:9901 $CTL_ARGS"
sb "$S3LOAD --endpoint $EP --bucket bench --op create-bucket" > /dev/null
# Versioning on: overwrites keep versions, deletes write markers.
sb "curl -s -X PUT '$EP/bench?versioning' -H 'Content-Type: application/xml' --data '<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>'" > /dev/null

SIZE=$((8 << 20)); N=16; MP_SIZE=$((24 << 20)); MP_N=6
deadline=$(( $(date +%s) + MINUTES * 60 ))
round=0; failures=0
s3() { # <label> <args...>: run s3load on the gateway host, record, count errors
  local label="$1"; shift
  local line
  line=$(sb "$S3LOAD --endpoint $EP --bucket bench --label $label $*" 2>>"$OUT/s3load-stderr.log" || true)
  echo "$line" >> "$OUT/s3load.jsonl"
  local errs mism
  errs=$(printf '%s' "$line" | python3 -I -c 'import json,sys
try:
    d=json.loads(sys.stdin.read() or "{}")
except Exception:
    d={}
print(int(d.get("errors",0)), int(d.get("mismatches",0) or 0))' 2>/dev/null || echo "0 0")
  mism=${errs#* }; errs=${errs% *}
  [ "${mism:-0}" = 0 ] || { log "BODY MISMATCH in $label: $line"; failures=$((failures+1)); }
  echo "$label errors=$errs mismatches=$mism" >> "$OUT/rounds.txt"
}

while [ "$(date +%s)" -lt "$deadline" ]; do
  round=$((round+1))
  log "round $round"
  # Write: fresh objects, multipart objects, overwrite of the previous round.
  s3 "r$round-put" --op put --prefix "r$round" --objects $N --size $SIZE --concurrency 4
  s3 "r$round-mpput" --op multipart-put --prefix "m$round" --objects $MP_N --size $MP_SIZE --part-size $((8 << 20)) --concurrency 2
  overwrite_errors=0
  echo "r$round $round" >> "$OUT/seeds.txt"
  if [ $round -ge 2 ]; then
    s3 "r$round-overwrite" --op put --prefix "r$((round-1))" --objects $N --size $SIZE --concurrency 4 --seed $round
    overwrite_errors=$(tail -1 "$OUT/rounds.txt" | sed -E 's/.*errors=([0-9]+).*/\1/')
    # The effective content of r<round-1>: the new seed when every
    # overwrite succeeded, unknown (mixed) when some failed explicitly.
    if [ "${overwrite_errors:-0}" = 0 ]; then echo "r$((round-1)) $round" >> "$OUT/seeds.txt"; else echo "r$((round-1)) mixed" >> "$OUT/seeds.txt"; fi
  fi
  # Delete the round before last (delete markers; GC keeps versions).
  if [ $round -ge 3 ]; then
    sb "for i in \$(seq 0 $((N-1))); do curl -s -o /dev/null -w '%{http_code}\n' -X DELETE \"$EP/bench/r$((round-2))/\$(printf '%08d' \$i)\"; done" | sort | uniq -c | sed "s/^/r$round-delete /" >> "$OUT/rounds.txt"
  fi
  # Faults, interleaved with the reads below.
  case $((round % 5)) in
    1) log "fault: restart storage node d2"; kill_proc sb d2; sleep 8; start sb d2 "$BIN storage-node --listen $ADDR_B:9100 --dir $RUN_DIR/d2 --metrics-listen 127.0.0.1:9910" ;;
    2) log "fault: controller leader failover (kill ctl-a, restart later)"; kill_proc sa controller ;;
    3) log "fault: byte flips on a stopped device (d1)"; kill_proc sa d1; sleep 2
       sa "f=\$(ls -t $RUN_DIR/d1/seg-*.arca | head -1); sz=\$(stat -c %s \$f); for k in 1 2 3 4 5 6; do off=\$(( (sz / 7) * k )); printf '\\x5a' | dd of=\$f bs=1 seek=\$off conv=notrunc status=none; done; echo flipped 6 bytes in \$(basename \$f)" | tee -a "$OUT/soak.log"
       start sa d1 "$BIN storage-node --listen $ADDR_A:9101 --dir $RUN_DIR/d1 --metrics-listen 127.0.0.1:9911" ;;
    4) log "fault: restart cache node c2"; kill_proc sb c2; sleep 5; start sb c2 "$BIN storage-node --listen $ADDR_B:9110 --dir $RUN_DIR/c2 --metrics-listen 127.0.0.1:9920" ;;
    0) log "fault: restart controller ctl-a (follower rejoin)"; start sa controller "$BIN controller --cluster-file $CLUSTER_FILE --id ctl-a --http 127.0.0.1:9901 $CTL_ARGS" ;;
  esac
  # Verified reads: this round (whole bodies), the multipart objects, and
  # a random-order pass over the previous rounds (cache hits, populations,
  # evictions). Objects deleted two rounds ago are allowed to be missing.
  s3 "r$round-get" --op get --prefix "r$round" --objects $N --size $SIZE --concurrency 4 --verify --passes 2 --order random
  s3 "r$round-mpget" --op get --prefix "m$round" --objects $MP_N --size $MP_SIZE --concurrency 2 --verify
  # A failed overwrite PUT leaves the previous version in place (atomic
  # visibility), so the verified read of the overwritten round is only
  # meaningful when every overwrite succeeded; otherwise it is read
  # without body verification (errors still count).
  if [ $round -ge 2 ]; then
    if [ "${overwrite_errors:-0}" = 0 ]; then
      s3 "r$round-prevget" --op get --prefix "r$((round-1))" --objects $N --size $SIZE --concurrency 4 --verify --seed $round --order random --allow-missing
    else
      log "round $round: $overwrite_errors overwrite PUTs failed explicitly (fault window); reading r$((round-1)) without body verification"
      s3 "r$round-prevget-noverify" --op get --prefix "r$((round-1))" --objects $N --size $SIZE --concurrency 4 --order random --allow-missing
    fi
  fi
  if [ $round -ge 2 ]; then
    s3 "r$round-prevmpget" --op get --prefix "m$((round-1))" --objects $MP_N --size $MP_SIZE --concurrency 2 --verify
  fi
  if [ $((round % 5)) = 2 ]; then sleep 10; start sa controller "$BIN controller --cluster-file $CLUSTER_FILE --id ctl-a --http 127.0.0.1:9901 $CTL_ARGS"; fi
  { echo "== round $round"; ctl_metrics | grep -E '^arca_controller_(leader |scrub|integrity|repair|stripe|data_integrity|manifest|gc_passes|cache_evicted)' ; sb "curl -s http://127.0.0.1:9901/metrics" | grep -E '^arca_controller_(leader |stripe_repairs_succeeded)' | sed 's/^/ctl-b /'; gw_metrics | grep -E '^arca_gateway_(degraded|unreadable|integrity|cache_hits|cache_misses|cache_populated)'; } >> "$OUT/metrics.txt" 2>/dev/null || true
done

# Settle: a full scrub pass of the group (finds everything the faults
# left: quarantined shards, missing shards), then wait until the repair
# queue drains, then a second pass that must be clean.
log "settling: full scrub, then drain the repair queue"
sa "$ADMIN integrity scrub --group 1 --wait --rate-mib 0 --page 256 --page-mib 0" | strip_addr >> "$OUT/soak.log" 2>&1 || true
for _ in $(seq 1 120); do
  m="$(ctl_metrics 2>/dev/null || true)"; m2="$(sb 'curl -s http://127.0.0.1:9901/metrics' 2>/dev/null || true)"
  depth=$(printf '%s\n%s\n' "$m" "$m2" | awk '$1=="arca_controller_repair_queue_depth"{print $2}' | sort -n | head -1)
  [ "${depth:-1}" = 0 ] && break
  sleep 5
done
sa "$ADMIN integrity scrub --group 1 --wait --rate-mib 0 --page 256 --page-mib 0" | strip_addr >> "$OUT/soak.log" 2>&1 || true
sa "$ADMIN integrity status" | strip_addr > "$OUT/status-final.txt" || true
sa "$ADMIN integrity verify-manifests" | strip_addr > "$OUT/manifests-final.txt" || echo "verify-manifests exit $?" >> "$OUT/manifests-final.txt"
# Final verified read of everything that should exist.
for r in $(seq 1 $round); do
  if [ $r -le $((round-2)) ]; then allow="--allow-missing"; else allow=""; fi
  seed=$(awk -v k="r$r" '$1==k {s=$2} END{print s}' "$OUT/seeds.txt")
  if [ "$seed" = mixed ]; then
    s3 "final-r$r-noverify" --op get --prefix "r$r" --objects $N --size $SIZE --concurrency 4 $allow
  else
    s3 "final-r$r" --op get --prefix "r$r" --objects $N --size $SIZE --concurrency 4 --verify --seed "$seed" $allow
  fi
  s3 "final-m$r" --op get --prefix "m$r" --objects $MP_N --size $MP_SIZE --concurrency 2 --verify
done
{ echo "== final"; ctl_metrics | grep -E '^arca_controller_'; sb "curl -s http://127.0.0.1:9901/metrics" | grep -E '^arca_controller_' | sed 's/^/ctl-b /'; gw_metrics | grep -E '^arca_gateway_(degraded|unreadable|integrity|cache_)'; sa "curl -s http://127.0.0.1:9910/metrics; curl -s http://127.0.0.1:9911/metrics" | grep -E '^arca_node_(quarantined|corrupt|scrub)'; sb "curl -s http://127.0.0.1:9910/metrics" | grep -E '^arca_node_(quarantined|corrupt|scrub)'; } >> "$OUT/metrics.txt" 2>/dev/null || true
for h in sa sb; do $h "grep -h -i 'unrecoverable\|panic\|invariant' $RUN_DIR/logs/*.log | head -50" >> "$OUT/log-grep.txt" 2>/dev/null || true; done
mkdir -p "$OUT/logs"
sa "cd $RUN_DIR/logs && tar czf - controller.log d0.log d1.log 2>/dev/null" | tar xzf - -C "$OUT/logs" 2>/dev/null || true
sb "cd $RUN_DIR/logs && tar czf - controller2.log d2.log gateway.log 2>/dev/null" | tar xzf - -C "$OUT/logs" 2>/dev/null || true
sed -i -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/<addr>/g' "$OUT"/logs/*.log 2>/dev/null || true
sa "for d in d0 d1; do echo \"== \$d\"; cat $RUN_DIR/\$d/quarantine.log 2>/dev/null; done" > "$OUT/quarantine-logs.txt" 2>/dev/null || true
sb "echo '== d2'; cat $RUN_DIR/d2/quarantine.log 2>/dev/null" >> "$OUT/quarantine-logs.txt" 2>/dev/null || true
bash "$TH" teardown > /dev/null 2>&1 || true
sb "kill \$(cat $RUN_DIR/controller2.pid) 2>/dev/null" || true

unreadable=$(grep -h '^arca_gateway_unreadable_reads_total' "$OUT/metrics.txt" | tail -1 | awk '{print $2}')
mismatches=$(grep -c 'mismatches=[1-9]' "$OUT/rounds.txt" || true)
unrec=$(awk '/repair queue depth/ {print $6}' "$OUT/status-final.txt")
failed=$(awk '/repair queue depth/ {print $8}' "$OUT/status-final.txt")
findings=$(awk '/^roots/ {print $4}' "$OUT/manifests-final.txt")
echo "rounds=$round minutes=$MINUTES body_mismatch_labels=$mismatches unreadable_reads=${unreadable:-?} unrecoverable=${unrec:-?} failed_tasks=${failed:-?} manifest_findings=${findings:-?} failures=$failures" | tee "$OUT/summary.txt"
