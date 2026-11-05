#!/usr/bin/env bash
# End of a soak (benchmarks/linux/soak24.sh) with a GC/compaction
# convergence test (Phase 3.4): run it on the soak host while the soak
# runs; it waits for the workload to end and then
#
#   1. stops the fault loop once the workload stops writing (at
#      started + DURATION_SECS) and waits for the workload's final
#      verification against its acknowledged-write ledger;
#   2. saves the end-of-soak state (metrics of every process, logs,
#      samples, metadata queues, admin status) before anything else;
#   3. takes integrity snapshot 1 (soakcheck: every version read in full);
#   4. leaves GC, compaction and reconciliation running with their normal
#      settings and records every minute: GC backlog, pending generations,
#      repair queue, open uploads, per node segment and live bytes,
#      compaction counters, segment files, free disk, reconcile passes;
#      converged once queues are empty and nothing moved for SETTLE_SECS
#      after at least two reconcile intervals (or CONVERGE_MAX_SECS);
#   5. checks idempotence: another IDEMPOTENCE_SECS of passes change
#      nothing (no compaction, no shard deleted, same files);
#   6. takes snapshot 2 and compares (nothing lost, added or changed);
#   7. multipart manifests and integrity status;
#   8. stops the storage nodes and breaks every device down offline
#      (live, dead, needed and droppable tombstones, slack);
#   9. marks the run finished (the sampler and the monitor stop), writes
#      the summary, and tears the cluster down by PID.
#
#   RUN=~/arca-soak OUT=~/arca-soak-end TOOLS=~/arca-soak-monitor/tools \
#     MON=~/arca-soak-monitor soak-converge.sh
#
# TOOLS holds `soakcheck` and an `arca-lake` new enough for
# `storage-node segment-report` (read-only; never the soak's binary).
# Admin commands use the soak's own binary.
set -uo pipefail
RUN="${RUN:?}"; OUT="${OUT:?}"; TOOLS="${TOOLS:?}"; MON="${MON:?}"
REPO="${REPO:-$HOME/arca-lake}"
BIN="${BIN:-$REPO/target/release/arca-lake}"
DURATION_SECS="${DURATION_SECS:-90000}"
SETTLE_SECS="${SETTLE_SECS:-900}"
IDEMPOTENCE_SECS="${IDEMPOTENCE_SECS:-600}"
CONVERGE_MAX_SECS="${CONVERGE_MAX_SECS:-14400}"
RECONCILE_SECS="${RECONCILE_SECS:-900}"
TD="soak.arca-lake.invalid"; FDB_TD="fdb-test.arca-lake.invalid"
P="$RUN/pki"; F="$RUN/fdb"; CF="$F/fdb.cluster"
AK="soak-ak"; SK="soak-secret-key-not-a-real-credential"
NODES="d0 d1 d2 d3 c0 c1"
declare -A METRICS=([d0]=9700 [d1]=9701 [d2]=9702 [d3]=9703 [c0]=9720 [c1]=9721 [gw-a]=9510 [gw-b]=9511 [ctl-a]=9550 [ctl-b]=9551)
mkdir -p "$OUT"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "$(ts) $*" | tee -a "$OUT/converge.log" >&2; }
alive() { [ -f "$RUN/pids/$1" ] && kill -0 "$(cat "$RUN/pids/$1")" 2>/dev/null; }
admin() {
  env ARCA_FDB_TLS_CA="$F/pki/ca/ca.pem" ARCA_FDB_TLS_CERT="$F/pki/admin/cert.pem" ARCA_FDB_TLS_KEY="$F/pki/admin/key.pem" \
    ARCA_FDB_TLS_VERIFY_PEERS="Check.Valid=1,S.subjectAltName>=URI:spiffe://$FDB_TD/fdb/" \
    ARCA_TLS_CA="$P/ca/ca.pem" ARCA_TLS_CERT="$P/admin/current/cert.pem" ARCA_TLS_KEY="$P/admin/current/key.pem" \
    ARCA_TRUST_DOMAIN="$TD" "$BIN" admin --cluster-file "$CF" "$@"
}
scrape_all() { # dir
  mkdir -p "$1"
  for n in "${!METRICS[@]}"; do curl -s -m 10 "http://127.0.0.1:${METRICS[$n]}/metrics" > "$1/metrics-$n.txt"; done
}
metric() { # name process -> value (sum of series)
  curl -s -m 10 "http://127.0.0.1:${METRICS[$2]}/metrics" | awk -v m="$1" '$1==m || index($1, m"{")==1 {s+=$2} END {printf "%.0f", s}'
}
meta_counts() { F="$F" python3 -I "$MON/meta.py" "$(date +%s)"; }
reconcile_passes() { cat "$RUN"/logs/ctl-*.log 2>/dev/null | grep -c "reconcile pass"; }

# ---- 1. workload end ------------------------------------------------------
started="$(cat "$RUN/started")"
end=$((started + DURATION_SECS))
log "waiting for the workload to stop writing at $(date -u -d "@$end" +%FT%TZ)"
while [ "$(date +%s)" -lt "$end" ] && alive soakload; do sleep 30; done
log "workload duration reached: stopping faults"
touch "$RUN/stop-faults"
while alive faults; do sleep 2; done
log "faults stopped; waiting for the workload's final verification"
while alive soakload; do sleep 10; done
grep '"final"' "$RUN/logs/soakload.log" | tail -n 1 | tee "$OUT/workload-final.json"
violations="$(grep -c '"violation":' "$RUN/logs/soakload.log")"
log "workload final: violations $violations"

# ---- 2. end-of-soak state --------------------------------------------------
E="$OUT/end-of-soak"; mkdir -p "$E"
scrape_all "$E"
cp "$RUN"/faults.log "$RUN"/soak.log "$RUN"/disk.log "$RUN"/samples.jsonl "$RUN"/fdb.jsonl "$E"/ 2>/dev/null
cp "$RUN"/metrics.log.gz "$E"/ 2>/dev/null
cp "$MON"/*.jsonl "$MON"/alerts.log "$E"/ 2>/dev/null
cp "$RUN/logs/soakload.log" "$E"/
for l in "$RUN"/logs/*.log; do tail -n 3000 "$l" > "$E/$(basename "$l" .log)-tail.log"; done
meta_counts > "$E/meta.json"
admin status > "$E/status.txt" 2>&1
admin integrity status > "$E/integrity.txt" 2>&1
df -B1 "$RUN" > "$E/df.txt"; du -sb "$RUN"/* > "$E/du.txt" 2>/dev/null
python3 -I "$REPO/benchmarks/linux/soak-summary.py" "$RUN" > "$E/summary.json" 2>/dev/null
log "end-of-soak state saved in $E"
if [ "$violations" -gt 0 ]; then
  log "VIOLATIONS: stopping here, the cluster is left running for investigation"
  exit 2
fi

# ---- 3. snapshot 1 ----------------------------------------------------------
log "integrity snapshot 1"
"$TOOLS/soakcheck" snapshot --endpoint http://127.0.0.1:9500 --access-key "$AK" --secret-key "$SK" \
  --bucket soak-u --bucket soak-v --out "$OUT/snapshot-1.jsonl" > "$OUT/snapshot-1.txt" 2>&1
s1=$?; log "snapshot 1: $(head -n 1 "$OUT/snapshot-1.txt") (exit $s1)"

# ---- 4. convergence ---------------------------------------------------------
sample() {
  local t; t="$(date +%s)"
  local m; m="$(meta_counts)"
  local line="{\"t\":$t,\"meta\":${m:-null},\"free_bytes\":$(df --output=avail -B1 "$RUN" | tail -n 1),\"reconcile_passes\":$(reconcile_passes)"
  for n in $NODES; do
    line+=",\"$n\":{\"segment_bytes\":$(metric arca_node_segment_bytes "$n"),\"live_bytes\":$(metric arca_node_live_bytes "$n"),\"compactions\":$(metric arca_node_compactions_total "$n"),\"copied\":$(metric arca_node_compaction_copied_bytes_total "$n"),\"reclaimed\":$(metric arca_node_compaction_reclaimed_bytes_total "$n"),\"files\":$(find "$RUN/$n" -maxdepth 1 -name 'seg-*.arca' | wc -l),\"du\":$(du -sb "$RUN/$n" | cut -f1)}"
  done
  local leader=ctl-a; [ "$(metric arca_controller_leader ctl-b)" = 1 ] && leader=ctl-b
  line+=",\"gc_collected\":$(metric arca_controller_gc_collected_total $leader),\"shards_deleted\":$(metric arca_controller_shards_deleted_total $leader),\"repair_queue\":$(metric arca_controller_repair_queue_depth $leader)}"
  echo "$line" | tee -a "$OUT/converge.jsonl"
}
# A fingerprint of everything that should stop moving.
moving() { # json line -> fingerprint
  python3 -I -c '
import json,sys
r=json.loads(sys.stdin.read()); m=r.get("meta") or {}
fp=[m.get("gc"), m.get("pend"), m.get("repair")]
for n in "d0 d1 d2 d3 c0 c1".split():
    x=r[n]; fp += [x["segment_bytes"], x["compactions"], x["files"]]
fp += [r["gc_collected"], r["shards_deleted"]]
print(json.dumps(fp))'
}
log "convergence: GC, compaction and reconciliation continue with their normal settings"
c0="$(date +%s)"; rp0="$(reconcile_passes)"
last_fp=""; still_since="$c0"
while :; do
  l="$(sample)"; fp="$(echo "$l" | moving)"
  now="$(date +%s)"
  if [ "$fp" != "$last_fp" ]; then last_fp="$fp"; still_since="$now"; fi
  empty="$(echo "$l" | python3 -I -c 'import json,sys; m=json.loads(sys.stdin.read()).get("meta") or {}; print(int(m.get("gc")==0 and m.get("pend")==0 and m.get("repair")==0))')"
  passes=$(( $(reconcile_passes) - rp0 ))
  if [ "$empty" = 1 ] && [ $((now - still_since)) -ge "$SETTLE_SECS" ] && [ "$passes" -ge 2 ]; then
    log "converged after $((now - c0)) s (queues empty, nothing moved for $((now - still_since)) s, $passes reconcile passes)"
    echo converged > "$OUT/converge-result"; break
  fi
  if [ $((now - c0)) -ge "$CONVERGE_MAX_SECS" ]; then
    log "NOT converged within $CONVERGE_MAX_SECS s (queues empty: $empty, still for $((now - still_since)) s)"
    echo not-converged > "$OUT/converge-result"; break
  fi
  sleep 60
done

# ---- 5. idempotence ----------------------------------------------------------
before="$(sample | moving)"
ls -l "$RUN"/d? "$RUN"/c? > "$OUT/files-before-idempotence.txt"
sleep "$IDEMPOTENCE_SECS"
after="$(sample | moving)"
ls -l "$RUN"/d? "$RUN"/c? > "$OUT/files-after-idempotence.txt"
if [ "$before" = "$after" ] && cmp -s "$OUT/files-before-idempotence.txt" "$OUT/files-after-idempotence.txt"; then
  log "idempotent: $IDEMPOTENCE_SECS s of further passes changed nothing"; echo idempotent > "$OUT/idempotence-result"
else
  log "NOT idempotent: $before -> $after"; echo changed > "$OUT/idempotence-result"
fi

# ---- 6. snapshot 2 -------------------------------------------------------------
log "integrity snapshot 2"
"$TOOLS/soakcheck" snapshot --endpoint http://127.0.0.1:9500 --access-key "$AK" --secret-key "$SK" \
  --bucket soak-u --bucket soak-v --out "$OUT/snapshot-2.jsonl" > "$OUT/snapshot-2.txt" 2>&1
"$TOOLS/soakcheck" compare "$OUT/snapshot-1.jsonl" "$OUT/snapshot-2.jsonl" > "$OUT/compare.txt" 2>&1
cmpr=$?; log "compare: $(head -n 1 "$OUT/compare.txt") (exit $cmpr)"

# Snapshot 2 read every version through the gateway: that populates the
# cache tier and the next eviction pass deletes the copies again. Let that
# settle too before anything is measured (soak 6: the breakdown was taken
# 12 s after those writes and counted them, with a 30 s old active-segment
# gauge).
log "settling after snapshot 2"
q0="$(date +%s)"; last_fp=""; still_since="$q0"
while :; do
  fp="$(sample | moving)"; now="$(date +%s)"
  if [ "$fp" != "$last_fp" ]; then last_fp="$fp"; still_since="$now"; fi
  if [ $((now - still_since)) -ge "${POST_SNAPSHOT_SETTLE_SECS:-180}" ]; then
    log "settled after snapshot 2 in $((now - q0)) s"; break
  fi
  if [ $((now - q0)) -ge 3600 ]; then log "NOT settled after snapshot 2 within 3600 s"; break; fi
  sleep 30
done

# ---- 7. manifests, integrity ----------------------------------------------------
admin integrity verify-manifests > "$OUT/manifests.txt" 2>&1; log "verify-manifests exit $?"
admin integrity status > "$OUT/integrity-final.txt" 2>&1
admin status > "$OUT/status-final.txt" 2>&1
scrape_all "$OUT/final-metrics"
grep -h "reconcile pass" "$RUN"/logs/ctl-*.log > "$OUT/reconcile-passes.log" 2>/dev/null

# ---- 8. offline breakdown ---------------------------------------------------------
# The segment each node appends to (never compacted) and its compaction
# gauges, before the nodes stop.
for n in $NODES; do
  echo "$n $(metric arca_node_active_segment_id "$n") $(metric arca_node_compaction_backlog_segments "$n") $(metric arca_node_compaction_blocked_segments "$n") $(metric arca_node_dead_records "$n")"
done > "$OUT/active-segments.txt"
touch "$RUN/finished"   # the sampler and the monitor stop
for n in $NODES; do
  pid="$(cat "$RUN/pids/$n" 2>/dev/null)"
  if [ -n "$pid" ] && [ "$(readlink "/proc/$pid/exe")" = "$(readlink -f "$BIN")" ] \
     && tr '\0' ' ' < "/proc/$pid/cmdline" | grep -q -- "$RUN/$n"; then
    kill -TERM "$pid"; for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
  fi
done
for n in $NODES; do
  "$TOOLS/arca-lake" storage-node segment-report --dir "$RUN/$n" > "$OUT/segments-$n.jsonl" 2>&1
  log "breakdown $n: $(tail -n 1 "$OUT/segments-$n.jsonl")"
done

# Verdict: every sealed segment is under the compaction threshold, or
# holds nothing reclaimable but tombstones that are still needed.
python3 -I - "$OUT" "${MIN_DEAD_FRACTION:-0.5}" > "$OUT/space-verdict.json" <<'PY'
import json, sys, os
out, thr = sys.argv[1], float(sys.argv[2])
active = {}
for l in open(os.path.join(out, "active-segments.txt")):
    p = l.split()
    if len(p) >= 2 and p[1].isdigit():
        active[p[0]] = int(p[1])
res, ok = {}, True
for f in sorted(os.listdir(out)):
    if not (f.startswith("segments-") and f.endswith(".jsonl")):
        continue
    n = f[len("segments-"):-len(".jsonl")]
    rows = [json.loads(l) for l in open(os.path.join(out, f)) if l.startswith("{")]
    segs = [r for r in rows if r["what"] == "segment"]
    tot = next((r for r in rows if r["what"] == "total"), {})
    bad = []
    for s in segs:
        if s["segment"] == active.get(n):
            continue
        nonlive = (s["file_bytes"] - s["live_bytes"]) / max(s["file_bytes"], 1)
        if nonlive >= thr and (s["dead_records"] > 0 or s["tombstones_droppable"] > 0):
            bad.append({"segment": s["segment"], "nonlive_fraction": round(nonlive, 3),
                        "dead_records": s["dead_records"], "tombstones_droppable": s["tombstones_droppable"]})
    act = next((s for s in segs if s["segment"] == active.get(n)), None)
    res[n] = {"total": tot, "active_segment": act, "unreclaimed_over_threshold": bad}
    ok = ok and not bad
print(json.dumps({"ok": ok, "threshold": thr, "nodes": res}, indent=1))
PY
log "space verdict: $(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["ok"])' "$OUT/space-verdict.json")"

# ---- 9. summary, teardown -----------------------------------------------------------
python3 -I "$REPO/benchmarks/linux/soak-summary.py" "$RUN" > "$OUT/summary.json" 2>/dev/null
RUN="$RUN" bash "$REPO/benchmarks/linux/soak24.sh" teardown >> "$OUT/converge.log" 2>&1
log "done: converge $(cat "$OUT/converge-result"), idempotence $(cat "$OUT/idempotence-result"), snapshot 1 exit $s1, compare exit $cmpr"
