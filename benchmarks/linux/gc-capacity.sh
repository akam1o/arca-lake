#!/usr/bin/env bash
# GC capacity matrix (Phase 3.5, R-64) on the soak cluster layout
# (soak24.sh setup: 4 durable + 2 cache nodes, 2 gateways, 2 controllers,
# 3 FoundationDB processes, TLS everywhere; GC every 60 s, grace 300 s).
# No soak workload and no faults: for each mode, gcload steps the
# retirement rate up, then the load stops and the drain is timed. A
# sampler records the leader controller's GC gauges and counters and the
# nodes' compaction gauges every 15 s.
#
#   RUN=~/arca-gccap OUT=~/arca-gccap-out gc-capacity.sh
#
# Output: OUT/<mode>.jsonl (gcload steps), OUT/samples.jsonl, OUT/drain.txt.
set -uo pipefail
RUN="${RUN:?}"; OUT="${OUT:?}"
REPO="${REPO:-$HOME/arca-lake}"
GCLOAD="$REPO/target/release/gcload"
RATES="${RATES:-20,40,80,160,320}"
STEP_SECS="${STEP_SECS:-240}"
DRAIN_MAX_SECS="${DRAIN_MAX_SECS:-1200}"
MODES="${MODES:-put-only overwrite put-delete versioned multipart overwrite-get overwrite-scrub}"
AK="soak-ak"; SK="soak-secret-key-not-a-real-credential"
mkdir -p "$OUT"
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

leader_metrics() {
  for p in 9550 9551; do
    m="$(curl -s -m 5 "http://127.0.0.1:$p/metrics")"
    if echo "$m" | grep -q '^arca_controller_leader 1'; then echo "$m"; return; fi
  done
}
value() { awk -v n="$1" '$1==n {print $2; exit}'; }

sampler() {
  while [ ! -f "$OUT/stop" ]; do
    m="$(leader_metrics)"
    line="{\"t\":$(date +%s)"
    for n in arca_controller_gc_backlog arca_controller_gc_backlog_oldest_age_seconds arca_controller_gc_collected_total arca_controller_shards_deleted_total arca_controller_gc_passes_total; do
      line+=",\"${n#arca_controller_}\":$(echo "$m" | value "$n" | sed 's/^$/null/')"
    done
    for p in 9700 9701 9702 9703; do
      nm="$(curl -s -m 5 "http://127.0.0.1:$p/metrics")"
      line+=",\"n$p\":{\"segment_bytes\":$(echo "$nm" | value arca_node_segment_bytes | sed 's/^$/null/'),\"live_bytes\":$(echo "$nm" | value arca_node_live_bytes | sed 's/^$/null/'),\"compaction_copied\":$(echo "$nm" | value arca_node_compaction_copied_bytes_total | sed 's/^$/null/'),\"anon\":$(echo "$nm" | awk '$1=="arca_node_process_memory_bytes{kind=\"anon\"}" {print $2}' | sed 's/^$/null/')}"
    done
    echo "$line}" >> "$OUT/samples.jsonl"
    sleep 15
  done
}

backlog() { leader_metrics | value arca_controller_gc_backlog; }

sampler & SAMPLER=$!
echo "$(ts) start" >> "$OUT/drain.txt"
for mode in $MODES; do
  base="$mode"; extra=()
  case "$mode" in
    overwrite-get) base=overwrite; extra=(--get-pct 50) ;;
    overwrite-scrub) base=overwrite
      # Scrub every durable group while the load runs.
      for g in 1 2 3 4 5 6; do
        env ARCA_FDB_TLS_CA="$RUN/fdb/pki/ca/ca.pem" ARCA_FDB_TLS_CERT="$RUN/fdb/pki/admin/cert.pem" ARCA_FDB_TLS_KEY="$RUN/fdb/pki/admin/key.pem" \
          ARCA_FDB_TLS_VERIFY_PEERS="Check.Valid=1,S.subjectAltName>=URI:spiffe://fdb-test.arca-lake.invalid/fdb/" \
          ARCA_TLS_CA="$RUN/pki/ca/ca.pem" ARCA_TLS_CERT="$RUN/pki/admin/current/cert.pem" ARCA_TLS_KEY="$RUN/pki/admin/current/key.pem" \
          ARCA_TRUST_DOMAIN=soak.arca-lake.invalid "$REPO/target/release/arca-lake" admin --cluster-file "$RUN/fdb/fdb.cluster" integrity scrub --group "$g" >/dev/null 2>&1
      done ;;
  esac
  echo "$(ts) $mode load" >> "$OUT/drain.txt"
  "$GCLOAD" --endpoint http://127.0.0.1:9500 --access-key "$AK" --secret-key "$SK" \
    --bucket "gc-${mode}" --mode "$base" --rates "$RATES" --step-secs "$STEP_SECS" "${extra[@]}" \
    > "$OUT/$mode.jsonl" 2> "$OUT/$mode.err"
  t0=$(date +%s)
  while :; do
    b="$(backlog)"; now=$(date +%s)
    if [ "${b:-x}" = 0 ]; then echo "$(ts) $mode drained in $((now - t0)) s" >> "$OUT/drain.txt"; break; fi
    if [ $((now - t0)) -ge "$DRAIN_MAX_SECS" ]; then echo "$(ts) $mode NOT drained in $DRAIN_MAX_SECS s (backlog $b)" >> "$OUT/drain.txt"; break; fi
    sleep 15
  done
done
touch "$OUT/stop"; wait "$SAMPLER"
echo "$(ts) finished" >> "$OUT/drain.txt"
