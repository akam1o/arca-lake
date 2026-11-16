#!/usr/bin/env bash
# Phase 4.0 object-count scaling (real S3 PUTs). One bucket is populated
# in steps (CHECKPOINTS, cumulative), and at each checkpoint three rounds
# in rotated order measure GET and HEAD of uniformly random populated keys
# (bodies verified, one read in ten), PUT and then DELETE of fresh keys in
# a prefix of their own, and a full ListObjectsV2 of the bucket (its key
# count checked). At the largest checkpoints a scrub pass of every group
# is timed. Needs a cluster from bench-cluster.sh setup (same environment).
#
#   HOST_A=... HOST_B=... OUT=... benchmarks/linux/object-count.sh
# Environment: CHECKPOINTS ("1000 10000 100000 1000000"), SIZE (1024),
#   CONC (32), OC_WINDOW (30 s, after 10 s of warm-up; not WINDOW, which
#   screen(1) sets), NEW (key range of a
#   timed PUT sample, 200000; its keys are deleted after it in one pass),
#   SCRUB_FROM (100000), RUN_DIR (as bench-cluster.sh), OC_HAVE (keys
#   already populated: resume after them [0]). The population retries
#   503 (write admission throttles a fast fill): a key it failed to write
#   would read as lost.
set -uo pipefail
cd "$(dirname "$0")/../.."
B=benchmarks/linux/bench-cluster.sh
CHECKPOINTS="${CHECKPOINTS:-1000 10000 100000 1000000}"
SIZE="${SIZE:-1024}"; CONC="${CONC:-32}"; WINDOW="${OC_WINDOW:-30}"; NEW="${NEW:-200000}"
SCRUB_FROM="${SCRUB_FROM:-100000}"; RUN_DIR="${RUN_DIR:-/mnt/arca-fdb/p40-run}"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/object-count.log"; }
mkdir -p "$OUT"
bash $B run oc-buckets --workload create-buckets --buckets 1 --bucket-prefix oc > /dev/null
have="${OC_HAVE:-0}"
for n in $CHECKPOINTS; do
  [ -n "${QUIESCE_EACH:-}" ] && export QUIESCE=1
  log "populate $have..$n"
  bash $B run "oc-$n-populate" --workload put --bucket-prefix oc --prefix obj --first "$have" --objects $((n - have)) \
    --size "$SIZE" --concurrency 64 --retries 30 > /dev/null || log "populate rc=$?"
  have=$n
  for round in 1 2 3; do
    case $round in 1) order="get head put list" ;; 2) order="head put list get" ;; 3) order="put list get head" ;; esac
    for w in $order; do
      case $w in
        get|head) CLIENT_BASE=$((round * 16)) bash $B run "oc-$n-$w-r$round" --workload "$w" --bucket-prefix oc --prefix obj --objects "$n" \
                    --size "$SIZE" --concurrency "$CONC" --duration-secs "$WINDOW" --warmup-secs 10 --verify-every 10 \
                    > /dev/null || log "$w rc=$?" ;;
        put) st="$RUN_DIR/oc-new$n-$round.state"
             bash $B run "oc-$n-put-r$round" --workload put --bucket-prefix oc --prefix "new$n-$round" --objects "$NEW" \
               --size "$SIZE" --concurrency "$CONC" --duration-secs "$WINDOW" --warmup-secs 10 --state-file "$st" > /dev/null || log "put rc=$?"
             bash $B run "oc-$n-delete-r$round" --workload delete --bucket-prefix oc --prefix "new$n-$round" --objects "$NEW" \
               --concurrency "$CONC" --state-file "$st" > /dev/null || log "delete rc=$?" ;;
        list) bash $B run "oc-$n-list-r$round" --workload list --bucket-prefix oc --prefix obj --objects "$n" \
                --expect-count "$n" --concurrency 1 > /dev/null || log "list rc=$?" ;;
      esac
    done
  done
  if [ "$n" -ge "$SCRUB_FROM" ]; then
    log "scrub at $n"
    bash $B scrub "oc-$n-scrub" || log "scrub rc=$?"
  fi
  bash $B snapshot "oc-$n-state" || true
done
log "done"
