#!/usr/bin/env bash
# FoundationDB client TLS versus plaintext (Phase 3.4 §2): the same
# metadata transactions (metabench) against two identical three-process
# test clusters made by deploy/dev/fdb-tls-cluster.sh, one with TLS and
# one without, on the same host. Transports alternate within each
# repetition so that drift of shared storage affects both alike.
#
#   fdb-tls-perf.sh TLS_DIR PLAIN_DIR [REPS] [SECONDS]
#
# Output: one metabench JSON report per run on stdout.
set -euo pipefail
TLS_DIR="${1:?TLS cluster directory}"
PLAIN_DIR="${2:?plaintext cluster directory}"
REPS="${3:-3}"
SECS="${4:-20}"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
MB="$REPO/target/release/metabench"

run() { # transport mode tasks rep
  local t="$1" mode="$2" tasks="$3" rep="$4"
  if [ "$t" = tls ]; then
    (. "$TLS_DIR/env.sh"; "$MB" --backend fdb --cluster-file "$TLS_DIR/fdb.cluster" \
      --mode "$mode" --tasks "$tasks" --seconds "$SECS" --label "rep$rep")
  else
    (unset ARCA_FDB_TLS_CA ARCA_FDB_TLS_CERT ARCA_FDB_TLS_KEY ARCA_FDB_TLS_VERIFY_PEERS
     "$MB" --backend fdb --cluster-file "$PLAIN_DIR/fdb.cluster" \
      --mode "$mode" --tasks "$tasks" --seconds "$SECS" --label "rep$rep")
  fi
}

for rep in $(seq 1 "$REPS"); do
  for mode in insert-remove touch; do
    for tasks in 8 64; do
      if [ $((rep % 2)) -eq 1 ]; then order="plain tls"; else order="tls plain"; fi
      for t in $order; do run "$t" "$mode" "$tasks" "$rep"; done
    done
  done
done
