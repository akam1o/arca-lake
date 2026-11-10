#!/usr/bin/env bash
# Run the FoundationDB-backed test suites on a validation host against a
# private, single-process fdbserver (its own port and data directory; the
# host's system FoundationDB is never touched). Meant to run on the host
# itself, from the repository checkout:
#
#   bash benchmarks/linux/fdb-suite.sh <out-dir> [test filters...]
#
# Environment: FDB_PORT (4700), FDB_DATA (/mnt/arca-fdb/suite-$$),
#   TEST_THREADS (1), FDB_PROCS (1; 3 runs three processes on ports
#   FDB_PORT.. in separate zones, configured triple), FDB_BACKUP (unset;
#   1 starts a backup_agent and a file:// backup directory under FDB_DATA
#   for the restore tests, e.g. fdb_restore::), BACKUP_AGENT (the
#   agent binary; default: the package's).
set -uo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?output directory}"; shift
FILTERS=("${@:-e2e_fdb:: faults_fdb:: cache_fdb:: cache_ec_fdb:: rebalance_fdb::}")
FDB_PORT="${FDB_PORT:-4700}"
FDB_DATA="${FDB_DATA:-/mnt/arca-fdb/suite-$$}"
FDB_PROCS="${FDB_PROCS:-1}"
mkdir -p "$OUT" "$FDB_DATA"
CF="$FDB_DATA/fdb.cluster"
coordinators=""
for i in $(seq 0 $((FDB_PROCS - 1))); do
  coordinators="$coordinators,127.0.0.1:$((FDB_PORT + i))"
done
echo "arca:suite@${coordinators#,}" > "$CF"
FDB_PIDS=()
for i in $(seq 0 $((FDB_PROCS - 1))); do
  mkdir -p "$FDB_DATA/data$i" "$FDB_DATA/log$i"
  fdbserver -p "127.0.0.1:$((FDB_PORT + i))" -C "$CF" -d "$FDB_DATA/data$i" -L "$FDB_DATA/log$i" \
    --locality-zoneid "zone$i" --locality-machineid "machine$i" > "$OUT/fdbserver$i.out" 2>&1 &
  FDB_PIDS+=($!)
done
cleanup() {
  kill "${AGENT_PID:-}" 2>/dev/null
  kill "${FDB_PIDS[@]}" 2>/dev/null; wait "${FDB_PIDS[@]}" 2>/dev/null
  rm -rf "$FDB_DATA"
}
trap cleanup EXIT
sleep 2
if [ "$FDB_PROCS" -ge 3 ]; then
  fdbcli -C "$CF" --exec 'configure new triple ssd' > /dev/null 2>&1
else
  fdbcli -C "$CF" --exec 'configure new single ssd' > /dev/null 2>&1
fi
for _ in $(seq 1 30); do
  fdbcli -C "$CF" --exec 'status minimal' 2>/dev/null | grep -q 'is available' && break
  sleep 1
done
fdbcli -C "$CF" --exec 'status minimal' | tee "$OUT/fdb-status.txt"
source "$HOME/.cargo/env"
export ARCA_FDB_TESTS=1 ARCA_FDB_CLUSTER_FILE="$CF"
if [ -n "${FDB_BACKUP:-}" ]; then
  mkdir -p "$FDB_DATA/backup"
  "${BACKUP_AGENT:-/usr/lib/foundationdb/backup_agent/backup_agent}" -C "$CF" > "$OUT/backup_agent.out" 2>&1 &
  AGENT_PID=$!
  export ARCA_FDB_BACKUP_URL="file://$FDB_DATA/backup"
fi
start=$(date +%s)
cargo test -p arca-lake-testkit --features fdb --test it -- --nocapture \
  --test-threads="${TEST_THREADS:-1}" ${FILTERS[@]} > "$OUT/tests.log" 2>&1
rc=$?
echo "rc=$rc seconds=$(( $(date +%s) - start ))" | tee "$OUT/result.txt"
grep -E '^test .*(ok|FAILED)$|test result' "$OUT/tests.log" | tail -40 > "$OUT/summary.txt"
exit "$rc"
