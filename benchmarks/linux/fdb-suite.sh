#!/usr/bin/env bash
# Run the FoundationDB-backed test suites on a validation host against a
# private, single-process fdbserver (its own port and data directory; the
# host's system FoundationDB is never touched). Meant to run on the host
# itself, from the repository checkout:
#
#   bash benchmarks/linux/fdb-suite.sh <out-dir> [test filters...]
#
# Environment: FDB_PORT (4700), FDB_DATA (/mnt/arca-fdb/suite-$$),
#   TEST_THREADS (1).
set -uo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?output directory}"; shift
FILTERS=("${@:-e2e_fdb:: faults_fdb:: cache_fdb:: cache_ec_fdb:: rebalance_fdb::}")
FDB_PORT="${FDB_PORT:-4700}"
FDB_DATA="${FDB_DATA:-/mnt/arca-fdb/suite-$$}"
mkdir -p "$OUT" "$FDB_DATA/data" "$FDB_DATA/log"
CF="$FDB_DATA/fdb.cluster"
echo "arca:suite@127.0.0.1:$FDB_PORT" > "$CF"
fdbserver -p "127.0.0.1:$FDB_PORT" -C "$CF" -d "$FDB_DATA/data" -L "$FDB_DATA/log" > "$OUT/fdbserver.out" 2>&1 &
FDB_PID=$!
cleanup() {
  kill "$FDB_PID" 2>/dev/null; wait "$FDB_PID" 2>/dev/null
  rm -rf "$FDB_DATA"
}
trap cleanup EXIT
sleep 2
fdbcli -C "$CF" --exec 'configure new single ssd' > /dev/null 2>&1
for _ in $(seq 1 30); do
  fdbcli -C "$CF" --exec 'status minimal' 2>/dev/null | grep -q 'is available' && break
  sleep 1
done
fdbcli -C "$CF" --exec 'status minimal' | tee "$OUT/fdb-status.txt"
source "$HOME/.cargo/env"
export ARCA_FDB_TESTS=1 ARCA_FDB_CLUSTER_FILE="$CF"
start=$(date +%s)
cargo test -p arca-lake-testkit --features fdb --test it -- --nocapture \
  --test-threads="${TEST_THREADS:-1}" ${FILTERS[@]} > "$OUT/tests.log" 2>&1
rc=$?
echo "rc=$rc seconds=$(( $(date +%s) - start ))" | tee "$OUT/result.txt"
grep -E '^test .*(ok|FAILED)$|test result' "$OUT/tests.log" | tail -40 > "$OUT/summary.txt"
exit "$rc"
