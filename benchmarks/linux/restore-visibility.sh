#!/usr/bin/env bash
# What a client still running against FoundationDB observes while
# `fdbrestore` restores a backup into it (Phase 3.11, R-94(b)): a private
# fdbserver and backup_agent (own port and data directory; the host's
# system FoundationDB is never touched), a backup of a few keys, the
# keyspace cleared, then a restore while a client writes and reads in a
# loop and records every outcome with its time.
#
#   bash benchmarks/linux/restore-visibility.sh <out-dir>
# Environment: FDB_PORT (4711), FDB_DATA (/mnt/arca-fdb/p311-restore-vis).
set -uo pipefail
OUT="${1:?output directory}"
FDB_PORT="${FDB_PORT:-4711}"
FDB_DATA="${FDB_DATA:-/mnt/arca-fdb/p311-restore-vis}"
mkdir -p "$OUT" "$FDB_DATA/data" "$FDB_DATA/log" "$FDB_DATA/backup"
CF="$FDB_DATA/fdb.cluster"
echo "p311:vis@127.0.0.1:$FDB_PORT" > "$CF"
fdbserver -p "127.0.0.1:$FDB_PORT" -C "$CF" -d "$FDB_DATA/data" -L "$FDB_DATA/log" > "$OUT/fdbserver.out" 2>&1 &
S=$!
AGENT=/usr/lib/foundationdb/backup_agent/backup_agent
"$AGENT" -C "$CF" > "$OUT/agent.out" 2>&1 &
A=$!
cleanup() { kill "$S" "$A" "${W:-}" 2>/dev/null; wait 2>/dev/null; rm -rf "$FDB_DATA"; }
trap cleanup EXIT
sleep 2
fdbcli -C "$CF" --exec 'configure new single ssd' > /dev/null 2>&1
for _ in $(seq 1 30); do fdbcli -C "$CF" --exec 'status minimal' 2>/dev/null | grep -q 'is available' && break; sleep 1; done
fdbcli -C "$CF" --exec 'writemode on; set p311_lineage 1; set p311_a x; set p311_b y' > /dev/null
fdbbackup start -C "$CF" -d "file://$FDB_DATA/backup" -w > "$OUT/backup.out" 2>&1
URL=$(fdbbackup list -b "file://$FDB_DATA/backup" 2>/dev/null | head -1)
echo "backup: $URL" >> "$OUT/backup.out"
# After the backup point, as a running leader would.
fdbcli -C "$CF" --exec 'writemode on; set p311_lineage 2' > /dev/null
# The client: write, then read, in a loop, each with its outcome.
(
  i=0
  while true; do
    i=$((i + 1))
    t=$(date +%s.%N)
    w=$(timeout 5 fdbcli -C "$CF" --timeout 3 --exec "writemode on; set p311_client $i" 2>&1 | tr '\n' ' ' | cut -c1-120)
    r=$(timeout 5 fdbcli -C "$CF" --timeout 3 --exec 'get p311_lineage' 2>&1 | tr '\n' ' ' | cut -c1-120)
    echo "$t write[$w] read[$r]"
    sleep 0.2
  done
) > "$OUT/client.log" 2>&1 &
W=$!
sleep 2
echo "$(date +%s.%N) clear" >> "$OUT/events.log"
fdbcli -C "$CF" --exec 'writemode on; clearrange "" \xff' > /dev/null
echo "$(date +%s.%N) restore start" >> "$OUT/events.log"
fdbrestore start --dest-cluster-file "$CF" -r "$URL" -w > "$OUT/restore.out" 2>&1
echo "$(date +%s.%N) restore end rc=$?" >> "$OUT/events.log"
sleep 3
kill "$W" 2>/dev/null
fdbcli -C "$CF" --exec 'get p311_lineage; get p311_client' > "$OUT/after.txt" 2>&1
echo done
