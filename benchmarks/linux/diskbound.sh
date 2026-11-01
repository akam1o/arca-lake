#!/usr/bin/env bash
# Disk-bound measurements on top of two-host.sh (Phase 2.2 §6): a data set
# larger than the page cache of every node, read sequentially and in random
# order, large and small objects, and concurrent PUT/GET. Nothing here
# drops caches, formats, or uses direct I/O; the data set size alone makes
# the storage device the bound, and the page cache state is recorded
# (`free -m` and vmstat) before and after each phase.
#
#   HOST_A=.. HOST_B=.. [HOST_C=..] benchmarks/linux/diskbound.sh <out> [large_gib] [small_count]
# Durable only (no cache tier), RS 2+1, 4 MiB chunks.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?results dir}"; LARGE_GIB="${2:-24}"; SMALL_COUNT="${3:-8192}"
TH=benchmarks/linux/two-host.sh
export OUT CHUNK_BYTES=$((4 << 20))
MIB=$((1 << 20))
LARGE_OBJECTS=$((LARGE_GIB * 1024 / 16))

mem() { # record memory and cache state on every host into <file>
  local f="$1"; : > "$f"
  for h in "$HOST_A" "$HOST_B" ${HOST_C:-}; do
    echo "== host" >> "$f"
    ssh -o BatchMode=yes "$h" 'free -m; vmstat 1 2 | tail -1' >> "$f" 2>&1
  done
}

bash "$TH" setup durable >/dev/null 2>&1
mkdir -p "$OUT"
echo "large_gib=$LARGE_GIB large_objects=$LARGE_OBJECTS small_count=$SMALL_COUNT" > "$OUT/plan.txt"

echo "== large objects: PUT $LARGE_GIB GiB (16 MiB objects), then sequential and random GET"
mem "$OUT/mem-before.txt"
# PUT at concurrency 4 with the driver's PUT phase, then GET passes:
# sequential first touch, random, random again (order fixed by the seed).
OBJECTS=$LARGE_OBJECTS SIZE=$((16 * MIB)) CONC=4 PASSES=1 bash "$TH" run large-seq 2>&1 | grep -E '^\{' | cut -c1-110
mem "$OUT/mem-after-put.txt"
SKIP_PUT=1 OBJECTS=$LARGE_OBJECTS SIZE=$((16 * MIB)) CONC=4 PASSES=2 S3LOAD_EXTRA="--order random" bash "$TH" run large-random 2>&1 | grep -E '^\{' | cut -c1-110
mem "$OUT/mem-after-random.txt"
echo "== small objects: $SMALL_COUNT × 256 KiB, sequential and random"
# Under their own prefix: the default key space holds the large objects,
# which the mixed phase below reads again.
PREFIX=small OBJECTS=$SMALL_COUNT SIZE=$((256 * 1024)) CONC=8 PASSES=1 bash "$TH" run small-seq 2>&1 | grep -E '^\{' | cut -c1-110
PREFIX=small SKIP_PUT=1 OBJECTS=$SMALL_COUNT SIZE=$((256 * 1024)) CONC=8 PASSES=2 S3LOAD_EXTRA="--order random" bash "$TH" run small-random 2>&1 | grep -E '^\{' | cut -c1-110
echo "== concurrent PUT and GET (large, concurrency 4 each)"
SKIP_PUT=1 OBJECTS=$LARGE_OBJECTS SIZE=$((16 * MIB)) CONC=4 PASSES=1 S3LOAD_EXTRA="--order random" MIXED_PUT_OBJECTS=256 bash "$TH" run mixed 2>&1 | grep -E '^\{' | cut -c1-110
mem "$OUT/mem-after.txt"
bash "$TH" teardown >/dev/null 2>&1 || true
bash benchmarks/linux/sanitize.sh "$OUT" >/dev/null
echo "done: $OUT"
