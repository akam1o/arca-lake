#!/usr/bin/env bash
# Repeated, order-randomized measurements on top of two-host.sh (RFC-009
# §4, Phase 2.2 §5). Every configuration is set up fresh for each
# repetition; the order of configurations is shuffled per repetition from
# a seed; one warm-up PUT+GET pass precedes the measured passes.
#
#   HOST_A=.. HOST_B=.. [HOST_C=..] benchmarks/linux/repeat.sh <results-dir> [repeats]
#
# Configurations: CONFIGS (space separated "mode:conc[:extra]"), default
# "durable:4 none:4 repl:4 ec:4"; extra "multipart" uploads with multipart
# uploads (labels "<mode>-c<conc>-multipart-r<N>"). Environment: OBJECTS SIZE PASSES SEED
# CHUNK_BYTES CACHE_CAPACITY CACHE_MAX_POPULATIONS (passed to two-host.sh).
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?results directory}"
REPEATS="${2:-3}"
TH=benchmarks/linux/two-host.sh
CONFIGS="${CONFIGS:-durable:4 none:4 repl:4 ec:4}"
SEED="${SEED:-1}"
export OUT OBJECTS="${OBJECTS:-96}" SIZE="${SIZE:-$((16 << 20))}" PASSES="${PASSES:-3}"
export CHUNK_BYTES="${CHUNK_BYTES:-$((4 << 20))}"

shuffle() { # deterministic shuffle of "$@" from SEED and the repetition
  local rep="$1"; shift
  printf '%s\n' "$@" | awk -v seed="$((SEED * 7919 + rep))" 'BEGIN{srand(seed)} {print rand() "\t" $0}' | sort -n | cut -f2-
}

mkdir -p "$OUT"
echo "repeats=$REPEATS configs=\"$CONFIGS\" seed=$SEED objects=$OBJECTS size=$SIZE passes=$PASSES" > "$OUT/plan.txt"
for rep in $(seq 1 "$REPEATS"); do
  for cfg in $(shuffle "$rep" $CONFIGS); do
    mode="${cfg%%:*}"; rest="${cfg#*:}"; conc="${rest%%:*}"
    extra="${rest#*:}"; [ "$extra" = "$rest" ] && extra=""
    label="$mode-c$conc${extra:+-$extra}-r$rep"
    # extra "multipart": the PUT phase uses multipart uploads (8 MiB parts).
    if [ "$extra" = multipart ]; then export PUT_OP=multipart-put PUT_EXTRA="--part-size $((8 << 20))"; else unset PUT_OP PUT_EXTRA; fi
    echo "== repetition $rep: $label"
    bash "$TH" setup "$mode" >/dev/null 2>&1
    # Warm-up: one PUT and one GET pass on a separate key range, not recorded.
    CONC="$conc" PASSES=1 OBJECTS=16 bash "$TH" run "warmup-$label" >/dev/null 2>&1 || true
    rm -rf "$OUT/warmup-$label"
    CONC="$conc" bash "$TH" run "$label" 2>&1 | grep -E '^\{' | cut -c1-110
    echo "$cfg" > "$OUT/$label/config.txt"
  done
done
bash "$TH" teardown >/dev/null 2>&1 || true
bash benchmarks/linux/sanitize.sh "$OUT" >/dev/null
echo "done: $OUT"
