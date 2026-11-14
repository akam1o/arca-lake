# Phase 3.14 soak settings (soak24.sh; directories replaced by placeholders)
export RUN=<run-dir on the data disk> FDB_DIR=<directory on another disk> DURATION_SECS=86400
export NODE_EXTRA_ARGS="--capacity-bytes 3221225472 --reclaim-retention-secs 7200 --reclaim-skew-secs 60"
export CONTROLLER_EXTRA_ARGS="--rebalance --rebalance-interval-secs 60"
# Faults stopped at 2026-11-14T00:24:16Z (touch stop-faults); the workload
# stopped with SIGTERM after its 7th periodic full verification
# (soak-workload-stopped, epoch seconds); then soak24.sh finish and teardown.
