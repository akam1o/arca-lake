# Disk-bound scrub, run 1 (Phase 3.4 §5)

`HOST_A=… HOST_B=… REPS=3 MODES="none 32 adaptive" WINDOW=120
benchmarks/linux/scrub-diskbound.sh` on two 4-vCPU, 7.8 GiB virtual
machines with shared virtual storage; 1024 objects × 16 MiB (24 GiB of
shards, larger than RAM), random 1 MiB range GETs and PUTs from host B.

**The `adaptive` windows of this run are not adaptive.** `admin integrity
scrub --wait --adaptive` went through `scrub_group_now`, which ignored the
adaptive policy (fixed in 9ca323b): those windows ran at the fixed
32 MiB/s, which is why their scrub rate is exactly 32.0 MiB/s. They count as
three more fixed-32 repetitions. Run 2 measures the adaptive budget.

Medians of three windows per mode (`summary.json`):

| mode | GET MiB/s | GET p50 / p95 / p99 ms | PUT MiB/s | scrub MiB/s | host A iowait | queue depth |
| --- | --- | --- | --- | --- | --- | --- |
| none | 49.5 | 72.5 / 142.9 / 180.3 | 26.0 | 0 | 30.0 % | 8.3 |
| fixed 32 MiB/s | 44.1 | 81.8 / 154.4 / 183.7 | 24.7 | 32.0 | 24.6 % | 8.4 |
| "adaptive" (fixed 32, see above) | 44.7 | 79.7 / 150.9 / 184.8 | 25.0 | 32.0 | 30.7 % | 8.8 |

One no-scrub window (`none-r2`, 69.9 MiB/s) is an outlier of the shared
storage; the medians exclude its effect. At 32 MiB/s a full pass over the
24 GiB of shards takes about 0.21 h. No request failed in any window.
