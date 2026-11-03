# Disk-bound scrub, run 2 (Phase 3.4 §5): off / fixed 32 MiB/s / adaptive

Same setup as run 1 (two 4-vCPU, 7.8 GiB virtual machines on shared
virtual storage; 1024 × 16 MiB objects, 24 GiB of shards; random 1 MiB
range GETs and PUTs from host B; 120 s windows; three repetitions, mode
order rotated per repetition: r1 none/32/adaptive, r2 32/adaptive/none, r3
adaptive/none/32). Build 9ca323b (adaptive policy applied by
`scrub --wait --adaptive`; nodes report GET serve p99, PUT store p99 and
device utilisation/queue depth). Adaptive: start 32 MiB/s, floor 1 MiB/s,
ceiling 128 MiB/s, targets utilisation 85 %, queue depth 4, GET p99 50 ms,
PUT p99 250 ms. No request failed in any window.

Per window (`summary.json`):

| window | GET MiB/s | GET p50 / p95 / p99 ms | PUT MiB/s | scrub MiB/s | host A queue depth |
| --- | --- | --- | --- | --- | --- |
| none-r1 | 79.7 | 38.7 / 113.1 / 150.2 | 20.4 | 0 | 9.6 |
| 32-r1 | 75.1 | 40.8 / 115.4 / 162.1 | 26.1 | 32.0 | 11.2 |
| adaptive-r1 | 77.1 | 39.8 / 115.3 / 158.7 | 25.5 | 1.6 | 11.5 |
| 32-r2 | 74.7 | 41.5 / 117.7 / 164.0 | 25.7 | 32.0 | 12.0 |
| adaptive-r2 | 72.1 | 43.5 / 117.3 / 141.0 | 29.0 | 1.5 | 10.1 |
| none-r2 | 64.0 | 49.8 / 123.9 / 142.6 | 37.7 | 0 | 7.3 |
| adaptive-r3 | 46.4 | 75.1 / 153.8 / 202.4 | 24.8 | 1.5 | 8.6 |
| none-r3 | 48.0 | 75.0 / 150.4 / 202.9 | 24.4 | 0 | 8.7 |
| 32-r3 | 43.6 | 80.6 / 158.1 / 209.4 | 23.9 | 32.0 | 9.5 |

Relative to the no-scrub window of the same repetition:

| | GET MiB/s | GET p50 | GET p99 | PUT MiB/s |
| --- | --- | --- | --- | --- |
| fixed 32, r1 / r3 | −5.8 % / −9.2 % | +5.4 % / +7.5 % | +7.9 % / +3.2 % | +27.9 % / −2.0 % |
| adaptive, r1 / r3 | −3.3 % / −3.3 % | +2.8 % / +0.1 % | +5.7 % / −0.2 % | +25.0 % / +1.6 % |

(r2 is left out of this comparison: its no-scrub window ran last and the
storage slowed during the repetition, so both scrub modes come out ahead
of it. PUT varies by ±30 % between adjacent windows whatever the mode.)

Reading:

- The client load alone keeps host A's queue depth at 7–10, above the
  adaptive target of 4: the adaptive budget halved down to its floor
  within seconds and stayed there (1.5–1.6 MiB/s including the calm
  samples' steps). Scrubbing never stopped; a full pass of the 24 GiB at
  that rate takes about 4.5 h instead of 0.21 h.
- At that rate the client impact is about half of the fixed budget's in
  the two clean repetitions, but the variance of the shared storage
  between repetitions (r3 is about 40 % slower for every mode) is larger
  than either effect: three repetitions do not establish the difference
  statistically.
- Decision (ADR-0018): the default stays the fixed budget; the adaptive
  budget is opt-in. With it, the floor bounds the scrub interval on a
  busy device: set `--scrub-min-rate-mib` at least to the device's shard
  bytes divided by the scrub interval (24 GiB per day needs 0.3 MiB/s;
  10 TiB per day needs 121 MiB/s, which no floor on a busy device can
  give: lengthen the interval or add devices).
