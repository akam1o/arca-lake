# Phase 4.1 follow-up: ADR-0035 measurements (two gateways, skewed access)

Status: **measured**. The atomic-mutation layout (ADR-0035 option B) is
slower than the ADR-0034 path in every case measured; recording only where
tiering acts (option A) is the option the data supports. Results:
`benchmarks/linux/results/phase41-adr35/summary.json`.

## 1. Setup

- Three test hosts: FoundationDB (the dedicated test cluster's, one
  process) and the load generator on the first; the four storage nodes
  and gateway 0 on the second; gateway 1 alone on the third. Two of the
  hosts also run a shared system FoundationDB, which was only checked
  (`status minimal`) before every run and by the watchdog.
- The dedicated cluster started from its existing files (no
  initialisation; the same incarnation; the delete fence as recorded).
  Before the runs, every body of the 10⁵-object data set was read through
  both gateways and checked: 0 errors, mismatches or missing.
- A watchdog (`benchmarks/linux/watchdog.sh`) checked memory, disk, load
  and reachability of the three hosts and both system FoundationDBs every
  10 s, ready to stop the generator: it did not trigger.
- One binary for every arm (the experimental branch: ADR-0034 plus the
  option B prototype behind `--access-records atomic`, not merged): arms
  `off`, `on` (ADR-0034, batches of 128) and `atomic`.
- Workload: GET 4 KiB over 10⁵ objects, 60 s after 10 s of warm-up, one
  read in ten checked; concurrency 32 per gateway (closed loop: the
  in-flight bound is the load limit); the controller stopped. Key
  distributions: uniform; Zipf, s = 1.1; hot set, 1 % of the keys taking
  90 % of the reads. One gateway (uniform) and two (all three
  distributions; the generator alternates between them).
- One discarded warm-up round, then three rounds in rotated arm order
  (36 runs). No run had errors, integrity violations or a saturated
  generator; arms' runs within 0.1–3.4 % of their median.

## 2. Results (medians of three runs)

| Layout, keys | arm | ops/s | vs off | p99 ms | FoundationDB reads / GET | commits / GET | conflicted txns / 1,000 GETs | FoundationDB CPU s / 1,000 GETs | FoundationDB cores used | reads coalesced |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 gateway, uniform | off | 3,970 | — | 11.5 | 6.00 | 0.0001 | 0 | 0.150 | 0.59 | — |
| | on | 3,427 | −13.7 % | 18.4 | 7.95 | 0.0077 | 0 | 0.194 | 0.67 | 3.0 % |
| | atomic | 3,244 | −18.3 % | 24.3 | 6.99 | 0.0078 | 0 | 0.226 | 0.73 | 2.7 % |
| 2 gateways, uniform | off | 4,170 | — | 23.9 | 6.01 | 0.0001 | 0 | 0.227 | 0.95 | — |
| | on | 3,475 | −16.7 % | 33.4 | 8.05 | 0.0080 | 0.27 | 0.282 | 0.98 | 1.4 % |
| | atomic | 3,217 | −22.8 % | 41.5 | 7.00 | 0.0080 | 0 | 0.316 | 1.02 | 1.2 % |
| 2 gateways, Zipf | off | 4,171 | — | 23.4 | 6.00 | 0.0001 | 0 | 0.226 | 0.94 | — |
| | on | 3,858 | −7.5 % | 28.5 | 6.90 | 0.0035 | 0.23 | 0.250 | 0.97 | 58.5 % |
| | atomic | 3,674 | −11.9 % | 32.7 | 6.43 | 0.0036 | 0 | 0.269 | 0.99 | 58.0 % |
| 2 gateways, hot set | off | 4,328 | — | 23.4 | 6.00 | 0.0001 | 0 | 0.212 | 0.92 | — |
| | on | 3,923 | −9.4 % | 28.5 | 7.07 | 0.0040 | 0.40 | 0.242 | 0.95 | 52.2 % |
| | atomic | 3,842 | −11.2 % | 31.5 | 6.50 | 0.0041 | 0 | 0.248 | 0.95 | 50.9 % |

Every arm recording reads wrote all of them (recorded share 1.000), with
nothing dropped or uncertain; no flush failed (the conflicts of `on` were
retried within their transactions).

## 3. Findings

1. **Option B does not pay.** It removes one FoundationDB read per
   flushed generation (7.95 → 6.99 per GET) and every conflict between
   gateways, yet uses more FoundationDB CPU per GET than ADR-0034 (0.226
   against 0.194 s per 1,000 with one gateway) and is slower in every
   case (by 2–6 % of `off`). Consistent with the counters, not isolated:
   an atomic mutation costs the storage server a read of the current
   value when it is applied, and the layout writes two keys per
   generation instead of one, so the read moved from the client's
   transaction to the storage server and grew. ADR-0035's rule (close at
   least half the gap, or remove conflicts that cost throughput) is not
   met.
2. **Conflicts between gateways are not the cost.** With two gateways on
   the same keys, `on` saw 0.23–0.40 conflicted transactions per 1,000
   GETs, all retried successfully; option B's absence of conflicts gained
   nothing.
3. **With two gateways the single FoundationDB process is the
   bottleneck** (0.92–1.02 cores used in every arm; `off` itself grew
   only from 3,970 to 4,170 GET/s). Throughput then follows the
   FoundationDB CPU per GET, which is what access recording adds. A
   cluster with more FoundationDB processes would not saturate the same
   way; not measured.
4. **Skew narrows the gap** (more reads coalesce into one flushed
   generation): 7.5 % (Zipf) and 9.4 % (hot set) below `off`, against
   16.7 % for uniform keys with two gateways.
5. **Tiering accuracy:** within these runs every read was recorded; no
   loss to compare against tiering decisions.

## 4. Not measured

A multi-process FoundationDB; GET 64 KiB and HEAD with two gateways;
option A itself (its cost where no policy acts is that of `off`, measured
here; where one acts, that of `on`); flushes under a FoundationDB stall;
a gateway restart during the runs.
