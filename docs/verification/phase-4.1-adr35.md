# Phase 4.1 follow-up: ADR-0035 measurements (two gateways, skewed access)

Status: **measured**. The atomic-mutation layout (ADR-0035 option B) is
slower than the ADR-0034 path in every case measured; recording only where
tiering acts (option A) is the option the data supports. Results:
`benchmarks/linux/results/phase41-adr35/summary.json`. Option A, since
implemented, was measured on a disposable schema-4 cluster (§6): with
tiering confirmed disabled, GETs perform as with `off`.

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
2. **Conflicts between gateways were not a measurable cost here.** With two gateways on
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
option A itself here (measured once implemented, §6); flushes under a FoundationDB stall;
a gateway restart during the runs.

## 5. Option A: the tests of ADR-0035 §10.5

Option A is implemented in three steps (the tiering record and
`admin tiering`; the controller under the record with schema 4; the
gateways). Each item of §10.5 and the test that covers it; a fix whose
removal fails its test is marked †.

| §10.5 item | Test |
| --- | --- |
| Gateway: each row of §10.3 (no record, enabled, unacknowledged, new term, earlier generation, no leader, expired or released lease, lease within the skew, stale) | `ops::tiering::tests::a_gateway_stops_recording_only_on_a_fresh_acknowledged_disable`; `cluster::backend::tiering_watch::recording_stops_only_while_the_confirmation_is_fresh_and_current` (also another incarnation) |
| Gateway: a read failure and its ageing, a stalled refresh | the same watch test (an observation older than 10 s records); the observation is aged from the read's start |
| Gateway: enabling and disabling reach it within one refresh; a restart | `tiering::a_gateway_records_reads_only_while_tiering_is_not_confirmed_disabled` † |
| Flush: the promotion cutoff (an entry before, across and after it; an old record; the transaction's value, not a gateway's copy) | `cluster::access::tests::the_flush_applies_the_promotion_cutoff_of_the_tiering_record` † |
| Controller: seeding from flags, no seeding without them, the record winning over different flags | `process::tests::the_tiering_record_is_seeded_once_and_acknowledged_per_term_and_incarnation` |
| Controller: the acknowledgment once per generation and term, lost to a racing admin change, term 0 refused | `ops::tiering::tests::the_acknowledgment_names_the_generation_and_stamps_the_grace`, `term_zero_never_acknowledges` |
| Controller: the acknowledgment waits for a running pass; acknowledgment by a leader without a policy (health pass) | the process test above (gate held and released) |
| Controller: a pass whose policy changed (switch refused, nothing reset, cursor kept); its reset refused after a change | `tiering::a_changed_policy_refuses_the_pass_but_not_rebalancing` †; `ops::tiering::tests::a_reset_by_a_pass_of_an_earlier_policy_is_refused` |
| Controller: rebalancing while tiering is disabled | `tiering::a_changed_policy_refuses_the_pass_but_not_rebalancing` |
| Controller: disabling refused below schema 4 (any change leaving tiering disabled) | `ops::tiering::tests::seeding_happens_once_and_disabling_waits_for_the_schema` †; `previous_release::upgrade_and_finalize_to_the_tiering_schema` (CI) |
| Controller: a restore (another incarnation) restarts the grace | the acknowledgment and process tests above |
| Controller: the grace for no record, an old record and a reset record, with a skewed clock; after enabling and after a widening change | `tiering::generations_without_or_with_old_records_wait_for_the_grace`; `tiering::under_the_tiering_record_old_counts_do_not_promote_and_demotion_waits_for_the_grace` †; the stamps in the acknowledgment test |
| Controller: old counts not promoting | the same test (controller side) †, the flush test (writer side) † |
| Controller: disable then enable | the acknowledgment test |
| Schema 4: the upgrade from the last release of schema 3, finalize, the previous release refused; the Phase 3.9 path through that release | `previous_release::upgrade_and_finalize_to_the_tiering_schema`, `upgrade_from_and_rollback_to_the_previous_release` (CI, the upgrade job) |

Not covered by a test: two controllers seeding at the same instant (the
seed is one insert-if-absent transaction, tested sequentially); a real
controller failover between two processes with gateways (each side
tested on its own).

## 6. Option A on a cluster (ADR-0035 §10.6)

Results: `benchmarks/linux/results/phase41-adr35-optiona/` (`summary.json`
medians and every run; `ab.jsonl` the raw FoundationDB and gateway
counters around each run).

### 6.1 Setup

- The same three hosts as §1, but a disposable cluster: its own
  FoundationDB (one process, another port and directory), four storage
  nodes on the second host, gateways on the second and third. It was
  initialised by the current main build, so its store is at schema 4
  from the start (no `upgrade finalize`; the retained schema-3 cluster
  was not touched). 10⁵ × 4 KiB objects were written and every body read
  back and checked: 0 errors, mismatches or missing.
- The controller runs in every arm (it acknowledges the tiering record).
  Its background passes (reconcile, GC, scrub, reclaim, rebalance,
  repair, stripe repair, evict, tiering) run every 7,200 s; health and
  the lease are unchanged. Idle, the store then saw about 70 reads/s
  (under 0.02 per GET at the measured rates).
- Arms, one binary: `off` (`--access-records off`), `rec` (tiering
  enabled with policy tiers `default`: gateways record, no moves), `dis`
  (`admin tiering disable`, acknowledged by the leader: gateways do not
  record). Before and after every run the gateways'
  `arca_gateway_access_recording` gauge was checked against the arm (a
  mismatch would void the run; none did).
- Workload as §1: GET 4 KiB, uniform over 10⁵ objects, 60 s after 10 s
  of warm-up, one read in ten checked, concurrency 32 per gateway; one
  and two gateways. One discarded warm-up round, then four rounds in
  rotated arm order (24 runs). The watchdog did not trigger; no run had
  errors, integrity violations or a saturated generator.
- An earlier campaign with the controller's default intervals was
  discarded: its background passes added 2.7–8.9 FoundationDB reads per
  GET to about half of the runs, in every arm, making the comparison
  noise (spreads of 8–21 %).

### 6.2 Results (medians of four runs)

| Layout | arm | ops/s (four runs) | vs off | p50 / p95 / p99 ms | FoundationDB reads / GET | commits / GET | FoundationDB CPU s / 1,000 GETs | gateway CPU s / 1,000 GETs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 gateway | off | 3,919 (3,893–3,950) | — | 8.11 / 10.18 / 11.69 | 6.01 | 0.0008 | 0.148 | 0.481 |
| | dis | 3,958 (3,946–3,969) | +1.0 % | 8.03 / 9.98 / 11.58 | 6.01 | 0.0007 | 0.146 | 0.476 |
| | rec | 3,432 (3,401–3,471) | −12.4 % | 8.69 / 14.11 / 16.87 | 7.97 | 0.0085 | 0.196 | 0.519 |
| 2 gateways | off | 4,250 (4,159–4,273) | — | 15.12 / 20.97 / 23.85 | 6.01 | 0.0007 | 0.219 | 0.873 |
| | dis | 4,209 (4,150–4,248) | −1.0 % | 15.28 / 21.18 / 24.33 | 6.01 | 0.0007 | 0.221 | 0.874 |
| | rec | 3,460 (3,451–3,511) | −18.6 % | 18.08 / 27.67 / 32.74 | 8.07 | 0.0087 | 0.284 | 0.912 |

### 6.3 Findings

- **Acceptance (§10.6, first criterion) met as observed here.** With
  tiering confirmed disabled, GET throughput is within the spread of
  `off` in both layouts (the four runs of `dis` and `off` overlap: +1.0 %
  and −1.0 % against spreads of 1.4 % and 2.7 %), and so are latency,
  FoundationDB reads and commits per GET and CPU per GET: `dis` does the
  work of `off`. Four runs per arm with overlapping ranges show no
  measurable difference; they are not a statistical proof of
  equivalence. The result holds for this quiet setup (controller
  background passes held off, §6.1). With those passes running, both
  arms are expected to pay their cost alike (the passes do not depend on
  recording), but that is an assumption: the discarded campaign could not
  resolve it from noise.
- Gateways counted every read as unrecorded in `dis` (about 2.8 × 10⁵ per
  run per gateway layout) and none as recorded; in `rec` every read was
  recorded and flushed (reads equal flushed reads in every run; no read
  dropped or uncertain).
- Where tiering acts (`rec`), the cost of ADR-0034 remains (−12.4 % and
  −18.6 % against `off`, as §2 measured on the retained cluster): option
  A removes it only where no policy acts, as decided (R-109 stays
  Medium).
- Also observed on this cluster: after `admin tiering disable` and
  `enable` both gateways' gauges had changed when checked 10 s later
  (the checks were not finer: this bounds the propagation at 10 s, not at
  one 2 s refresh), and after a controller restart (a new term) the new
  leader acknowledged the record. The other criteria of §10.6 are not
  established by this measurement: they rest on the unit and
  integration tests of §5, with the gaps listed there (simultaneous
  seeding, a two-process failover with gateways), and this run made no
  policy-driven moves.
