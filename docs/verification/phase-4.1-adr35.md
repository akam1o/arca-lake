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

The two items this table left uncovered, two controllers seeding at the
same instant and a real controller failover between two processes with
gateways, are covered since by §7 (forced races on FoundationDB, and real
processes on three hosts); §7.6 says for every item of §10.5 and §10.6 at
which level it is verified.

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
  and two gateways. One discarded warm-up round, then four rounds (24
  runs) in the arm orders off/rec/dis, rec/dis/off, dis/off/rec and
  off/dis/rec. The watchdog did not trigger; no run had errors,
  integrity violations or a saturated generator (`client_saturated` in
  `summary.json`).
- Per-GET FoundationDB figures are counter differences from 30 s of idle
  before a run to 3 s after it; FoundationDB's status counters did not
  always refresh within those 3 s, so the windows carry a few seconds of
  slack, alike in every arm.
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
  run with one gateway, 2.9–3.0 × 10⁵ with two together) and none as
  recorded; in `rec` every read was
  recorded and flushed (reads equal flushed reads in every run; no read
  dropped or uncertain).
- Where tiering acts (`rec`), the cost of ADR-0034 remains (−12.4 % and
  −18.6 % against `off`, close to §2's −13.7 % and −16.7 % on the
  retained cluster): option
  A removes it only where no policy acts, as decided (R-109 stays
  Medium).
- Also observed by the operator on this cluster (not in the committed
  data): after `admin tiering disable` and
  `enable` both gateways' gauges had changed when checked 10 s later
  (the checks were not finer: this bounds the propagation at 10 s, not at
  one 2 s refresh), and after a controller restart (a new term) the new
  leader acknowledged the record. The other criteria of §10.6 are not
  established by this measurement: they rest on the unit and
  integration tests of §5, with the gaps listed there (simultaneous
  seeding, a two-process failover with gateways), and this run made no
  policy-driven moves.

## 7. Option A: integration on three hosts (ADR-0035 §10.5, §10.6)

Two controller processes, two gateways and storage nodes on a real
FoundationDB, driven by `benchmarks/linux/adr35-integration.py` (load:
`adr35-loader.py`; summary: `adr35-summary.py`). Results:
`benchmarks/linux/results/phase41-adr35-integration/` (`summary.json`, the
final run: every check, the timing tables and the leader-change
timelines; `runs.json`, every run including the failed ones; `fdb-races.txt`,
the forced races on FoundationDB). This is a functional verification, not
a performance one: the limits of §6 (the quiet measurement) stand.

### 7.1 Setup

- A disposable cluster of its own (own FoundationDB process, port and
  directories, initialised by this build: schema 4 from the start, no
  `upgrade finalize`); the retained cluster and the hosts' system
  FoundationDB untouched (only checked, `status minimal`, before and after
  every run and by the watchdog). Removed afterwards.
- First host: FoundationDB, controller `ctl-a`, the load generator and the
  admin commands. Second: six storage nodes (three of class hdd, tier
  `default`; three of class ssd, tier `fast`; one 2+1 group each), gateway
  `gw0`. Third: controller `ctl-c`, gateway `gw1`. The controllers on
  different hosts; lease 10 s, health pass 5 s, tiering pass 5 s,
  `--tiering-clock-skew-secs 5` (grace margin 47 s), gateways' lease skew
  2 s. The hosts' clocks NTP-synchronised; their offsets, measured at the
  start and end of the run, agreed within 1 ms.
- 213 objects of 64 KiB (`o000`–`o199`, ten hot objects, three for the
  cutoff and the held move), read back through both gateways (0 bad).
- Load: closed loop, at most two requests in flight and 20 GET/s per
  gateway (10 for the hot set), anonymous GETs logged one per line by the
  generator (the independent count). Deadlines: every wait at most
  30–240 s, the run about 30 minutes; the watchdog (memory, disk, load,
  reachability, both system FoundationDBs) did not trigger.
- One binary for every process, built with `--features
  fdb,fault-injection`. The final run is `20261120T045036Z-run` (82
  checks, all passed), with the binary built from commit b7396ab; `runs.json`
  lists every run before it, the failed ones included, with the binary
  each used. How b7396ab relates to the merged change is in §7.9. The feature adds, for the harness only, a control
  file (`ARCA_TEST_FAULT_FILE`) through which a gateway's combined tiering
  read fails or is held before it is observed, and a controller's
  tiering move is held before its switch; release builds do not contain
  it, and a binary that does says so at start.
- Diagnostics added for the timing evidence: gateways log every combined
  tiering read at debug (wall-clock start and end, what it observed),
  each change of what the decision rests on at info, and each change of
  the recording decision with the age of its observation; controllers log
  the seed with its policy and term, each acknowledgment with the grace
  and the cutoff, and once per stretch as a follower who holds the lease;
  `admin tiering objects` prints each object's tier and access record.

### 7.2 Concurrent initialisation (§10.5 "seeding by two controllers at once")

Forced races (`crates/metadata/tests/tiering_race.rs`, in memory and on
FoundationDB): a test-only store wrapper holds a participant's first
commit after its reads until the test releases it, so that both read
before either commits, or the other commits in between; every attempt and
commit outcome is logged (`fdb-races.txt`).

| Race | Order forced | Outcome on FoundationDB |
| --- | --- | --- |
| two seeds, different flags | both read "no record", a commits first | b's commit conflicts; its retry sees a's record and writes nothing; the record is a's policy, whole, generation 1 |
| two seeds, equal flags | b first | a conflicts, retries, writes nothing; written once |
| a deposed leader's seed against its successor's | the old leader holds after reading under its term; the successor takes the lease and seeds | the old commit conflicts; its retry is fenced (`leadership token 1 … is no longer current`); the successor's policy stands |
| a seed against `admin tiering enable` | each way | admin first: the seed's retry writes nothing; seed first: the admin change applies on top (generation 2, the admin's policy); never a mix |
| an acknowledgment against an admin change | each way | an acknowledgment that read generation 1 never names generation 2 |

Removing the insert-if-absent check, the acknowledgment's generation
condition or the fence makes these tests fail (`mutations.txt`).

Real processes (scenarios A1, A2): both controllers started at the same
wall-clock instant on a store without a record (A1 equal flags, A2
different flags after clearing the record of the disposable store). They
started 2.2 ms and 1.4 ms apart; one took the lease and seeded 7 and 8 ms
later, the other logged that it stood by 2.2 ms and 5.7 ms after the lease
was taken. In deployment the seed is serialised by leader election: only
the leader seeds, through its fenced store, so two controllers do not seed
at once; the unfenced race above is the operation's own guarantee. In
both runs the record was the leader's flags, whole, at generation 1,
acknowledged by the leader's term (1, then 2); gateways recorded
throughout (no record, then an enabled one). In B1 the controller whose
flags differ became leader: it logged the difference and ran from the
record, whose policy stayed unchanged.

### 7.3 Reflection and stale confirmations (§10.6 third criterion)

C1, three rounds of `disable`, `set` while disabled, `enable` and `set`
while enabled under load (24 measurements, both gateways). The target
state is the leader's acknowledgment (disable, a set), or the admin commit
(enable: an enabled record is recorded without acknowledgment).

| Change | admin commit to acknowledgment | target to the start of the first refresh after it | that refresh reflects it | recording decision changed |
| --- | --- | --- | --- | --- |
| disable | 0.57–4.55 s | 0.23–0.60 s | every time | 0.24–0.62 s after the target |
| set while disabled | 0.06–0.48 s | 1.19–1.56 s | every time | (acknowledged before any refresh saw the new generation: stayed not recording) |
| enable | 0.48–0.60 s (not needed) | 0.73–1.00 s | every time | 0.83–1.04 s |
| set while enabled | 0.15–0.51 s | 1.11–1.48 s | every time | (stays recording) |

Every change was reflected by the first refresh that started after its
target state, on both gateways; refreshes started every 2.003–2.007 s.
The acknowledgment follows the leader's health pass (every 5 s; the
ranges differ between runs with where the pass falls). A set while
disabled makes gateways record from the first refresh that sees the
unacknowledged generation until the one after the acknowledgment, as
§10.3 requires: in an earlier run (`20261120T041031Z-run`) the
acknowledgment came 0.27–3.75 s after the commit and the gateways recorded
until 1.8–1.9 s after it; in the final run it always came before the
gateways' next refresh.

C2, on `gw0` only (`gw1` the control, which stayed confirmed throughout):

| Fault | Result |
| --- | --- |
| every combined read fails | recording resumed 10.091 s after the start of the last good read (the stale bound is 10 s from the read's start); confirmed again 1.8 s after the fault was cleared |
| every read held 15 s before it is observed (old snapshots, late) | recording resumed 10.071 s after the last good read's start; the late replies, 15 s old when observed, never stopped it; confirmed again 0.9 s after the hold ended |
| one read held 4 s spanning an `enable` (a disabled snapshot from 1.06 s before the enable, observed 2.9 s after it, with 4.9 s of lease left) | the old confirmation kept the gateway from recording after the enable until its read's start plus 10.09 s, 9.0 s after the enable: within the stale bound after the change, the allowance of §10.6 |

The age is measured from the read's start on the monotonic clock, as
implemented and as §10.3 states. A hold longer than about 6 s can leave
the snapshot's lease within the 2 s skew at its observation; the rule then
correctly refuses it as a confirmation (seen in a run, `runs.json`).

### 7.4 Leader changes with gateways (§10.6 fourth and fifth criteria)

B1, tiering confirmed disabled, load on both gateways; the leader
stopped (it releases its lease). Times from the first event shown:

| t (s) | Event |
| --- | --- |
| 3.752 | `ctl-a` (term 2) released leadership |
| 4.008 / 4.145 | `gw0` / `gw1` observe the released lease: recording |
| 6.143 | `ctl-c` took term 3; 6.149 acknowledged generation 16 for term 3 |
| 6.151 / 8.017 | `gw1` / `gw0` observe the acknowledgment by term 3: not recording |

Across B1, each gateway served 294 GETs: 214 and 254 counted unrecorded
(confirmed disabled), 80 and 40 recorded (between the release and the new
confirmation, two and one refresh periods), all flushed and persisted (120
in total, equal to the access counts written), none dropped or
uncertain. The record changed only in its acknowledgment.

B3a, the leader paused (SIGSTOP) past its lease, then resumed: the
gateways kept the confirmation while the paused leader's lease outlasted
their reads by more than 2 s and recorded from their first read within the
skew (0.07 s and 1.97 s before the lease expired); `ctl-a` took term 4
0.49 s after the expiry and acknowledged 7 ms later, and the gateways were
confirmed again 1.94 s and 2.14 s after the expiry. The resumed leader
gave up its term (fenced) 5.4 s after the expiry and changed nothing: no
acknowledgment by it, no gateway decision changed after its return.

For B1, B3a and B2, the decision state of each gateway 0.8 s after every
observation (7, 14 and 29 observations per gateway, every one parsed and
checked) matched the rule of §10.3 applied to that observation.

B2, tiering enabled on two tiers (promote after 5 reads, demote after
60 s), reads on both gateways through a leader change:

- An object read 20 times under an earlier policy, then tiering disabled
  and enabled again: its next read replaced the old count (1), and it was
  not promoted; five more reads promoted it. An object never read before
  was promoted after 6 reads; the ten hot objects were promoted under
  load.
- The leader was stopped 4.5 s before the first promoted object would
  have been demoted without the grace; `ctl-c` took term 5 0.4 s later.
  Generation, policy, `grace_from_ms` and `promotion_from_ms` were
  unchanged by the leader change (only the acknowledging term changed).
- The two promoted objects were still in the upper tier at a poll 2.8 s
  after the bound `max(reset, grace_from + 47 s) + 60 s` and in the lower
  one at the next (4.3 s after it), demoted by the new leader; without the
  grace they would have been demoted 36 and 42 s before the bound. The hot
  objects stayed in the upper tier.
- Accounting over the window spanning the change: each gateway served
  1,002 GETs, recorded 1,002, flushed 1,002; the access counts persisted
  for the ten objects rose by 2,004, equal per object to the GETs served;
  nothing unrecorded, dropped or uncertain.

B3b, the leader's own tiering move held before its switch: a fresh object
read 6 times; the leader (`ctl-c`, term 5, policy generation 19) copied
it to the upper tier and was held; the leader was paused, 3 more reads
were served, `ctl-a` took term 6. Under the same policy `ctl-a` found the
move in flight on two passes, moved nothing and kept the record (9
reads). The policy was then changed (generation 20, acknowledged by term
6), the old leader resumed and its move released: its switch was refused
(`fenced: leadership token 5 … is no longer current`), its pass ended, and
it gave up its term once the held task ended. The object stayed in the
lower tier with its 9 reads; the successor kept its term and the new
policy. This shows a deposed term's move refused; the old leader is
fenced by its term as well as by the policy.

B3c, the policy fence alone: the current leader's move of a fresh object
(6 reads) held before its switch, 2 more reads served, the policy changed
(generation 22). The change was not acknowledged while the held pass ran
(the leader's health passes skipped it, 12 s); once released, the switch
was refused for the changed policy (`the tiering record changed, pass
stopped`), the object stayed in the lower tier with its 8 reads, and the
same term (6) acknowledged generation 22 0.16 s after the release.

### 7.5 Found and fixed

A tiering pass that found another migration of a chunk in flight (the
intent held by a deposed leader's attempt, which may never switch, for up
to an hour) counted the generation as settled and reset its access record,
although it moved nothing: the reads recorded meanwhile were lost. An
early run of B3b showed it (`runs.json`; there the reset happened to be
refused by a concurrent policy change). Fixed: such a skip
(`MigrateOutcome::in_flight`) leaves the generation unsettled and its
record kept, and is logged; the regression test
`tiering::a_move_already_in_flight_resets_no_access_record` (also on
FoundationDB) fails without the fix. Rebalancing and other callers are
unchanged.

Not a product defect: the hosts' `date +%s%3N` (uutils coreutils) does not
zero-pad milliseconds below 100; the harness reads the hosts' clocks from
bash's `EPOCHREALTIME` instead.

### 7.6 Where each item is verified

Unit: in-memory store and in-process clusters; FoundationDB: the same
code against a real FoundationDB (`tiering_race`, `tiering_fdb::`, CI's
backend job); processes: §7 on three hosts.

| ADR-0035 item | Unit | FoundationDB | Processes |
| --- | --- | --- | --- |
| §10.3 each row (no record, enabled, unacknowledged, new term, earlier generation, no leader, expired or released lease, within the skew, stale, another incarnation) | §5 watch tests | — | released lease and new term (B1), lease within the skew and expiry (B3a), unacknowledged generation (C1 set while disabled), stale (C2) |
| A read failure and its ageing; a stalled refresh; a late reply | §5 | — | C2 |
| Enable, disable, a policy change reaching every gateway within one refresh | §5 (one gateway) | — | C1 (two gateways, 24 measurements) |
| A gateway restart | §5 | — | not exercised |
| Seeding from flags, none without, the record winning over different flags | §5 | — | A1, A2, B1 |
| Seeding by two controllers at once | `tiering_race` | `tiering_race` | A1, A2 (serialised by election) |
| Acknowledgment once per generation and term, lost to a racing admin change | §5, `tiering_race` | `tiering_race` | C1 |
| Acknowledgment after a takeover; failover with gateways | §5 | — | B1, B3a, B2, B3b |
| A changed policy refusing a pass (switch, reset); nothing reset | §5 | `tiering_fdb::` | B3c (the policy alone), B3b (with the old term also fenced) |
| The acknowledgment waiting for the pass that read the previous generation | §5 | — | B3c |
| A move found in flight resetting nothing | `tiering::a_move_already_in_flight…` | `tiering_fdb::` | B3b |
| Rebalancing unaffected by the tiering fence | §5 | `tiering_fdb::` | not exercised |
| Disabling refused below schema 4; the upgrade | §5 | CI upgrade job | — |
| Grace: no record, old record, reset record, skewed clock | §5 | `tiering_fdb::` | B2 (reset records, through a leader change) |
| Old counts not promoting (controller and flush side) | §5 | `tiering_fdb::` | B2 |
| No record lost or counted twice with tiering enabled | §5 (flush accounting) | — | B2 (exact per object), B1 (with transitions) |
| A restore restarting the grace | §5 | — | not exercised |
| §10.6 first criterion (throughput) | — | — | §6, quiet background passes only |
| R-94(b), R-112, R-114 unchanged | existing tests | CI | — |

### 7.7 Limits

- Pre-existing and unchanged: other outcomes that may not have acted on
  a generation still reset its access record (§7.8, R-118); the in-flight
  skip is told apart by its reason constant, not by its own variant.
- One FoundationDB process; small data and load; no host, network or
  FoundationDB faults (only the targeted process faults above).
- The leader changes were a graceful stop and a pause past the lease;
  a controller crash without release is covered by the pause (its lease
  expires the same way), not run as a kill.
- Gateway restarts, a restore, capacity rebalancing and the upgrade were
  not exercised by processes here (unit, FoundationDB or CI tests above).
- Timing figures depend on the 5 s health pass and the 2 s refresh of
  this build; the reflection rule (first refresh after the target state)
  is what was checked, not a bound in seconds.
- Throughput with the controller's normal background passes was not
  measured (§6 stands as a quiet-condition result).

### 7.8 Access-record resets after outcomes that may not have acted (R-118)

The fix of §7.5 covers one path: a chunk whose migration another attempt
holds. Two further paths through the same decision are pre-existing,
unchanged by this work, and tracked as R-118; whether either is a defect
or the intended behaviour is not decided here.

The decision (`crates/controller/src/tiering.rs`, the sweep): for a
generation chosen for a move, every chunk is migrated in turn; the
generation counts as *settled* when any chunk's outcome is `Switched` or
any other `Ok` outcome except the in-flight skip and `PolicyChanged`
(that is `AlreadyThere`, or `Skipped`); once settled, the generation's
access record is reset (`reset_access_fenced`: `{now, 0}`). `Err`
outcomes do not settle; a target at its reserve ends the pass before
the reset. The rule dates from the Phase 3.12 liveness audit (finding 15,
"tiering forgot a generation whose moves all failed",
[Phase 3.12 report](phase-3.12-restore-architecture.md) §10; commit 0761473),
which made the reset conditional on a settled chunk and counted "found
there or changed" as settled; the audit records no decision on the two
cases below.

| | Path 1: a refused switch | Path 2: a partly moved generation |
| --- | --- | --- |
| Code | `crates/tiering/src/migrate.rs`: `switch_placement_fenced` returns `Refused` (the attempt no longer holds the chunk's intent; the generation is gone or not committed; the chunk's placement or id changed; the target group's epoch changed or it is no longer Active), mapped to `Skipped("record changed, generation not committed, or intent lost")`, which settles | the `settled` flag is one per generation: one chunk `Switched` (or `AlreadyThere`, or refused as in path 1) settles it even if another chunk failed (`Err`) or was skipped in flight |
| When | an intent that expired (TTL 1 h) and was then removed (`expire_migrations`, at the start of each tiering and rebalancing sweep) or taken over by another attempt before its switch (expiry alone does not refuse: the switch compares the attempt); a membership change of the target group during the copy; another mover (rebalancing, repair, another tiering attempt) changed the chunk's placement; a delete of the generation during the copy | a multi-chunk generation whose chunks' moves end differently in one pass |
| What is consumed | the generation's whole access record: the count (promotion progress) goes to 0, `last_ms` to now (demotion postponed by `demote_after`) | the same, for the whole generation, whatever its remaining chunks did |
| Effect on decisions | a promotion that did not happen needs `promote_reads` new reads before it is tried again; a demotion that did not happen waits `demote_after` again | the chunks left behind are not retried until the record qualifies again; the generation's tier is read from its first chunk, so a generation whose first chunk moved and the others did not is judged in the upper tier (not confirmed by a test) |
| Data integrity | none expected: the reset writes only the access record; the switch is conditional and the source stays referenced until it commits | the same; a generation spread over two tiers stays readable (each chunk record names its placement) |
| Tiering accuracy | observation loss (as R-109's, but caused by the pass, not the recording path) | the same, and placement that may stay split |

Confirmed by reading the code: the conditions, the reset and its scope.
Not confirmed: how often either path occurs; the split-tier judgement of
path 2 and how long a split generation stays split; whether a refused
switch whose chunk another mover put in the target tier should count as
done (the audit's "changed").

Minimal reproductions to write next (in-memory tests through the pass,
`run_once_bounded_fenced`), each recording the access count, `last_ms`
and every chunk's placement after one and two passes, and the hook it
needs. The pass calls `migrate_chunk_for_tiering` with its own closure
(only the file-driven hold of the `fault-injection` build), so the
`before_switch` hook of `migrate_chunk_with` is not reachable from it:

1. A one-chunk generation whose switch is refused because the target
   group's epoch changes between the copy and the switch: today the
   record is reset; the test fixes whichever behaviour is decided. Needs
   a test seam for a hook in the pass, or a `fault-injection` test build
   using the hold (`ARCA_TEST_FAULT_FILE`, process-global) to pause the
   move while the test changes the epoch.
2. The same with the attempt's intent expired and then removed before
   its switch (`expire_migrations` at `now` past the TTL while the move is
   held): the same seam or hold.
3. A two-chunk generation, chunk 0 switched, chunk 1 held in flight by
   another attempt (`begin_migration` beforehand, as in
   `a_move_already_in_flight_resets_no_access_record`): feasible with the
   existing helpers; today the record is reset; the test also records
   the tier the next pass judges the generation by and whether chunk 1
   moves.
4. The same with chunk 1 failing (`Err`, a node error on chunk 1 only):
   needs a per-chunk fault (the node clients' fault hook keyed on the
   chunk's shards, or the seam of 1).
5. The same with the target at its reserve after chunk 0 (the pass ends
   before the reset: the record is kept, the generation split): needs a
   capacity set-up leaving room for chunk 0 only, or the seam of 1.

A change of behaviour (for example a settled flag per chunk, a reset only
when every chunk is in the target tier, or distinct outcomes in
`MigrateOutcome`) is a separate design and review; it is not part of the
ADR-0035 work, which stays complete as reported.

### 7.9 The binary and the merged change

The commits named in this section are commits of the pull request
before it was merged by rebase; the merged commits on `main` have other
identifiers. Compared tree to tree (not through a merge base):

- b7396ab, the final run's binary source, and `main` at 42d44cb (the
  merged head): identical product sources (every crate's `src/`,
  `Cargo.toml` and build inputs), identical `fault_control.rs` of the
  gateway, controller and storage node, identical root `Cargo.toml`,
  `Cargo.lock` and `rust-toolchain.toml`. Under `crates/` two test files
  differ: `crates/metadata/tests/tiering_race.rs` and
  `crates/testkit/tests/it/tiering_fdb.rs` (the destructive FoundationDB
  tests' cluster-file guards, added after the run); outside `crates/`,
  the harness, CI, these results and this report differ. The final test
  code was run by `main`'s CI at 42d44cb (workflow run 44698389240, push
  to `main`: every job passed, the FoundationDB upgrade job included). The same source does not mean the same binary
  bytes, and the run used a `fault-injection` build, not a release
  build.
- b7396ab and 5b2e7af (the harness of the final run) have the same trees
  as `main`'s f941b59 and e7965a3, and both are reachable from the pull
  request's head (`refs/pull/16/head`).
- f81ed65, c6312ff and c3db9d6, named in `runs.json` for earlier runs,
  were local commits never pushed (the branch was reorganised before its
  first push). f81ed65's `crates/` equals that of the first pushed head
  (1096b85, `main`'s 38b9c42); c3db9d6, the binary of the first debug
  runs, predates the in-flight fix. They are named as recorded and not
  re-derived.

