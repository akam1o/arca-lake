# ADR-0034: Access records at scale

Status: **accepted; decisions 1–5, 7 and 8 implemented** (Phase 4.1;
proposed in Phase 4.0); decision 6 not implemented. Implementation
notes and the deviations from the text below: §12. The performance
targets are targets until measured (`docs/verification/phase-4.1-adr34.md`). Concerns the gateway's recording of reads
for automatic tiering (RFC-006 §5.8) and the records tiering reads; no
change to data safety, deletes, fencing or deferred reclamation.

## 1. Background and problem

A GET of an object records the read of its generation: the gateway counts
reads per generation (`CacheState::flush_access`, at most one write per
generation per second, `ACCESS_INTERVAL_MS`) and, when the interval
passed, spawns a task that runs `object::record_access`: one
read-modify-write transaction of the generation's key `access(g)`
(`AccessRecord { last_ms, count }`, `last_ms = max`, `count += reads`).
Tiering (`crates/controller/src/tiering.rs`) is the only reader:
promotion when `count ≥ promote_reads` and the last read is within
`promote_window`; demotion when `now − last_ms > demote_after`; a settled
move resets the record (`reset_access`: `{now, 0}`). GC's `finish` clears
the key with the generation. Cache eviction does not use it (cache
placements carry their own recency).

Measured (Phase 4.0, `docs/verification/phase-4.0-scalability.md` §6, §7,
§14):

- FoundationDB commits per GET 0.23 / 0.79 / 0.99 / 1.02 at 10³ / 10⁴ /
  10⁵ / 10⁶ objects read uniformly: beyond about 10⁴ objects almost every
  GET is its generation's first read within the second, so almost every
  GET commits;
- GET 4 KiB with `--access-records off`: 3,950 ops/s against 3,237 on
  (+22 %), commits 3.7/s against 2,784/s.

Defects and limits of the current code (code analysis, Phase 4.0 and
4.1):

1. **Unbounded tasks.** Every eligible GET spawns a task; under a
   FoundationDB slowdown they accumulate without limit (memory, and
   commits arriving late in a burst).
2. **One commit per flushed generation**, as measured above.
3. **Orphan records.** A write that runs after GC finished the generation
   recreates `access(g)` (about 50 bytes, never read or removed). The same
   holds for tiering's `reset_access`, a blind write.
4. **Silent loss.** Reads counted after a generation's last flush are
   written only at its next read after the interval: a burst followed by
   silence leaves its tail unwritten. When the coalescing map exceeds
   `STATE_CAP` (100,000 generations) it is cleared, pending counts and
   all, without a counter.
5. **Unread objects look old.** A generation never read has no record;
   tiering reads `last_ms = 0` and may demote it at its first pass, before
   `demote_after` from its write. With `--access-records off` every object
   that gateway serves looks unread.
6. **The promotion count is not windowed**: `count` is the reads since
   the last reset, whenever they happened; only the last one must be
   inside `promote_window`. (Not changed by this ADR; recorded because a
   drop's effect on promotion depends on it, §6.)

## 2. Requirements

- R1 Commits per GET at most 0.1 at 10⁵ uniformly read objects (target).
- R2 GET throughput within the measured spread of `--access-records off`
  on the same layout (target); otherwise the remaining difference is
  measured and its cause named.
- R3 A fixed bound on the gateway's memory and pending work for access
  records, whatever the request rate or FoundationDB's state.
- R4 Defined behaviour under overflow, FoundationDB timeouts, unknown
  commit outcomes, gateway shutdown and crash; every loss the process
  can observe counted (a crash's cannot be: stated, not counted).
- R5 No access record written for a generation GC finished.
- R6 Tiering's safety unchanged (moves stay fenced and verified); its
  accuracy (which objects are promoted or demoted) evaluated separately
  and bounded by measurable drop rates.
- R7 Several gateways recording the same generation combine correctly.
- R8 Observable state with metrics whose cardinality does not depend on
  the number of objects.
- R9 No change of the record's format; mixed versions keep working.

## 3. Considered alternatives

| Alternative | Commits per GET | Assessment |
| --- | --- | --- |
| A. Longer per-generation interval (e.g. 60 s) | unchanged for high-cardinality reads (each object read about once) | does not address the measured case; recency coarser |
| B. Batched flush of a bounded, coalescing per-gateway table (**chosen**) | about `flushes / GETs`: for reads of distinct objects about `1 / batch_size` (0.002 with 512) as long as the flusher keeps up, less when reads repeat (estimate) | one transaction per batch; keeps the record format and its read-modify-write semantics |
| C. FoundationDB atomic mutations (`add` for the count, `max` for the time) on two raw keys | as B when batched; no read conflicts | a key-layout change and migration of every existing record; an atomic write cannot check that the generation still exists, so the orphan race (R5) stays open |
| D. Sampling (record one read in `p`, weight `1/p`) | `p` | changes the meaning of `promote_reads`; needs calibration; biased against rarely read objects |
| E. Recording nothing where no tiering policy exists | 0 for those tiers | complementary to B (decision 6); not a fix where a policy exists |
| F. A separate access log (append-only, aggregated by the controller) | about `1 / batch` | a new write path and a consumer to operate; more than R1 needs |

## 4. Decision

1. **A bounded coalescing table per gateway.** The GET path adds
   `(generation, now)` to a sharded in-memory table keyed by generation:
   an existing entry takes `last_ms = max`, `count += 1` (coalesced); a
   new entry is inserted if the table and the batch being flushed hold
   fewer than `capacity` entries together (default 65,536), otherwise
   the read is dropped and counted (`reason="full"`). An entry records
   when its oldest unwritten read arrived; the flusher checks it at every
   attempt of a batch's transaction (retries and split batches included,
   against its own clock when the attempt starts) and leaves out an entry
   older than `max_age` (default 30 s), dropped (`reason="stale"`). The
   check runs when an attempt starts: on FoundationDB, whose transactions
   cannot commit more than about 5 s after their read version, no read
   is written later than `max_age` + 5 s after it happened (§5, reset);
   on a backend without such a limit the bound holds at the attempt's
   start only (an attempt stalled after it can commit later). A hot entry keeps coalescing new reads but
   ages from its oldest unwritten one: it goes stale only when flushes
   fall behind, and those drops are counted in the same reason and
   measured in the overflow accuracy test (hot objects are the ones
   promotion depends on). No task
   per GET; the GET path never waits on FoundationDB for this.
2. **A single flusher per gateway.** Every `flush_interval` (default 1 s),
   or at once when the table reaches `high_watermark` (default 50 % of
   capacity), the flusher takes up to `batch_size` (default 128; 512 in
   the first design, see §12) entries,
   oldest first, and writes them in one transaction: for each generation
   it reads the header and the access record, and writes
   `{max(last_ms), count + reads}` only if the header exists (R5). At most
   one flush runs at a time; entries arriving meanwhile coalesce into the
   table.
3. **Failures.**
   - Conflict: retried by `transact` (another gateway or tiering wrote the
     same record; the retry re-reads).
   - Retries exhausted, a backend error, transaction too large: the batch
     is split in two and retried down to one generation (as ADR-0033's
     halving); a single generation that still fails goes back to the
     table once, marked as failed: merged into the entry of the same
     generation if one arrived meanwhile (the mark kept), inserted if
     there is room, otherwise dropped (`reason="full"`); a marked entry
     that fails again is dropped (`reason="error"`). The entries of a
     batch count against `capacity` until its flush ends, so the table,
     the batch and the retried entries together never exceed it.
   - Unknown commit outcome: **not retried** (the write adds; a retry
     after an applied commit counts twice). The batch's reads are counted
     *uncertain* (`arca_gateway_access_uncertain_total`): written or not,
     unknown. Under-counting by one batch is accepted; over-counting is
     not.
   - A generation whose header is gone: skipped and counted
     (`skipped_finished`).
4. **Shutdown and crash.** On a graceful stop the flusher writes what the
   table holds within `shutdown_flush` (default 2 s) and counts the rest
   dropped (`reason="shutdown"`). A crash loses the table: at most
   `capacity` generations, at most about `flush_interval` of reads in
   steady state; under a stalled flusher, whatever accumulated (the
   number of generations is bounded, the reads coalesced into them are
   not). The table's oldest entry age is a metric. These losses cannot
   be counted (the counters die with the process); nothing is persisted
   locally.
5. **Tiering reads what was never written as written at birth.** A
   committed generation without a record has `last_ms` = the time in its
   id (UUIDv7) and `count = 0` (defect 5). `reset_access` reads the
   header in its transaction and writes nothing for a generation that is
   gone (defect 3).
6. **No recording where it is not read** (second step, after 1–5 are
   measured): the leader writes whether a tiering policy is configured
   (a new key under `("al","tiering")`, read by gateways at their
   cluster-state refresh; earlier releases neither write nor read it); a
   gateway records only when it is set. `--access-records on|off` keeps
   overriding: `off` never records, `on` records as decided by the key
   (`always`: records regardless, for measurements).
7. **Flush timing across gateways.** Each flush is scheduled at the
   interval with ±20 % random jitter, so that gateways do not flush the
   same hot generations at the same instants; a generation that the
   halving of decision 3 singled out (FoundationDB does not name the
   conflicting key; a batch of one that still conflicts identifies it)
   is flushed for the next
   `isolate_flushes` (default 8) in a batch of its own (bounded set of
   64 generations), so that one contended key does not make whole batches
   fail (review: a hot generation is in every gateway's every batch).
8. **Configuration**: `--access-capacity`, `--access-batch`,
   `--access-flush-interval-ms`, `--access-high-watermark`,
   `--access-max-age-ms`, `--access-shutdown-flush-ms`; defaults above.

## 5. Consistency and failure semantics

- **What a record means** after this change: `last_ms` is at most the
  latest of the read times flushed by any gateway (each from that
  gateway's clock) and the time of the last tiering reset (a reset
  writes its own time); `count` is at most the reads served since
  `max_age` + 5 s before the last reset. Both may be lower than the truth by the losses
  of decisions 3 and 4 (counted, except a crash's) and the uncertain
  batches (counted apart: they may have been written). A record never
  counts a read twice (no retry after an unknown outcome; a split batch
  writes disjoint generations).
- **Several gateways**: each read-modify-write is serializable; `max` and
  `+` commute, so the order of flushes does not matter. A hot generation
  costs one write per gateway per flush, not per read. A conflict on any
  generation of a batch aborts and retries the whole batch: with several
  gateways a hot generation is in every gateway's every batch, so their
  batches contend; jittered flush times and the isolation of contended
  generations (decision 7) bound this, and the multi-gateway acceptance
  test measures it (losses from contention, not capacity, are a failure
  of the design).
- **GC**: `finish` clears `access(g)` together with the generation in one
  transaction; a flush reads the header of every generation it writes, so
  either it commits before `finish` (whose clear removes the record) or
  it retries after and finds no header (writes nothing). Same for
  `reset_access` (decision 5).
- **Tiering reset**: a reset writes `{now, 0}`; a flush that commits after
  it adds the reads counted meanwhile (correct: reads after the move); a
  flush whose reads happened before the reset but commits after it adds
  them to the new count. No entry is written later than `max_age` after
  its oldest unwritten read plus about 5 s (decision 1: age checked at
  every attempt, FoundationDB's transaction age limit), so the
  over-count is at most the reads of that span before the reset, per
  gateway (accepted and stated; a reset generation stored in the record
  would remove it, at the cost of a format change).
- **Time**: `last_ms` comes from the gateway's wall clock at the read and
  is combined with `max`, so a record's time never goes back. A gateway
  whose clock runs ahead keeps objects looking recent by its skew; one
  that runs behind makes its reads look older by its skew. Tiering's
  windows are minutes to days; clocks are required to be synchronized
  (operations guide: NTP), as for the leases elsewhere. Not detected.
- **Restart**: no state survives; the first flush after a restart writes
  only what was read since.

## 6. Tiering implications

Safety and accuracy are separate:

- **Safety (unchanged).** Access records only choose candidates. Every
  move is a migration with its intent, copy, verification, switch and
  fenced cleanup (RFC-006, ADR-0028); a wrong choice moves data to the
  wrong tier, never loses or corrupts it. No delete, reclaim or fence
  decision reads an access record.
- **Accuracy (changed; to be measured).** Effects of a lost read:
  - *Wrong demotion of a read object*: only if every read of the object
    within `demote_after` (default 1 day) is lost. Drops hit new table
    entries (decision 1), that is objects not already pending; an object
    read once a day under sustained overflow is the exposed case.
    Decision 5 removes the case of never-read objects being demoted at
    once.
  - *Missed or late promotion*: `count` reaches `promote_reads` (default
    10) later by the reads lost; a hot object coalesces in its existing
    entry and is rarely dropped, so promotions are delayed rather than
    missed except for objects whose reads arrive while the table is full
    and none is pending.
  - *Hot/cold bias*: overflow drops favour objects already in the table
    (hot), so under overflow cold objects are under-counted more than hot
    ones; between gateways, one that overflows under-counts its share.
  - *Eviction fairness*: unaffected (cache eviction does not read access
    records).
  - *Ordering between gateways*: irrelevant to the result (`max`, `+`).
  - *Records of finished generations*: none (R5).
- **What tiering should see**: the drop rate per gateway (metrics) and,
  in the acceptance tests, the promotions and demotions of a reference
  workload with and without forced overflow compared against those of
  lossless recording.

## 7. Metrics

Per gateway, no per-object or per-generation label:

| Metric | Kind | Meaning |
| --- | --- | --- |
| `arca_gateway_access_queue_entries` | gauge | generations pending in the table |
| `arca_gateway_access_queue_capacity` | gauge | the table's capacity (table and batch in flight) |
| `arca_gateway_access_queue_in_flight` | gauge | generations of the batch being written |
| `arca_gateway_access_queue_oldest_seconds` | gauge | age of the oldest pending read |
| `arca_gateway_access_isolated_generations` | gauge | generations written alone after contention (at most 64) |
| `arca_gateway_access_reads_total` | counter | reads offered to the table |
| `arca_gateway_access_coalesced_total` | counter | reads merged into a pending entry |
| `arca_gateway_access_inserted_total` | counter | reads that started a new entry |
| `arca_gateway_access_dropped_total{reason}` | counter | reads lost: `full`, `stale`, `error`, `shutdown` |
| `arca_gateway_access_uncertain_total` | counter | reads of batches whose commit outcome is unknown (written or not) |
| `arca_gateway_access_uncertain_batches_total` | counter | those batches |
| `arca_gateway_access_flushes_total` | counter | flush transactions committed |
| `arca_gateway_access_flushed_generations_total` | counter | generations written |
| `arca_gateway_access_flushed_reads_total` | counter | reads written |
| `arca_gateway_access_skipped_finished_total` | counter | generations not written: finished |
| `arca_gateway_access_skipped_finished_reads_total` | counter | their reads |
| `arca_gateway_access_flush_batch_max` | gauge | largest batch written |
| `arca_gateway_access_flush_seconds` | histogram | flush transaction time per (sub-)batch, its conflict retries included |
| `arca_gateway_access_flush_errors_total{kind}` | counter | failed attempts: `conflict`, `backend`, `too_large`, `other` |

Commits per GET are derived: `flushes_total` over the gateway's GET count
(`arca_gateway_s3_requests_total` of the GET operation), alongside FoundationDB's
committed transactions in benchmarks.

## 8. Testing strategy

- **Unit, deterministic** (tokio paused time): the table's bound (a full
  table drops and counts, never grows); coalescing (`max`, `+`); the
  high-watermark flush; one flush at a time; split on failure; unknown
  outcome not retried and counted uncertain; a single failing generation
  retried once, then dropped, also while the table refills (merge, mark,
  no room); the bound with a batch in flight; stale entries dropped;
  shutdown flush within its deadline; the counters add up (`reads =
  coalesced into a pending entry + taken by a new entry + dropped
  full at insertion`, in reads; and for every entry, its reads `=
  written + pending + dropped (stale, error, full at re-insertion,
  shutdown) + uncertain + skipped (generation finished)`, merges moving
  reads between entries without changing the sums).
- **Metadata**: a batch writes exactly the per-generation sums; a
  generation finished by GC before or during a flush gets no record
  (interleaved transactions with the fault store); `reset_access` on a
  finished generation writes nothing; two flushers on one generation sum.
- **Model**: random sequences of reads on several gateways, flushes,
  stalls, GC finishes, tiering resets, unknown commits (applied or not)
  and crashes, against a reference: no record of a finished generation;
  `count` never above the reads since `max_age` + 5 s before the last
  reset;
  `last_ms` never above the latest read or reset.
- **Tiering accuracy**: the in-process tiering simulator with a
  hot/cold (Zipf) read workload, lossless versus a forced overflow (tiny
  capacity): promotions and demotions compared (missed, delayed, wrong),
  per hot and cold class, on one and on several gateways.
- **Cluster A/B** (on the Phase 4.0 layout, only after the environment
  is cleared for load tests): 10⁵ objects read uniformly, `on` against
  `off` against the Phase 4.1 base, three runs in rotated order:
  commits per GET, GET ops/s and p99, drop counters; a forced-overflow
  run; two gateways reading one hot generation and many cold ones
  (contention: losses and isolation counters).

## 9. Rollout and rollback

- Record format unchanged: gateways of the previous release and of this
  one write compatible records; the controller reads either.
- Step 1 (decisions 1–5, 7, 8) behind the existing switch: `on` uses the
  batched path; a `legacy` value keeps the per-GET path for one release
  (A/B and rollback without a binary change). The legacy path and
  gateways of the previous release keep the orphan race (defect 3): R5
  holds only once every gateway runs the batched path.
- Decision 5 changes tiering on upgrade: a committed object in an upper
  tier that was never read, demoted at the first pass today, waits
  `demote_after` from its write. Intended; stated in the release notes.
- Step 2 (decision 6) adds the leader's key; until a leader writes it,
  gateways behave as `always` (no change in behaviour on upgrade);
  downgrading leaves an unread key.
- Rollback: `--access-records legacy` or `off`; nothing to undo in
  metadata.

## 10. Risks and limitations

- Under-counting (overflow, stale entries, errors, shutdown: counted;
  unknown commits: counted as uncertain; crash: not countable):
  tiering may act later or, under sustained overflow, demote an object
  read rarely; visible in the drop metrics; no data risk.
- Reads of at most `max_age` + 5 s before a tiering reset counted after
  it, per gateway (§5).
- Clock skew between gateways shifts recency by the skew; not detected.
- A flusher whose commit never returns keeps its batch: after a stop's
  wait those reads are neither written nor counted (the process exits
  with them in flight); a commit already sent may still apply after the
  stop returned.
- The age bound assumes a prompt read version after the attempt's check
  (a stall between them is not covered).
- A refused flush backs off for its interval; a back-off ends with the
  next flush, which retries what went back.
- Hot generations read through many gateways: one write per gateway per
  interval, with conflicts among them (retried); not measured at more
  than one gateway.
- Flush throughput: one flusher writes at most `batch_size` generations
  per transaction, one transaction at a time; reads of distinct objects
  faster than that (about `batch_size / flush duration` per second)
  fill the table and are dropped (counted). The bound is to be measured;
  more flushers in parallel would need their own conflict analysis.
- The promotion count stays unwindowed (defect 6): a separate decision.
- Decision 6 needs a new metadata key (additive; this ADR).

## 11. Acceptance criteria

1. At most 0.1 FoundationDB commit per GET at 10⁵ uniformly read objects
   on the Phase 4.0 layout (3 runs, rotated).
2. GET throughput within the spread of `--access-records off` on the same
   runs; otherwise the remaining difference measured and its cause named.
3. Memory and pending work bounded by configuration under a FoundationDB
   stall (test: a stalled store, sustained GETs; table and batch at
   capacity, drops counted, no task growth).
4. No access record for a finished generation written by the batched
   path (metadata and model tests, the GC race and `reset_access`
   included); legacy writers excluded (rollout).
5. Every observable loss counted by reason and uncertain batches apart;
   the counters add up (unit tests); crash losses bounded by `capacity`
   and `max_age` (stated).
6. With two gateways and a hot generation, no reads lost to contention
   in the reference run.
7. Tiering: safety tests unchanged and passing; accuracy under forced
   overflow reported (missed, delayed and wrong moves per class) and the
   default capacity chosen so that the reference workload shows none at
   the measured GET rates.
8. Two independent reviews; Critical and High findings fixed with tests
   that fail without the fix.

## 12. Implementation (Phase 4.1)

- `crates/gateway/src/cluster/access.rs`: `AccessTable` (one mutex
  around a map of pending generations and their insertion order; O(1)
  per read), `spawn_flusher` (one task per gateway; holds no strong
  reference while it sleeps, so a dropped backend ends it), `run_flush`,
  `flush_batch` (split on failure, one re-insertion, isolation of a
  generation that failed by contention). `crates/metadata/src/ops/object.rs`:
  `record_accesses` (headers and records read together with
  `Txn::get_many`, the age checked at every attempt), `reset_access`
  reads the header. `crates/controller/src/tiering.rs`: decision 5.
- `--access-records on|off|legacy` (default `on`: the batched path;
  `legacy`: the per-GET path of earlier releases, kept for one release)
  and `--access-capacity`, `--access-batch`, `--access-flush-interval-ms`,
  `--access-high-watermark`, `--access-max-age-ms`,
  `--access-shutdown-flush-ms`.
- Deviations from the text above:
  - one mutex instead of a sharded table: each read is a map lookup and
    at most an insertion under it; to be revisited only if the GET path
    shows contention on it in the measurements;
  - decision 5 uses the header's `created_ms` (the writer's clock when
    the generation was created, a recorded field) rather than the time
    in the id's UUIDv7 bits: the same clock and moment, without relying
    on the id's layout; tiering reads the header anyway;
  - the batch size is exposed as generations and flushes counted plus
    the largest batch (`arca_gateway_access_flush_batch_max`), not as a
    histogram (the shared histogram type measures durations);
  - decision 6 (recording only where a tiering policy exists) is not
    implemented: it needs a new metadata key and its own measurement;
  - the default batch is 128 generations, not 512: on the cluster, a
    second's reads written as back-to-back batches of 512 (1,024 reads
    issued at once and a 512-write commit each) raised GET p99 to about
    twice the base gateway's; batches of 128 brought it back to the base's
    level at 0.008 commit per GET (`docs/verification/phase-4.1-adr34.md`
    §6).
