# ADR-0035: The remaining cost of access-record flushes

Status: **proposed; measured** (Phase 4.1 follow-up). Compares two ways
to close the GET throughput gap ADR-0034 left. The measurements of §6
were made (§8): option B is slower than ADR-0034 in every case, so the
data supports option A alone; the choice is the maintainer's. Concerns the gateway's recording of reads for automatic
tiering (RFC-006 §5.8) and the records tiering reads; data safety,
deletes, fencing and deferred reclamation are not to change.

## 1. Problem

ADR-0034 brought the FoundationDB commits of access recording from about
one per GET to 0.0067–0.0077 per GET, but GET throughput stayed 14 %
(4 KiB, 10⁵ objects) and 7 % (64 KiB, 10⁴ objects) below
`--access-records off` (`docs/verification/phase-4.1-adr34.md` §6.1).
The counters of that campaign are consistent with this explanation, not
a proof of it (no arm isolated it):

| Per GET (4 KiB, uniform keys) | before ADR-0034 | ADR-0034 | off |
| --- | --- | --- | --- |
| FoundationDB reads | 7.95 | 7.95 | 6.00 |
| transactions started | 2.97 | 2.01 | 2.00 |
| commits | 0.97 | 0.0077 | 0.0001 |
| FoundationDB CPU s per 1,000 | 0.32 | 0.20 | 0.15 |

Each flush reads, for every generation in it, the generation's header
(the GC-safety check: no record is written for a generation GC finished)
and its access record (read-modify-write of `{last_ms, count}`), and
writes the record. With uniform keys almost every GET is a distinct
generation within a flush interval (2.8–3.0 % of reads coalesce over 10⁵
objects), so the two reads and one write are paid about once per GET.
FoundationDB is one process in that measurement and is the shared
resource.

Not measured yet, and able to change the comparison: two or more
gateways reading the same generations (their flushes conflict on the
record), skewed workloads (more coalescing), a multi-process
FoundationDB.

## 2. Option A: decision 6 of ADR-0034 (record only where tiering acts)

No access record is written unless a tiering policy could use it.

- **What it needs.** The tiering policy is today a controller argument
  (`--tiering-tier`, `--tiering-promote-reads`, …), not metadata: a
  gateway cannot know whether one exists. The controller (leader) would
  publish a record, for example `tiering = {enabled, tiers}`, and the
  gateways watch it as they watch the cluster state and the admission
  record; without a record (or with tiering off) the gateway does not
  note reads.
- **Cost when no policy applies:** that of `off` (6.00 reads, 2.00
  transactions per GET in the measurement above): the whole gap closes.
- **Cost when a policy applies:** unchanged from ADR-0034. A finer scope
  (only buckets or tiers a policy covers) narrows it, at the price of a
  per-GET lookup of the object's tier or bucket in the gateway's copy of
  the record.
- **Safety:** unchanged (no record path changes). Turning tiering on
  starts recording from then on: generations count as unread until read
  again (decision 5 of ADR-0034 keeps them from being demoted before
  `demote_after` from their write, not from the switch: a policy enabled
  on old data may demote objects read before the switch; a grace period
  from the record's `enabled_ms` would avoid it).
- **Format and compatibility:** one new metadata key, written by the
  controller; no change to existing records. A gateway of an earlier
  release ignores the key and keeps recording (correct, only slower);
  rollback leaves an unused key.

## 3. Option B: access records kept with atomic mutations

The record's read-modify-write is replaced by two atomic mutations on
separate keys, so that the flush no longer reads the record:

- `acc_n/<generation>`: count, little-endian `u64`, `ADD`;
- `acc_t/<generation>`: last read, little-endian `u64` milliseconds,
  `MAX` (FoundationDB compares little-endian unsigned values; a missing
  value takes the parameter).

Per generation in a flush: one read (the header), two mutations (no
read conflict between gateways on the record; atomic mutations commute).

- **Existence check (GC).** The header is still read in the flush's
  transaction: it is the only way to know that GC has not already
  finished the generation (a read-conflict range alone would not see a
  finish committed before the flush began). GC's `finish` clears the
  header, the legacy record and both new keys in one transaction, so the
  two orders stay as today: a flush after the finish sees no header and
  writes nothing; a finish after the flush clears what it wrote; a flush
  racing the finish conflicts on the header and retries.
- **CommitUnknown.** Unchanged: the mutations add, so a batch with an
  unknown outcome is not resent; its reads are counted uncertain.
- **reset_access.** Sets `acc_n = 0` and `acc_t = now` (plain writes)
  after reading the header, and clears the legacy record. A flush
  committing before it is overwritten; after it, it adds to zero and
  raises the time: the same outcomes as today's serialized
  read-modify-write.
- **Two gateways.** Their flushes of one hot generation no longer
  conflict on the record (they still read the header, which only GC and
  state changes write), so the contention handling of ADR-0034 decision 7
  (isolation of a contended generation) matters less.
- **Cost:** about one FoundationDB read per GET fewer than ADR-0034
  (about 7.0 against 7.95 in the measurement above; `off` 6.00): about
  half the read gap closes; the mutations replace one write by two.
  Whether that moves throughput or CPU by the same share is not known:
  it is the first thing to measure (§6).
- **Format and compatibility:** a new record layout.
  - Readers (tiering, `admin`) merge: count = legacy count + `acc_n`,
    last = max(legacy last, `acc_t`). Writers of the new layout never
    touch the legacy record except to clear it on reset; earlier
    gateways keep writing the legacy record; the sum is correct during a
    mixed-version window (no double count: each read is written to
    exactly one layout).
  - A release that writes the new keys must not run with one that would
    leave them behind: GC of an earlier release clears only the legacy
    record, so after a rollback the new keys of finished generations
    would stay (orphans). The new layout is therefore written only once
    the cluster's `schema_write` says every process understands it
    (ADR-0016, RFC-014 §5.4): a schema step, with the upgrade's finalize.
  - The `KvStore` abstraction gains `atomic_max` (FoundationDB
    `MutationType::Max`; the in-memory and fault-injecting stores
    implement it as read-modify-write, as `atomic_add` already does).

## 4. Both

The options compose: A removes the cost where no policy acts; B lowers
it where one does. A is the smaller change; B is a schema step.

## 5. Not chosen

- Dropping the header read and sweeping orphaned records later: breaks
  the invariant ADR-0034 established (no record for a finished
  generation) and needs a new background pass.
- Per-flush append-only keys (versionstamped `{gen, flush}` entries
  summed by readers): no read or conflict at all, but the existence
  check is still needed and readers pay a range read per generation.
- Longer flush intervals: more coalescing only for skewed workloads; the
  uniform case does not change.

## 6. Measurements before a decision

On the dedicated test cluster (FoundationDB on one host, storage nodes
on another, gateways on two hosts), with host monitoring that stops a
run on resource limits and checks of the shared system FoundationDB:

1. Arms: `off`; ADR-0034 (`on`); option B as an experimental gateway
   mode (not a release default; on a test cluster only); option A is
   `off` where no policy applies, so it needs no arm of its own.
2. Workloads: GET 4 KiB uniform over 10⁵ objects; Zipf (s = 1.1) and a
   hot set (1 % of the keys, 90 % of the reads); one gateway and two
   gateways, the two reading the same keys.
3. Per arm: throughput, p50/p95/p99, FoundationDB reads, commits and
   conflicts per GET, FoundationDB and gateway CPU per 1,000 GETs, flush
   errors by kind and isolated generations, records against reads
   served (accuracy).
4. Decision rule: B is worth its schema step if, with tiering on, it
   closes at least half of the gap to `off` on the uniform workload or
   removes flush conflicts between two gateways that otherwise cost
   throughput; otherwise A alone.

## 7. Consequences

Until a decision, ADR-0034 stays as implemented, R-109 stays Medium, and
the GET-throughput target of ADR-0034 stays not met.

## 8. Results (Phase 4.1 follow-up)

`docs/verification/phase-4.1-adr35.md`: three hosts, one FoundationDB
process, one or two gateways, uniform, Zipf (s = 1.1) and hot-set (1 % /
90 %) GETs of 4 KiB over 10⁵ objects, three rounds per arm.

| GET/s against `off` | ADR-0034 (`on`) | option B (`atomic`) |
| --- | --- | --- |
| 1 gateway, uniform | −13.7 % | −18.3 % |
| 2 gateways, uniform | −16.7 % | −22.8 % |
| 2 gateways, Zipf | −7.5 % | −11.9 % |
| 2 gateways, hot set | −9.4 % | −11.2 % |

- Option B removed one FoundationDB read per flushed generation and every
  conflict between gateways, but used more FoundationDB CPU per GET
  (0.226 against 0.194 s per 1,000 with one gateway): an atomic mutation
  is applied with a read at the storage server, and the layout writes two
  keys per generation. It fails the decision rule of §6.
- Conflicts between two gateways reading the same keys were few (0.23–0.40
  conflicted transactions per 1,000 GETs) and all retried successfully:
  not the cost.
- With two gateways the one FoundationDB process was saturated in every
  arm; throughput followed FoundationDB CPU per GET.
- Recommendation: option A (record only where a tiering policy acts);
  option B not pursued. Where a policy acts, the cost stays that of
  ADR-0034 (smaller under skewed access, where reads coalesce).

