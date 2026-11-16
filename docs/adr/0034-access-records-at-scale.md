# ADR-0034: Access records at scale (proposal)

Status: **proposed** (Phase 4.0). Not implemented beyond the switch
`gateway --access-records on|off` (default `on`). To be decided and
implemented in Phase 4.1.

## Context (measured, Phase 4.0)

A GET records the read of the object's generation for automatic tiering
(RFC-006 §5.8): a read-modify-write transaction of the generation's
access key, coalesced per gateway to at most one per generation per second
and always at a generation's first read (`note_access`, `flush_access`,
`object::record_access`). Measured on the Phase 4.0 cluster
(`docs/verification/phase-4.0-scalability.md` §6, §14):

- FoundationDB commits per GET: 0.23, 0.79, 0.99 at 10³, 10⁴, 10⁵ objects
  read uniformly (beyond about 10⁴ objects almost every GET is a first
  read within its second); GET 1 KiB fell from 3,346 to 2,512 ops/s while
  HEAD (no access record) stayed at 6,100–6,800 ops/s;
- GET 4 KiB with access records off: 3,950 ops/s (3,851–4,146) against
  3,237 (3,210–3,312) on, p99 24.8 against 29.1 ms, FoundationDB commits
  3.7/s against 2,784/s.

What reads the records (review, Phase 4.0): only tiering
(`crates/controller/src/tiering.rs`): promotion needs `count ≥
promote_reads` within `promote_window`, demotion `now − last_ms >
demote_after`; a settled move resets the record. Cache eviction does not:
it uses the cache placements' own recency. No data-safety decision depends
on access records.

Defects found on the way (pre-existing): every eligible GET spawns an
unbounded task; a delayed write can recreate the access key of a
generation GC already finished (an orphan key of about 50 bytes, never
read or removed); an object in an upper tier that was never read has no
record, so its `last_ms` is 0 and the first tiering pass may demote it
before `demote_after` (code analysis); with
`--access-records off` every object served by that gateway looks unread.

## Decision (proposed)

1. A bounded queue per gateway; a flusher writes up to `N` generations per
   transaction (each: the maximum time seen and the reads counted), by
   age and by size; drops counted in a metric.
2. Each write reads the generation's header in the same transaction and
   writes nothing if it is gone (closes the orphan race; one read more).
3. `last_ms` of a generation without a record is taken from its id's
   time (UUIDv7) by tiering: an object never read is not demoted before
   `demote_after` from its write.
4. Recording off by default where no tiering policy exists for the
   object's tier, and on again when one is configured, with the leader
   publishing that fact (mixed gateways read it, not a flag each).

## Expected effect (estimated)

At most `1/N` commits per GET on high-cardinality reads instead of about
one; the measured upper bound of the gain is the "off" figure above (+22 %
GET 4 KiB on this setup). To be measured in Phase 4.1 at 10⁵–10⁶ objects.

## Alternatives

- A longer coalescing window: helps re-reads of hot keys, not the
  measured high-cardinality case; delays recency.
- Sampling: changes the meaning of the thresholds; needs calibration.
- Piggybacking on other writes: a GET has none to ride on.
