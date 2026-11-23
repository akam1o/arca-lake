# ADR-0040: When a tiering pass resets a generation's access record

Status: **accepted** (0.2.0 preparation, R-118). Implemented with its
reproduction tests; no record, key or format change.

## 1. Problem

A tiering pass (`crates/controller/src/tiering.rs`, the sweep) chooses a
generation for a move from its access record, migrates its chunks one by
one, and then resets the record (`{now, 0}`): the reads that decided the
move are consumed, and the demotion clock restarts. Until this ADR the
reset followed whenever one chunk was *settled*: `Switched`,
`AlreadyThere` or `Skipped` (every `Ok` outcome but the in-flight skip and
`PolicyChanged`, which the Phase 4.1 work had already excluded,
[report](../verification/phase-4.1-adr35.md) §7.5). The report's §7.8
recorded three outcomes after which the reset may consume reads of a move
that did not happen (R-118):

1. a switch refused (`Skipped`): the attempt lost its intent (expired and
   removed, or taken over), the target group's epoch or state changed
   during the copy, another mover changed the chunk's placement, or the
   generation was deleted;
2. a generation moved in part: one chunk settled the whole generation
   whatever its other chunks did (failed, held in flight by another
   move);
3. and, not listed there, reads recorded while the pass ran: the reset
   wrote `{now, 0}` over them, also after a complete move.

A related limit made the reset matter more: the pass judged a generation's
tier by its first chunk only, so a generation whose first chunk moved and
whose others did not was judged in the upper tier and its chunks left
behind were not moved by a later pass (until it was demoted).

Five in-memory reproductions through the pass fail on the previous
behaviour (§5). Data integrity is not affected in any of them: the reset
writes only the access record, and each switch stays conditional.

## 2. Decision

The intent of the existing rule stays: a pass consumes the reads that
decided a move once the move is done; a pass that did not do it consumes
nothing (Phase 3.12 liveness audit finding 15: a generation whose every
move failed keeps its record). The rule is made precise:

1. **Completion condition.** After the chunk loop, the pass reads the
   generation's chunk records back and resets the access record only when
   every chunk is in the target tier, or past it in the move's direction
   (a promotion: the target tier or a higher one; a demotion: the target
   tier or a lower one), whoever put it there (this pass, an earlier one,
   another mover). The outcomes of the loop do not decide it: a refused
   switch whose chunk another mover put in the target tier counts as done
   (the Phase 3.12 audit's "found there or changed"); one whose chunk is
   still in its source does not. A chunk whose group is unknown to the
   pass's group snapshot, or whose generation is gone, counts as not done.
2. **Unfinished chunks.** A generation with a chunk left behind keeps its
   whole access record and is examined again by the next pass. The pass
   judges a generation by the tiers of all its chunks: promotion from its
   lowest chunk (target: one tier above it), demotion from its highest
   (target: one tier below it); a chunk already in the target tier, or
   past it, is not moved again. A generation whose chunks share one tier
   is judged exactly as before. So a partly moved generation is finished
   by a later pass while its record still qualifies (a promotion within
   `promote_window` of its last read), or demoted as a whole once it is
   cold.
3. **Accesses recorded during the pass.** The reset consumes only the
   count the pass read (`seen`): in one transaction, still fenced by the
   tiering record's generation and writing nothing for a generation that
   is gone, it writes `count = current − seen` (saturating) and
   `last_ms = max(now, current.last_ms)`
   (`object::reset_access_seen`). Reads flushed between the pass's read
   and its reset therefore survive, with their time. The subtraction
   assumes the count only grew meanwhile. The one way it can shrink
   under a fenced pass is the flush's promotion-cutoff rule (ADR-0035
   §10.4) after a cutoff moved within the same record generation (an
   acknowledgement by a new cluster incarnation, after a restore). The
   count then holds only new reads, and up to `seen` of them are
   consumed: never more than the previous reset, which consumed all.

The rest is unchanged: `PolicyChanged` ends the pass with nothing reset;
a target at its reserve ends the pass before the reset; the in-flight
skip keeps the record (§7.5); errors are counted, not returned.

## 3. Compatibility

No record, key, schema, protocol or format change: the access record
keeps its two fields and its encoding (identical to 0.1.0's,
`crates/metadata/src/ops/object.rs` `AccessRecord`); the new reset writes
the same record. A 0.1.0 leader in a mixed cluster keeps resetting as
before (the old behaviour, no new hazard); gateways are not involved.
`reset_access` and `reset_access_fenced` remain with their old
behaviour for their other callers.

## 4. Costs and residual

- One extra read-only transaction (the chunk records read back) per
  generation a pass chose for a move; moves themselves copy whole chunks.
- A generation with a chunk that can never move (unreadable, a group
  without a record) keeps its record: each pass in which it qualifies
  tries that chunk again, and skips its other chunks. Before this
  change such a generation was reset and retried only once its record
  qualified again. A repair of the chunk, or its deletion, ends it.
- A partly promoted generation whose reads age past `promote_window`
  before a later pass finishes it stays split until demotion moves its
  upper chunks back (readable throughout: each chunk record names its
  placement). Finishing it independently of the record (a per-chunk
  pending move) is a larger policy change, not made.
- Reads flushed during the pass but counted by the pass's own read are
  consumed, as intended; reads lost on the recording path stay R-109.
- Not measured: how often each path occurs in a real cluster.

## 5. Tests

`crates/testkit/tests/it/tiering.rs` (in memory, through
`run_once_bounded_fenced_with`, a hook before each switch added for these
tests): `a_switch_refused_by_a_target_epoch_change_resets_no_access_record`,
`a_switch_skipped_after_its_intent_expired_resets_no_access_record`,
`a_partly_moved_generation_keeps_its_access_record_and_is_finished_later`,
`a_generation_with_a_failed_chunk_keeps_its_access_record`,
`reads_recorded_during_a_pass_survive_its_reset`: all five fail on the
previous pass (each at its access-record assertion) and pass with this
change; the earlier tiering tests are unchanged and pass. Unit test of
the reset: `ops::tiering::tests::a_reset_by_a_pass_keeps_the_reads_recorded_since`.
Not covered: the target at its reserve after one chunk (the pass ends
before the reset, as before), and FoundationDB-backed variants of the
five tests.
