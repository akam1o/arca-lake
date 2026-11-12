# ADR-0029: Repair retargeting and repair liveness (R-104)

Status: **accepted** (Phase 3.13). Builds on RFC-008 §7 (member
replacement), ADR-0023 (delete fence) and the epoch change intents of
Phase 3.11 (M2). ADR-0028 (deferred reclamation) is unchanged.

## Context

A member rebuild commits the new membership first (epoch `E → E+1`,
state `Repairing`, a cursor naming the slot), then reconstructs the
slot's shards onto the new member (the *target*), then runs the
completeness check, which alone makes the group `Active` again.

Phase 3.12 left R-104 open (High). Found again at `cddd078`:

1. **Nothing re-chooses a target after the commit.** The `Repairing`
   branch of the leader's repair pass always rebuilds onto
   `members[cursor.member]`. A target that goes down, loses its disk,
   refuses writes (below its reserve, disk errors) or stores bytes that
   do not check fails every pass, for good. Only an intent that had not
   committed yet could be abandoned.
2. **A second failure blocks completion.** The check needs every member
   to answer; replacing a failed member is done only for `Active`
   groups, so a member that fails while another slot is rebuilt is never
   replaced: incomplete, cursor reset, full re-walk, every pass.
3. **One stuck group stops the cluster's background work.** Any durable
   group `Repairing` stopped rebalancing and tiering; any group at all
   `Repairing` stopped every drain from completing; any durable repair
   error stopped cache repair. None of this was reported.
4. **Nothing records why.** Errors went to logs only.

## Scenarios (the reproduction, `repair_retarget.rs`, `membership.rs`)

| Scenario | Before | Persisted state | Retarget? | Readable shards / EC |
| --- | --- | --- | --- | --- |
| Target down after the commit | `Repairing` onto it forever | group (epoch, cursor), no intent | yes, at once (target unhealthy past `unhealthy_after`) | all chunks have `k` on the others: rebuilt onto the new target |
| Target refuses writes (`RESOURCE_EXHAUSTED`, I/O) | retried forever | same | yes, after `RETARGET_AFTER` (5) consecutive target failures | unchanged |
| Target stores bytes that do not check | retried forever | same | yes, after 5; marked *untrusted* (never a direct-copy source) | unchanged |
| Target's disk lost (node back empty) | rebuilt again onto it (records missing) | same | no (it answers; the walk writes again) | unchanged |
| Second member fails, `k` still readable (two parities) | incomplete forever | same | the second member is replaced once the first slot is whole | rebuilt from the others |
| Second member fails, fewer than `k` (one parity) | incomplete forever | same | never: reported `awaiting-nodes`, the member kept | data needs that member: waits for it |
| Source and target both unavailable | as above | same | target replaced if a node can take it; the chunks with fewer than `k` wait | waits for nodes |
| Node back, then down again | flapping retries | status keeps the counters per epoch | per the rules above, at each observation | — |
| Leader switch | the new leader resumes from the group record | group record + status record | — | — |
| Leader crash between fence and commit of a retarget | the intent was settled (membership unchanged) if the old target still answered | epoch intent | the intent is resumed: the slot under rebuild counts as leaving | — |
| Repair vs rebalance / tiering | any durable `Repairing` group held them | status | — | only a progressing or temporarily blocked repair holds them |
| Repair vs drain | any `Repairing` group held every drain | — | — | a drained node waits only while it holds shards of a group being rebuilt, and says so |
| Repair vs GC / reclaim | unchanged: a former member's shards are kept while its group is `Repairing` (reconciliation, `shard_needed_in`) | — | — | the old target's shards are a former member's |

## Options

| | A. Retarget the existing intent | B. Attempt generation/token | C. Separate repair task | D. A + B (on the epoch) |
| --- | --- | --- | --- | --- |
| Safety | the epoch intent is fenced and conditioned already | needs a new fence on every write and cursor update | a second source of truth beside the group record | the epoch *is* the generation |
| Idempotency | intent `begin` / `commit` idempotent | new token protocol | new | existing |
| Leader failover | intents are resumed or settled | new resume logic | new | existing, plus a rule for the slot under rebuild |
| Crash recovery | existing | new | new | existing |
| Stale RPC rejection | nodes refuse epoch `E` once the target acknowledged `E+1` | needs a node-side token | needs one | existing epoch fence |
| Placement correctness | `choose_for` (failure domain, device class) | same | same | same |
| Capacity reservation | no reservation exists (checked at choice) | — | could add one | as A; documented |
| Concurrent repair | one cursor per group | — | could allow several | one at a time per group; other groups unaffected |
| EC reconstruction | unchanged | unchanged | unchanged | unchanged |
| Liveness | target changes; the rest is scheduling | — | — | with the scheduling rules below |
| Complexity | small | protocol change (nodes) | large (new record type, migration) | small |
| Upgrade / rollback | no format change | protocol change | schema change | one new advisory key |

**Decision: D.** A target change is a membership change of the slot under
rebuild through the existing epoch intent (`replace_member_with`): the
new target must acknowledge `E+1` before the commit, so an attempt of
epoch `E` cannot write to it, and every cursor or completion update of
that attempt is conditioned on `epoch == E` and fails. No new protocol,
no new group encoding. Scheduling is driven by a per-group status
record, advisory only.

## Design

- **Status record** `("al","repairstat",group)` (`RepairStatus`, golden
  `records-v2.hex`): slot, target, epoch, reason, detail, since, last
  progress, consecutive target failures, consecutive stalls, next
  attempt, the nodes that left the slot during this repair (`former`),
  the untrusted ones, whether the slot is whole (`walked_clean`),
  retargets. Removed when the group is `Active` again or gone.
- **Reasons**: `progressing`, `temporarily-blocked`, `awaiting-capacity`,
  `awaiting-nodes`, `data-unrecoverable`, `permanently-blocked` (needs an
  operator: `Repairing` without a cursor). The group state stays
  `Repairing` for all of them.
- **Decision per pass** (`repair_liveness::decide`, a pure function):
  1. target unhealthy, draining, retired or quarantined → retarget;
  2. `RETARGET_AFTER` consecutive target failures → retarget;
  3. the slot is whole and another member is unhealthy → replace it
     (auto repair) or report `awaiting-nodes`;
  4. before the recorded next attempt → wait (unless the nodes waited
     for are back);
  5. otherwise run.
- **Outcome** (`repair_liveness::after`): budget spent → progressing;
  walk finished, check failed → `awaiting-nodes` if unhealthy members
  did not answer, else `temporarily-blocked`; chunks with fewer than `k`
  → `awaiting-nodes` if a member is down, else `data-unrecoverable`
  (retried hourly); target failure → counted. Backoff 15 s doubling to
  10 min (`ControllerConfig::repair_pacing`).
- **Choosing a new target**: `choose_for` with the capacity check of a
  first choice, excluding every node that left the slot in this repair.
  None → `awaiting-capacity` / `awaiting-nodes`; a target that still
  answers is then tried again after the backoff.
- **Sources**: the nodes that left the slot (the first node, earlier
  targets) are used as that slot's source while they answer, as a
  drained node is. An untrusted one is used only as one of the `k`
  inputs of a reconstruction (validated against the chunk's checksum),
  never as a direct copy.
- **Resume**: an open intent naming the slot under rebuild is resumed,
  not settled, whatever the old target's health.
- **Background work**: rebalancing and tiering wait only for a group
  whose repair is progressing or temporarily blocked (or has no status
  yet); cache repair likewise. A drained node is decommissioned once no
  group lists it and it holds no shard of a group being rebuilt; what it
  waits for is its drain's `blocked` reason.
- **Reporting**: `admin status` prints every status;
  `arca_controller_repair_retargets_total`,
  `arca_controller_groups_repair_waiting`.

## Invariants

- **R1 (no completion without the shards).** A group becomes `Active`
  only through the completeness check: every member answers, every
  chunk placed in the group is listed on every member, the target's
  shards verify, no stripe-repair task of the slot is open. Unchanged;
  no timeout or status changes it.
- **R2 (stale attempts fenced).** After a retarget committed `E+1`, no
  write of an attempt of epoch `E` is accepted by the new target, and
  no cursor or completion update of epoch `E` is applied.
- **R3 (one change at a time).** At most one epoch intent per group;
  a change is committed only conditioned on the epoch it began at.
- **R4 (no needed member replaced).** A member other than the slot
  under rebuild is replaced only once that slot is whole (its shards
  are then not needed for any chunk to have `k`).
- **R5 (targets).** A target is healthy, not draining, not retired or
  quarantined when chosen, and never a node that left the slot in this
  repair.
- **R6 (retention).** The old target's shards are a former member's:
  kept by reconciliation and needed for deferred reclamation while the
  group is `Repairing`, deleted (retired, ADR-0028) once it is `Active`.
  A retarget does not narrow ADR-0028's protection.
- **L1.** A repairable group with a node that can take the slot makes
  progress within the longest backoff while failures are not
  continuous.
- **L2.** A group that cannot progress holds neither other groups'
  repairs, rebalancing, tiering, cache repair, nor drains of nodes that
  hold none of its shards.
- **L3.** Retries are bounded in rate (backoff; hourly for unrecoverable
  data), and retargets by the number of nodes.

## Second failures: the guards (independent reviews)

A second failed member is replaced only on the outcome of the walk just
run, and only if: that walk found the slot whole; no chunk it saw has
fewer than `k` shards on the members that answered (a truncated list of
missing shards counts as short); at least `k` members answered; every
shard of the slot that a former node of the slot (trusted or not) still
lists is on the slot's member; and members down plus former nodes down are
at most `m`. A chunk listed by no answering member is invisible to the
walk; within `m` failures every chunk keeps a shard on an answering
member. Beyond `m` it is a residual (Medium), as it already was for
completion: the former copies are then retired, not deleted, and kept for
the retention. With one parity (`m = 1`) the last guard means a second
failure after a lost first member is never replaced automatically: the
group waits (`awaiting-nodes`) for a node to return or an operator.

## Capacity

No reservation exists on a target: capacity is checked when a target is
chosen (failure repair: down to the repair floor; a planned move keeps
the client reserve). A retarget therefore frees nothing explicitly; the
old target's partial shards are freed by reconciliation once the group
is `Active` (after the retention of ADR-0028).

## Verification

- `repair_retarget.rs`: target down (with a leader change, and a stale
  epoch write refused), target refusing writes, second failure with one
  parity (waits, the member kept) and with two (replaced once the slot
  is whole), unrecoverable group not holding a drain or background work,
  leader crash between fence and commit of a retarget.
- `membership.rs`: the Phase 3.10 isolation test now expects the stuck
  group's slot rebuilt elsewhere.
- `repair_liveness_sim.rs`: deterministic simulation of the rules over
  seeds (S1 completion, S2/S2-strict shard counts, S3 targets, L1, L2);
  a mutation replacing a second member early is caught.
- Mutations (removed retarget, removed whole-slot guard, slot always
  whole, settled instead of resumed, global drain hold, global
  background hold): each fails its test (Phase 3.13 verification).

## Compatibility

- New key `repairstat`, never read by earlier releases. A rolled-back
  leader ignores it (and behaves as Phase 3.12: a stuck target stays
  stuck); a status of another epoch than its group's is stale and
  replaced; statuses of `Active` groups are removed.
- No protocol or segment format change for this decision.
- Rollback while a retarget is in flight: the intent is an existing
  record; an earlier leader resumes or settles it by its own rules.
