# ADR-0032: Capacity reservations of member moves

Status: **accepted** (Phase 3.14). Amends ADR-0029 (target choice) and
the rebalancing rules of Phase 3.10 (R-93). Closes the Phase 3.13
residual "no capacity reservation for planned moves".

## Context

A member move (a failure repair, a retarget, an operator's drain or
replacement) chooses its target on the node's room as measured when the
move begins (`capacity::node_room`: down to the repair floor for a
repair, the client reserve for a planned move), and writes the member's
shards over several passes (a chunk budget per pass). Nothing recorded
the room the move was going to use. So:

- two moves begun before the first had written its member chose the same
  node on the same room; both stalled at the node's reserve half done,
  each holding its group `Repairing` (and a planned one keeping a
  healthy source busy);
- since Phase 3.13 a repair waiting for a node (`awaiting-nodes`) no
  longer holds rebalancing or tiering, which may then fill its target;
- the in-memory set of planned moves (`Metrics.planned_moves`) was lost
  with the leadership: a new leader ran a started drain as failure work
  (no per-pass budget, down to the repair floor).

## Severity, re-evaluated per scenario

What already held before this ADR, from the node's own admission (it
refuses a client or migration write above the client reserve, a repair
write above the repair floor) and from the pass rules (rebalancing only
while no repair is in progress, only into `Active` groups, a chunk at a
time, the placement threshold above the reserve checked before each):

| Scenario | Data protection | Recoverability | Repair priority | Reserves | Progress | Leak |
| --- | --- | --- | --- | --- | --- | --- |
| Rebalancing fills the target's room | kept (it stops at the placement threshold, above the client reserve) | kept: the band down to the repair floor stays for repair | kept | kept (node-enforced) | the move pauses | none (no reservation) |
| Then another node fails | kept | the repair's member may not fit in the repair band: `awaiting-capacity`, the group degraded meanwhile | kept | kept | waits for an operator or room | — |
| Repair finds no target | kept | as above | — | kept | `awaiting-capacity` with the numbers | — |
| Retention keeps the room | kept | retained bytes count as used (ADR-0030): never over-promised | kept | kept | waits for the retention | — |
| Leader failover | kept | kept | a started planned move ran as failure work (repair floor, no budget) | kept by the nodes | yes | the planned set lost |
| Two controllers | only the leader schedules (fenced writes) | — | — | — | — | — |
| Two moves onto one room | kept | **both moves can stall half done** | a planned move could take room a later repair needed (above the client reserve only) | kept | **stalls until room comes back** | — |
| A move fails after it began | kept | the target keeps what was written (retired by reconciliation when retargeted) | — | kept | retried / retargeted | — |
| Controller crash before release | — | — | — | — | — | nothing to release |

No sequence found loses data or breaks a reserve: the reserves are
enforced by the nodes on every write. What the missing reservation
costs is **liveness and the length of reduced redundancy**: double
booking stalls moves half done, and work of lower priority can take a
waiting repair's room. Severity: **Medium** (as in Phase 3.13), with the
double-booking and waiting-repair cases fixed here.

## Decision

- **Record** `("al","capresv",node,group)` (`CapacityReservation`:
  node, group, epoch, slot, kind `repair` | `planned`, bytes, written,
  created). Keys of their own; earlier releases never read them (golden
  encoding in `records-v2.hex`).
- **Validity is derived, never stored.** A reservation counts only while
  its group is `Repairing` at the reservation's epoch or a later one
  (another slot's change or a settle keeps it: second review) with the
  node in the reservation's slot, or while the move is under way: the group's
  open epoch intent goes from the epoch before to this node in this slot
  (review: the reservation is written before the membership change
  commits, and a move between the two must not be granted the room). A
  completed, retargeted or never-begun move's
  reservation, or one a former leader wrote for an epoch that did not
  happen, counts for nothing. The leader's repair pass sweeps those
  (each checked again in its own transaction); the move's own completion
  also clears it. A lost release therefore cannot leak room, and a stale
  controller cannot make room count (its writes are fenced as well).
- **Reserve in one transaction** (`reservation::reserve`): read every
  reservation of the node and the groups they name, sum what the valid
  ones of the counted kinds still need, and write only if that plus the
  move's bytes fits the room measured just before (the room of
  `capacity::fits`: 10 % and 256 MiB of margin kept). Two moves (two
  leaders, or one pass) reserving one node read the same range and
  conflict; the retried one sees the other.
- **Repair first.** A repair counts only other repairs' reservations: it
  may take room a planned move reserved (the planned move then pauses at
  the client reserve, as before). A planned move counts every
  reservation.
- **Written bytes.** After each attempt, the bytes the move wrote to the
  target are added (`note_written`, only at the reservation's epoch): the
  node's own figures count them from then on, so they are not held twice.
  Shards found already there are not counted (review: a walk started
  again from the beginning, as after a completeness check that resets the
  cursor, finds the same ones; counting them would release room twice).
  An attempt that fails before reporting, or a write before a crash,
  adds nothing (the reservation is then larger than needed: the safe
  side).
- **Retained bytes are not counted twice.** A reservation holds only
  bytes not yet written; retired-but-retained records are in the node's
  used space (ADR-0030), never in a reservation.
- **Who honours it.** Target choice of every member move (`start_move`:
  the node's room minus the reservations that count), and capacity
  rebalancing (`target_takes`: a node's headroom minus every valid
  reservation on it, read in one transaction before each chunk, not once
  per pass: review). Tiering does not check room before a move (it
  relies on the node's client reserve, as before): a waiting repair's
  room above the client reserve can still be taken by a tiering move
  (residual below).
- **Planned moves survive a failover.** A `planned` reservation marks
  the move planned for any leader (the in-memory set stays as a
  fallback).
- **Never loosened.** The repair floor and the reserves are unchanged; a
  repair that finds no node with room reports `awaiting-capacity` with
  the node's room and what other moves hold there.

## Tests and mutants

| Test | Shows | Mutants killed |
| --- | --- | --- |
| `concurrent_reservations_of_one_room_grant_only_what_fits` | 16 racing reservations of 1 GiB on 4 GiB: exactly 4 granted | the in-transaction check removed |
| `only_reservations_of_current_moves_count_and_repair_comes_first` | repair counts only repairs; written bytes released; stale epoch, completed and retargeted moves count nothing; sweep removes exactly those | repair counts planned; validity without epoch / state; `written` ignored; sweep not clearing |
| `a_second_repair_is_not_started_onto_room_the_first_holds` | two groups lose a member to one node; the spare fits one; the second is never started onto it, across a leader failover; released at completion; repair floor kept | target choice ignoring reservations |
| `rebalance::rebalancing_leaves_room_reserved_by_a_waiting_repair` | a waiting repair's reserved room stops a rebalancing pass before any write; it moves once the repair is over | rebalancing ignoring reservations |
| `a_move_under_way_keeps_its_reservation` (review) | a reservation whose membership change is open (intent recorded) holds the room; settled, it is swept | pending moves not counted |
| `a_walk_started_again_releases_nothing_twice` (review) | a walk from the beginning counts only what it writes | found shards counted as written |
| `a_later_epoch_of_the_group_keeps_the_reservation` (second review) | another slot's epoch change keeps a running move's reservation | validity at the exact epoch only |

## Residuals

- **Between `reserve` and the epoch intent** the reservation neither holds
  nor is pending: a concurrent reservation would not count it. Only the
  leader starts moves, one at a time, so nothing reserves in that window
  today (Low).
- **A slot finished while another slot of the group still rebuilds**
  keeps its reservation until the group is `Active` (its written bytes
  bring what it holds down to about nothing; the safe side).
- **Bytes written by an attempt that fails** (or before a crash) are not
  subtracted: the reservation stays larger than needed until the move
  completes (the safe side; Low).
- **Check and write are not atomic across processes.** A reservation is
  checked against the node's room measured just before, and rebalancing
  checks the reservations just before a chunk: a rebalancing chunk in
  flight when a repair reserves can still land in the reserved room. At
  most one chunk per rebalancing pass: one shard of at most
  `max_chunk_bytes / k` per member (2 MiB with the default 4 MiB chunks
  of 2+1), within the 10 % + 256 MiB margin the reservation's room check
  keeps as long as a shard is below 256 MiB; the node's reserves are
  enforced on every write regardless (second review).
- **A shard written twice by one move** (written, found decayed by the
  completeness check, rewritten) is counted twice: what the reservation
  holds is then short by that shard's size (the node's own figures count
  both copies). Low: only shards that decayed during their move.

- **Tiering** is not reservation-aware (above). Medium → Low: tiering
  runs only when no tier is throttled and stops at the first refusal of
  the client reserve; a repair keeps the band below the client reserve.
- **Estimate.** The member's bytes are measured on a surviving member
  when the move begins (as before); a group that grows during its
  repair can need more than reserved.
- **Moves begun by an earlier release** carry no reservation until they
  end (counted by nothing). Rolling back to 3.13 or 3.12 ignores the
  reservations; going forward again, those that no longer hold are swept
  (Phase 3.14 verification document, rollback). One exception (second
  review, Low): if, while rolled back, the group completed and is
  `Repairing` again with the same node still in that slot, the old
  reservation holds again (a later epoch) until the group is `Active`:
  room held on that node for nothing, liveness only.
- **Client writes** do not see reservations (only the node reserves):
  a planned move's room above the client reserve can be taken by
  clients, as before; the move pauses, safe.
