# RFC-015: Backup, Restore and Disaster Recovery

- Status: draft (Phase 3.0 design; implementation and drills Phase 3.3)
- Authors: arca-lake maintainers
- Created: 2026-11-02
- Depends on: RFC-003, RFC-004, RFC-006, RFC-008, RFC-011, RFC-014
- Related ADRs: ADR-0007 (GC safety), ADR-0008 (deletion epochs), ADR-0013

This is a public document.

## 1. Problem

The metadata store is the only place that says which objects exist and
where their shards are; the storage nodes hold the bytes and know
nothing about objects. A FoundationDB backup is therefore necessary but
not a backup of the system: restoring metadata to time `T` while the
nodes hold the state of time `now` creates three kinds of
contradiction, and the controller's reconciliation and GC — designed to
remove orphans — would turn one of them into data loss if they ran
unchanged. This RFC defines what is backed up, what a restore means,
what must be verified before the cluster is allowed to delete anything,
and what the recovery guarantees are.

## 2. Goals

- Continuous, point-in-time-capable backup of metadata with FoundationDB's
  own tools; a documented RPO and RTO.
- A restore procedure that reconciles metadata-at-`T` with nodes-at-`now`
  safely: nothing is deleted until verification says it is unreferenced
  under the restored metadata *and* the post-`T` writes are accounted
  for.
- Explicit handling of orphaned shards (written after `T`), missing
  shards (deleted after `T` by GC), stale group epochs (raised after
  `T`), and cache metadata.
- Disaster recovery scenarios with their outcomes stated honestly.

## 3. Non-goals

Backing up shard data (durability of the bytes is erasure coding across
failure domains; a copy of every segment would double the footprint —
site-level replication is a later RFC); object-level restore of a
deleted object (versioning covers that within its retention);
continuous replication of metadata to another site (FoundationDB DR
agents can do it; the procedure below applies to the DR copy as to a
backup).

## 4. Requirements

| # | Requirement | Status |
| --- | --- | --- |
| B1 | `fdbbackup` continuous backup to an object store or file system, with point-in-time restore to any `T` in the retention window | Assumption (FoundationDB feature; procedure to be exercised) |
| B2 | Restore never results in GC or reconciliation deleting shards that the restored metadata does not reference *until* a verification pass has classified them | Open (§5.3) |
| B3 | Objects whose shards were collected after `T` are detected and reported, never served as bytes | Open (§5.3) |
| B4 | Group epochs on nodes ahead of restored metadata are caught up without data movement | Open (§5.4) |
| B5 | Cache metadata after restore is treated as advisory: wrong placements fall back and are invalidated; the usage counter is recomputed | Partly verified (fallback and invalidation exist, RFC-006 §5.3); recompute Open |
| B6 | A full DR drill (restore into an empty FoundationDB cluster with the existing nodes) is executed on the validation hosts and timed | Open (Phase 3.3) |

## 5. Proposed design

### 5.1 What is backed up

| Item | How | Why |
| --- | --- | --- |
| Metadata (`("al",…)`) | `fdbbackup start -d <url> --continuous` (whole database; arca-lake owns the keyspace) | the truth about objects, placements, groups, epochs, leases, GC queues |
| Layout and `min_software` keys | inside the above | needed for RFC-014's startup gate after restore |
| Node identities | node records inside the above; each node's `group-epochs.txt` and segment directory stay on the node | the restore procedure treats nodes as live, not as backups |
| Credentials, certificates, cluster file | **not** in FoundationDB; backed up by the deployment's secret management | RFC-012 §5.10 |
| Configuration (flags, profiles) | deployment's configuration management | reproducibility |

Backups contain no credentials. They contain object keys and user
metadata: encrypt the backup location (FoundationDB supports encrypted
backup URLs; otherwise encrypt the target).

### 5.2 Restore semantics

Restoring to `T` means: every object committed before `T` exists with
its placements as of `T`; every object committed after `T` does not
exist; every deletion after `T` is undone *in metadata*. On the nodes,
however, time went on to `now`:

| Node state vs restored metadata | Name | Consequence | Handling |
| --- | --- | --- | --- |
| Shards of generations committed after `T` | **orphans** | metadata does not reference them; reconciliation would delete them (RFC-008 §6) | they are *not* recoverable as objects (their headers are gone); deleting them is correct, **but only after** verification (§5.3) so that the next class is found first |
| Shards collected by GC after `T` for objects deleted after `T` | **missing** | metadata says the object exists; its shards are gone | detected by verification; the object is marked `Unreadable` and reported; it cannot be served |
| Shards partially collected (some members) | **degraded** | fewer than all, maybe fewer than `k` | ≥ `k` → repair rewrites; < `k` → unreadable, reported |
| Group epochs raised after `T` (repairs, membership changes) | **stale epochs** | restored group records have lower epochs than nodes; nodes reject writes with those epochs | epoch catch-up (§5.4) |
| Membership changed after `T` | **stale membership** | restored group lists an old member that has since been replaced | verification reads from any `k` members that still hold the shard; repair re-establishes the restored membership or the operator accepts the node's view (§5.4) |
| Cache placements | **advisory** | copies may be gone or belong to different generations | fallback and invalidation (exists); usage recomputed |
| Pending generations at `T` | **expired** | leases long gone | GC's expiry pass handles them normally |

### 5.3 Restore procedure (metadata)

1. **Freeze**: stop gateways and controllers (or set the cluster
   `read_only` flag, §5.6, so gateways serve reads only and controllers
   stop GC, reconciliation, repair, eviction, tiering, lifecycle).
2. **Restore** FoundationDB to `T` (`fdbrestore`), or into a fresh
   cluster with the same cluster file name for the processes.
3. **Mark the cluster `restored_at = T, verification_pending = true`**
   (`admin restore mark --at T`): while set, the controller's
   reconciliation and GC are **disabled** and refuse to run; the gateway
   serves reads and refuses writes with `503` (the epoch catch-up has not
   happened yet).
4. **Epoch catch-up** (§5.4).
5. **Verification pass** (`admin restore verify`, runs on the controller
   leader, resumable, rate limited): for every group, walk the chunk
   records of every committed generation that places in it and
   `VerifyShards` on every member; classify each chunk `ok`, `degraded`
   (≥ `k` good), `unreadable` (< `k`). Write the result to
   `("al","meta","restore","report")` and export counters; mark
   unreadable generations with `integrity: Unreadable` so that GET/HEAD
   return an integrity error, listing shows the flag, and nothing serves
   bytes that are not there. Then walk every node's `ListShards` and
   classify shards as referenced / orphan (post-`T` generations, by
   UUIDv7 time > `T`) / former-member.
6. **Report** (`admin restore report`): counts and a list of unreadable
   objects (bucket/key/version) for the operator; orphans by node with
   their total bytes.
7. **Decide**: the operator either accepts the report (`admin restore
   accept`: clears `verification_pending`, re-enables writes, GC and
   reconciliation, which may now delete orphans after `min_orphan_age`
   as usual) or restores to a different `T` and repeats.
8. **Repair**: degraded chunks are queued for ordinary repair.

The hard rule (B2): with `verification_pending` set, no code path on any
component deletes a shard. The flag is a metadata key checked by every
GC/reconcile transaction (fenced, so a stale leader cannot bypass it).

### 5.4 Epoch catch-up

Nodes persist the highest epoch per group (`group-epochs.txt`); restored
group records may carry a lower epoch. Writers would be rejected
(`FAILED_PRECONDITION`). Catch-up: for every group, the controller reads
each member's epoch (`Health` extended with per-group epochs, Phase 3.3)
and sets the group record's epoch to `max(member epochs) + 1`, then
fences every member to it (`UpdateGroupEpoch`), as a membership change
does (RFC-008 §6.4). This is metadata-only and does not move data. If a
restored member no longer holds the group (it was replaced after `T`),
the operator chooses: keep the restored membership and let repair
rewrite the member (data movement), or `admin restore adopt-membership`
to take the nodes' current membership for that group (metadata-only;
valid only if ≥ `k` of the restored data shards verify on it).

### 5.5 Cache metadata recovery

After restore the cache tier's placements may point at copies that no
longer exist or that hold different generations' data. Nothing needs a
special pass: a lookup hit that cannot be read falls back to the durable
read and invalidates (RFC-006 §5.3); the recency index is advisory. The
usage counter (`("al","cusage",tier)`) is recomputed by `admin cache
recount` (sum of placement bytes, one range read per tier) so that
capacity checks are correct again; eviction then proceeds normally.

### 5.6 Cluster flags

`("al","meta","flags") = { read_only: bool, verification_pending: bool,
restored_at: Option<ms> }`: read by gateways (reject writes with `503
ServiceUnavailable` and a `x-amz-error-detail: cluster read-only`
header) and by controller schedulers (GC, reconcile, repair writes,
eviction, tiering, lifecycle) at the start of every pass and inside
every deleting transaction. `admin cluster set-read-only on|off` is also
the tool for planned maintenance.

### 5.7 Recovery objectives

| Scenario | RPO | RTO (estimate; measured in the drill) | Outcome |
| --- | --- | --- | --- |
| FoundationDB process loss within redundancy | 0 | seconds (FoundationDB) | no action |
| FoundationDB quorum loss, disks intact | 0 | minutes (FoundationDB recovery) | no action in arca-lake |
| FoundationDB data loss, continuous backup | backup lag (seconds–minutes) | restore time + epoch catch-up + verification (proportional to shard count; the drill measures per million shards) | post-`T` writes lost; post-`T` deletes undone except where GC already ran → reported unreadable |
| Loss of `≤ m` nodes per group | 0 | repair time | no action beyond repair |
| Loss of `> m` nodes in one group | n/a | — | those chunks unrecoverable; reported; objects marked unreadable |
| Loss of a whole site | everything (no cross-site copy) | rebuild | only what a client kept; cross-site replication is a future RFC |
| Operator error (wrong `T`) | — | repeat restore | no deletion happened before `accept` |

### 5.8 Backup validation

A scheduled job restores the latest backup into a scratch FoundationDB
(no nodes), runs `admin restore verify --metadata-only` (consistency of
records: every current pointer has a header, every header's chunks are
present, every group's members are node records) and reports; a
backup that fails this check pages.

## 6. Alternatives considered

| Decision | Chosen | Alternatives |
| --- | --- | --- |
| Backup unit | FoundationDB backup only + live nodes | full node snapshots (2× capacity, inconsistent with metadata time anyway); application-level export of manifests (slower, same gaps) |
| Post-restore safety | explicit `verification_pending` gate checked in every deleting transaction | rely on `min_orphan_age` (post-`T` orphans older than that would be deleted before anyone looked); a long global pause (humans forget) |
| Unreadable objects | marked, refused, reported | silently 404 (hides loss); serve partial bytes (never) |
| Epochs | catch-up from nodes, metadata-only | rewrite all shards (unnecessary); ignore epochs (breaks fencing) |
| Read-only flag | metadata key checked per transaction | process flags (not fenced; forgotten on restart) |

Evaluated on durability (nothing deleted without classification),
consistency (unreadable is a state, not a lie), performance
(verification is a scan proportional to shard count, rate limited),
operations (five commands, one report), security (flags and reports are
metadata under the controller's identity).

## 7. Failure scenarios

| Scenario | Behaviour |
| --- | --- |
| Controller crashes during verification | cursor; the next leader resumes; the gate stays set |
| A node is down during verification | its shards count as missing for that pass; the report marks the pass incomplete for that node; re-run after the node returns before `accept` |
| Operator runs `accept` with an incomplete report | refused unless `--force`, which is audited |
| Backup target unreachable | FoundationDB backup lags; alert on backup age (RFC-013 §8 gains a rule) |
| Restore into a cluster with a different layout version than the backup | RFC-014 startup gate refuses the processes until the matching software is used |

## 8. Consistency and durability guarantees

After `accept`, the cluster satisfies RFC-008's invariants with respect
to the restored metadata: every referenced shard verified or is queued
for repair, every unreferenced shard is subject to the ordinary orphan
rules, every unreadable object is marked and never served. Durable ACK
holds for writes after the restore; writes between `T` and the failure
are lost and that loss is reported, not hidden.

## 9. Performance considerations

Verification reads every shard header and, with `--deep`, every byte;
header-only verification of a million shards is minutes, deep
verification is bound by disk read throughput (RFC-009 §6.13). Both are
rate limited so that reads continue to be served during the pass.

## 10. Open questions

- Should verification also compare shard CRCs across members to catch
  post-`T` *overwrites* of a chunk id (impossible by construction: chunk
  ids are generation-unique UUIDv7)? Proposal: no.
- Retention of the restore report: keep until the next restore
  (proposal) or forever.

## 11. Acceptance criteria

- Drill on the validation hosts: write objects, take a continuous
  backup, write and delete more objects, run GC, kill FoundationDB's
  data, restore to `T`, catch up epochs, verify, report lists exactly
  the deleted-after-`T` objects as unreadable and the post-`T` objects as
  orphans, GET of an unreadable object returns the integrity error,
  `accept`, reconciliation deletes the orphans after the age gate,
  repair fixes degraded chunks, every surviving object verifies against
  its client-side checksum. Timed and recorded in RFC-013.
- No deleting transaction succeeds while `verification_pending` is set
  (fault test with an injected stale leader).
- Backup validation job detects a corrupted backup fixture.
