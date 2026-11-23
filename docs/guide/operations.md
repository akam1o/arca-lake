# Operational requirements

What a deployment must provide. Each item is a requirement of the
design, not a tuning hint; violating one voids a guarantee.

| Requirement | Why | Reference |
| --- | --- | --- |
| FoundationDB on its own storage, with a supervisor restarting `fdbserver` | A transaction log commit stalled for more than 10 s stops FoundationDB (`io_timeout`); on shared virtual storage this happened under load in Phase 3.5 and 3.6 | R-79 |
| One file system (device) per storage node, or `--capacity-bytes` allotments that sum to at most its size | Capacity admission is per node; nodes sharing free space can together cross their reserves. The controller detects shared file systems and keeps their groups out of placement | R-80, `admin check-config` |
| Free space above each node's reserve in normal operation | Writes are throttled near it and refused at it; the reserve is for compaction, rollover, quarantine and repair | RFC-008 §6.8 |
| Distinct failure domains (`--failure-domain` label values) per group member | A group tolerates `m` member losses only if they fail independently. Replacements use the controller's `--failure-domain`: keep it the label the groups were created with (a group's own label is not recorded; R-119) | RFC-004 §5.3 |
| Mutual TLS for node RPCs and TLS to FoundationDB | Plaintext is refused unless explicitly enabled | ADR-0017 |
| A TLS terminator in front of the gateways | The S3 listener speaks plain HTTP | RFC-012 |
| At least two controllers | Leadership fails over by lease; a single controller stops GC, repair and write admission while down (gateways then refuse new data after 5 minutes) | RFC-011, RFC-008 §6.8 |
| Clock synchronisation (NTP), skew below `--reclaim-skew-secs` (60 s) | Leases, grace periods and admission record ages depend on clocks; deferred reclamation's protection of restores assumes the skew stays below the allowance | RFC-008 §6.7, ADR-0028 |
| Prometheus scraping every process and the alert rules loaded | Capacity, GC, repair and integrity problems are detected by alerts | RFC-013 §7–8 |
| Metadata backups and their verification | Restores are the only recovery from metadata loss | RFC-015, [Backup and restore](backup-restore.md) |
| Upgrades in the documented order (storage nodes first) | Mixed versions are supported within one compatibility window only | RFC-014 |
| To save the cost of access records where no tiering policy is wanted: `admin upgrade finalize` to schema 4, then `admin tiering disable` | Gateways stop recording reads only once the leader has acknowledged the disable (`admin tiering status`: `acknowledged true`; `arca_gateway_access_recording` 0); the finalize is one way | ADR-0035 |
| Tiering paused for one pass after an upgrade of a cluster with `--tiering-*` flags, then no demotion for `demote_after` plus the grace margin | The flags seed the tiering record, which runs once acknowledged; the grace keeps generations from being demoted on reads that were never recorded | ADR-0035 §10.4 |
| A store of schema 2 upgraded (and finalized) through a release of schema 3 before this one | This release reads schemas 3 and 4 only; after its finalize (schema 4) no earlier release starts and no downgrade is possible | RFC-014 §5.4, ADR-0035 |
| The delete fence closed (`admin delete-fence close`) before a metadata restore | Without it a delete decided before the restore can still remove shards the restored metadata needs | ADR-0023, RFC-013 §5.9 |
| `--fence-lease-secs` (storage nodes) well above the controllers' `--health-interval-secs` (default 30 s against 5 s) | A node admits deletes only within the lease after the leader's last push; a lease shorter than a pass stops deletes | ADR-0024, RFC-013 §10.12 |
| Every storage node reachable (or decommissioned) for `admin upgrade finalize`, and reachable or declared absent for `admin recovery begin` | Both collect the nodes' schema floor and incarnation sequence; a node that missed them could take a previous release's deletes | ADR-0024 |
| After `upgrade finalize`, no controller or gateway of an earlier release; restored metadata only through `admin recovery begin` | The earlier release refuses to start on the finalized schema; `recovery begin` brings restored metadata back up to it | RFC-013 §5.8 |
| Every controller upgraded before a leader of Phase 3.11 or later runs | Once nodes hold identities they refuse deletes that carry none, which earlier controllers send: GC and reconciliation would stall under an earlier leader | ADR-0027 |
| Metadata restores through `admin restore`, of backups younger than the nodes' retention less twice the skew allowance; `admin recovery begin` after any other restore | Deferred reclamation protects every restore of such a backup whatever its path; `admin restore` adds its checks; older backups lose what was reclaimed since (R-94(b), bounded) | ADR-0026, ADR-0028, [Backup and restore](backup-restore.md) |
| `--reclaim-retention-secs` (storage nodes, default 86 400) at least the age of the oldest backup that may be restored, the same on every node; capacity for a retention's worth of deletes | A deleted durable shard stays on disk for the retention (`reclaim_pending` in health); a node with 0 deletes at once and is outside the protection (`arca_controller_nodes_not_deferring`) | ADR-0028, R-106 |
| A node whose `reclaim_damaged` is reported: stop it, move `pending-reclaim.log`, `pending-reclaim.prev` and `pending-reclaim.retention` aside, start it | Nothing is reclaimed on it while its state is damaged (it fails closed, at any retention, 0 included: its durable deletes and writes may be refused); reconciliation and GC retire what is still unneeded again | ADR-0028, ADR-0030 |
| Capacity for the deletes of one retention: `reclaim_retained_bytes` in node health is the payload the retention holds; it is counted as used, never as room | Space returns only once reclaimed; write admission counts reclaims, not GC's deletes, as room coming back while nodes defer | ADR-0030 |
| Lowering `--reclaim-retention-secs` is safe for what was retired before (it keeps the earlier one); roll a node back to Phase 3.12 only after a clean stop (no `pending-reclaim.prev` left) and with the earlier retention configured | Phase 3.12 reads only `pending-reclaim.log` and applies its configured retention to every record | ADR-0030 |
| Up to about 1e7 retired records per node (measured): start-up replays them (about 0.7 µs each), memory about 108 B each | The node holds its retirements in memory and replays them at start | ADR-0030 |
| A `Repairing` group's status in `admin status` (`repair group … status …`): `awaiting-nodes`, `awaiting-capacity` and `data-unrecoverable` need an operator (bring a node back, add capacity, or accept the loss of the chunks listed by stripe repair); `permanently-blocked` needs one at once | The leader rebuilds onto another node when a target is gone or keeps failing, and replaces a second failed member once the first slot is whole; it never makes a group `Active` without every chunk's shards, and never replaces a member whose shards are still needed | ADR-0029 |
| Watch `arca_controller_repair_groups{reason=...}` (one series per reason; `data-unrecoverable` means chunks with fewer than `k` readable shards: possible data loss), `arca_controller_repair_longest_stall_seconds` with `_group`, and `arca_controller_repair_attempt_max_seconds`; `admin status` prints each waiting repair's stall time and the operator's options | A repair that waits is safe (never `Active` without every shard, nothing deleted for it) but keeps redundancy reduced; the reasons are not summed into one figure on purpose | ADR-0029, Phase 3.14 |
| Capacity for a member move above what other moves reserved: `admin status` lists reservations (`reservation node … remaining … holds …`), `arca_controller_capacity_reserved_bytes`; a move refused for it shows `awaiting-capacity` with the room and what is reserved | Reservations stop two moves from being started onto one room; the repair floor and the reserves are never lowered for a move | ADR-0032 |
| Leave `--reconcile-pairs-per-pass` at 0 (every pair per pass) unless reconciliation passes are too long; a budget makes a sweep take several passes, and rebalancing's settle time (`--rebalance-settle-secs` defaults from the reconcile interval) must then cover a whole sweep | Moved sources are freed by reconciliation; a plan made before they are freed would move data back | Phase 3.14 review |
| Alert on `arca_node_reclaim_damaged` = 1 and on `arca_node_reclaim_retained_bytes` against the free space; `arca_controller_reconcile_errors_total` and `_tiering_errors_total` rising mean records passed over (see the controller log) | A damaged retirement state reclaims nothing; passed-over items are retried at the next sweep | ADR-0030, Phase 3.14 |
| Nodes taken out with `admin node decommission` then `admin node unregister` (or `keep-removed` after a manual restore) | Removal closes the node's fence with a hold on its own disk; a node simply left offline can be named again by restored metadata | ADR-0027, RFC-013 §5.10 |

## Versions and compatibility

The software version (0.2.0) is separate from the compatibility axes
that decide which builds can run together and which data they read:

| Axis | 0.2.0 writes | 0.2.0 accepts or reads | 0.1.0 writes / reads | Source |
| --- | --- | --- | --- | --- |
| Wire protocol (node RPCs) | 4 | 3–4 | 4 / 3–4 | `crates/protocol/src/lib.rs` |
| Metadata schema | 5 (4 until finalized) | 4–5 | 4 / 3–4 | `crates/metadata/src/keys.rs` |
| Metadata record version byte | 2 | 1–2 | 2 / 1–2 | `crates/metadata/src/records.rs` |
| Segment format | 2 | 1–2 | 2 / 1–2 | `crates/storage/src/format.rs` |
| Storage record version | 1 | 1 | 1 / 1 | `crates/storage/src/format.rs` |

- A new 0.2.0 store initialises at schema 5 and segment format 2.
- Schema 5 ([ADR-0039](../adr/0039-schema-5-and-the-0.1.0-upgrade.md))
  changes no record layout. It marks the point after which no 0.1.0
  process runs, so the 0.2.0 protections that 0.1.0 would bypass
  (strict write ownership, the failure domain recorded on each group)
  are mandatory. Below it they are degraded: `admin status --versions`
  prints `protections degraded` or `protections mandatory`, and flags
  each 0.1.0 (or unknown) process and node with `below-schema-5`.
- 0.1.0 is the supported upgrade predecessor of 0.2.0. Stores of schema
  3 or below (development commits before 0.1.0) are refused by 0.2.0:
  upgrade them to 0.1.0 and finalize first. 0.1.0 itself has no
  supported predecessor; the schema 2 and 3 "releases" in the changelog
  are pre-release development commits.
- Upgrades go one step at a time, in order, within one compatibility
  window (adjacent versions only): storage nodes first, then controllers
  (followers, leader last), then gateways (RFC-014 §5.8).
- `admin upgrade finalize` is one-way: after it, earlier builds refuse
  to start and no downgrade is possible; restore of a pre-finalize
  backup is the only way back (RFC-014, RFC-015).

### Upgrading from 0.1.0 to 0.2.0

1. Take a backup (RFC-015) and read the 0.2.0 release notes.
2. Storage nodes, one failure domain at a time: stop, replace the
   binary, start, and wait until the node answers and is seen
   (`admin node list`).
3. Controllers: followers first, the leader last.
4. Gateways: drain, restart, return.
5. Check `admin status --versions`. No line may end in `below-schema-5`
   (a 0.1.0 or unknown process or node). The heartbeat of a process you
   stopped stays for 90 s; name it with `--ignore-absent <id>` instead
   of waiting.
6. When satisfied, run `admin upgrade finalize --wait`. It refuses while
   any 0.1.0 process or node takes part, at the request, again before
   the cut-off, and at the last step. The reason is shown by
   `admin upgrade status`; the finalize goes on once the process is
   stopped. It completes across a controller restart or failover.

Until the finalize raises `schema_write` (the cut-off), you can return
any process to 0.1.0, in any order: records are unchanged. 0.2.0
re-checks the cluster when it returns. `admin upgrade cancel` withdraws a
finalize that has not reached the cut-off.

After the finalize (schema 5) there is no way back to 0.1.0 except
restoring a pre-finalize backup:

- 0.1.0 controllers and gateways refuse to start.
- A 0.1.0 storage node still starts (it reads no metadata), but the
  0.2.0 leader quarantines it: no deletes, and with `--auto-repair` its
  members are repaired away. `admin node rejoin` refuses it. Run 0.2.0
  on it, then `admin node rejoin --node <id> --yes`. Until its members
  move, gateways may still write shards to it, so never start a 0.1.0
  node after the finalize.
