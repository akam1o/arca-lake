# Backup and restore

The unit of backup is the metadata: object data lives on the storage
nodes, erasure-coded, and is not copied by a backup. A restore brings
back the metadata of a point in time; the recovery pass then verifies
that the shards it names exist (RFC-015).

## Safety boundary

Use `admin restore` for managed restores and follow the recovery procedure
after a manual restore. Deferred reclamation also protects restores through
other paths, but only under the assumptions of
[ADR-0028](../adr/0028-deferred-reclamation.md):

- The backup is no older than the applicable retention minus twice the
  clock-skew allowance when the restored metadata becomes current.
- Every node holding durable shards supports deferred reclamation and has
  sufficient retention. A node with retention 0 provides no such protection.
- Controller, node and backup-system clocks stay within the skew allowance.
- Restored metadata is operated under a new cluster incarnation, established
  by `admin restore` or `admin recovery begin`.

Older backups may need shards already reclaimed. A surviving leader that
continues on directly restored metadata under the old incarnation, followed
by another restore of that intervening history, is outside the documented
protection. Delete fences, rejoin leases and the restore trip wire are
additional guards; they do not remove these conditions.

## Backup, verification and restore

1. Back up FoundationDB with its own tools (`fdbbackup`), on a schedule,
   to storage outside the cluster.
2. Verify backups continuously: `admin backup verify-restored` and
   `admin backup key-range`, or `deploy/backup/verify-backup.sh`, which
   exports metrics (`arca_backup_*`) for the alert rules.
3. Restore with `admin restore` (the supported managed procedure):
   `plan` checks the cluster and every node and prints the target
   cluster, the source and the destination with a confirmation token;
   `run --confirm TOKEN` freezes the cluster, closes and holds the delete
   fence on every node (each verified on its own disk), clears the
   destination, runs `fdbrestore`, marks the restored metadata and
   verifies it; `finish` finalizes and waits for a new fence generation
   on every node. Every step is recorded in a local journal: re-run
   `run` or `finish` after any failure. Its sequencing guarantee applies
   to this workflow under its stated conditions (no controller or
   gateway running, nobody changing node fences by hand meanwhile, one
   restore at a time; ADR-0026). `plan` also refuses a
   backup outside deferred reclamation's protection (below) unless
   `--accept-unprotected-backup` is given, and checks it again right
   before the clear; the destination is identified by content (a probe
   written through `admin restore`'s own connection must be read back by
   `fdbcli` where `fdbrestore` runs), and the restored metadata must carry
   this cluster's uid. If the restore was of another cluster's backup,
   `admin restore amend --source ... --version ...` replaces it and `run`
   resumes.

   **Deferred reclamation (ADR-0028).** Storage nodes keep
   every deleted durable shard readable for `--reclaim-retention-secs`
   (default one day) and remove it only when a controller confirms, at
   least that long later and under the same cluster incarnation, that the
   metadata no longer needs it. Every restore, whatever its path (a direct
   `fdbrestore`, a DR switch, a snapshot rollback, `admin restore`), of a
   backup within the retention less twice `--reclaim-skew-secs` is protected
   under the conditions above, including against retirements decided before
   the restore. Restore older backups only knowing that what
   was reclaimed since is lost (the recovery pass reports it). Nodes of
   earlier releases do not defer. Run `admin recovery begin` (or use
   `admin restore`) after any restore: a restored history operated under
   the old incarnation, followed by a second restore, is outside the
   protection (ADR-0028 H5).

   **Retention changes (ADR-0030).** Lowering
   `--reclaim-retention-secs` on a node shortens the protection only of
   what is retired after the change: records retired before keep the
   earlier retention (`pending-reclaim.retention` on the node). `admin
   restore` bounds a backup's age by the retention configured now, so
   after a lowering it refuses backups older than the new value even
   though the older records are still kept. A Phase 3.12 node does not
   honour the history: keep the earlier retention configured when rolling
   a node back.
   **Mixed releases and rollback (Phase 3.14).** Verified with real
   binaries: `admin restore` (plan, run, finish) with nodes of Phase 3.12
   and 3.13 among those of this release, then every process back on this
   release: a member lost afterwards is repaired, and deletes are
   reclaimed after the retention while earlier retirements keep theirs.
   A node rolled back to Phase 3.12 with a `pending-reclaim.prev` left
   (a crash during the log's compaction) forgets those retirements while
   it runs: it deletes nothing for them, does not reclaim them, and a
   later release merges them back; stop a node cleanly (no `.prev`)
   before rolling it back to 3.12. Capacity reservations of this release
   are ignored by earlier controllers and swept when it is back.
4. When the current metadata is gone (`admin restore` then refuses to
   plan), the manual runbook: first `admin delete-fence close --reason restore --yes`
   (every storage node stops applying deletes decided on the current
   metadata; ADR-0023), then stop gateways and controllers,
   `fdbrestore` into an empty database, then follow `admin recovery`
   (RFC-015 §5, RFC-013 §5.9): the cluster
   enters a recovery state, a new incarnation fences stale operations from
   the old one, verification checks the restored metadata against the
   nodes, and `admin recovery finalize` reopens it. Objects whose data
   was physically reclaimed after the backup point are reported as lost.

A manual close (`admin delete-fence close`, `admin recovery begin`)
counts a node as acknowledging only if the disk at its address holds
that node's identity (Phase 3.13, R-105): a disk holding another node's
identity, or another registration of the same id, is listed as
`identity MISMATCH` and the close does not complete; a node that cannot
be verified (an earlier release, or no identity adopted yet) is counted
and listed as `identity unverified`. Resolve a mismatch before the
restore (`admin node adopt-identity` for a node whose identity the
metadata lost, or the node's own disk back at its address).

Do not start controllers or gateways on manually restored metadata before
the recovery procedure permits it. Nodes admit deletes only under a valid
leader lease, and a controller that detects restored metadata holds the
fence (ADR-0024); this is not a substitute for the safety boundary above.
`recovery begin` needs every storage node to answer (or `--absent-node` for
one whose process is stopped).

A metadata backup cannot show that the shards it names still exist
(R-67): the recovery pass and scrubbing do. The full procedure and its
drills: [RFC-015](../rfcs/RFC-015-backup-restore-and-disaster-recovery.md),
RFC-013 §5.9.
