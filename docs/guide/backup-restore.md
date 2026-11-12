# Backup and restore

The unit of backup is the metadata: object data lives on the storage
nodes, erasure-coded, and is not copied by a backup. A restore brings
back the metadata of a point in time; the recovery pass then verifies
that the shards it names exist (RFC-015).

1. Back up FoundationDB with its own tools (`fdbbackup`), on a schedule,
   to storage outside the cluster.
2. Verify backups continuously: `admin backup verify-restored` and
   `admin backup key-range`, or `deploy/backup/verify-backup.sh`, which
   exports metrics (`arca_backup_*`) for the alert rules.
3. Restore with `admin restore` (Phase 3.11, the supported procedure):
   `plan` checks the cluster and every node and prints the target
   cluster, the source and the destination with a confirmation token;
   `run --confirm TOKEN` freezes the cluster, closes and holds the delete
   fence on every node (each verified on its own disk), clears the
   destination, runs `fdbrestore`, marks the restored metadata and
   verifies it; `finish` finalizes and waits for a new fence generation
   on every node. Every step is recorded in a local journal: re-run
   `run` or `finish` after any failure. The guarantee covers restores
   done this way only, under its stated conditions (no controller or
   gateway running, nobody changing node fences by hand meanwhile, one
   restore at a time; ADR-0026). Since Phase 3.12 `plan` also refuses a
   backup outside deferred reclamation's protection (below) unless
   `--accept-unprotected-backup` is given, and checks it again right
   before the clear; the destination is identified by content (a probe
   written through `admin restore`'s own connection must be read back by
   `fdbcli` where `fdbrestore` runs), and the restored metadata must carry
   this cluster's uid. If the restore was of another cluster's backup,
   `admin restore amend --source ... --version ...` replaces it and `run`
   resumes.

   **Deferred reclamation (Phase 3.12, ADR-0028).** Storage nodes keep
   every deleted durable shard readable for `--reclaim-retention-secs`
   (default one day) and remove it only when a controller confirms, at
   least that long later and under the same cluster incarnation, that the
   metadata no longer needs it. Every restore, whatever its path (a direct
   `fdbrestore`, a DR switch, a snapshot rollback, `admin restore`), of a
   backup younger than the retention less twice `--reclaim-skew-secs` finds
   the shards its metadata needs, even those deleted after the backup point
   by a leader still running. Restore older backups only knowing that what
   was reclaimed since is lost (the recovery pass reports it). Nodes of
   earlier releases do not defer. Run `admin recovery begin` (or use
   `admin restore`) after any restore: a restored history operated under
   the old incarnation, followed by a second restore, is outside the
   protection (ADR-0028 H5).

   **Retention changes (Phase 3.13, ADR-0030).** Lowering
   `--reclaim-retention-secs` on a node shortens the protection only of
   what is retired after the change: records retired before keep the
   earlier retention (`pending-reclaim.retention` on the node). `admin
   restore` bounds a backup's age by the retention configured now, so
   after a lowering it refuses backups older than the new value even
   though the older records are still kept. A Phase 3.12 node does not
   honour the history: keep the earlier retention configured when rolling
   a node back.
4. When the current metadata is gone (`admin restore` then refuses to
   plan), the manual runbook: first `admin delete-fence close --reason restore --yes`
   (every storage node stops applying deletes decided on the current
   metadata; ADR-0023), then stop gateways and controllers,
   `fdbrestore` into an empty database, then follow `admin recovery`
   (RFC-015 §5, RFC-013 §5.9): the cluster
   enters a recovery state, a new incarnation fences every process of
   the old one, verification checks the restored metadata against the
   nodes, and `admin recovery finalize` reopens it. Objects whose data
   was deleted after the backup point are reported as lost.

A manual close (`admin delete-fence close`, `admin recovery begin`)
counts a node as acknowledging only if the disk at its address holds
that node's identity (Phase 3.13, R-105): a disk holding another node's
identity, or another registration of the same id, is listed as
`identity MISMATCH` and the close does not complete; a node that cannot
be verified (an earlier release, or no identity adopted yet) is counted
and listed as `identity unverified`. Resolve a mismatch before the
restore (`admin node adopt-identity` for a node whose identity the
metadata lost, or the node's own disk back at its address).

Since Phase 3.10 a restore done out of order (without step 0, or with
controllers started before `recovery begin`) deletes nothing: nodes
admit deletes only under the current leader's lease, and a controller on
restored metadata holds the fence (ADR-0024). `recovery begin` needs
every storage node to answer (or `--absent-node` for one whose process
is stopped).

A metadata backup cannot show that the shards it names still exist
(R-67): the recovery pass and scrubbing do. The full procedure and its
drills: [RFC-015](../rfcs/RFC-015-backup-restore-and-disaster-recovery.md),
RFC-013 §5.9.
