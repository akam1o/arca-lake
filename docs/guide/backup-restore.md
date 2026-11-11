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
   restore at a time; ADR-0026). A restore done any other way (a direct
   `fdbrestore`, a DR switch, a snapshot rollback) is open risk R-94(b)
   (High).
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
