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
3. Restore: first `admin delete-fence close --reason restore --yes`
   (every storage node stops applying deletes decided on the current
   metadata; ADR-0023), then stop gateways and controllers,
   `fdbrestore` into an empty database, then follow `admin recovery`
   (RFC-015 §5, RFC-013 §5.9): the cluster
   enters a recovery state, a new incarnation fences every process of
   the old one, verification checks the restored metadata against the
   nodes, and `admin recovery finalize` reopens it. Objects whose data
   was deleted after the backup point are reported as lost.

A metadata backup cannot show that the shards it names still exist
(R-67): the recovery pass and scrubbing do. The full procedure and its
drills: [RFC-015](../rfcs/RFC-015-backup-restore-and-disaster-recovery.md),
RFC-013 §5.9.
