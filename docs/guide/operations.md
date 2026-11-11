# Operational requirements

What a deployment must provide. Each item is a requirement of the
design, not a tuning hint; violating one voids a guarantee.

| Requirement | Why | Reference |
| --- | --- | --- |
| FoundationDB on its own storage, with a supervisor restarting `fdbserver` | A transaction log commit stalled for more than 10 s stops FoundationDB (`io_timeout`); on shared virtual storage this happened under load in Phase 3.5 and 3.6 | R-79 |
| One file system (device) per storage node, or `--capacity-bytes` allotments that sum to at most its size | Capacity admission is per node; nodes sharing free space can together cross their reserves. The controller detects shared file systems and keeps their groups out of placement | R-80, `admin check-config` |
| Free space above each node's reserve in normal operation | Writes are throttled near it and refused at it; the reserve is for compaction, rollover, quarantine and repair | RFC-008 §6.8 |
| Distinct failure domains (`--failure-domain` label values) per group member | A group tolerates `m` member losses only if they fail independently | RFC-004 §5.3 |
| Mutual TLS for node RPCs and TLS to FoundationDB | Plaintext is refused unless explicitly enabled | ADR-0017 |
| A TLS terminator in front of the gateways | The S3 listener speaks plain HTTP | RFC-012 |
| At least two controllers | Leadership fails over by lease; a single controller stops GC, repair and write admission while down (gateways then refuse new data after 5 minutes) | RFC-011, RFC-008 §6.8 |
| Clock synchronisation (NTP), skew below `--reclaim-skew-secs` (60 s) | Leases, grace periods and admission record ages depend on clocks; deferred reclamation's protection of restores assumes the skew stays below the allowance | RFC-008 §6.7, ADR-0028 |
| Prometheus scraping every process and the alert rules loaded | Capacity, GC, repair and integrity problems are detected by alerts | RFC-013 §7–8 |
| Metadata backups and their verification | Restores are the only recovery from metadata loss | RFC-015, [Backup and restore](backup-restore.md) |
| Upgrades in the documented order (storage nodes first) | Mixed versions are supported within one compatibility window only | RFC-014 |
| The delete fence closed (`admin delete-fence close`) before a metadata restore | Without it a delete decided before the restore can still remove shards the restored metadata needs | ADR-0023, RFC-013 §5.9 |
| `--fence-lease-secs` (storage nodes) well above the controllers' `--health-interval-secs` (default 30 s against 5 s) | A node admits deletes only within the lease after the leader's last push; a lease shorter than a pass stops deletes | ADR-0024, RFC-013 §10.12 |
| Every storage node reachable (or decommissioned) for `admin upgrade finalize`, and reachable or declared absent for `admin recovery begin` | Both collect the nodes' schema floor and incarnation sequence; a node that missed them could take a previous release's deletes | ADR-0024 |
| After `upgrade finalize`, no controller or gateway of an earlier release; restored metadata only through `admin recovery begin` | The earlier release refuses to start on the finalized schema; `recovery begin` brings restored metadata back up to it | RFC-013 §5.8 |
| Every controller upgraded before a leader of Phase 3.11 or later runs | Once nodes hold identities they refuse deletes that carry none, which earlier controllers send: GC and reconciliation would stall under an earlier leader | ADR-0027 |
| Metadata restores through `admin restore`, of backups younger than the nodes' retention less twice the skew allowance; `admin recovery begin` after any other restore | Deferred reclamation protects every restore of such a backup whatever its path; `admin restore` adds its checks; older backups lose what was reclaimed since (R-94(b), bounded) | ADR-0026, ADR-0028, [Backup and restore](backup-restore.md) |
| `--reclaim-retention-secs` (storage nodes, default 86 400) at least the age of the oldest backup that may be restored, the same on every node; capacity for a retention's worth of deletes | A deleted durable shard stays on disk for the retention (`reclaim_pending` in health); a node with 0 deletes at once and is outside the protection (`arca_controller_nodes_not_deferring`) | ADR-0028, R-106 |
| A node whose `reclaim_damaged` is reported: stop it, move `pending-reclaim.log` aside, start it | Nothing is reclaimed on it while its log is damaged (it fails closed); reconciliation and GC retire what is still unneeded again | ADR-0028 |
| Nodes taken out with `admin node decommission` then `admin node unregister` (or `keep-removed` after a manual restore) | Removal closes the node's fence with a hold on its own disk; a node simply left offline can be named again by restored metadata | ADR-0027, RFC-013 §5.10 |
