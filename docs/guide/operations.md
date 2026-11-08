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
| Clock synchronisation (NTP) | Safety does not depend on clocks, but leases, grace periods and admission record ages do | RFC-008 §6.7 |
| Prometheus scraping every process and the alert rules loaded | Capacity, GC, repair and integrity problems are detected by alerts | RFC-013 §7–8 |
| Metadata backups and their verification | Restores are the only recovery from metadata loss | RFC-015, [Backup and restore](backup-restore.md) |
| Upgrades in the documented order (storage nodes first) | Mixed versions are supported within one compatibility window only | RFC-014 |
| The delete fence closed (`admin delete-fence close`) before a metadata restore | Without it a delete decided before the restore can still remove shards the restored metadata needs | ADR-0023, RFC-013 §5.9 |
