# Troubleshooting

Runbooks are in [RFC-013 §10](../rfcs/RFC-013-operations-guide.md); each
alert names its section.

| Symptom | First check | Runbook |
| --- | --- | --- |
| Writes fail with `503 SlowDown` | `admin check-config`; `arca_controller_backpressure_state`; the leader's "write admission state changed" log line | §10.11 |
| Every request `503 ServiceUnavailable` | cluster state (`admin status`), FoundationDB availability | §10.1, §10.6c |
| PUT slow, GET fine | `arca_gateway_put_phase_seconds`, node group-commit latency | §10.2 |
| GC backlog growing | `arca_controller_gc_backlog*`, `_gc_stage_seconds` | §10.10 |
| Repair stuck | group state, `arca_controller_groups_repairing` | §10.5 |
| Unrecoverable stripe | `admin integrity status` | §10.6 |
| Node refuses writes: epoch rejected | group epochs | §10.7 |
| Certificate errors | TLS metrics, reload logs | §10.8 |
| Storage node memory | `arca_node_process_memory_bytes` | §10.9 |
| One group of a tier much fuller than the others | `admin check-config` (group rooms), `arca_controller_rebalance_*`, `admin rebalance status` | RFC-008 §6.10 |
| `check-config` reports nodes sharing a file system | give each node its own file system, or `--capacity-bytes` allotments that fit | §6, R-80 |
| GC or reconciliation deletes nothing; "delete fence" refusals in logs | `admin delete-fence status`, `arca_controller_delete_fence_nodes_stale`, `arca_node_delete_fence_*` | §10.12 |
