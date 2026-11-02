# ADR-0015: Durable quarantine, controller-driven scrubbing, and stripe repair

Status: accepted (Phase 3.2)

## Context

Until Phase 3.2 a corrupt shard was dropped from the node's in-memory
index on the read that found it, with no record, no counter and no
repair: the next restart re-indexed it (or hid every later record of
its segment), readers decoded around it silently, and the only repair
was whole-member replacement with an epoch bump. RFC-008 §8 adds
scrubbing, classification, quarantine and per-stripe repair. The
decisions below cut across the storage engine, the node, the controller
and the gateway.

## Decisions

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| Quarantine persistence | sidecar `quarantine.log` next to the segments (append + fsync, idempotent replay); bytes stay until compaction | tombstoning the record (destroys evidence, indistinguishable from a delete); a new record flag (on-disk format change gated by RFC-014, old readers reject it) |
| Quarantine trigger | only a failed header or block CRC on the node's own read; never an I/O error, never a metadata-level finding | quarantining on `VerifyShards` errors of any kind (today's behaviour: a transient read error could hide a healthy shard) |
| Open recovery | skip a record whose payload fails CRC (quarantine it) and resynchronise after a corrupt header at the next valid aligned record | stop at the first invalid record (hides every later record of the segment) |
| Who drives scrubbing | the controller leader, with the cursor in metadata (fenced), nodes stateless | node-autonomous scrubbing (no cluster-wide pacing, no metadata-level classification, progress lost with the node; a node cannot decide liveness) |
| Scrub pacing | a byte budget per second across groups plus one scrub RPC in flight per node, page size bounded by the node | unpaced (competes with GETs for disk) ; per-node token buckets only (a controller still has to decide which groups) |
| Repair unit | per-stripe task keyed by `(group, chunk)`, writes under the current epoch, no membership change | member replacement for every finding (epoch churn, full-group rewrite for one bad shard) |
| Repair verification | reconstructed chunk must match the chunk record's CRC and length before any write; node-returned CRC and a `VerifyShards` read-back after it | trust the decode |
| Unrecoverable stripes | a persisted task state, an integrity error on read, a metric and an operator listing; cleared only by finding `k` good shards | silent 500 (hides loss); marking the object unreadable in its header (a metadata write for every stripe finding, and the finding may be transient) |
| Degraded reads | reported by the gateway to the repair queue, deduplicated per chunk | waiting for the next scrub pass only |
| Deletion | never by scrub or repair (I16); reconciliation keeps its age gate | letting scrub delete unexpected shards (a second deletion path to get wrong) |

## Consequences

- RFC-005 gains the quarantine log and the resynchronising scan; RFC-004
  §5.13 `VerifyShards` semantics change (durable quarantine, no
  quarantine on I/O errors); the storage node gains `ScrubShards`,
  `include_quarantined` on `ListShards` and quarantine counters in
  `Health`.
- Metadata gains `("al","scrub",group)` cursors and `("al","repair",group,chunk)` tasks; the controller gains the scrub and stripe-repair schedulers and the integrity metrics; the gateway reports degraded reads.
- Member replacement (§7) is unchanged and remains the response to a
  failed node; both mechanisms coordinate through the group epoch.
