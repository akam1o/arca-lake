# Configuration

Every process is configured by command-line flags (`arca-lake <process>
--help` lists them with their defaults) and a few environment variables.
This page lists the settings a deployment has to decide; the RFCs
describe each in full.

## Storage node (`arca-lake storage-node`)

| Setting | Purpose |
| --- | --- |
| `--listen`, `--dir`, `--metrics-listen` | RPC address, store directory (one per device), Prometheus endpoint |
| `--segment-max-bytes` | Segment size (default 1 GiB); sets the default reserve |
| `--capacity-bytes` | Allotment: the bytes this node may use. Required when nodes share a file system (R-80) |
| `--reserve-bytes` | Bytes kept free for compaction, rollover, quarantine and repair (default `max(2 segments + 64 MiB, 2 %)`, at most a quarter); client writes are refused when a write would leave less (RFC-008 §6.8). A delete reserve (one tombstone per record held) is added on top automatically (RFC-008 §6.10) |
| `--compaction-*` | Background compaction (RFC-005 §5.6) |
| `--fence-lease-secs` | Delete-fence rejoin lease (default 30 s); keep it above the controller health interval (ADR-0024) |
| `--reclaim-retention-secs`, `--reclaim-skew-secs` | Deferred-reclamation retention (default 86,400 s) and clock-skew allowance (default 60 s). Protection depends on all durable nodes and backup age; 0 retention disables protection on that node ([Backup and restore](backup-restore.md)) |
| `--tls-ca`, `--tls-cert`, `--tls-key`, `--tls-crl`, `--trust-domain` (or `ARCA_TLS_*`), `--insecure-plaintext` | Mutual TLS for node RPCs; plaintext only explicitly (ADR-0017) |

## Controller (`arca-lake controller`)

| Setting | Purpose |
| --- | --- |
| `--cluster-file`, `--id`, `--lease-secs`, `--http` | FoundationDB, leader election, metrics |
| `--failure-domain` | Topology label separating group members |
| `--gc-interval-secs`, `--gc-grace-secs`, `--min-orphan-age-secs`, `--reconcile-interval-secs` | Garbage collection (RFC-008 §6) |
| `--auto-repair`, `--repair-interval-secs`, `--unhealthy-after-secs`, `--scrub-*`, `--stripe-repair-interval-secs` | Repair and integrity (RFC-008 §8) |
| `--health-interval-secs` | Node health and capacity reports; write admission is evaluated each pass |
| `--admission-dwell-secs`, `--admission-max-chunk-bytes`, `--admission-group-min-shards` | Write admission hysteresis and group room (RFC-008 §6.8) |
| `--allow-shared-filesystem` | Development only: keep placing on nodes that share a file system without fitting allotments |
| `--rebalance`, `--rebalance-interval-secs`, `--rebalance-mib-per-pass`, `--rebalance-start-imbalance`, `--rebalance-stop-imbalance` | Capacity rebalancing between the groups of a tier (off by default; `admin rebalance` pauses, resumes, switches dry run; RFC-008 §6.10) |
| `--cache-*`, `--tiering-*` | Cache and durable tiering (RFC-006) |

## Gateway (`arca-lake gateway`)

| Setting | Purpose |
| --- | --- |
| `--backend cluster`, `--cluster-file`, `--listen`, `--metrics-listen` | Cluster backend, S3 listener (plain HTTP behind a TLS terminator), metrics |
| `--access-key`/`--secret-key` or `--credentials-file`, `--authorize`, `--trusted-proxy` | Authentication and authorization (RFC-012) |
| `--k`, `--m`, `--chunk-bytes`, `--small-threshold`, `--failure-domain`, `--tier` | The profile new objects are written with (RFC-004) |
| `--multipart-min-part-bytes`, `--lease-secs`, `--read-parallelism`, `--write-pipeline` | Multipart and pipeline tuning |
| `--cache-*` | Read-through cache (RFC-006) |

### Authentication and authorization

`--credentials-file` with `--backend cluster` enables `owner-policy`
authorization by default. Static `--access-key`/`--secret-key` authenticates
requests but requires explicit `--authorize owner-policy` to enable bucket
owner/policy checks. `--authorize none` allows every authenticated request;
the memory backend cannot enable `owner-policy`. See
[Security model](security-model.md).

### Commits to one key

Commits to one key through a gateway take turns (ADR-0041): fewer
metadata conflicts when many clients write the same key. A write refused
here is answered `503 SlowDown` before any commit attempt: it was not
applied and may be retried.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--commit-queue` | `on` | `off` commits every write at once with immediate conflict retries (0.1.0 behaviour). |
| `--commit-queue-wait-ms` | `2000` | Longest wait for a commit's turn; a longer wait is refused. |
| `--commit-queue-max-waiters` | `256` | Writes waiting for one key at most; one more is refused. |

See [ADR-0041](../adr/0041-hot-key-commit-queue.md).

### Read access records

Access records inform automatic tiering. They are not required for data
durability. The default batches observations in a bounded in-memory table;
full, stale, failed or unflushed observations can be lost.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--access-records` | `on` | Batched recording. `off` records no reads through this gateway; `legacy` retains the earlier path for the compatibility period. |
| `--access-capacity` | `65536` | Maximum generations held across pending entries and the in-flight batch. |
| `--access-batch` | `128` | Maximum generations per flush transaction. Larger batches can add latency bursts. |
| `--access-flush-interval-ms` | `1000` | Flush interval, with jitter. |
| `--access-high-watermark` | `0.5` | Fraction of capacity that triggers an immediate flush. |
| `--access-max-age-ms` | `30000` | Maximum observation age when a flush attempt starts; older entries are dropped. |
| `--access-shutdown-flush-ms` | `2000` | Time allowed to flush pending observations during graceful shutdown. |

See [ADR-0034](../adr/0034-access-records-at-scale.md) for the fault semantics
and metrics, and [Known limitations](known-limitations.md#access-record-performance)
for the measured performance tradeoff. Turning recording off changes the
observations available to tiering as well as the request cost.

## FoundationDB

`--cluster-file` (or `ARCA_FDB_CLUSTER_FILE`) and the `--fdb-tls-*`
options; `--fdb-insecure-plaintext` only explicitly. FoundationDB needs
its own device (R-79, [Operational requirements](operations.md)).

## Admin

Administrative subcommands include `status`, `check-config`, `register-node`,
`create-groups`, `node`, `rebalance`, `integrity`, `delete-fence`, `recovery`,
`restore`, `upgrade` and `backup`. Run `arca-lake admin --help` and the
subcommand's `--help` for exact arguments. Procedures are in
[RFC-013](../rfcs/RFC-013-operations-guide.md); metadata restore also requires
the conditions in [Backup and restore](backup-restore.md).

## Versions

`arca-lake --version` prints the software version. Protocol, schema and
format versions, the upgrade order and the one-way finalize are in
[Versions and compatibility](operations.md#versions-and-compatibility).
