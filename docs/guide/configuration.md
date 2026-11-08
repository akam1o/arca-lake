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

## FoundationDB

`--cluster-file` (or `ARCA_FDB_CLUSTER_FILE`) and the `--fdb-tls-*`
options; `--fdb-insecure-plaintext` only explicitly. FoundationDB needs
its own device (R-79, [Operational requirements](operations.md)).

## Admin

`arca-lake admin --cluster-file … status | check-config | rebalance … | register-node
| create-groups | replace-member | decommission-node | integrity … |
recovery … | upgrade … | backup …` (RFC-013 §5).
