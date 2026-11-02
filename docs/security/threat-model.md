# Threat model

Status: draft (Phase 3.0). Companion to [RFC-012](../rfcs/RFC-012-security-architecture.md),
which specifies the controls; this page states what is being protected,
from whom, and which threats the design accepts. It is a public document
(no deployment-specific detail).

## 1. System and trust boundaries

```text
            ┌────────────── tenant network / Internet ──────────────┐
  S3 clients (SDK, CLI, browsers via presigned URLs)
            │  HTTPS (TLS terminated at the gateway or a fronting proxy)
   ─────────┼───────────────────────── boundary B1: S3 edge ─────────
            ▼
        [gateway]  ──── S3 auth (SigV4), bucket authorization, limits, audit
            │  gRPC + mTLS (service identity)
   ─────────┼───────────────────── boundary B2: cluster network ─────
            ▼                                   ▲
   [storage nodes] ◄──── gRPC + mTLS ────── [controller]
            │                                   │
            └───── FoundationDB (cluster file + TLS) ◄──────┘
   ─────────┼───────────────────── boundary B3: operator access ─────
        operators: `arca-lake admin`, FoundationDB tools, host shells
```

| Boundary | Crosses | Default stance |
| --- | --- | --- |
| B1 S3 edge | untrusted clients → gateway | authenticate every request, authorize per bucket, bound every resource a request can consume |
| B2 cluster network | gateway ↔ nodes ↔ controller ↔ metadata | every peer presents a service identity; nothing on the cluster network is trusted by address alone |
| B3 operator access | humans and automation → metadata, hosts, binaries | whoever holds the cluster file or a host shell is fully trusted; protect those with the platform's controls and audit |

## 2. Assets

| Asset | Why it matters | Where it lives |
| --- | --- | --- |
| A1 Object data | tenant data; confidentiality and integrity | storage node segments (shards), cache nodes (copies), gateway memory in flight |
| A2 Object metadata | names, sizes, user metadata, placement; integrity decides what "exists" | FoundationDB (`("al",…)`), controller and gateway memory |
| A3 Credentials | S3 access keys and secrets, service certificates and keys, FoundationDB cluster file | gateway credential store, node and controller key files, host file systems |
| A4 Cluster control | group membership, epochs, leader lease, GC and repair decisions | FoundationDB; controller process |
| A5 Availability | the service itself and its capacity | all components |
| A6 Audit record | who did what; needed for incident response | gateway audit log, controller log |

## 3. Actors

| Actor | Capability | Trust |
| --- | --- | --- |
| T1 Anonymous network client | can reach the S3 edge | untrusted |
| T2 Authenticated tenant | holds valid S3 credentials for some buckets | trusted only for what its credentials authorize |
| T3 Compromised tenant credential | T2's credentials in the wrong hands | as T2; detection via audit and rotation |
| T4 Network attacker on the cluster network | can connect to any cluster port, sniff and spoof if traffic is unprotected | untrusted (B2 must hold without network isolation) |
| T5 Compromised storage node | runs attacker code with the node's identity and its shards | semi-trusted: it may lie about its own shards; it must not be able to read or write other nodes' shards or metadata it does not own |
| T6 Compromised gateway | has every tenant's data in flight and the gateway's service identity | trusted for the data path by design; blast radius bounded by what the gateway identity may do to metadata and nodes |
| T7 Operator | holds cluster file, host access, binaries | fully trusted; mistakes, not malice, are the threat (see RFC-013) |
| T8 Supply chain | dependencies, build and CI | trusted after review; mitigated by pinning, cargo-deny, CI isolation (RFC-012 §5.11) |

## 4. Threats and controls

Notation: **C-n** are the controls of RFC-012 §5. "Accepted" means the
design knowingly does not prevent the threat and says why.

### 4.1 S3 edge (B1)

| Threat | Actor | Impact | Control |
| --- | --- | --- | --- |
| Unauthenticated read or write | T1 | A1, A2 | C-1 SigV4 on every request; anonymous mode is an explicit, logged deployment choice |
| Credential theft by sniffing | T1, T4 | A3 | C-2 TLS at the edge; SigV4 never sends the secret; presigned URLs expire |
| Replay of a captured signed request | T1 | A1, A2 | C-1 SigV4 15-minute clock window (s3s); idempotent operations only within that window; presigned expiry |
| Cross-tenant access with valid credentials | T2, T3 | A1, A2 | C-3 bucket ownership and bucket policy evaluated per request; no bucket is readable without an allow statement; `ListBuckets` is owner-scoped |
| Bucket name squatting | T2 | A2 | C-3 bucket creation requires an allow for the principal; names are global, ownership is recorded at creation |
| Stolen credential used for a long time | T3 | A1, A2 | C-4 rotation with two concurrent keys per principal and a documented cadence; C-6 audit records the key id of every request |
| Oversized or endless request body | T1, T2 | A5 | C-7 request size limits (s3s `put_object_max_size`, XML body limit, multipart part size); C-8 per-connection header/idle timeouts and body progress timeouts |
| Request flood | T1, T2 | A5 | C-7 per-principal and per-source token buckets; connection cap; C-8 in-flight caps per gateway |
| Slowloris / slow body | T1 | A5 | C-8 header timeout, body progress timeout (as the shard streams already have, ADR-0011) |
| Memory exhaustion via many concurrent large PUTs/GETs | T2 | A5 | C-8 per-gateway in-flight bytes budget (`read_parallelism`, `write_pipeline`, population queue are already bounded); admission refuses with 503 when the budget is spent |
| Error-message information leak | T1 | A2 | C-6 error bodies carry S3 codes and request IDs, never internal addresses or paths |
| Signature or policy bypass through a parser difference | T1 | A1, A2 | rely on s3s's single parser; conformance tests with AWS SDK/CLI (RFC-007); never accept unsigned requests when an auth provider is set |

### 4.2 Cluster network (B2)

| Threat | Actor | Impact | Control |
| --- | --- | --- | --- |
| Sniffing shard traffic | T4 | A1 | C-9 mTLS on every gRPC connection |
| Spoofing a storage node (accept writes, serve wrong data) | T4 | A1 | C-9 node identity is the certificate's SAN (`node/<id>`), checked by the client against the node record; chunk CRC-32C and shard CRC catch wrong data regardless |
| Spoofing a gateway or controller toward nodes | T4 | A1, A4 | C-9 nodes require a client certificate with role `gateway` or `controller`; epoch-bearing RPCs (`UpdateGroupEpoch`, `DeleteShard`) additionally require role `controller` or a gateway acting on a placement it holds |
| Raising a group epoch to fence out legitimate writers | T4, T5 | A5 | C-9 `UpdateGroupEpoch` is controller-only; a node cannot raise another node's epoch |
| Compromised node serving corrupted shards | T5 | A1 | end-to-end chunk CRC-32C at the gateway (RFC-004 §5.10); reconstruction from other members; scrubbing (Phase 3.2) reports and repairs; a node cannot forge the gateway's CRC check |
| Compromised node deleting shards it holds | T5 | A1 (durability) | accepted for that node's shards: erasure coding and repair tolerate `m` such members; detection by scrub and completeness checks (RFC-008) |
| Compromised node reaching metadata | T5 | A2, A4 | nodes never hold the FoundationDB cluster file; their only metadata-affecting path is their own health and shard listings |
| Compromised gateway reading everything | T6 | A1 | accepted by design (the gateway terminates TLS and holds plaintext); bounded by C-6 audit and by C-9: a gateway identity cannot change group membership or epochs |
| Unauthenticated metrics/health endpoints leak topology | T4 | A2 (minor) | C-10 metrics bind to loopback by default; exposure requires an explicit flag and should sit behind the platform's scrape network; no addresses or key names in metric labels |

### 4.3 Operator access (B3)

| Threat | Actor | Impact | Control |
| --- | --- | --- | --- |
| Anyone with the cluster file has full metadata control | T7 (or theft) | A2, A4 | C-9 FoundationDB TLS with client certificates (deployment guide); cluster file treated as a secret (RFC-013); R-19 stays open until that is in place |
| Admin mistake (wrong group, wrong node decommissioned) | T7 | A2, A5 | RFC-013 runbooks with dry-run and confirmation; GC grace periods and reconcile age gates make most mistakes recoverable within the grace window |
| Backup leak | T7 | A2, A3 (if credentials were in metadata; they are not) | RFC-015: backups contain metadata only; encrypt at rest with the platform's tools; credentials never live in FoundationDB |
| Rolling upgrade to a bad build | T7 | A5, A1 | RFC-014: format and protocol gates, nodes first, downgrade window |

### 4.4 Supply chain and CI

| Threat | Actor | Impact | Control |
| --- | --- | --- | --- |
| Malicious or vulnerable dependency | T8 | all | C-11 `cargo-deny` advisories and licenses in CI, `Cargo.lock` committed, dependency review on update |
| Third-party GitHub Action compromise | T8 | A3 (CI secrets), build integrity | C-11 actions pinned, minimal `permissions`, no secrets in CI beyond the default token (read-only) |
| Untrusted code on a persistent self-hosted runner | T8, T1 | runner host, everything it can reach | C-11 self-hosted runners run only code from `main` pushed by maintainers; pull requests from forks run on GitHub-hosted or ephemeral runners or not at all; runner hosts have no access to production or validation hosts |
| Benchmark artifacts leaking environment identifiers | T7 | reconnaissance | `docs/security/public-artifacts.md` sanitizer and CI check |

## 5. Accepted risks (with reasons)

- **Encryption at rest is not provided by arca-lake.** Shards are stored
  as written; the deployment provides disk encryption. Client-side
  encryption (SSE-C semantics) is a candidate for a later phase; SSE-S3
  with gateway-held keys would make the gateway a key server and is not
  planned.
- **The gateway sees plaintext.** Inherent to an S3 gateway; mitigated by
  audit, identity scoping and the ability to run gateways per tenant.
- **A compromised node can destroy its own shards.** Durability is
  provided by coding, not by node integrity; the threat is handled as a
  failure (RFC-008).
- **Metadata access control is coarse.** FoundationDB has no row-level
  authorization; the cluster file is the credential and only the
  gateway, controller and operators hold it.

## 6. Verification

Each control has acceptance tests in RFC-012 §11; this table is
re-checked whenever a boundary changes (a new endpoint, a new credential
kind, a new network path).
