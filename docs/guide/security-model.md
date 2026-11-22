# Security model

Summary of [RFC-012](../rfcs/RFC-012-security-architecture.md), the
[threat model](../security/threat-model.md) and
[ADR-0017](../adr/0017-service-mtls-rpc-authorization-and-metadata-tls.md).

- **Service identities.** Every process has an X.509 certificate with a
  SPIFFE ID (`spiffe://<trust-domain>/<role>/<name>`). Storage nodes
  authorize each RPC by the caller's role; certificates, trust bundles
  and CRLs are reloaded without restart, and connections whose trust was
  withdrawn are closed.
- **Metadata.** FoundationDB is reached over TLS with a client
  certificate; a cluster file without TLS is refused unless plaintext is
  explicitly allowed.
- **S3 clients.** SigV4 (header and presigned) with access keys from a
  credentials file (several keys per principal, rotation without
  restart); authorization by bucket ownership and a bucket policy subset.
  The S3 listener is plain HTTP: terminate TLS in front of it and list
  the terminator with `--trusted-proxy`.
- **Fencing.** A cluster incarnation and group epochs fence deletes and
  writes of stale processes; record tokens make deletes conditional
  (ADR-0016, ADR-0019). A storage node that has adopted an incarnation
  refuses a shard write carrying another one (R-117): a writer of another
  cluster reaching it at a reused address is refused, not counted
  written. Still accepted: a write carrying no incarnation (a writer that
  holds none, such as a tool or a process of an earlier build; deletes
  refuse that case), and any write to a node that has not adopted an
  incarnation yet (new, never reached by its controller).
- **One trust domain per cluster.** The handshake pins a node's SPIFFE ID,
  and node IDs are small integers that repeat across clusters: give each
  cluster its own CA or its own trust domain (`arca-lake tls init-ca`
  per cluster). Clusters sharing a CA and a trust domain can authenticate
  each other's nodes. Plaintext (`--insecure-plaintext`) has no peer
  verification at all and is for tests on a private loopback or
  network only.
- **Audit.** One audit record per S3 request, without secrets.
- **Not provided.** Encryption at rest, KMS integration, object ACLs,
  per-principal rate limits and quotas.

## S3 authorization defaults

Authentication and authorization are separate settings:

| Configuration | Authorization behavior |
| --- | --- |
| `--backend cluster` with `--credentials-file` | Defaults to `owner-policy`: checks bucket ownership and the supported bucket-policy subset. |
| Static `--access-key` and `--secret-key` | Authenticates requests; owner/policy checks require explicit `--authorize owner-policy` and the cluster backend. |
| `--authorize none` | Every authenticated request is allowed; intended for experiments. |
| `--backend memory` | Does not support `owner-policy`. |

For experiments without configured credentials, `--allow-anonymous` permits
unauthenticated operation. The quick start uses one public demonstration key
on loopback; it is not a configuration for separating users or tenants.

The gateway trusts `x-forwarded-proto` for policy evaluation only from
addresses supplied by `--trusted-proxy`. Configure the TLS terminator and
its trusted address together; the gateway itself does not provide HTTPS.

Reporting vulnerabilities: [SECURITY.md](../../SECURITY.md).
