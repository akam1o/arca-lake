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
  (ADR-0016, ADR-0019).
- **Audit.** One audit record per S3 request, without secrets.
- **Not provided.** Encryption at rest, KMS integration, object ACLs,
  per-principal rate limits and quotas.

Reporting vulnerabilities: [SECURITY.md](../../SECURITY.md).
