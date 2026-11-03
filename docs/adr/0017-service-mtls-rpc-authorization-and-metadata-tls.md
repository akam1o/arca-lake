# ADR-0017: Service mTLS, per-RPC authorization and metadata TLS

Status: accepted (Phase 3.4)

## Context

RFC-012 §5.5 (C-9) designed mutually authenticated gRPC with role-scoped
RPC authorization and FoundationDB TLS; neither existed when Phase 3.4
started (every storage-node connection and every metadata connection was
plaintext). This ADR fixes how the RFC is realised and where it is
amended.

## Decisions

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| TLS stack | rustls (ring provider) through `tokio-rustls`, with arca-lake's own accept loop on the node and its own connector on the client; tonic serves the already-authenticated streams | tonic's built-in `tls-ring` (`ServerTlsConfig`/`ClientTlsConfig`): no custom identity check, no reload without restarting the server, no way to drop connections whose trust was withdrawn |
| Identity | X.509 URI SAN in SPIFFE form `spiffe://<trust-domain>/<role>/<name>`; roles `node`, `gateway`, `controller`, `admin`; a node's name is its node id | RFC-012's `arca://<cluster-id>/…` (amended: the SPIFFE form lets SPIRE or any SPIFFE-aware CA issue certificates unchanged); CN matching (no structure, easy to collide) |
| Chain and time validation | rustls-webpki against the configured trust bundle (one or more roots: two during a CA rotation), optional CRLs, both directions; client certificates are mandatory | trust-on-first-use; accepting any certificate and checking only the SAN |
| Server identity on the client | the client requires role `node` in the configured trust domain and, when the caller knows which node an address belongs to (the gateway's map, the controller's node records), that exact node id; host names and IP SANs are not used | DNS/IP SAN verification (node addresses are often bare IPs and change on re-registration; the node id is what metadata names) |
| Authorization | per RPC inside each storage-node handler, before any argument is read, from the peer identity attached to the connection: table below; an unauthenticated or unauthorised call is `PermissionDenied` and counted | a tower layer keyed by path (the connection info is attached after the layer); mTLS alone (any valid certificate could call `UpdateGroupEpoch`) |
| `Health` with cluster facts | the plain `Health` probe is open to every role; a `Health` that carries `format_write` or an incarnation is controller/admin only | treating `Health` as read-only (it changes what a node writes and which deletes it accepts) |
| Fencing | unchanged and independent: group epoch on every write/delete, incarnation on deletes, leader token on every controller metadata commit; an authenticated controller with a stale token or incarnation is still refused | relying on certificates for freshness (certificates outlive leaderships) |
| Rotation | certificate, key, trust bundle and CRL files are re-read when their contents change (polled, default 30 s) and on demand; a reload that fails validation keeps the previous material and is counted; new connections use the new material at once; when the trust bundle or CRLs change, connections authenticated under the old ones are closed so that a withdrawn identity cannot keep a connection | restart-only rotation; closing every connection on every certificate renewal (needless churn) |
| Plaintext | refused unless explicitly requested (`--insecure-plaintext` / `ARCA_INSECURE_PLAINTEXT=1`), logged at startup and exported as a gauge; TLS flags together with the plaintext flag are refused | silent plaintext when no certificate is configured |
| Metadata TLS | FoundationDB's own client TLS (network options `TLS_cert_path`, `TLS_key_path`, `TLS_ca_path`, `TLS_verify_peers`) set before the client network starts; the cluster file must list only `:tls` coordinators when TLS is configured, and a `:tls` cluster file without TLS configuration is refused; plaintext metadata needs `--fdb-insecure-plaintext` / `ARCA_FDB_INSECURE_PLAINTEXT=1`; FoundationDB refreshes the certificate files itself | environment variables only (`FDB_TLS_*`: invisible in arca-lake's configuration and status); a TLS proxy |
| Secrets | key files are read into memory and never logged; errors name the file path and the failure, never contents; startup logs print certificate identity and expiry only | — |

### RPC authorization table

| RPC | gateway | controller | admin | node |
| --- | --- | --- | --- | --- |
| `PutShard`, `GetShard`, `ListShards`, `DeleteShard` | yes | yes | yes | no |
| `VerifyShards`, `ScrubShards`, `UpdateGroupEpoch` | no | yes | yes | no |
| `Health` without cluster facts | yes | yes | yes | yes |
| `Health` carrying `format_write` or an incarnation | no | yes | yes | no |

The node role receives no data-path rights: nodes do not call nodes
today. RFC-012's table (node may read shards for repair) is amended
accordingly; a future node-to-node operation adds its row here.

### Paths that are not gRPC

- Gateway and controller to FoundationDB: metadata TLS above; the
  identity is the client certificate the FoundationDB cluster verifies
  (`TLS_verify_peers`), not an arca-lake role. FoundationDB has no
  per-key authorization, so a process holding a metadata certificate can
  write any key: R-19 is narrowed to holders of a valid certificate, not
  closed.
- Storage nodes hold no metadata client (RFC-012 S-A10).
- There is no gateway-to-controller API; the controller's `/metrics`
  and `/healthz` listen on loopback (RFC-012 §5.6). Admin commands talk
  to FoundationDB (metadata TLS) and, for `integrity scrub --wait` and
  `verify-object`, to nodes with an `admin` certificate.

## Consequences

- New flags on gateway, controller, storage node and admin:
  `--tls-ca`, `--tls-cert`, `--tls-key`, `--tls-crl` (optional),
  `--trust-domain`, `--insecure-plaintext`; metadata:
  `--fdb-tls-ca`, `--fdb-tls-cert`, `--fdb-tls-key`,
  `--fdb-tls-verify-peers`, `--fdb-insecure-plaintext`.
- Scripts and tests that ran plaintext now say so explicitly.
- RFC-012 §5.5 is amended (SAN form, node role rights, reload
  semantics); S-A11 moves with the tests.
