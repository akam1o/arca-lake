# FoundationDB client TLS versus plaintext (Phase 3.4 §2)

- Script: `benchmarks/linux/fdb-tls-perf.sh TLS_DIR PLAIN_DIR 3 20`.
- Environment: one virtual machine, 4 vCPU, shared virtual storage; two
  identical test clusters from `deploy/dev/fdb-tls-cluster.sh` (three
  `fdbserver` processes, `double ssd`, FoundationDB 7.3), one with TLS
  (server-to-server and client), one plaintext, side by side on the same
  disk. Client: `metabench`, 20 s per run, 3 repetitions, transports
  alternating within each repetition.
- The TLS cluster verifies clients by chain (`Check.Valid=1`); the client
  verifies servers by chain and SPIFFE URI SAN
  (`S.subjectAltName>=URI:spiffe://<td>/fdb/`).

| mode | tasks | plain ops/s (median) | TLS ops/s (median) | change | p50 ms plain / TLS | p99 ms plain / TLS |
| --- | --- | --- | --- | --- | --- | --- |
| insert-remove | 8 | 3383 | 3121 | −7.7 % | 2.26 / 2.46 | 4.66 / 4.86 |
| insert-remove | 64 | 11625 | 10240 | −11.9 % | 5.25 / 6.08 | 9.17 / 10.38 |
| touch | 8 | 3677 | 3448 | −6.2 % | 2.10 / 2.21 | 4.35 / 4.30 |
| touch | 64 | 13458 | 11548 | −14.2 % | 4.36 / 5.27 | 8.31 / 9.38 |

Open to first committed read: 3.8 ms plaintext, 7.1–8.6 ms TLS (median).
No errors in any run. The cost includes TLS between the FoundationDB
servers themselves, which a TLS cluster always has; it grows with
concurrency because the CPU-bound servers share four vCPUs with the
client.
