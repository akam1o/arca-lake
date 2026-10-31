# ADR-0001: Apache-2.0 license

- Status: accepted
- Date: 2026-11-01
- Related RFC: —

## Context

arca-lake is developed in public and intended for unrestricted redistribution
and commercial use. The shortlisted dependencies (Tokio, hyper, tonic,
rustls, `foundationdb`, `tikv-client`, `rocksdb`, `s3s`, ISA-L,
`reed-solomon-simd`, OpenTelemetry, Prometheus client) are all under
permissive licenses (MIT, Apache-2.0, BSD-3-Clause, ISC, CC0). RocksDB is
dual-licensed GPL-2.0 or Apache-2.0 and may be used under Apache-2.0.

## Decision

arca-lake is licensed under the Apache License, Version 2.0. The full text is
in `LICENSE`. Contributions are accepted under the same license (see
ADR-0003 for the sign-off mechanism).

## Alternatives rejected

- **MIT**: simpler, but lacks an explicit patent grant. For storage software
  that may embed algorithms with patent history, the explicit grant and
  termination clause of Apache-2.0 give users clearer protection.
- **Dual MIT/Apache-2.0**: common in the Rust ecosystem for libraries, but
  arca-lake is primarily a deployable system, and a single license keeps
  compliance simpler for operators.
- **Copyleft (AGPL/GPL)**: would restrict integration into some
  environments and conflicts with the goal of unrestricted reuse.

## Consequences

- `cargo-deny` enforces a permissive-license allow-list; adding a copyleft
  dependency requires a new ADR.
- Source files do not need per-file headers; the `LICENSE` file and the
  workspace `license` field are authoritative. A `NOTICE` file will be added
  if a dependency's license requires attribution beyond its own notice.
- This decision is re-verified when the first real dependency tree exists in
  CI.
