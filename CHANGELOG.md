# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
will follow [Semantic Versioning](https://semver.org/) once it has releases.

## [Unreleased]

### Added

- Architecture review documents, design decisions, technology selection,
  risk register, and open decisions.
- RFC and ADR templates and index; ADR-0001 through ADR-0005.
- Phase 0 proof-of-concept plan.
- Rust workspace skeleton with crate stubs, CI, license and advisory policy.
- `arca-lake-ec`: `ErasureCoder` trait, `SchemeId`, `CodeParams`,
  `StripeLayout`, replication / classic GF(2^8) / Leopard GF(2^16)
  backends, property tests, golden parity vectors, and criterion
  benchmarks (Phase 0 task 0.2).
