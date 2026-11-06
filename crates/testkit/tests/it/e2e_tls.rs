//! The end-to-end suite with every node RPC over mutual TLS and per-RPC
//! authorization (ADR-0017): gateway, controller-side helpers and test
//! clients each present their own role certificate.

arca_lake_testkit::e2e_suite!(Some(arca_lake_testkit::mem_tls_starter()));
