//! The testkit integration tests, linked into one binary: each file was a
//! test binary of its own that linked the whole stack, and linking them
//! dominated CI. Select a former binary with a filter, for example
//! `cargo test -p arca-lake-testkit --test it -- e2e::`.
//!
//! Tests that install process-wide state stay in binaries of their own:
//! `io_faults_cluster.rs` (the storage I/O hook), `security.rs` (the global
//! tracing subscriber) and `fdb_tls.rs` (FoundationDB network TLS options,
//! set before the network starts).
//!
//! The `*_fdb`, `fdb_*` and `mixed_version` modules need `--features fdb`,
//! the FoundationDB client library, and `ARCA_FDB_TESTS=1` with
//! `ARCA_FDB_CLUSTER_FILE`; otherwise they skip. They share one keyspace,
//! so run them with `--test-threads=1`.

mod background_cursor;
mod background_scale;
mod backup_verify;
mod cache;
mod cache_ec;
#[cfg(feature = "fdb")]
mod cache_ec_fdb;
#[cfg(feature = "fdb")]
mod cache_fdb;
mod cache_tokens;
mod capacity;
mod capacity_bench;
mod capacity_failover;
mod capacity_reservation;
mod commit_timeouts;
mod controller;
mod deferred_reclaim;
mod delete_fence;
mod e2e;
#[cfg(feature = "fdb")]
mod e2e_fdb;
mod e2e_tls;
mod epoch_intent;
mod failure_domain;
mod faults;
#[cfg(feature = "fdb")]
mod faults_fdb;
#[cfg(feature = "fdb")]
mod fdb_failover;
#[cfg(feature = "fdb")]
mod fdb_restore;
mod gc_bench;
mod gc_safety;
mod hot_key;
mod identity_hardening;
mod integrity;
mod managed_restore;
mod member_move_redundancy;
mod membership;
mod membership_faults;
#[cfg(feature = "fdb")]
mod mixed_version;
mod multipart_backpressure;
mod multipart_integrity;
mod node_lifecycle;
mod paging;
mod parallel_read;
mod phase31;
mod placement_balance;
mod population_queue;
#[cfg(feature = "fdb")]
mod previous_release;
mod rebalance;
#[cfg(feature = "fdb")]
mod rebalance_fdb;
mod recovery;
mod recovery_finalize;
mod refused_bodies;
mod repair;
mod repair_completion;
mod repair_paging;
mod repair_retarget;
mod repair_verify;
mod request_metrics;
#[cfg(feature = "fdb")]
mod retirement_crash;
mod s3_compat;
mod s3_content_md5;
mod s3_errors;
mod s3_xml_checksums;
mod scrub_classes;
mod system_model;
mod tiering;
#[cfg(feature = "fdb")]
mod tiering_fdb;
mod tls_cluster;
mod transport;
mod upgrade;
#[cfg(feature = "fdb")]
mod upgrade_v010;
mod verify_object;
mod write_ownership;
