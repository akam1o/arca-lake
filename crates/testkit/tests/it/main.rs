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
mod commit_timeouts;
mod controller;
mod e2e;
#[cfg(feature = "fdb")]
mod e2e_fdb;
mod e2e_tls;
mod faults;
#[cfg(feature = "fdb")]
mod faults_fdb;
#[cfg(feature = "fdb")]
mod fdb_failover;
#[cfg(feature = "fdb")]
mod fdb_restore;
mod gc_bench;
mod gc_safety;
mod integrity;
mod member_move_redundancy;
mod membership;
mod membership_faults;
#[cfg(feature = "fdb")]
mod mixed_version;
mod multipart_backpressure;
mod multipart_integrity;
mod paging;
mod parallel_read;
mod phase31;
mod placement_balance;
mod population_queue;
mod recovery;
mod recovery_finalize;
mod refused_bodies;
mod repair;
mod repair_completion;
mod request_metrics;
mod s3_compat;
mod tiering;
mod tls_cluster;
mod transport;
mod upgrade;
mod verify_object;
