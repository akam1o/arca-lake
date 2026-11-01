//! The end-to-end suite on a real FoundationDB cluster.
//!
//! Requires `--features fdb`, the FoundationDB client library, and
//! `ARCA_FDB_TESTS=1` with `ARCA_FDB_CLUSTER_FILE` pointing at a cluster
//! file; otherwise every test skips. All tests share one keyspace, which is
//! cleared when a cluster starts, so run with `--test-threads=1`
//! (`deploy/dev/fdb-test.sh` does).

arca_lake_testkit::e2e_suite!(arca_lake_testkit::fdb::starter());
