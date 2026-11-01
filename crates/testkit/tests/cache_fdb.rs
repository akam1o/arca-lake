//! Read-through cache cases on a real FoundationDB cluster (same
//! environment rules as `e2e_fdb.rs`; run with `--test-threads=1`).

arca_lake_testkit::cache_suite!(arca_lake_testkit::fdb::starter());
