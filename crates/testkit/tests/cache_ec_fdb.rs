//! Protected cache groups on a real FoundationDB cluster (same environment
//! rules as `e2e_fdb.rs`; run with `--test-threads=1`).

arca_lake_testkit::cache_ec_suite!(
    replication,
    arca_lake_testkit::fdb::starter(),
    arca_lake_testkit::suite_cache_ec::replication()
);
arca_lake_testkit::cache_ec_suite!(
    erasure_coding,
    arca_lake_testkit::fdb::starter(),
    arca_lake_testkit::suite_cache_ec::erasure_coding()
);
