//! Protected cache groups on the in-memory backend, for replication and
//! erasure coding.

arca_lake_testkit::cache_ec_suite!(
    replication,
    Some(arca_lake_testkit::mem_starter()),
    arca_lake_testkit::suite_cache_ec::replication()
);
arca_lake_testkit::cache_ec_suite!(
    erasure_coding,
    Some(arca_lake_testkit::mem_starter()),
    arca_lake_testkit::suite_cache_ec::erasure_coding()
);
