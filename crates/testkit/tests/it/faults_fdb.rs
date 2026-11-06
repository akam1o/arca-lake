//! Metadata commit-fault cases on a real FoundationDB cluster (the fault
//! wrapper applies or drops real transactions). Same environment rules as
//! `e2e_fdb.rs`; run with `--test-threads=1`.

arca_lake_testkit::fault_suite!(arca_lake_testkit::fdb::fault_starter());
