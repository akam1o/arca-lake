//! Tiering under the tiering record on a real FoundationDB cluster
//! (ADR-0035 §10.1, §10.4; the same tests as `tiering.rs` on the in-memory
//! store, environment as `e2e_fdb.rs`, plus `ARCA_FDB_CLEAR_OK` naming the
//! same disposable cluster file; run with `--test-threads=1`).

use arca_lake_metadata::fdb::FdbKv;
use arca_lake_metadata::records::GroupRecord;
use arca_lake_testkit::TestCluster;

async fn cluster() -> Option<(TestCluster<FdbKv>, GroupRecord)> {
    // The keyspace is cleared: only a cluster named explicitly, confirmed
    // as disposable by naming it again in ARCA_FDB_CLEAR_OK, and never the
    // system default.
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok()?;
    let ok = std::env::var("ARCA_FDB_CLEAR_OK").ok()?;
    assert!(
        ok == path && !is_system_cluster(&path),
        "ARCA_FDB_CLEAR_OK must name the disposable cluster file itself (not the system default)"
    );
    let start = arca_lake_testkit::fdb::starter()?;
    Some(super::tiering::cluster_on(start(super::tiering::options()).await).await)
}

/// Old counts do not promote; demotion waits for the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_the_tiering_record_old_counts_do_not_promote_and_demotion_waits_for_the_grace() {
    let Some((c, fast)) = cluster().await else {
        eprintln!("ARCA_FDB_TESTS, ARCA_FDB_CLUSTER_FILE or ARCA_FDB_CLEAR_OK not set; skipping");
        return;
    };
    super::tiering::under_the_tiering_record_old_counts_do_not_promote_and_demotion_waits_for_the_grace_on(c, fast).await;
}

/// A changed policy refuses the pass (no reset), not rebalancing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_policy_refuses_the_pass_but_not_rebalancing() {
    let Some((c, fast)) = cluster().await else {
        eprintln!("ARCA_FDB_TESTS, ARCA_FDB_CLUSTER_FILE or ARCA_FDB_CLEAR_OK not set; skipping");
        return;
    };
    super::tiering::a_changed_policy_refuses_the_pass_but_not_rebalancing_on(c, fast).await;
}

/// No record and an old record wait for the grace (skewed clock).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generations_without_or_with_old_records_wait_for_the_grace() {
    let Some((c, fast)) = cluster().await else {
        eprintln!("ARCA_FDB_TESTS, ARCA_FDB_CLUSTER_FILE or ARCA_FDB_CLEAR_OK not set; skipping");
        return;
    };
    super::tiering::generations_without_or_with_old_records_wait_for_the_grace_on(c, fast).await;
}

/// A move in flight resets no access record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_already_in_flight_resets_no_access_record() {
    let Some((c, fast)) = cluster().await else {
        eprintln!("ARCA_FDB_TESTS, ARCA_FDB_CLUSTER_FILE or ARCA_FDB_CLEAR_OK not set; skipping");
        return;
    };
    super::tiering::a_move_already_in_flight_resets_no_access_record_on(c, fast).await;
}

/// Whether `path` names the host's system cluster: the same file (after
/// resolving links and relative paths) or the same coordinators as the
/// default cluster file.
fn is_system_cluster(path: &str) -> bool {
    const SYSTEM: &str = "/etc/foundationdb/fdb.cluster";
    let canon = |p: &str| std::fs::canonicalize(p).ok();
    let coordinators = |p: &str| {
        std::fs::read_to_string(p)
            .ok()
            .and_then(|s| s.trim().split_once('@').map(|(_, c)| c.to_string()))
    };
    (canon(path).is_some() && canon(path) == canon(SYSTEM))
        || (coordinators(path).is_some() && coordinators(path) == coordinators(SYSTEM))
}
