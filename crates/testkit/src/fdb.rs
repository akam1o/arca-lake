//! Starting test clusters on a real FoundationDB cluster.

use std::sync::Arc;

use arca_lake_metadata::fdb::FdbKv;
use arca_lake_metadata::keys;
use futures::future::BoxFuture;

use crate::suite::Starter;
use crate::{ClusterOptions, TestCluster};

/// A starter that opens the cluster named by `ARCA_FDB_CLUSTER_FILE` (or the
/// client default), clears the arca-lake keyspace, and starts a cluster on
/// it. `None` when `ARCA_FDB_TESTS` is not set.
///
/// # Panics
///
/// The returned starter panics if the cluster cannot be opened or cleared.
pub fn starter() -> Option<impl Starter<FdbKv>> {
    std::env::var_os("ARCA_FDB_TESTS")?;
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    Some(
        move |opts: ClusterOptions| -> BoxFuture<'static, TestCluster<FdbKv>> {
            let path = path.clone();
            Box::pin(async move {
                let kv = FdbKv::open(path.as_deref()).expect("open FoundationDB");
                kv.clear_range_blocking(&keys::all_range())
                    .await
                    .expect("clear the arca-lake keyspace");
                TestCluster::start_with(Arc::new(kv), opts).await
            })
        },
    )
}
