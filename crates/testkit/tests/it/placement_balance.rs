//! Write placement spreads over the eligible groups (Phase 3.4 soak
//! finding): single-chunk objects all went to one group per gateway
//! process (the group choice was salted by the chunk index, and the
//! candidates came in the order of a per-process hash map), and a gateway
//! restart moved every new write to another group.

#![allow(clippy::many_single_char_names)]

use std::collections::HashMap;

use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "balance";

async fn spread(c: &TestCluster, prefix: &str, n: u64) -> HashMap<u64, u64> {
    let mut by: HashMap<u64, u64> = HashMap::new();
    for i in 0..n {
        let k = format!("{prefix}{i}");
        let d = Bytes::from(random_bytes(usize::try_from(MIB / 4).unwrap(), i));
        s3::put(&c.backend, BUCKET, &k, d, &PutOptions::default())
            .await
            .unwrap();
        let g = c.manifest(BUCKET, &k).await.chunks[0].1.placement.group_id;
        *by.entry(g).or_default() += 1;
    }
    by
}

#[tokio::test]
async fn single_chunk_objects_spread_over_groups_and_a_gateway_restart_changes_nothing() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 3,
        ..ClusterOptions::rs(5, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let groups = c.groups().await.iter().filter(|g| !g.is_cache()).count();
    assert!(groups >= 3, "{groups} groups");
    let n = 300;
    for round in 0..2 {
        let by = spread(&c, &format!("r{round}-"), n).await;
        assert_eq!(
            by.len(),
            groups,
            "round {round}: every group is used: {by:?}"
        );
        for (g, k) in &by {
            // Uniform would be n / groups = 100; allow wide noise.
            assert!(
                *k >= n / u64::try_from(groups).unwrap() / 2,
                "round {round}: group {g} got {k} of {n}: {by:?}"
            );
        }
        c.restart_backend().await.unwrap();
    }
    c.shutdown().await;
}
