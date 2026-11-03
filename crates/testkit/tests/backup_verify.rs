//! Verification of restored metadata (RFC-015 B-A5, Phase 3.4 §6): clean
//! metadata has no finding; damaged records are reported, read-only.

#![allow(clippy::many_single_char_names, clippy::implicit_clone)]

use arca_lake_controller::backup::verify_restored;
use arca_lake_metadata::records::{GenerationHeader, GroupRecord, decode, encode};
use arca_lake_metadata::{KvStore, Txn, keys, transact};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "backup";

async fn snapshot<S: KvStore>(meta: &S) -> Vec<(Vec<u8>, Vec<u8>)> {
    transact(meta, (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&keys::all_range(), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn clean_metadata_verifies_and_damage_is_reported_without_writes() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    for i in 0..5u64 {
        let d = Bytes::from(random_bytes(usize::try_from(MIB + 17 * i).unwrap(), i));
        s3::put(
            &c.backend,
            BUCKET,
            &format!("k{i}"),
            d,
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    s3::delete(&c.backend, BUCKET, "k4").await.unwrap();
    let r = verify_restored(c.meta.as_ref()).await.unwrap();
    assert_eq!(r.findings_total, 0, "{:?}", r.findings);
    assert_eq!((r.buckets, r.groups), (1, 1));
    assert!(
        r.versions >= 4 && r.committed >= 4 && r.bytes >= 4 * MIB,
        "{r:?}"
    );

    // Damage: a header that disagrees with its chunk records, and a group
    // epoch behind a chunk's placement epoch.
    let g = c.manifest(BUCKET, "k1").await.pointer.generation;
    transact(c.meta.as_ref(), g, |tx, g| {
        Box::pin(async move {
            let k = keys::gen_header(g);
            let mut h: GenerationHeader = decode(&tx.get(&k).await?.unwrap())?;
            h.size += 1;
            tx.set(&k, &encode(&h)?);
            Ok(())
        })
    })
    .await
    .unwrap();
    let gid = c.groups().await[0].id;
    transact(c.meta.as_ref(), gid, |tx, gid| {
        Box::pin(async move {
            let k = keys::group(*gid);
            let mut g: GroupRecord = decode(&tx.get(&k).await?.unwrap())?;
            g.epoch = 0;
            tx.set(&k, &encode(&g)?);
            Ok(())
        })
    })
    .await
    .unwrap();
    let before = snapshot(c.meta.as_ref()).await;
    let r = verify_restored(c.meta.as_ref()).await.unwrap();
    assert_eq!(
        snapshot(c.meta.as_ref()).await,
        before,
        "verification wrote metadata"
    );
    assert!(
        r.findings.iter().any(|f| f.contains("header says")),
        "{:?}",
        r.findings
    );
    assert!(
        r.findings.iter().any(|f| f.contains("epoch 0")),
        "{:?}",
        r.findings
    );
    assert!(
        r.findings.iter().any(|f| f.contains("ahead of group")),
        "{:?}",
        r.findings
    );
    c.shutdown().await;
}
