//! Conditional deletes of cache copies (Phase 3.6, R-73): a cache copy's
//! shards are deleted by the tokens the nodes returned when it was
//! written, so that an eviction, an invalidation or a failed population's
//! cleanup never removes a newer copy written under the same key since.
//! Copies without tokens (recorded before, or on an older node) are
//! deleted unconditionally, as before; a node that restarted refuses old
//! tokens and its copy is left to reconciliation.

use std::collections::HashMap;

use arca_lake_common::chunk_id;
use arca_lake_metadata::ops::{cache as meta_cache, cluster};
use arca_lake_metadata::records::{CacheCopyTokens, GenId, GroupRecord, NodeRecord};
use arca_lake_protocol::client::ShardRef;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use arca_lake_tiering::cache::{self as tcache, PopulateOutcome};
use arca_lake_tiering::durable::{Coders, write_coded};
use bytes::Bytes;

const CHUNK: u64 = 256 << 10;
const BUCKET: &str = "ctok";

struct Setup {
    c: TestCluster,
    group: GroupRecord,
    generation: GenId,
    chunk: Bytes,
    nodes: HashMap<u64, NodeRecord>,
}

impl Setup {
    async fn new() -> Self {
        let c = TestCluster::start(ClusterOptions {
            groups: 1,
            cache_nodes: 1,
            ..ClusterOptions::rs(3, 2, 1, CHUNK, 0)
        })
        .await;
        s3::create_bucket(&c.backend, BUCKET).await.unwrap();
        let chunk = Bytes::from(random_bytes(usize::try_from(CHUNK).unwrap() - 5, 3));
        s3::put(
            &c.backend,
            BUCKET,
            "o",
            chunk.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        let generation = c.manifest(BUCKET, "o").await.pointer.generation;
        let group = c
            .groups()
            .await
            .into_iter()
            .find(GroupRecord::is_cache)
            .unwrap();
        let nodes = Self::node_map(&c).await;
        Self {
            c,
            group,
            generation,
            chunk,
            nodes,
        }
    }

    async fn node_map(c: &TestCluster) -> HashMap<u64, NodeRecord> {
        cluster::list_nodes(c.meta.as_ref())
            .await
            .unwrap()
            .into_iter()
            .map(|n| (n.id, n))
            .collect()
    }

    fn addr(&self) -> String {
        self.nodes[&self.group.members[0].node_id].address.clone()
    }

    async fn populate(&self) -> PopulateOutcome {
        let addr_of = |id: u64| self.nodes.get(&id).map(|n| n.address.clone());
        tcache::populate(
            self.c.meta.as_ref(),
            &self.c.clients,
            &Coders::default(),
            &ClusterOptions::default_cache(),
            &self.group,
            &addr_of,
            &self.generation,
            0,
            self.chunk.clone(),
            now_ms(),
        )
        .await
        .unwrap()
    }

    async fn delete(&self, epoch: u64, tokens: &CacheCopyTokens) -> usize {
        let addr_of = |id: u64| self.nodes.get(&id).map(|n| n.address.clone());
        tcache::delete_copy(
            &self.c.clients,
            &self.group,
            &addr_of,
            chunk_id(&self.generation, 0),
            epoch,
            tokens,
        )
        .await
    }

    /// The copy on the cache node (its stored shard), if any.
    async fn copy(&self) -> Option<Bytes> {
        let shard = ShardRef {
            group_id: self.group.id,
            chunk_id: chunk_id(&self.generation, 0),
            shard_index: 0,
        };
        self.c
            .clients
            .get_shard(&self.addr(), shard, self.group.epoch, 0)
            .await
            .ok()
    }
}

#[tokio::test]
async fn an_eviction_does_not_delete_a_copy_populated_after_it_removed_the_record() {
    let s = Setup::new().await;
    assert_eq!(s.populate().await, PopulateOutcome::Inserted);
    // Eviction, step 1: the record goes (with the copy's tokens).
    let (p, tokens) = meta_cache::remove_with_tokens(s.c.meta.as_ref(), &s.generation, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tokens.shards.len(), 1, "{tokens:?}");
    // Meanwhile a read populates the chunk again under the same key.
    assert_eq!(s.populate().await, PopulateOutcome::Inserted);
    // Eviction, step 2: its delete names the old record and is refused.
    assert_eq!(s.delete(p.epoch, &tokens).await, 0);
    assert!(s.copy().await.is_some(), "the newer copy was deleted");
    assert!(
        meta_cache::get(s.c.meta.as_ref(), &s.generation, 0)
            .await
            .unwrap()
            .is_some()
    );
    // The newer copy's own eviction removes it.
    let (p, tokens) = meta_cache::remove_with_tokens(s.c.meta.as_ref(), &s.generation, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.delete(p.epoch, &tokens).await, 0);
    assert_eq!(s.copy().await, None);
    s.c.shutdown().await;
}

#[tokio::test]
async fn a_failed_population_cleans_up_only_the_shards_it_wrote() {
    let s = Setup::new().await;
    let addr_of = |id: u64| s.nodes.get(&id).map(|n| n.address.clone());
    // Population A writes its copy, then loses (its insert would fail).
    let a = write_coded(
        &s.c.clients,
        &s.group,
        &addr_of,
        chunk_id(&s.generation, 0),
        std::slice::from_ref(&s.chunk),
        None,
    )
    .await
    .unwrap();
    let a_tokens = CacheCopyTokens {
        group_id: s.group.id,
        shards: a
            .iter()
            .map(|(_, sh, t)| arca_lake_metadata::records::CacheShardToken {
                index: sh.shard_index,
                store_epoch: t.store_epoch,
                sequence: t.sequence,
            })
            .collect(),
    };
    // Population B rewrites the key and records its copy.
    assert_eq!(s.populate().await, PopulateOutcome::Inserted);
    // A's cleanup: B's copy stays (before R-73 it was deleted here).
    assert_eq!(s.delete(s.group.epoch, &a_tokens).await, 0);
    assert!(s.copy().await.is_some());
    // A copy recorded without tokens (before this change) is still
    // deleted unconditionally.
    assert_eq!(
        s.delete(s.group.epoch, &CacheCopyTokens::default()).await,
        0
    );
    assert_eq!(s.copy().await, None);
    s.c.shutdown().await;
}

#[tokio::test]
async fn repair_records_its_tokens_and_a_restarted_node_refuses_old_ones() {
    let mut s = Setup::new().await;
    assert_eq!(s.populate().await, PopulateOutcome::Inserted);
    // A repair rewrites the copy: its tokens replace the old ones, so the
    // eviction deletes the rewritten shard (no orphan).
    let addr_of = |id: u64| s.nodes.get(&id).map(|n| n.address.clone());
    tcache::repair_shards(
        s.c.meta.as_ref(),
        &s.c.clients,
        &Coders::default(),
        &s.group,
        &addr_of,
        &s.generation,
        0,
        &s.chunk,
        &[0],
    )
    .await
    .unwrap();
    let (p, tokens) = meta_cache::remove_with_tokens(s.c.meta.as_ref(), &s.generation, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.delete(p.epoch, &tokens).await, 0);
    assert_eq!(s.copy().await, None, "the repaired shard was left behind");

    // A copy whose node restarted: the node took a new store epoch and
    // refuses the old token; the copy is an orphan that reconciliation
    // (which lists fresh tokens) removes once no placement names it.
    assert_eq!(s.populate().await, PopulateOutcome::Inserted);
    let i =
        s.c.nodes
            .iter()
            .position(|n| n.id == s.group.members[0].node_id)
            .unwrap();
    s.c.stop_node(i).await;
    s.c.start_node(i).await;
    let (p, tokens) = meta_cache::remove_with_tokens(s.c.meta.as_ref(), &s.generation, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.delete(p.epoch, &tokens).await, 0);
    assert!(
        s.copy().await.is_some(),
        "kept: token of another store epoch"
    );
    s.nodes = Setup::node_map(&s.c).await;
    assert_eq!(s.c.reconcile_cache().await, 1);
    assert_eq!(s.copy().await, None);
    s.c.shutdown().await;
}
