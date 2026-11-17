//! Phase 4.1 (ADR-0033): `attribute_shards`, the batched judgment of
//! reconciliation and the reclaim pass, answers for every shard exactly
//! what `attribute_shard` answers for it alone, on random metadata:
//! generations in every state or without a header, prefixes shared by two
//! generations, chunk records in either group or missing, migration
//! intents to either group, records that do not decode, and prefixes no
//! generation has. The same on FoundationDB: `fdb.rs`.

#[path = "support/attribute.rs"]
mod support;

use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::gc;
use proptest::prelude::*;
use support::{compare, generation, load};

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn a_batch_answers_what_each_shard_alone_answers(
        gens in prop::collection::vec(generation(), 0..8),
        intents in prop::collection::vec((0..7u8, 0..4u32, 0..3u64), 0..4),
        queries in prop::collection::vec((0..7u8, 0..4u32, 1..3u64), 1..24),
    ) {
        let diff = futures::executor::block_on(async {
            let kv = MemKv::default();
            load(&kv, &gens, &intents).await;
            compare(&kv, &queries).await
        });
        prop_assert!(diff.is_none(), "{}", diff.unwrap_or_default());
    }
}

#[test]
fn an_empty_batch_reads_nothing() {
    futures::executor::block_on(async {
        let kv = MemKv::default();
        assert_eq!(gc::attribute_shards(&kv, &[]).await.unwrap(), Vec::new());
    });
}
