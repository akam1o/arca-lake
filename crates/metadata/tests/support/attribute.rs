//! Random metadata for the differential tests of `attribute_shards`
//! (Phase 4.1, ADR-0033): `attribute_batch.rs` on memory, `fdb.rs` on
//! FoundationDB.

#![allow(dead_code)]

use arca_lake_metadata::kv::{KvStore, Txn};
use arca_lake_metadata::ops::gc::{self, ShardAttribution};
use arca_lake_metadata::ops::object::MigrationIntent;
use arca_lake_metadata::records::{
    ChunkRecord, DurablePlacement, GenKind, GenState, GenerationHeader, encode,
};
use arca_lake_metadata::{MetaError, keys};
use proptest::prelude::*;

#[derive(Debug, Clone)]
pub enum Header {
    Absent,
    Pending,
    Committed,
    Retiring,
    Garbage,
}

#[derive(Debug, Clone)]
pub struct Gen {
    /// Index of the prefix (prefixes shared when equal).
    pub prefix: u8,
    pub suffix: u8,
    pub header: Header,
    /// Chunk index → group (1 or 2), or 0: a record that does not decode.
    pub chunks: Vec<(u32, u64)>,
}

pub fn header() -> impl Strategy<Value = Header> {
    prop_oneof![
        Just(Header::Absent),
        Just(Header::Pending),
        Just(Header::Committed),
        Just(Header::Retiring),
        Just(Header::Garbage),
    ]
}

pub fn generation() -> impl Strategy<Value = Gen> {
    (
        0..6u8,
        0..3u8,
        header(),
        prop::collection::vec((0..4u32, 0..3u64), 0..4),
    )
        .prop_map(|(prefix, suffix, header, chunks)| Gen {
            prefix,
            suffix,
            header,
            chunks,
        })
}

/// Prefixes with zero bytes inside (escaped by the tuple encoding of the
/// keys) and at the end.
pub fn prefix_of(p: u8) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0] = 0x01;
    b[5] = p % 2;
    b[11] = p;
    b
}

fn id_of(g: &Gen) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..12].copy_from_slice(&prefix_of(g.prefix));
    id[15] = g.suffix;
    id
}

fn chunk_id(prefix: &[u8; 12], index: u32) -> [u8; 16] {
    let mut c = [0u8; 16];
    c[..12].copy_from_slice(prefix);
    c[12..].copy_from_slice(&index.to_be_bytes());
    c
}

pub async fn load<S: KvStore>(kv: &S, gens: &[Gen], intents: &[(u8, u32, u64)]) {
    let mut tx = kv.begin().await.unwrap();
    for g in gens {
        let id = id_of(g);
        let state = match g.header {
            Header::Pending => Some(GenState::Pending { lease_until_ms: 10 }),
            Header::Committed => Some(GenState::Committed),
            Header::Retiring => Some(GenState::Retiring { since_ms: 5 }),
            Header::Absent | Header::Garbage => None,
        };
        if let Some(state) = state {
            let h = GenerationHeader {
                state,
                bucket_id: 1,
                key: "k".into(),
                size: 0,
                etag: String::new(),
                content_type: None,
                user_metadata: Vec::new(),
                chunk_count: 0,
                created_ms: 1,
                kind: GenKind::Object,
                checksum: None,
            };
            tx.set(&keys::gen_header(&id), &encode(&h).unwrap());
        } else if matches!(g.header, Header::Garbage) {
            tx.set(&keys::gen_header(&id), &[0xff, 1, 2]);
        }
        for &(index, group) in &g.chunks {
            let value = if group == 0 {
                vec![0xfe]
            } else {
                encode(&ChunkRecord {
                    chunk_id: chunk_id(&prefix_of(g.prefix), index),
                    offset: 0,
                    len: 1,
                    scheme: "rep-3".into(),
                    data_shards: 1,
                    parity_shards: 2,
                    crc32c: 0,
                    placement: DurablePlacement {
                        group_id: group,
                        epoch: 1,
                    },
                })
                .unwrap()
            };
            tx.set(&keys::gen_chunk(&id, index), &value);
        }
    }
    for &(p, index, group) in intents {
        let value = if group == 0 {
            vec![0xfd]
        } else {
            encode(&MigrationIntent {
                target_group: group,
                attempt: [7; 16],
                since_ms: 1,
            })
            .unwrap()
        };
        tx.set(&keys::migration(&chunk_id(&prefix_of(p), index)), &value);
    }
    tx.commit().await.unwrap();
}

pub fn same(
    a: &Result<ShardAttribution, MetaError>,
    b: &Result<ShardAttribution, MetaError>,
) -> bool {
    match (a, b) {
        (Ok(x), Ok(y)) => x == y,
        (Err(MetaError::Codec(_)), Err(MetaError::Codec(_))) => true,
        _ => false,
    }
}

/// `attribute_shards` against `attribute_shard` for every query: the
/// first disagreement, if any.
pub async fn compare<S: KvStore>(kv: &S, queries: &[(u8, u32, u64)]) -> Option<String> {
    let batch: Vec<gc::ShardQuery> = queries
        .iter()
        .map(|&(p, i, g)| (prefix_of(p), i, g))
        .collect();
    let got = gc::attribute_shards(kv, &batch).await.unwrap();
    assert_eq!(got.len(), batch.len());
    for ((prefix, index, group), got) in batch.iter().zip(&got) {
        let alone = gc::attribute_shard(kv, prefix, *index, *group).await;
        if !same(got, &alone) {
            return Some(format!(
                "shard {prefix:?}/{index} in group {group}: batch {got:?}, alone {alone:?}"
            ));
        }
    }
    None
}
