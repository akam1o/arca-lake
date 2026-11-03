//! Object-level integrity (R-54): verify every shard of one object version
//! on its members, and resolve repair findings to the objects they hit.
//!
//! The check uses the nodes' deep verification (`VerifyShards`: header and
//! every payload block CRC), so a shard that fails it is quarantined by
//! the node exactly as a scrub would (non-destructive, RFC-008 §8.2). The
//! S3 checksums of the object are not recomputed: that would need the
//! whole object decoded, and the per-shard CRCs plus the chunk records
//! already decide whether every byte is recoverable.

use std::collections::HashMap;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::integrity::{self, RepairState};
use arca_lake_metadata::ops::object::{self, ManifestFound};
use arca_lake_metadata::ops::{cluster, multipart};
use arca_lake_metadata::records::{ChunkRecord, GenId, GenKind, GroupRecord, NodeRecord};
use arca_lake_protocol::client::{NodeClients, ShardRef};

use crate::ControlError;

/// State of one shard on its member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// Present and verified.
    Ok,
    /// Present, failed verification (the node quarantined it).
    Corrupt,
    /// Not on the member.
    Missing,
    /// The member did not answer.
    Unanswered,
}

/// One chunk of an object version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkCheck {
    /// Generation holding the chunk record (a part for multipart objects).
    pub generation: GenId,
    /// Part number for multipart objects.
    pub part_number: Option<u32>,
    /// Chunk index within that generation.
    pub chunk_index: u32,
    /// Chunk id.
    pub chunk_id: [u8; 16],
    /// Group.
    pub group: u64,
    /// Shards needed to read.
    pub need: u16,
    /// Per member (index = shard index): node id and state.
    pub shards: Vec<(u64, ShardState)>,
}

impl ChunkCheck {
    /// Verified shards.
    #[must_use]
    pub fn good(&self) -> usize {
        self.shards
            .iter()
            .filter(|(_, s)| *s == ShardState::Ok)
            .count()
    }

    /// Whether the chunk is readable (at least `need` good shards).
    #[must_use]
    pub fn readable(&self) -> bool {
        self.good() >= usize::from(self.need)
    }

    /// Whether some member could not be checked.
    #[must_use]
    pub fn undecided(&self) -> bool {
        self.shards
            .iter()
            .any(|(_, s)| *s == ShardState::Unanswered)
    }
}

/// Overall verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Every shard verified.
    Healthy,
    /// Readable, some shards missing or corrupt (repair restores them).
    Degraded,
    /// Some chunk has fewer than `k` good shards, or a part reference
    /// dangles: bytes are lost.
    Unreadable,
    /// Some member did not answer; nothing worse was found.
    Incomplete,
}

/// Result of [`verify_object`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCheck {
    /// Bucket.
    pub bucket: String,
    /// Key.
    pub key: String,
    /// Version id.
    pub version_id: String,
    /// The version's generation (the root for multipart objects).
    pub generation: GenId,
    /// Whether this is a multipart object.
    pub multipart: bool,
    /// Every chunk.
    pub chunks: Vec<ChunkCheck>,
    /// Part numbers whose generation is gone.
    pub dangling_parts: Vec<u32>,
    /// Verdict.
    pub verdict: Verdict,
}

/// Chunk records of one generation: (generation, part number, records).
type ChunkSource = (GenId, Option<u32>, Vec<(u32, ChunkRecord)>);

fn chunk_pred(id: &[u8; 16]) -> Option<[u8; 16]> {
    let mut out = *id;
    for b in out.iter_mut().rev() {
        if *b > 0 {
            *b -= 1;
            return Some(out);
        }
        *b = 0xff;
    }
    None
}

async fn check_chunk(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    chunk: &ChunkRecord,
) -> Vec<(u64, ShardState)> {
    let after = chunk_pred(&chunk.chunk_id).map(|c| ShardRef {
        group_id: group.id,
        chunk_id: c,
        shard_index: u16::MAX,
    });
    let mut out = Vec::with_capacity(group.members.len());
    for (i, m) in group.members.iter().enumerate() {
        let state = match nodes.get(&m.node_id) {
            None => ShardState::Unanswered,
            Some(n) => match clients
                .verify_shards_page(&n.address, group.id, after, 8)
                .await
            {
                Err(_) => ShardState::Unanswered,
                Ok(p) => match p.shards.iter().find(|v| {
                    v.shard.chunk_id == chunk.chunk_id && usize::from(v.shard.shard_index) == i
                }) {
                    Some(v) if v.ok => ShardState::Ok,
                    Some(_) => ShardState::Corrupt,
                    None => ShardState::Missing,
                },
            },
        };
        out.push((m.node_id, state));
    }
    out
}

/// Verify every shard of one object version (the current one when
/// `version_id` is `None`), including every part of a multipart object.
///
/// # Errors
///
/// [`ControlError`] for metadata errors, a missing object or a delete
/// marker.
pub async fn verify_object<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<ObjectCheck, ControlError> {
    let manifest = match object::read_manifest_version(meta, bucket, key, version_id, None).await? {
        ManifestFound::Manifest(m) => m,
        ManifestFound::DeleteMarker { version_id, .. } => {
            return Err(ControlError::Other(format!(
                "{bucket}/{key}: version {version_id} is a delete marker"
            )));
        }
        ManifestFound::Missing => {
            return Err(ControlError::Other(format!(
                "{bucket}/{key}: no such version"
            )));
        }
    };
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let mut groups: HashMap<u64, GroupRecord> = HashMap::new();
    let multipart = matches!(manifest.header.kind, GenKind::MultipartRoot { .. });
    let mut sources: Vec<ChunkSource> = Vec::new();
    let mut dangling_parts = Vec::new();
    if multipart {
        for part in &manifest.parts {
            match object::read_part_manifest(meta, &part.generation).await? {
                Some((chunks, _)) => {
                    sources.push((part.generation, Some(part.part_number), chunks));
                }
                None => dangling_parts.push(part.part_number),
            }
        }
    } else {
        sources.push((manifest.pointer.generation, None, manifest.chunks.clone()));
    }
    let mut chunks = Vec::new();
    for (generation, part_number, records) in sources {
        for (index, ch) in records {
            let gid = ch.placement.group_id;
            if let std::collections::hash_map::Entry::Vacant(e) = groups.entry(gid) {
                e.insert(cluster::get_group(meta, gid).await?);
            }
            let group = &groups[&gid];
            let shards = check_chunk(clients, &nodes, group, &ch).await;
            chunks.push(ChunkCheck {
                generation,
                part_number,
                chunk_index: index,
                chunk_id: ch.chunk_id,
                group: gid,
                need: group.data_shards,
                shards,
            });
        }
    }
    let verdict =
        if !dangling_parts.is_empty() || chunks.iter().any(|c| !c.readable() && !c.undecided()) {
            Verdict::Unreadable
        } else if chunks.iter().any(ChunkCheck::undecided) {
            Verdict::Incomplete
        } else if chunks.iter().any(|c| c.good() < c.shards.len()) {
            Verdict::Degraded
        } else {
            Verdict::Healthy
        };
    Ok(ObjectCheck {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: manifest.pointer.version_id.clone(),
        generation: manifest.pointer.generation,
        multipart,
        chunks,
        dangling_parts,
        verdict,
    })
}

/// A repair task resolved to the object it affects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffectedTask {
    /// Group.
    pub group: u64,
    /// Chunk id.
    pub chunk_id: [u8; 16],
    /// Task state.
    pub state: RepairState,
    /// Shards the finding named.
    pub shards: Vec<u16>,
    /// Bucket (`#id` when its record is gone; empty when unresolved).
    pub bucket: String,
    /// Key (empty when unresolved).
    pub key: String,
    /// Version id, when a version of the key references the chunk.
    pub version_id: Option<String>,
    /// Whether that version is current.
    pub is_current: bool,
    /// Generation holding the chunk record.
    pub generation: Option<GenId>,
    /// Part number for multipart objects.
    pub part_number: Option<u32>,
    /// Chunk index.
    pub chunk_index: u32,
}

/// Every task that is `Unrecoverable` or `Failed`, resolved to its object
/// (`admin integrity status --objects`). One lookup per task, bounded by
/// the versions of the affected key; output only, never a metric label.
///
/// # Errors
///
/// Metadata errors.
pub async fn affected_objects<S: KvStore>(meta: &S) -> Result<Vec<AffectedTask>, ControlError> {
    let mut out = Vec::new();
    for (group, chunk_id, task) in integrity::tasks(meta, usize::MAX).await? {
        if !matches!(
            task.state,
            RepairState::Unrecoverable { .. } | RepairState::Failed { .. }
        ) {
            continue;
        }
        let (prefix, index) = arca_lake_common::split_chunk_id(&chunk_id);
        let mut a = AffectedTask {
            group,
            chunk_id,
            state: task.state.clone(),
            shards: task.hint_targets.clone(),
            bucket: String::new(),
            key: String::new(),
            version_id: None,
            is_current: false,
            generation: None,
            part_number: None,
            chunk_index: index,
        };
        if let Some(g) =
            arca_lake_metadata::ops::gc::live_generation_with_prefix(meta, &prefix).await?
        {
            a.generation = Some(g);
            let header = object::get_generation(meta, &g).await?;
            a.key.clone_from(&header.key);
            a.bucket = match multipart::bucket_of(meta, header.bucket_id).await {
                Ok(b) => b.name,
                Err(_) => format!("#{}", header.bucket_id),
            };
            if let Some(l) = object::locate_generation(meta, &g).await? {
                a.version_id = l.version_id;
                a.is_current = l.is_current;
                a.part_number = l.part_number;
            }
        }
        out.push(a);
    }
    Ok(out)
}
