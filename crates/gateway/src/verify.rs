//! End-to-end object verification (`admin integrity verify-object
//! --decode`, Phase 3.4 §9): decode every chunk of one object version from
//! shards that verified good, check each chunk against its record, and
//! recompute the object's S3 checksums and ETag over the decoded bytes in
//! order. Read-only: shards are read only after a read-only verification
//! found them good, nothing is written or quarantined, and memory holds one
//! chunk at a time.

use std::collections::HashMap;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::object::{self, ManifestFound};
use arca_lake_metadata::records::{
    Checksum, ChecksumType, ChunkRecord, GenKind, GroupRecord, NodeRecord,
};
use arca_lake_protocol::client::NodeClients;
use arca_lake_tiering::durable::{Coders, read_chunk_detailed};
use s3s::crypto::{Checksum as _, Md5};

use crate::checksum::{self, Hasher};

/// Which members may be read for a chunk: `(generation, chunk index)` →
/// member indexes whose shard verified good. Chunks absent from the map
/// may be read from any member.
pub type GoodShards = HashMap<([u8; 16], u32), Vec<usize>>;

/// Result of [`decode_object`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodeReport {
    /// Chunks decoded and matching their records.
    pub chunks_ok: usize,
    /// Bytes decoded.
    pub bytes: u64,
    /// Chunks that could not be decoded or did not match (part, index, why).
    pub chunk_errors: Vec<(Option<u32>, u32, String)>,
    /// Size, offset and count mismatches between records.
    pub layout_errors: Vec<String>,
    /// Checks of recorded checksums and ETags: (what, matched).
    pub checks: Vec<(String, bool)>,
}

impl DecodeReport {
    /// Whether every byte decoded and every recorded value matched.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.chunk_errors.is_empty()
            && self.layout_errors.is_empty()
            && self.checks.iter().all(|(_, m)| *m)
    }
}

struct Digests {
    md5: Md5,
    hasher: Option<Hasher>,
}

impl Digests {
    fn new(checksum: Option<&Checksum>) -> Self {
        Self {
            md5: Md5::new(),
            hasher: checksum.map(|c| Hasher::new(c.algorithm)),
        }
    }

    fn update(&mut self, b: &[u8]) {
        self.md5.update(b);
        if let Some(h) = &mut self.hasher {
            h.update(b);
        }
    }

    fn finish(self) -> (String, Option<Vec<u8>>) {
        (
            checksum::hex(self.md5.finalize().as_ref()),
            self.hasher.map(Hasher::finalize),
        )
    }
}

fn etag_bare(e: &str) -> &str {
    e.trim_matches('"')
}

#[allow(clippy::too_many_arguments)]
async fn decode_chunks(
    clients: &NodeClients,
    coders: &Coders,
    nodes: &HashMap<u64, NodeRecord>,
    groups: &mut HashMap<u64, GroupRecord>,
    meta: &impl KvStore,
    generation: [u8; 16],
    part: Option<u32>,
    records: &[(u32, ChunkRecord)],
    good: &GoodShards,
    digests: &mut [&mut Digests],
    report: &mut DecodeReport,
) -> Result<u64, String> {
    let mut size = 0u64;
    for (expected_index, (index, ch)) in records.iter().enumerate() {
        if usize::try_from(*index).ok() != Some(expected_index) {
            report.layout_errors.push(format!(
                "part {part:?}: chunk index {index} at position {expected_index}"
            ));
        }
        let gid = ch.placement.group_id;
        if let std::collections::hash_map::Entry::Vacant(e) = groups.entry(gid) {
            e.insert(
                cluster::get_group(meta, gid)
                    .await
                    .map_err(|e| e.to_string())?,
            );
        }
        let group = &groups[&gid];
        let allowed = good.get(&(generation, *index));
        let addr_of = |node_id: u64| {
            let i = group.members.iter().position(|m| m.node_id == node_id)?;
            if allowed.is_some_and(|a| !a.contains(&i)) {
                return None;
            }
            nodes.get(&node_id).map(|n| n.address.clone())
        };
        let coder = match coders.get(&group.scheme, group.data_shards, group.parity_shards) {
            Ok(c) => c,
            Err(e) => {
                report.chunk_errors.push((part, *index, e.to_string()));
                continue;
            }
        };
        match read_chunk_detailed(clients, group, &addr_of, ch, coder.as_ref()).await {
            Ok(r) => {
                let mut n = 0u64;
                for b in &r.parts {
                    for d in digests.iter_mut() {
                        d.update(b);
                    }
                    n += b.len() as u64;
                }
                if n == u64::from(ch.len) {
                    report.chunks_ok += 1;
                } else {
                    report.chunk_errors.push((
                        part,
                        *index,
                        format!("decoded {n} bytes, recorded {}", ch.len),
                    ));
                }
                report.bytes += n;
                size += n;
            }
            Err(e) => report.chunk_errors.push((part, *index, e.to_string())),
        }
    }
    Ok(size)
}

/// Decode one object version (the current one when `version_id` is
/// `None`) and check it end to end.
///
/// # Errors
///
/// Metadata errors, a missing version or a delete marker.
#[allow(clippy::too_many_lines)]
pub async fn decode_object<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    good: &GoodShards,
) -> Result<DecodeReport, String> {
    let manifest = match object::read_manifest_version(meta, bucket, key, version_id, None)
        .await
        .map_err(|e| e.to_string())?
    {
        ManifestFound::Manifest(m) => m,
        ManifestFound::DeleteMarker { version_id, .. } => {
            return Err(format!(
                "{bucket}/{key}: version {version_id} is a delete marker"
            ));
        }
        ManifestFound::Missing => return Err(format!("{bucket}/{key}: no such version")),
    };
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let coders = Coders::default();
    let mut groups = HashMap::new();
    let mut report = DecodeReport::default();
    let h = &manifest.header;
    if matches!(h.kind, GenKind::MultipartRoot { .. }) {
        let mut whole = Digests::new(
            h.checksum
                .as_ref()
                .filter(|c| c.kind == ChecksumType::FullObject),
        );
        let mut offset = 0u64;
        let mut part_etags = Vec::new();
        let mut part_sums: Vec<(u64, Vec<u8>)> = Vec::new();
        for p in &manifest.parts {
            if p.offset != offset {
                report.layout_errors.push(format!(
                    "part {}: offset {}, expected {offset}",
                    p.part_number, p.offset
                ));
            }
            let Some((records, _)) = object::read_part_manifest(meta, &p.generation)
                .await
                .map_err(|e| e.to_string())?
            else {
                report.layout_errors.push(format!(
                    "part {}: generation gone (dangling)",
                    p.part_number
                ));
                continue;
            };
            if records.len() != usize::try_from(p.chunk_count).unwrap_or(usize::MAX) {
                report.layout_errors.push(format!(
                    "part {}: {} chunk records, recorded {}",
                    p.part_number,
                    records.len(),
                    p.chunk_count
                ));
            }
            let mut part_digests = Digests::new(p.checksum.as_ref().or(h.checksum.as_ref()));
            let size = decode_chunks(
                clients,
                &coders,
                &nodes,
                &mut groups,
                meta,
                p.generation,
                Some(p.part_number),
                &records,
                good,
                &mut [&mut part_digests, &mut whole],
                &mut report,
            )
            .await?;
            if size != p.size {
                report.layout_errors.push(format!(
                    "part {}: {size} bytes decoded, recorded {}",
                    p.part_number, p.size
                ));
            }
            offset += p.size;
            let (md5, sum) = part_digests.finish();
            report.checks.push((
                format!("part {} ETag", p.part_number),
                md5 == etag_bare(&p.etag),
            ));
            if let (Some(want), Some(got)) = (&p.checksum, &sum) {
                report.checks.push((
                    format!("part {} {:?} checksum", p.part_number, want.algorithm),
                    *got == want.value,
                ));
            }
            part_etags.push(md5);
            if let Some(s) = sum {
                part_sums.push((p.size, s));
            }
        }
        if offset != h.size {
            report.layout_errors.push(format!(
                "parts total {offset} bytes, object recorded {}",
                h.size
            ));
        }
        report.checks.push((
            "multipart ETag".into(),
            checksum::composite_etag(&part_etags) == etag_bare(&h.etag),
        ));
        let (_, full) = whole.finish();
        if let Some(c) = &h.checksum {
            let got = match c.kind {
                ChecksumType::FullObject => full,
                ChecksumType::Composite => (part_sums.len() == manifest.parts.len())
                    .then(|| checksum::derive(c.algorithm, c.kind, &part_sums))
                    .flatten(),
            };
            report.checks.push((
                format!("object {:?} {:?} checksum", c.algorithm, c.kind),
                got.as_deref() == Some(c.value.as_slice()),
            ));
            if c.kind == ChecksumType::Composite {
                report.checks.push((
                    "composite checksum part count".into(),
                    c.parts == Some(u32::try_from(manifest.parts.len()).unwrap_or(u32::MAX)),
                ));
            }
        }
    } else {
        if manifest.chunks.len() != usize::try_from(h.chunk_count).unwrap_or(usize::MAX) {
            report.layout_errors.push(format!(
                "{} chunk records, header says {}",
                manifest.chunks.len(),
                h.chunk_count
            ));
        }
        let mut digests = Digests::new(h.checksum.as_ref());
        let size = decode_chunks(
            clients,
            &coders,
            &nodes,
            &mut groups,
            meta,
            manifest.pointer.generation,
            None,
            &manifest.chunks,
            good,
            &mut [&mut digests],
            &mut report,
        )
        .await?;
        if size != h.size {
            report
                .layout_errors
                .push(format!("{size} bytes decoded, recorded {}", h.size));
        }
        let (md5, sum) = digests.finish();
        report
            .checks
            .push(("ETag (MD5)".into(), md5 == etag_bare(&h.etag)));
        if let (Some(c), Some(got)) = (&h.checksum, sum) {
            report
                .checks
                .push((format!("object {:?} checksum", c.algorithm), got == c.value));
        }
    }
    Ok(report)
}
