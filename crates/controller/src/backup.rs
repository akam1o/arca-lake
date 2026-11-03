//! Verification of restored metadata (RFC-015 B-A5, Phase 3.4 §6): the
//! read-only checks a backup verification job runs against a metadata
//! backup restored into an isolated FoundationDB cluster. Nothing is
//! written; no node is contacted (a restored copy has no storage nodes:
//! this proves the metadata backup is usable, not that shards exist).

use std::collections::{BTreeSet, HashMap};

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::list::{ListVersionsRequest, list_versions};
use arca_lake_metadata::ops::object::{self, ManifestFound};
use arca_lake_metadata::ops::{bucket, cluster, compat, integrity};
use arca_lake_metadata::records::{GenKind, GenState, GroupRecord};

use crate::ControlError;

/// Findings kept in the report (the count is exact).
const MAX_FINDINGS: usize = 1000;

/// What the restored metadata holds and what is wrong with it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoredReport {
    /// Metadata schema version stored.
    pub schema: Option<u64>,
    /// Segment format nodes write.
    pub format_write: Option<u64>,
    /// Cluster incarnation (hex) and phase.
    pub cluster: Option<(String, String)>,
    /// Groups, nodes and buckets.
    pub groups: usize,
    /// Registered nodes.
    pub nodes: usize,
    /// Buckets.
    pub buckets: usize,
    /// Object versions (delete markers included) and delete markers.
    pub versions: u64,
    /// Delete markers.
    pub delete_markers: u64,
    /// Generations by state.
    pub committed: u64,
    /// Pending generations (in-flight writes at backup time).
    pub pending: u64,
    /// Retiring generations (GC backlog at backup time).
    pub retiring: u64,
    /// Bytes of committed object and part generations.
    pub bytes: u64,
    /// Multipart roots and parts verified.
    pub multipart_roots: u64,
    /// Multipart parts verified.
    pub multipart_parts: u64,
    /// Every finding counted.
    pub findings_total: u64,
    /// The first findings.
    pub findings: Vec<String>,
}

impl RestoredReport {
    fn finding(&mut self, f: String) {
        self.findings_total += 1;
        if self.findings.len() < MAX_FINDINGS {
            self.findings.push(f);
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        use std::fmt::Write as _;
        let _ = write!(s, "{x:02x}");
        s
    })
}

/// Verify restored metadata end to end (read-only).
///
/// # Errors
///
/// Metadata errors (an unreadable store is an error, not a finding).
#[allow(clippy::too_many_lines)]
pub async fn verify_restored<S: KvStore>(meta: &S) -> Result<RestoredReport, ControlError> {
    let mut r = RestoredReport::default();
    // Schema and versions.
    let gate = compat::gate(meta).await?;
    r.schema = gate.schema;
    r.format_write = gate.format_write;
    match gate.schema {
        None => r.finding("no schema version record".into()),
        Some(v)
            if !(arca_lake_metadata::keys::SCHEMA_VERSION_MIN
                ..=arca_lake_metadata::keys::SCHEMA_VERSION)
                .contains(&v) =>
        {
            r.finding(format!("schema {v} is outside what this build reads"));
        }
        Some(_) => {}
    }
    // Cluster state, incarnation, epochs.
    match compat::cluster_state(meta).await? {
        Some(s) => {
            if s.incarnation == [0; 16] {
                r.finding("cluster incarnation is zero".into());
            }
            r.cluster = Some((hex(&s.incarnation), s.state.name().to_string()));
        }
        None => r.finding("no cluster state record".into()),
    }
    let groups: HashMap<u64, GroupRecord> = cluster::list_groups(meta)
        .await?
        .into_iter()
        .map(|g| (g.id, g))
        .collect();
    r.groups = groups.len();
    for g in groups.values() {
        if g.epoch == 0 {
            r.finding(format!("group {}: epoch 0", g.id));
        }
        let distinct: BTreeSet<u64> = g.members.iter().map(|m| m.node_id).collect();
        if g.members.is_empty() || distinct.len() != g.members.len() {
            r.finding(format!("group {}: members {:?}", g.id, g.members));
        }
    }
    r.nodes = cluster::list_nodes(meta).await?.len();
    // Every generation: state counts; committed objects against their
    // chunk records and groups.
    let mut after = None;
    loop {
        let page = object::list_generations(meta, after, 500).await?;
        let Some(last) = page.last().map(|(g, _)| *g) else {
            break;
        };
        for (gid, h) in &page {
            match h.state {
                GenState::Pending { .. } => r.pending += 1,
                GenState::Retiring { .. } => r.retiring += 1,
                GenState::Committed => r.committed += 1,
            }
            if h.state != GenState::Committed {
                continue;
            }
            if matches!(h.kind, GenKind::MultipartRoot { .. }) {
                continue; // verify_manifests below
            }
            r.bytes += h.size;
            let Some(chunks) = object::read_manifest_of(meta, gid).await? else {
                r.finding(format!("generation {}: chunk records unreadable", hex(gid)));
                continue;
            };
            if chunks.len() != usize::try_from(h.chunk_count).unwrap_or(usize::MAX) {
                r.finding(format!(
                    "generation {}: {} chunk records, header says {}",
                    hex(gid),
                    chunks.len(),
                    h.chunk_count
                ));
            }
            let total: u64 = chunks.iter().map(|(_, c)| u64::from(c.len)).sum();
            if total != h.size {
                r.finding(format!(
                    "generation {}: chunks hold {total} bytes, header says {}",
                    hex(gid),
                    h.size
                ));
            }
            for (i, c) in &chunks {
                match groups.get(&c.placement.group_id) {
                    None => r.finding(format!(
                        "generation {} chunk {i}: group {} unknown",
                        hex(gid),
                        c.placement.group_id
                    )),
                    Some(g) => {
                        if c.placement.epoch > g.epoch {
                            r.finding(format!(
                                "generation {} chunk {i}: placement epoch {} ahead of group {} epoch {}",
                                hex(gid),
                                c.placement.epoch,
                                g.id,
                                g.epoch
                            ));
                        }
                        if c.scheme != g.scheme
                            || c.data_shards != g.data_shards
                            || c.parity_shards != g.parity_shards
                        {
                            r.finding(format!(
                                "generation {} chunk {i}: recorded {} {}+{}, group {} is {} {}+{}",
                                hex(gid),
                                c.scheme,
                                c.data_shards,
                                c.parity_shards,
                                g.id,
                                g.scheme,
                                g.data_shards,
                                g.parity_shards
                            ));
                        }
                    }
                }
            }
        }
        after = Some(last);
    }
    // Multipart roots against their parts.
    let mut cursor = None;
    loop {
        let m = integrity::verify_manifests(meta, cursor, 1000).await?;
        r.multipart_roots += m.roots;
        r.multipart_parts += m.parts;
        for (g, f) in &m.findings {
            r.finding(format!("multipart root {}: {f:?}", hex(g)));
        }
        match m.next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    // Versioning: one latest version per key; every version resolves.
    let buckets = bucket::list_buckets(meta).await?;
    r.buckets = buckets.len();
    for b in &buckets {
        let mut req = ListVersionsRequest {
            prefix: String::new(),
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
        };
        let mut latest: HashMap<String, u32> = HashMap::new();
        loop {
            let page = list_versions(meta, &b.name, &req).await?;
            for v in &page.versions {
                r.versions += 1;
                if v.is_latest {
                    *latest.entry(v.key.clone()).or_default() += 1;
                } else {
                    latest.entry(v.key.clone()).or_default();
                }
                if v.delete_marker {
                    r.delete_markers += 1;
                    continue;
                }
                match object::read_manifest_version(
                    meta,
                    &b.name,
                    &v.key,
                    Some(&v.version_id),
                    Some(0..0),
                )
                .await
                {
                    Ok(ManifestFound::Manifest(m)) => {
                        if m.header.state != GenState::Committed {
                            r.finding(format!(
                                "{}/{} version {}: generation not committed",
                                b.name, v.key, v.version_id
                            ));
                        }
                    }
                    Ok(_) => r.finding(format!(
                        "{}/{} version {}: does not resolve",
                        b.name, v.key, v.version_id
                    )),
                    Err(e) => r.finding(format!(
                        "{}/{} version {}: {e}",
                        b.name, v.key, v.version_id
                    )),
                }
            }
            match (&page.next_markers, page.is_truncated) {
                (Some((k, v)), true) => {
                    req.key_marker = Some(k.clone());
                    req.version_id_marker = Some(v.clone());
                }
                _ => break,
            }
        }
        for (k, n) in latest {
            if n > 1 {
                r.finding(format!("{}/{k}: {n} versions marked latest", b.name));
            }
        }
    }
    Ok(r)
}
