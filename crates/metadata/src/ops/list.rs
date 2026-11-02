//! `ListObjectsV2` over the current-pointer subspace.
//!
//! Keys are read in order from the prefix range. When a delimiter is set and
//! a key contains it after the prefix, the common prefix is emitted once and
//! the scan jumps past every key sharing it (a key-selector skip), so the
//! cost is proportional to the output, not to the number of keys hidden
//! behind common prefixes. Each page is one read-only transaction.
//!
//! Pagination uses an opaque continuation token: the raw key position at
//! which the next page starts. After a common prefix the position is the
//! end of that prefix's range, so a prefix is never emitted twice.

use crate::error::Result;
use crate::kv::{Key, KeyRange, KvStore, Txn, strinc};
use crate::ops::bucket_by_name;
use crate::records::{CurrentPointer, NULL_VERSION_ID, VersionRecord, decode};
use crate::{MetaError, keys, transact};

/// Listing request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListRequest {
    /// Only keys starting with this prefix.
    pub prefix: String,
    /// Group keys by the part up to and including the first occurrence of
    /// this delimiter after the prefix.
    pub delimiter: Option<String>,
    /// Return keys strictly after this one (`start-after`).
    pub start_after: Option<String>,
    /// Opaque token from a previous truncated response (`continuation-token`).
    /// Takes precedence over `start_after` when it points further ahead.
    pub continuation: Option<Key>,
    /// Maximum objects plus common prefixes to return.
    pub max_keys: usize,
}

/// One listed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListEntry {
    /// Object key.
    pub key: String,
    /// Size.
    pub size: u64,
    /// ETag.
    pub etag: String,
    /// Last modified, Unix milliseconds.
    pub last_modified_ms: u64,
}

/// Listing response.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListResponse {
    /// Objects, in key order.
    pub objects: Vec<ListEntry>,
    /// Common prefixes, in key order.
    pub common_prefixes: Vec<String>,
    /// Whether more results exist.
    pub is_truncated: bool,
    /// Token for the next page, if truncated.
    pub next_continuation: Option<Key>,
}

/// Page size for the underlying range reads.
const PAGE: usize = 1000;

/// List current (non-delete-marker) objects.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`]; `max_keys == 0` is an invalid argument.
pub async fn list_objects<S: KvStore>(
    store: &S,
    bucket: &str,
    req: &ListRequest,
) -> Result<ListResponse> {
    if req.max_keys == 0 {
        return Err(MetaError::InvalidArgument("max_keys must be >= 1".into()));
    }
    let b = transact(store, bucket, |tx, bucket| {
        Box::pin(async move { bucket_by_name(tx, bucket).await })
    })
    .await?;
    let full = keys::object_prefix_range(b.id, &req.prefix);
    let mut begin = full.begin.clone();
    if let Some(after) = &req.start_after {
        begin = begin.max(keys::object_after(b.id, after));
    }
    if let Some(token) = &req.continuation {
        begin = begin.max(token.clone());
    }
    let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());

    let mut resp = ListResponse::default();
    // Position at which the next page would start if we stopped now.
    let mut resume: Key = begin.clone();
    'outer: while begin < full.end {
        let range = KeyRange::new(begin.clone(), full.end.clone());
        let page = transact(store, range, |tx, range| {
            Box::pin(async move { tx.get_range(range, PAGE, false).await.map_err(Into::into) })
        })
        .await?;
        if page.is_empty() {
            break;
        }
        for (k, v) in &page {
            let key = keys::object_key_from(k)
                .ok_or_else(|| MetaError::Codec("unexpected key in object range".into()))?;
            if resp.objects.len() + resp.common_prefixes.len() >= req.max_keys {
                resp.is_truncated = true;
                resp.next_continuation = Some(resume);
                return Ok(resp);
            }
            if let Some(delim) = delimiter {
                let tail = &key[req.prefix.len()..];
                if let Some(pos) = tail.find(delim) {
                    let common = key[..req.prefix.len() + pos + delim.len()].to_string();
                    // Skip everything under this common prefix.
                    begin = strinc(&keys::object_prefix_range(b.id, &common).begin);
                    resume.clone_from(&begin);
                    resp.common_prefixes.push(common);
                    continue 'outer;
                }
            }
            resume.clone_from(k);
            resume.push(0);
            let pointer: CurrentPointer = decode(v)?;
            if pointer.delete_marker {
                continue;
            }
            resp.objects.push(ListEntry {
                key,
                size: pointer.size,
                etag: pointer.etag,
                last_modified_ms: pointer.last_modified_ms,
            });
        }
        // Continue after the last key of this page.
        begin.clone_from(&resume);
    }
    Ok(resp)
}

/// ListObjectVersions request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListVersionsRequest {
    /// Only keys starting with this prefix.
    pub prefix: String,
    /// Delimiter for common prefixes.
    pub delimiter: Option<String>,
    /// Resume after this key (and version, if `version_id_marker` is set;
    /// otherwise after every version of the key).
    pub key_marker: Option<String>,
    /// Resume after this version of `key_marker`.
    pub version_id_marker: Option<String>,
    /// Maximum versions plus delete markers plus common prefixes.
    pub max_keys: usize,
}

/// One listed version (data or delete marker).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionEntry {
    /// Object key.
    pub key: String,
    /// Version id (`"null"` for the null version).
    pub version_id: String,
    /// Whether this is the current version of the key.
    pub is_latest: bool,
    /// Whether this is a delete marker.
    pub delete_marker: bool,
    /// Size (0 for markers).
    pub size: u64,
    /// ETag (empty for markers).
    pub etag: String,
    /// Commit time, Unix milliseconds.
    pub last_modified_ms: u64,
}

/// ListObjectVersions response.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListVersionsResponse {
    /// Versions in key order, newest first within a key.
    pub versions: Vec<VersionEntry>,
    /// Common prefixes.
    pub common_prefixes: Vec<String>,
    /// Whether more results exist.
    pub is_truncated: bool,
    /// `(key, version id)` of the last entry returned, for the next page.
    pub next_markers: Option<(String, String)>,
}

/// Versions of one key, newest first: the current pointer first (it may
/// lack a version record: null version, or a key that was never versioned),
/// then the version records, deduplicated by version id.
fn versions_of_key(
    key: &str,
    current: Option<&CurrentPointer>,
    records: &[VersionRecord],
) -> Vec<VersionEntry> {
    let mut out = Vec::with_capacity(records.len() + 1);
    if let Some(c) = current {
        out.push(VersionEntry {
            key: key.to_string(),
            version_id: c.version_id.clone(),
            is_latest: true,
            delete_marker: c.delete_marker,
            size: c.size,
            etag: c.etag.clone(),
            last_modified_ms: c.last_modified_ms,
        });
    }
    for v in records {
        if current.is_some_and(|c| c.version_id == v.version_id) {
            continue;
        }
        out.push(VersionEntry {
            key: key.to_string(),
            version_id: v.version_id.clone(),
            is_latest: false,
            delete_marker: v.delete_marker,
            size: v.size,
            etag: v.etag.clone(),
            last_modified_ms: v.committed_ms,
        });
    }
    let _ = NULL_VERSION_ID;
    out
}

/// ListObjectVersions (RFC-007 §5.4). Each page is one read-only
/// transaction over the current pointers and the version records of the
/// keys it covers.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`]; `max_keys == 0` is an invalid argument.
#[allow(clippy::too_many_lines)]
pub async fn list_versions<S: KvStore>(
    store: &S,
    bucket: &str,
    req: &ListVersionsRequest,
) -> Result<ListVersionsResponse> {
    if req.max_keys == 0 {
        return Err(MetaError::InvalidArgument("max_keys must be >= 1".into()));
    }
    transact(store, (bucket, req), |tx, env| {
        Box::pin(async move {
            let (bucket, req) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let full = keys::object_prefix_range(b.id, &req.prefix);
            let mut begin = full.begin.clone();
            // With a version marker the key itself is revisited (its
            // remaining versions); without one the scan resumes after it.
            let mut skip_through_version: Option<&str> = None;
            if let Some(k) = &req.key_marker {
                if req.version_id_marker.is_some() {
                    begin = begin.max(keys::object_current(b.id, k));
                    skip_through_version = req.version_id_marker.as_deref();
                } else {
                    begin = begin.max(keys::object_after(b.id, k));
                }
            }
            let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());
            let mut resp = ListVersionsResponse::default();
            let count = |r: &ListVersionsResponse| r.versions.len() + r.common_prefixes.len();
            'outer: while begin < full.end {
                let page = tx
                    .get_range(&KeyRange::new(begin.clone(), full.end.clone()), PAGE, false)
                    .await?;
                if page.is_empty() {
                    break;
                }
                for (k, v) in &page {
                    let key = keys::object_key_from(k)
                        .ok_or_else(|| MetaError::Codec("unexpected key in object range".into()))?;
                    if count(&resp) >= req.max_keys {
                        resp.is_truncated = true;
                        return Ok(resp);
                    }
                    if let Some(delim) = delimiter {
                        let tail = &key[req.prefix.len()..];
                        if let Some(pos) = tail.find(delim) {
                            let common = key[..req.prefix.len() + pos + delim.len()].to_string();
                            begin = strinc(&keys::object_prefix_range(b.id, &common).begin);
                            resp.next_markers = Some((common.clone(), String::new()));
                            resp.common_prefixes.push(common);
                            skip_through_version = None;
                            continue 'outer;
                        }
                    }
                    let pointer: CurrentPointer = decode(v)?;
                    let mut records = Vec::new();
                    for (_, rv) in tx
                        .get_range(&keys::versions_range(b.id, &key), usize::MAX, false)
                        .await?
                    {
                        records.push(decode::<VersionRecord>(&rv)?);
                    }
                    let mut entries = versions_of_key(&key, Some(&pointer), &records);
                    if let Some(marker) = skip_through_version.take()
                        && let Some(pos) = entries.iter().position(|e| e.version_id == marker)
                    {
                        entries.drain(..=pos);
                    }
                    for e in entries {
                        if count(&resp) >= req.max_keys {
                            resp.is_truncated = true;
                            return Ok(resp);
                        }
                        resp.next_markers = Some((e.key.clone(), e.version_id.clone()));
                        resp.versions.push(e);
                    }
                    begin = keys::object_after(b.id, &key);
                }
            }
            Ok(resp)
        })
    })
    .await
}
