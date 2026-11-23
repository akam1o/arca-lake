//! Stored record types and their encoding.
//!
//! Encoding: one version byte followed by `postcard`. Postcard is compact
//! but not self-describing, so any field change requires a new record
//! version and a decoder for the old one (RFC-014 §5.4). Version 1 is the
//! Phase 0–2 encoding; version 2 (Phase 3.1) adds the generation kind and
//! checksum, bucket ownership and policy, and the multipart records.
//! Writers always encode the current version; readers accept the current
//! and the previous one.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::MetaError;
use crate::kv::Value;

/// Newest record encoding version this build can write (`M_cur`).
#[allow(clippy::cast_possible_truncation)]
pub const RECORD_VERSION: u8 = 2 + arca_lake_common::compat::BUMP as u8;

/// Oldest record encoding version this build decodes (`M_cur − 1`).
pub const RECORD_VERSION_MIN: u8 = RECORD_VERSION - 1;

/// Oldest record encoding version this build can *write*: versions 2 and
/// 3 share one body, so a writer told `schema_write = 2` by a build with
/// `M_cur = 3` stamps the byte 2 (ADR-0016). A future layout change needs
/// a down-encoder before this constant may stay below `RECORD_VERSION`.
pub const RECORD_WRITE_MIN: u8 = 2;

static SCHEMA_WRITE: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(RECORD_WRITE_MIN);

/// Record encoding version of metadata schema `schema`: schemas 1 and 2
/// are record versions 1 and 2; schemas 3 (Phase 3.10, ADR-0024), 4
/// (ADR-0035) and 5 (0.2.0, ADR-0039) changed no record and keep version
/// 2, so from schema 5 on the record version is three less than the
/// schema.
#[must_use]
pub fn record_version_of_schema(schema: u64) -> u64 {
    match schema {
        0..=2 => schema,
        3..=5 => 2,
        _ => schema - 3,
    }
}

/// Tell this process the cluster's `schema_write` (RFC-014 §5.4); its
/// record version ([`record_version_of_schema`]) is clamped into
/// `[RECORD_WRITE_MIN, RECORD_VERSION]`. Called at start and on every
/// refresh; a stale value is always the older one.
pub fn set_schema_write(schema: u64) {
    let v = u8::try_from(
        record_version_of_schema(schema)
            .clamp(u64::from(RECORD_WRITE_MIN), u64::from(RECORD_VERSION)),
    )
    .unwrap_or(RECORD_WRITE_MIN);
    SCHEMA_WRITE.store(v, std::sync::atomic::Ordering::Relaxed);
}

/// The record version this process currently writes.
#[must_use]
pub fn schema_write_version() -> u8 {
    SCHEMA_WRITE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Generation identifier: a UUID (version 7, time ordered) as raw bytes.
pub type GenId = [u8; 16];

/// Multipart upload identifier: a UUID (version 7) as raw bytes; rendered
/// as 32 hex characters on the wire.
pub type UploadId = [u8; 16];

/// Allocate a new generation ID.
#[must_use]
pub fn new_gen_id() -> GenId {
    *uuid::Uuid::now_v7().as_bytes()
}

/// Allocate a new S3 version ID string.
#[must_use]
pub fn new_version_id() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// Allocate a new upload ID.
#[must_use]
pub fn new_upload_id() -> UploadId {
    *uuid::Uuid::now_v7().as_bytes()
}

/// Render an upload ID for the wire.
#[must_use]
pub fn upload_id_to_string(id: &UploadId) -> String {
    uuid::Uuid::from_bytes(*id).simple().to_string()
}

/// Parse a wire upload ID (32 hex characters).
#[must_use]
pub fn upload_id_from_str(s: &str) -> Option<UploadId> {
    if s.len() != 32 {
        return None;
    }
    uuid::Uuid::try_parse(s).ok().map(|u| *u.as_bytes())
}

/// The version ID of objects in buckets without versioning.
pub const NULL_VERSION_ID: &str = "null";

/// A stored record type: how to decode its previous encoding version.
pub trait Record: DeserializeOwned {
    /// Decode the version-1 body (the bytes after the version byte).
    ///
    /// # Errors
    ///
    /// [`MetaError::Codec`] if the bytes are not a valid version-1 record.
    fn decode_v1(body: &[u8]) -> Result<Self, MetaError> {
        postcard::from_bytes(body).map_err(|e| MetaError::Codec(e.to_string()))
    }
}

/// Encode a record.
///
/// # Errors
///
/// Returns [`MetaError::Codec`] if serialization fails.
pub fn encode<T: Serialize>(v: &T) -> Result<Value, MetaError> {
    postcard::to_extend(v, vec![schema_write_version()])
        .map_err(|e| MetaError::Codec(e.to_string()))
}

/// Decode a record of the current or the previous encoding version.
///
/// # Errors
///
/// Returns [`MetaError::Codec`] on an unknown version or malformed bytes.
pub fn decode<T: Record>(b: &[u8]) -> Result<T, MetaError> {
    match b.split_first() {
        // Versions 2 and 3 share one body (ADR-0016).
        Some((&v, rest)) if (2..=RECORD_VERSION).contains(&v) => {
            postcard::from_bytes(rest).map_err(|e| MetaError::Codec(e.to_string()))
        }
        Some((&1, rest)) if RECORD_VERSION_MIN <= 1 => T::decode_v1(rest),
        Some((v, _)) => Err(MetaError::Codec(format!(
            "unknown record version {v} (this build reads {RECORD_VERSION_MIN}..={RECORD_VERSION})"
        ))),
        None => Err(MetaError::Codec("empty record".into())),
    }
}

// ---- cluster state, heartbeats, upgrade cursor (RFC-014/015, ADR-0016) ----

/// Cluster recovery phase (RFC-015 §5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusterPhase {
    /// Normal service.
    Active,
    /// Reads and repair only; no client writes, no deletes.
    ReadOnly,
    /// Metadata restored, processes being started; nothing but recovery.
    Restoring,
    /// Epochs caught up; verification running; reads may fail with an
    /// integrity error, never with wrong bytes.
    VerificationPending,
    /// Verification found unreadable or missing data or did not complete.
    RecoveryRequired,
}

impl ClusterStateRecord {
    /// The time separating pre- and post-restore-point orphans: the
    /// backup point when known, else the mark time.
    #[must_use]
    pub fn restore_point_ms(&self) -> Option<u64> {
        self.backup_point_ms.or(self.restored_at_ms)
    }
}

impl ClusterPhase {
    /// Which operation classes this phase admits (RFC-015 §5.3 table).
    #[must_use]
    pub fn admits(self, class: crate::guard::OpClass) -> bool {
        use crate::guard::OpClass as C;
        match self {
            Self::Active => true,
            Self::ReadOnly => matches!(
                class,
                C::Repair | C::Verification | C::Admin | C::Bookkeeping
            ),
            Self::Restoring => matches!(class, C::Admin),
            Self::VerificationPending => matches!(class, C::Verification | C::Admin),
            Self::RecoveryRequired => matches!(
                class,
                C::Repair | C::Verification | C::Admin | C::Bookkeeping
            ),
        }
    }

    /// Whether clients may read (RFC-015 §5.3 table).
    #[must_use]
    pub fn admits_client_reads(self) -> bool {
        self != Self::Restoring
    }

    /// Short name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::ReadOnly => "ReadOnly",
            Self::Restoring => "Restoring",
            Self::VerificationPending => "VerificationPending",
            Self::RecoveryRequired => "RecoveryRequired",
        }
    }
}

/// `("al","meta","cluster")`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStateRecord {
    /// Changes on every restore; every writer checks it (RFC-015 §5.3).
    pub incarnation: [u8; 16],
    /// Phase.
    pub state: ClusterPhase,
    /// When the last restore was marked, Unix milliseconds.
    pub restored_at_ms: Option<u64>,
    /// Backup the metadata was restored from (operator-supplied label).
    pub restored_from: Option<String>,
    /// Consistent point of that backup, Unix milliseconds (operator
    /// supplied): shards whose chunk id is newer were written after it
    /// (post-T orphans, RFC-015 §5.3.2). `None` = unknown, and the mark
    /// time is used instead (every orphan then counts as pre-T).
    pub backup_point_ms: Option<u64>,
    /// Last change, Unix milliseconds.
    pub changed_ms: u64,
    /// Who changed it (controller id or `admin`).
    pub changed_by: String,
}

/// Kind of a heartbeating process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProcKind {
    /// S3 gateway.
    Gateway,
    /// Controller.
    Controller,
}

/// `("al","proc",id)`: a gateway or controller heartbeat (RFC-014 §5.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcRecord {
    /// Process id (controller id, or gateway listen address).
    pub id: String,
    /// Kind.
    pub kind: ProcKind,
    /// Software version string.
    pub software: String,
    /// Capabilities.
    pub caps: arca_lake_common::compat::Capabilities,
    /// Incarnation the process started under.
    pub incarnation: [u8; 16],
    /// Last heartbeat, Unix milliseconds.
    pub last_ms: u64,
}

/// Steps of `admin upgrade finalize` (RFC-014 §5.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UpgradeStep {
    /// Written by the admin command; the leader picks it up.
    Requested,
    /// Step 1 passed: every node and process reaches the target.
    Preconditioned,
    /// Step 2 done: migrations complete (none for the test bump).
    Migrated,
    /// Step 3 done: `schema_write` raised (the downgrade cut-off).
    SchemaWriteRaised,
    /// Step 4 done: `format_write` raised.
    FormatWriteRaised,
}

impl UpgradeStep {
    /// Whether rollback is no longer possible (RFC-014 §5.6).
    #[must_use]
    pub const fn past_cut_off(self) -> bool {
        matches!(self, Self::SchemaWriteRaised | Self::FormatWriteRaised)
    }
}

/// `("al","meta","upgrade")`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeCursor {
    /// Versions to finalize to.
    pub target: arca_lake_common::compat::Target,
    /// Last completed step.
    pub step: UpgradeStep,
    /// Process ids whose absence does not block the precondition.
    pub ignore_absent: Vec<String>,
    /// When requested, Unix milliseconds.
    pub started_ms: u64,
    /// Last change.
    pub updated_ms: u64,
    /// Who requested.
    pub requested_by: String,
    /// Fencing token of the leader that took the last step.
    pub by_token: u64,
    /// Why the next step is blocked, if it is.
    pub last_error: Option<String>,
    /// Audit trail of steps.
    pub history: Vec<String>,
}

macro_rules! unchanged_since_v1 {
    ($($t:ty),* $(,)?) => { $(impl Record for $t {})* };
}

unchanged_since_v1!(ClusterStateRecord, ProcRecord, UpgradeCursor);

/// Bucket versioning state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Versioning {
    /// Never enabled. Overwrites replace the single `null` version.
    Disabled,
    /// Every write creates a new version; deletes create delete markers.
    Enabled,
    /// Writes replace the `null` version; earlier real versions are kept.
    Suspended,
}

/// Bucket configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRecord {
    /// Internal bucket ID.
    pub id: u64,
    /// Bucket name.
    pub name: String,
    /// Versioning state.
    pub versioning: Versioning,
    /// Creation time, Unix milliseconds.
    pub created_ms: u64,
    /// Owning principal (RFC-012 §5.3). Empty for buckets created before
    /// ownership existed: nobody but the operator can grant access.
    pub owner: String,
    /// Bucket policy document as registered (validated, RFC-012 §5.3.3).
    pub policy: Option<String>,
}

#[derive(Deserialize)]
struct BucketRecordV1 {
    id: u64,
    name: String,
    versioning: Versioning,
    created_ms: u64,
}

impl Record for BucketRecord {
    fn decode_v1(body: &[u8]) -> Result<Self, MetaError> {
        let v: BucketRecordV1 =
            postcard::from_bytes(body).map_err(|e| MetaError::Codec(e.to_string()))?;
        Ok(Self {
            id: v.id,
            name: v.name,
            versioning: v.versioning,
            created_ms: v.created_ms,
            owner: String::new(),
            policy: None,
        })
    }
}

/// The current version of an object key. Denormalizes what listing needs so
/// that `ListObjectsV2` does not read a header per key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentPointer {
    /// Generation holding the data (meaningless for delete markers).
    pub generation: GenId,
    /// S3 version ID (`null` when versioning is not enabled).
    pub version_id: String,
    /// Whether this current version is a delete marker.
    pub delete_marker: bool,
    /// Ordinal to use for the next version record of this key.
    pub next_ordinal: u64,
    /// Object size in bytes.
    pub size: u64,
    /// ETag as returned to clients.
    pub etag: String,
    /// Last-modified time, Unix milliseconds.
    pub last_modified_ms: u64,
}

/// One entry in a key's version history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRecord {
    /// Generation (meaningless for delete markers).
    pub generation: GenId,
    /// S3 version ID.
    pub version_id: String,
    /// Whether this version is a delete marker.
    pub delete_marker: bool,
    /// Commit time, Unix milliseconds.
    pub committed_ms: u64,
    /// Object size.
    pub size: u64,
    /// ETag.
    pub etag: String,
}

/// Lifecycle state of a generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenState {
    /// Being written. Reclaimable once the lease expires.
    Pending {
        /// Lease expiry, Unix milliseconds.
        lease_until_ms: u64,
    },
    /// Referenced by a current pointer or a version record.
    Committed,
    /// Unreferenced; chunks may be deleted after the grace period.
    Retiring {
        /// When it became unreferenced, Unix milliseconds.
        since_ms: u64,
    },
}

impl GenState {
    /// Short name for error messages.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Pending { .. } => "pending",
            Self::Committed => "committed",
            Self::Retiring { .. } => "retiring",
        }
    }
}

/// Checksum algorithms (`x-amz-checksum-*`, RFC-007 §5.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    /// CRC-32 (IEEE).
    Crc32,
    /// CRC-32C (Castagnoli).
    Crc32c,
    /// CRC-64/NVME.
    Crc64Nvme,
    /// SHA-1.
    Sha1,
    /// SHA-256.
    Sha256,
}

impl ChecksumAlgorithm {
    /// AWS header name suffix (`x-amz-checksum-<name>`), upper case as in
    /// the `ChecksumAlgorithm` API field.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
        }
    }

    /// Parse the API name (case-insensitive).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "CRC32" => Some(Self::Crc32),
            "CRC32C" => Some(Self::Crc32c),
            "CRC64NVME" => Some(Self::Crc64Nvme),
            "SHA1" => Some(Self::Sha1),
            "SHA256" => Some(Self::Sha256),
            _ => None,
        }
    }

    /// Length of a checksum value in bytes.
    #[must_use]
    pub const fn byte_len(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64Nvme => 8,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }

    /// Whether checksums of parts can be combined into a full-object
    /// checksum (CRCs can, digests cannot).
    #[must_use]
    pub const fn combinable(self) -> bool {
        matches!(self, Self::Crc32 | Self::Crc32c | Self::Crc64Nvme)
    }
}

/// Whether a checksum covers the whole object or is a checksum of the
/// parts' checksums (`x-amz-checksum-type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChecksumType {
    /// One checksum over all bytes.
    FullObject,
    /// Checksum of the concatenated part checksums, suffixed `-N`.
    Composite,
}

impl ChecksumType {
    /// API name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::FullObject => "FULL_OBJECT",
            Self::Composite => "COMPOSITE",
        }
    }

    /// Parse the API name (case-insensitive).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "FULL_OBJECT" => Some(Self::FullObject),
            "COMPOSITE" => Some(Self::Composite),
            _ => None,
        }
    }
}

/// A stored checksum.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Checksum {
    /// Algorithm.
    pub algorithm: ChecksumAlgorithm,
    /// Full-object or composite.
    pub kind: ChecksumType,
    /// Raw value (`algorithm.len()` bytes).
    pub value: Vec<u8>,
    /// Number of parts for a composite checksum (`-N` suffix).
    pub parts: Option<u32>,
}

/// What a generation is (RFC-007 §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenKind {
    /// A whole object with inline chunk records.
    Object,
    /// One part of a multipart upload, with inline chunk records.
    Part {
        /// The upload.
        upload_id: UploadId,
        /// Part number (1–10 000).
        part_number: u32,
    },
    /// A completed multipart object: no chunk records of its own; part
    /// manifest references `("al","g",root,"m",n)` instead.
    MultipartRoot {
        /// The upload it completed (its record is removed when the root is
        /// finished by GC).
        upload_id: UploadId,
        /// Number of part references.
        part_count: u32,
    },
}

/// One immutable generation of an object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationHeader {
    /// Lifecycle state.
    pub state: GenState,
    /// Owning bucket.
    pub bucket_id: u64,
    /// Object key.
    pub key: String,
    /// Total size in bytes.
    pub size: u64,
    /// ETag.
    pub etag: String,
    /// Content type, if set.
    pub content_type: Option<String>,
    /// User metadata (`x-amz-meta-*`), in request order.
    pub user_metadata: Vec<(String, String)>,
    /// Number of chunk records.
    pub chunk_count: u32,
    /// Creation time, Unix milliseconds.
    pub created_ms: u64,
    /// Kind (object, part, multipart root).
    pub kind: GenKind,
    /// Stored checksum, if the client supplied one.
    pub checksum: Option<Checksum>,
}

#[derive(Deserialize)]
struct GenerationHeaderV1 {
    state: GenState,
    bucket_id: u64,
    key: String,
    size: u64,
    etag: String,
    content_type: Option<String>,
    user_metadata: Vec<(String, String)>,
    chunk_count: u32,
    created_ms: u64,
}

impl Record for GenerationHeader {
    fn decode_v1(body: &[u8]) -> Result<Self, MetaError> {
        let v: GenerationHeaderV1 =
            postcard::from_bytes(body).map_err(|e| MetaError::Codec(e.to_string()))?;
        Ok(Self {
            state: v.state,
            bucket_id: v.bucket_id,
            key: v.key,
            size: v.size,
            etag: v.etag,
            content_type: v.content_type,
            user_metadata: v.user_metadata,
            chunk_count: v.chunk_count,
            created_ms: v.created_ms,
            kind: GenKind::Object,
            checksum: None,
        })
    }
}

/// State of a multipart upload (RFC-007 §5.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultipartState {
    /// Accepting parts.
    Open,
    /// Abort in progress: parts are being retired in batches.
    Aborting,
    /// Completed: the object generation is `root`.
    Completed {
        /// The multipart root generation.
        root: GenId,
    },
}

/// A multipart upload (`("al","mpu",bucket_id,key,upload_id)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartRecord {
    /// Upload ID.
    pub upload_id: UploadId,
    /// Bucket.
    pub bucket_id: u64,
    /// Object key.
    pub key: String,
    /// Initiation time, Unix milliseconds.
    pub initiated_ms: u64,
    /// Initiating principal.
    pub owner: String,
    /// Content type for the completed object.
    pub content_type: Option<String>,
    /// User metadata for the completed object.
    pub user_metadata: Vec<(String, String)>,
    /// Checksum algorithm announced at creation, if any.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Checksum type announced at creation, if any.
    pub checksum_type: Option<ChecksumType>,
    /// State.
    pub state: MultipartState,
}

/// A sealed part of an open upload (`("al","mpp",upload_id,part_number)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRecord {
    /// The part generation.
    pub generation: GenId,
    /// Part size.
    pub size: u64,
    /// MD5 ETag of the part.
    pub etag: String,
    /// Part checksum, if supplied.
    pub checksum: Option<Checksum>,
    /// Chunk records of the part generation.
    pub chunk_count: u32,
    /// Seal time, Unix milliseconds.
    pub uploaded_ms: u64,
}

/// A part manifest reference of a multipart root
/// (`("al","g",root,"m",part_number)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRef {
    /// The part generation (immutable, holds the chunk records).
    pub generation: GenId,
    /// Part number.
    pub part_number: u32,
    /// Byte offset of the part within the object.
    pub offset: u64,
    /// Part size.
    pub size: u64,
    /// Chunk records of the part.
    pub chunk_count: u32,
    /// MD5 ETag of the part.
    pub etag: String,
    /// Part checksum, if any.
    pub checksum: Option<Checksum>,
}

/// Where a chunk's durable copy lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurablePlacement {
    /// Stripe group.
    pub group_id: u64,
    /// Group epoch at write time.
    pub epoch: u64,
}

/// One chunk of a generation: manifest entry plus placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRecord {
    /// Cluster-unique chunk ID.
    pub chunk_id: [u8; 16],
    /// Byte offset within the object.
    pub offset: u64,
    /// Length in bytes.
    pub len: u32,
    /// Erasure coding scheme identifier (see `arca-lake-ec`).
    pub scheme: String,
    /// Data shards.
    pub data_shards: u16,
    /// Parity shards.
    pub parity_shards: u16,
    /// CRC-32C of the chunk's bytes.
    pub crc32c: u32,
    /// Durable placement.
    pub placement: DurablePlacement,
}

/// A discardable cache copy of a chunk on a cache tier (RFC-006 §5.2).
/// Never consulted for durability or visibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachePlacement {
    /// Cache tier name.
    pub tier: String,
    /// Cache group (scheme `cache-v1`, one member holding the whole chunk).
    pub group_id: u64,
    /// Group epoch at write time.
    pub epoch: u64,
    /// Bytes stored on the tier for this copy: the chunk length for an
    /// unprotected cache group, the sum of all shards for a replicated or
    /// coded one (RFC-006 §5.9). Counted in the tier usage.
    pub bytes: u64,
    /// Pinned entries are never evicted.
    pub pinned: bool,
    /// Last hit or population time, Unix milliseconds (recency index key).
    pub last_access_ms: u64,
    /// Population time, Unix milliseconds.
    pub created_ms: u64,
}

/// The delete token of one shard of a cache copy (R-73): the record
/// sequence and store epoch the node returned for the write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheShardToken {
    /// Member (shard) index in the cache group.
    pub index: u16,
    /// Node store epoch.
    pub store_epoch: u64,
    /// Record sequence (0: the node gave none, an older node).
    pub sequence: u64,
}

/// `("al","g",generation,"xt",index)`: the delete tokens of a cache copy
/// (Phase 3.6, R-73), next to its placement and removed with it, so that
/// a delete removes only the shards this copy wrote and never a newer
/// copy under the same key. A placement without tokens (written before)
/// is deleted unconditionally, as before.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheCopyTokens {
    /// Group the tokens belong to.
    pub group_id: u64,
    /// Per shard written.
    pub shards: Vec<CacheShardToken>,
}

impl CacheCopyTokens {
    /// The token of member `index`, if recorded.
    #[must_use]
    pub fn of(&self, index: u16) -> Option<CacheShardToken> {
        self.shards.iter().find(|t| t.index == index).copied()
    }

    /// Replace or add the tokens of `updates`.
    pub fn merge(&mut self, updates: &[CacheShardToken]) {
        for u in updates {
            match self.shards.iter_mut().find(|t| t.index == u.index) {
                Some(t) => *t = *u,
                None => self.shards.push(*u),
            }
        }
        self.shards.sort_by_key(|t| t.index);
    }
}

impl Record for CacheCopyTokens {}

/// A registered storage node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// Node ID.
    pub id: u64,
    /// gRPC address (`host:port`).
    pub address: String,
    /// Topology labels (`host`, `rack`, ...).
    pub labels: Vec<(String, String)>,
    /// Device IDs served by the node.
    pub devices: Vec<u32>,
    /// First registration, Unix milliseconds.
    pub registered_ms: u64,
    /// Last registration or heartbeat, Unix milliseconds.
    pub last_seen_ms: u64,
}

/// One member of a stripe group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupMember {
    /// Node.
    pub node_id: u64,
    /// Device within the node.
    pub device_id: u32,
}

/// Stripe group state (RFC-004 §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    /// Accepts new chunks.
    Active,
    /// No new chunks; existing chunks readable (draining).
    Sealed,
    /// A member is unhealthy; reads decode; writes avoid the group.
    Degraded,
    /// A member was replaced and is being reconstructed.
    Repairing,
}

/// Progress of a member repair: the last chunk ID (in sorted order) whose
/// shard was written to the member being rebuilt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairCursor {
    /// Member index being rebuilt.
    pub member: u16,
    /// Last chunk ID processed.
    pub chunk_id: [u8; 16],
}

/// What a group's shards mean (RFC-006 §5.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupRole {
    /// Durable placement: the only basis for durability.
    Durable,
    /// Discardable cache copies, whatever the scheme.
    Cache,
}

/// A stripe group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
    /// Group ID (assigned on creation).
    pub id: u64,
    /// EC profile name.
    pub profile: String,
    /// Tier name.
    pub tier: String,
    /// Scheme identifier.
    pub scheme: String,
    /// `k`.
    pub data_shards: u16,
    /// `m`.
    pub parity_shards: u16,
    /// Members in shard order (`k + m`).
    pub members: Vec<GroupMember>,
    /// Placement epoch (assigned 1 on creation; bumped on membership change).
    pub epoch: u64,
    /// State.
    pub state: GroupState,
    /// Repair progress, if a repair is in progress.
    pub repair_cursor: Option<RepairCursor>,
    /// Durable or cache.
    pub role: GroupRole,
}

impl GroupRecord {
    /// Whether this group holds discardable cache copies.
    #[must_use]
    pub fn is_cache(&self) -> bool {
        self.role == GroupRole::Cache
    }
}

unchanged_since_v1!(
    CurrentPointer,
    VersionRecord,
    ChunkRecord,
    CachePlacement,
    NodeRecord,
    GroupRecord,
    MultipartRecord,
    PartRecord,
    PartRef,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_1_records_decode_with_defaults() {
        let h1 = GenerationHeaderV1Enc {
            state: GenState::Committed,
            bucket_id: 1,
            key: "k".into(),
            size: 3,
            etag: "e".into(),
            content_type: Some("t".into()),
            user_metadata: vec![],
            chunk_count: 1,
            created_ms: 9,
        };
        let bytes = postcard::to_extend(&h1, vec![1u8]).unwrap();
        if RECORD_VERSION_MIN > 1 {
            // A version-bump build reads [M_cur - 1, M_cur] = [2, 3]: a
            // version-1 record is outside its window and refused.
            assert!(decode::<GenerationHeader>(&bytes).is_err());
            return;
        }
        let h = decode::<GenerationHeader>(&bytes).unwrap();
        assert_eq!(h.kind, GenKind::Object);
        assert_eq!(h.checksum, None);
        assert_eq!(h.etag, "e");
        let b1 = BucketRecordV1Enc {
            id: 5,
            name: "b".into(),
            versioning: Versioning::Enabled,
            created_ms: 1,
        };
        let bytes = postcard::to_extend(&b1, vec![1u8]).unwrap();
        let b = decode::<BucketRecord>(&bytes).unwrap();
        assert_eq!(b.owner, "");
        assert_eq!(b.policy, None);
        assert_eq!(b.versioning, Versioning::Enabled);
        // Unchanged records: version 1 and 2 bodies are identical.
        let c = ChunkRecord {
            chunk_id: [1; 16],
            offset: 0,
            len: 1,
            scheme: "s".into(),
            data_shards: 1,
            parity_shards: 1,
            crc32c: 0,
            placement: DurablePlacement {
                group_id: 1,
                epoch: 1,
            },
        };
        let mut v1 = encode(&c).unwrap();
        v1[0] = 1;
        assert_eq!(decode::<ChunkRecord>(&v1).unwrap(), c);
    }

    #[derive(Serialize)]
    struct GenerationHeaderV1Enc {
        state: GenState,
        bucket_id: u64,
        key: String,
        size: u64,
        etag: String,
        content_type: Option<String>,
        user_metadata: Vec<(String, String)>,
        chunk_count: u32,
        created_ms: u64,
    }

    #[derive(Serialize)]
    struct BucketRecordV1Enc {
        id: u64,
        name: String,
        versioning: Versioning,
        created_ms: u64,
    }

    #[test]
    fn upload_ids_roundtrip() {
        let u = new_upload_id();
        let s = upload_id_to_string(&u);
        assert_eq!(s.len(), 32);
        assert_eq!(upload_id_from_str(&s), Some(u));
        assert_eq!(upload_id_from_str("nope"), None);
    }

    #[test]
    fn roundtrip_and_version_check() {
        let h = GenerationHeader {
            state: GenState::Pending { lease_until_ms: 5 },
            bucket_id: 1,
            key: "k".into(),
            size: 0,
            etag: String::new(),
            content_type: None,
            user_metadata: vec![("a".into(), "b".into())],
            chunk_count: 0,
            created_ms: 1,
            kind: GenKind::Object,
            checksum: None,
        };
        let b = encode(&h).unwrap();
        // Records are stamped with the cluster's schema_write (RFC-014
        // §5.4), never above this build's newest version.
        assert_eq!(b[0], schema_write_version());
        assert!(b[0] <= RECORD_VERSION);
        assert_eq!(decode::<GenerationHeader>(&b).unwrap(), h);
        assert!(decode::<GenerationHeader>(&[9, 0]).is_err());
        assert!(decode::<GenerationHeader>(&[]).is_err());
    }

    /// Schema 5 (ADR-0039) changes no record: 0.1.0 (schema 4) and 0.2.0
    /// write the same record version, so records written either side of
    /// the finalize read back on both.
    #[test]
    fn schemas_4_and_5_share_record_version_2() {
        assert_eq!(record_version_of_schema(3), 2);
        assert_eq!(record_version_of_schema(4), 2);
        assert_eq!(record_version_of_schema(crate::keys::SCHEMA_V5), 2);
        assert_eq!(record_version_of_schema(6), 3);
        assert_eq!(
            record_version_of_schema(crate::keys::SCHEMA_VERSION),
            u64::from(RECORD_VERSION)
        );
    }
}
