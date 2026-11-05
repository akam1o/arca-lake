//! Authentication, authorization and audit (RFC-012 §5.1–5.3, §5.7).
//!
//! - [`CredentialStore`]: access keys mapped to principals, loaded from a
//!   TOML file (or given programmatically), reloadable without restart;
//!   implements `s3s::auth::S3Auth` so that SigV4 verification stays in
//!   `s3s`.
//! - [`Authorizer`]: `s3s::access::S3Access` implementing the nine rules of
//!   RFC-012 §5.3.2 over the bucket owner and the bucket policy subset.
//! - [`AuditRecord`]: one structured record per request on the tracing
//!   target `arca.audit`, filled by the authorizer and the HTTP layer and
//!   emitted when the response is complete. Never contains secrets.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::bucket;
use arca_lake_metadata::records::BucketRecord;
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Error, S3Result, s3_error};
use serde::Deserialize;

use crate::policy::{self, Decision, Policy, Request as PolicyRequest};

// ---- credential store -----------------------------------------------------

/// Credentials file (RFC-012 §5.2).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialsFile {
    #[serde(default)]
    principal: Vec<PrincipalDef>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalDef {
    name: String,
    #[serde(default)]
    key: Vec<KeyDef>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyDef {
    id: String,
    secret: Option<String>,
    secret_file: Option<PathBuf>,
    #[serde(default = "default_state")]
    state: String,
    not_after: Option<String>,
}

fn default_state() -> String {
    "active".into()
}

/// One access key.
#[derive(Clone, Debug)]
struct KeyEntry {
    principal: String,
    secret: SecretKey,
    active: bool,
    not_after: Option<SystemTime>,
}

#[derive(Default)]
struct KeySet {
    by_id: HashMap<String, KeyEntry>,
}

/// Why a key is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRefusal {
    /// No such access key id.
    Unknown,
    /// The key is marked inactive.
    Inactive,
    /// The key's `not_after` has passed.
    Expired,
}

/// Access keys mapped to principals, reloadable.
pub struct CredentialStore {
    keys: RwLock<Arc<KeySet>>,
    path: Option<PathBuf>,
    /// What the loaded key set was read from: the credentials file and
    /// every secret file it references, with a digest of their contents.
    loaded: Mutex<Option<Stamp>>,
    /// Failed reloads (`arca_gateway_credentials_reload_failures_total`).
    pub reload_failures: std::sync::atomic::AtomicU64,
    /// Refused access keys by [`KeyRefusal`] (unknown, inactive, expired;
    /// `arca_gateway_auth_failures_total{reason}`).
    pub refusals: [std::sync::atomic::AtomicU64; 3],
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for CredentialStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialStore")
            .field("path", &self.path)
            .field("keys", &self.keys.read().map_or(0, |k| k.by_id.len()))
            .finish()
    }
}

impl CredentialStore {
    /// A store with the given `(access key, secret, principal)` triples.
    #[must_use]
    pub fn from_keys(keys: &[(&str, &str, &str)]) -> Self {
        let mut set = KeySet::default();
        for (id, secret, principal) in keys {
            set.by_id.insert(
                (*id).to_string(),
                KeyEntry {
                    principal: (*principal).to_string(),
                    secret: SecretKey::from((*secret).to_string()),
                    active: true,
                    not_after: None,
                },
            );
        }
        Self {
            keys: RwLock::new(Arc::new(set)),
            path: None,
            loaded: Mutex::new(None),
            reload_failures: std::sync::atomic::AtomicU64::new(0),
            refusals: Default::default(),
        }
    }

    /// Load a credentials file. Refuses a world-readable file.
    ///
    /// # Errors
    ///
    /// A human-readable message (never containing a secret).
    pub fn load(path: &Path) -> Result<Self, String> {
        let (set, stamp) = Self::read_file(path)?;
        Ok(Self {
            keys: RwLock::new(Arc::new(set)),
            path: Some(path.to_path_buf()),
            loaded: Mutex::new(Some(stamp)),
            reload_failures: std::sync::atomic::AtomicU64::new(0),
            refusals: Default::default(),
        })
    }

    fn read_file(path: &Path) -> Result<(KeySet, Stamp), String> {
        let meta = std::fs::metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o004 != 0 {
                return Err(format!(
                    "{}: refusing a world-readable credentials file (mode {mode:o})",
                    path.display()
                ));
            }
            if mode & 0o040 != 0 {
                tracing::warn!(path = %path.display(), "credentials file is group-readable");
            }
        }
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut files = vec![path.to_path_buf()];
        let file: CredentialsFile =
            toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut set = KeySet::default();
        for p in file.principal {
            if p.name.is_empty() || p.name == policy::ANONYMOUS || p.name.contains('*') {
                return Err(format!("invalid principal name '{}'", p.name));
            }
            for k in p.key {
                let secret = match (k.secret, k.secret_file) {
                    (Some(s), None) => s,
                    (None, Some(f)) => {
                        let f = if f.is_absolute() {
                            f
                        } else {
                            path.parent().unwrap_or(Path::new(".")).join(f)
                        };
                        let secret = std::fs::read_to_string(&f)
                            .map_err(|e| format!("{}: {e}", f.display()))?
                            .trim()
                            .to_string();
                        files.push(f);
                        secret
                    }
                    _ => {
                        return Err(format!(
                            "key '{}' of '{}': exactly one of secret or secret_file is required",
                            k.id, p.name
                        ));
                    }
                };
                if k.id.is_empty() || secret.is_empty() {
                    return Err(format!("key of '{}': empty id or secret", p.name));
                }
                let active = match k.state.as_str() {
                    "active" => true,
                    "inactive" => false,
                    other => return Err(format!("key '{}': unknown state '{other}'", k.id)),
                };
                let not_after = match k.not_after {
                    None => None,
                    Some(s) => Some(parse_rfc3339(&s).ok_or_else(|| {
                        format!("key '{}': not_after must be an RFC 3339 time", k.id)
                    })?),
                };
                if set
                    .by_id
                    .insert(
                        k.id.clone(),
                        KeyEntry {
                            principal: p.name.clone(),
                            secret: SecretKey::from(secret),
                            active,
                            not_after,
                        },
                    )
                    .is_some()
                {
                    return Err(format!("duplicate access key id '{}'", k.id));
                }
            }
        }
        let digest = Stamp::digest_of(&files);
        Ok((set, Stamp { files, digest }))
    }

    /// Reload the file if it changed. Returns whether a new key set is in
    /// place. A file that fails to load keeps the previous set and counts a
    /// failure (RFC-012 §7).
    pub fn reload_if_changed(&self) -> bool {
        let Some(path) = &self.path else {
            return false;
        };
        // The credentials file or any secret file it references changed
        // (Phase 3.5 SEC-7: rotating only a secret file was not noticed).
        let unchanged = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|st| Stamp::digest_of(&st.files) == st.digest);
        if unchanged {
            return false;
        }
        match Self::read_file(path) {
            Ok((set, stamp)) => {
                *self
                    .keys
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(set);
                *self
                    .loaded
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stamp);
                tracing::info!(path = %path.display(), "credentials reloaded");
                true
            }
            Err(e) => {
                self.reload_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(path = %path.display(), error = %e, "credentials reload failed; keeping the previous keys");
                false
            }
        }
    }

    /// Periodically call [`Self::reload_if_changed`].
    pub fn spawn_reloader(
        self: &Arc<Self>,
        every: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let store = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                store.reload_if_changed();
            }
        })
    }

    fn lookup(&self, access_key: &str, now: SystemTime) -> Result<KeyEntry, KeyRefusal> {
        let set = Arc::clone(
            &self
                .keys
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let k = set.by_id.get(access_key).ok_or(KeyRefusal::Unknown)?;
        if !k.active {
            return Err(KeyRefusal::Inactive);
        }
        if k.not_after.is_some_and(|t| now >= t) {
            return Err(KeyRefusal::Expired);
        }
        Ok(k.clone())
    }

    /// Principal of an active access key.
    #[must_use]
    pub fn principal_of(&self, access_key: &str) -> Option<String> {
        self.lookup(access_key, SystemTime::now())
            .ok()
            .map(|k| k.principal)
    }

    /// Number of keys loaded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.read().map_or(0, |k| k.by_id.len())
    }

    /// Whether no key is loaded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Parse `YYYY-MM-DDTHH:MM:SSZ` (UTC only; enough for a rotation deadline).
#[allow(clippy::many_single_char_names)]
fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    let s = s.trim().strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-').map(|x| x.parse::<i64>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let mut t = time.split(':').map(|x| x.parse::<i64>().ok());
    let (h, mi, sec) = (t.next()??, t.next()??, t.next()??);
    if !(1..=12).contains(&m) || !(1..=31).contains(&day) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    // Days from civil (Howard Hinnant's algorithm).
    let (y2, m2) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let doy = (153 * m2 + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec;
    let secs = u64::try_from(secs).ok()?;
    Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs))
}

/// `S3Auth` over a shared [`CredentialStore`].
#[derive(Debug, Clone)]
pub struct StoreAuth(pub Arc<CredentialStore>);

#[async_trait::async_trait]
impl S3Auth for StoreAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.0.lookup(access_key, SystemTime::now()) {
            Ok(k) => Ok(k.secret),
            Err(refusal) => {
                tracing::debug!(?refusal, "access key refused");
                let i = match refusal {
                    KeyRefusal::Unknown => 0,
                    KeyRefusal::Inactive => 1,
                    KeyRefusal::Expired => 2,
                };
                self.0.refusals[i].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(s3_error!(InvalidAccessKeyId))
            }
        }
    }
}

// ---- audit ---------------------------------------------------------------

/// Fields of one audit record; shared between the HTTP layer (request id,
/// source, status, duration) and the authorizer (principal, action,
/// decision). Emitted once per request on target `arca.audit`.
#[derive(Debug, Default, Clone)]
pub struct AuditFields {
    /// Request id (also `x-amz-request-id`).
    pub request_id: String,
    /// Principal name or `anonymous`.
    pub principal: String,
    /// Access key id used (never the secret).
    pub key_id: Option<String>,
    /// Client address.
    pub source: Option<SocketAddr>,
    /// S3 operation name.
    pub operation: String,
    /// Policy action evaluated, if any.
    pub action: Option<String>,
    /// Bucket.
    pub bucket: Option<String>,
    /// Object key.
    pub key: Option<String>,
    /// `owner`, `allow:<sid>`, `deny:<sid>`, `default-deny`, `owner-override`, `anonymous-deny`, `error-deny`.
    pub decision: Option<String>,
}

/// Audit record handle placed in the request extensions by the HTTP layer.
#[derive(Debug, Clone, Default)]
pub struct AuditRecord(pub Arc<Mutex<AuditFields>>);

impl AuditRecord {
    /// A new record with a fresh request id.
    #[must_use]
    pub fn new(source: Option<SocketAddr>) -> Self {
        let f = AuditFields {
            request_id: uuid_v7_hex(),
            source,
            ..Default::default()
        };
        Self(Arc::new(Mutex::new(f)))
    }

    /// Mutate the fields.
    pub fn with(&self, f: impl FnOnce(&mut AuditFields)) {
        if let Ok(mut g) = self.0.lock() {
            f(&mut g);
        }
    }

    /// Snapshot.
    #[must_use]
    pub fn fields(&self) -> AuditFields {
        self.0.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Emit the record. `status` and `duration` come from the HTTP layer.
    pub fn emit(&self, status: u16, bytes_in: u64, bytes_out: u64, duration_ms: u128) {
        let f = self.fields();
        tracing::info!(
            target: "arca.audit",
            request_id = %f.request_id,
            principal = %f.principal,
            key_id = f.key_id.as_deref().unwrap_or(""),
            source = %f.source.map(|s| s.ip().to_string()).unwrap_or_default(),
            operation = %f.operation,
            action = f.action.as_deref().unwrap_or(""),
            bucket = f.bucket.as_deref().unwrap_or(""),
            key = f.key.as_deref().unwrap_or(""),
            decision = f.decision.as_deref().unwrap_or(""),
            status,
            bytes_in,
            bytes_out,
            duration_ms = u64::try_from(duration_ms).unwrap_or(u64::MAX),
            "s3"
        );
    }
}

fn uuid_v7_hex() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// The principal of a request (inserted into the request extensions by
/// the authorizer; read by the backend for ownership and listing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal(pub String);

/// Per-entry authorization of a request whose objects are named in its
/// body (`DeleteObjects`): `(key, version id)` → allowed. Inserted into the
/// request's extensions by the authorizer; a backend must consult it before
/// acting on each entry (Phase 3.5 audit SEC-1: the request itself names no
/// key, so object- and version-scoped policies did not apply).
#[derive(Clone)]
pub struct EntryAuthorizer(pub Arc<EntryCheck>);

/// `(key, version id)` → allowed.
pub type EntryCheck = dyn Fn(&str, Option<&str>) -> bool + Send + Sync;

impl std::fmt::Debug for EntryAuthorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EntryAuthorizer")
    }
}

// ---- authorizer ----------------------------------------------------------

/// Who may create buckets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketCreation {
    /// Any authenticated principal.
    Authenticated,
    /// Nobody (operator tooling only).
    Deny,
    /// The listed principals.
    Principals(Vec<String>),
}

/// Policy action of an S3 operation, and whether it is object-scoped.
#[must_use]
pub fn action_of(op: &str, has_version: bool) -> Option<&'static str> {
    Some(match op {
        "GetObject" | "HeadObject" | "GetObjectAttributes" => {
            if has_version {
                "s3:GetObjectVersion"
            } else {
                "s3:GetObject"
            }
        }
        "PutObject"
        | "CopyObject"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "UploadPartCopy"
        | "CompleteMultipartUpload" => "s3:PutObject",
        "DeleteObject" | "DeleteObjects" => {
            if has_version {
                "s3:DeleteObjectVersion"
            } else {
                "s3:DeleteObject"
            }
        }
        "ListObjects" | "ListObjectsV2" | "HeadBucket" => "s3:ListBucket",
        "ListObjectVersions" => "s3:ListBucketVersions",
        "ListMultipartUploads" => "s3:ListBucketMultipartUploads",
        "ListParts" => "s3:ListMultipartUploadParts",
        "AbortMultipartUpload" => "s3:AbortMultipartUpload",
        "GetBucketVersioning" => "s3:GetBucketVersioning",
        "PutBucketVersioning" => "s3:PutBucketVersioning",
        "GetBucketPolicy" => "s3:GetBucketPolicy",
        "PutBucketPolicy" => "s3:PutBucketPolicy",
        "DeleteBucketPolicy" => "s3:DeleteBucketPolicy",
        "GetBucketLocation" => "s3:GetBucketLocation",
        "DeleteBucket" => "s3:DeleteBucket",
        _ => return None,
    })
}

/// Actions the bucket owner keeps even under an explicit Deny (rule 4).
const OWNER_KEEPS: &[&str] = &[
    "s3:PutBucketPolicy",
    "s3:DeleteBucketPolicy",
    "s3:GetBucketPolicy",
];

/// `S3Access` implementing RFC-012 §5.3.2.
pub struct Authorizer<S> {
    meta: Arc<S>,
    store: Arc<CredentialStore>,
    bucket_creation: BucketCreation,
    /// Parsed policies by (bucket id, document hash), bounded.
    cache: Mutex<HashMap<u64, (u64, Arc<Policy>)>>,
    /// Peers whose `x-forwarded-proto` is believed (a TLS-terminating
    /// proxy that sets the header itself). Empty: the header is ignored.
    trusted_proxies: Vec<IpAddr>,
}

impl<S> std::fmt::Debug for Authorizer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authorizer").finish_non_exhaustive()
    }
}

/// Outcome of an authorization decision (also used by tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authz {
    /// Allowed; the string names the reason for the audit record.
    Allow(String),
    /// Denied; the string names the reason.
    Deny(String),
}

fn hash_str(s: &str) -> u64 {
    use std::hash::Hasher as _;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    h.write(s.as_bytes());
    h.finish()
}

impl<S: KvStore + 'static> Authorizer<S> {
    /// A new authorizer over the metadata store and the credential store.
    #[must_use]
    pub fn new(meta: Arc<S>, store: Arc<CredentialStore>, bucket_creation: BucketCreation) -> Self {
        Self {
            meta,
            store,
            bucket_creation,
            cache: Mutex::new(HashMap::new()),
            trusted_proxies: Vec::new(),
        }
    }

    /// Believe `x-forwarded-proto: https` (`aws:SecureTransport`) only from
    /// these peers. The gateway itself serves plaintext; any client could
    /// send the header, so by default it is ignored (Phase 3.5 SEC-2).
    #[must_use]
    pub fn with_trusted_proxies(mut self, proxies: Vec<IpAddr>) -> Self {
        self.trusted_proxies = proxies;
        self
    }

    fn policy_of(&self, b: &BucketRecord) -> Result<Option<Arc<Policy>>, String> {
        let Some(doc) = &b.policy else {
            return Ok(None);
        };
        let h = hash_str(doc);
        if let Ok(c) = self.cache.lock()
            && let Some((hh, p)) = c.get(&b.id)
            && *hh == h
        {
            return Ok(Some(Arc::clone(p)));
        }
        let p = Arc::new(Policy::parse(&b.name, doc).map_err(|e| e.0)?);
        if let Ok(mut c) = self.cache.lock() {
            if c.len() > 4096 {
                c.clear();
            }
            c.insert(b.id, (h, Arc::clone(&p)));
        }
        Ok(Some(p))
    }

    /// Decide a request. Pure given the bucket record (rules 1–7).
    ///
    /// # Errors
    ///
    /// Never: evaluation errors are denials (rule 6).
    #[must_use]
    pub fn decide(
        &self,
        principal: &str,
        action: &str,
        bucket: Option<&BucketRecord>,
        req: &PolicyRequest<'_>,
    ) -> Authz {
        let Some(b) = bucket else {
            return Authz::Deny("no-such-bucket".into());
        };
        decide_with(principal, action, b, &self.policy_of(b), req)
    }

    /// The credential store.
    #[must_use]
    pub fn store(&self) -> &Arc<CredentialStore> {
        &self.store
    }
}

/// Percent-decode a query value (`+` is not a space in S3 query strings).
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (
                hex_val(b.get(i + 1).copied()),
                hex_val(b.get(i + 2).copied()),
            )
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(c: Option<u8>) -> Option<u8> {
    match c? {
        c @ b'0'..=b'9' => Some(c - b'0'),
        c @ b'a'..=b'f' => Some(c - b'a' + 10),
        c @ b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// `(bucket, key, version id)` of an `x-amz-copy-source` header
/// (`/bucket/key?versionId=v` or `bucket/key`, percent-encoded).
#[must_use]
pub fn parse_copy_source(src: &str) -> Option<(String, String, Option<String>)> {
    let (path, query) = src
        .split_once('?')
        .map_or((src, None), |(p, q)| (p, Some(q)));
    let path = path.strip_prefix('/').unwrap_or(path);
    let (bucket, key) = path.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    let version = query.and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (k == "versionId").then(|| percent_decode(v))
        })
    });
    Some((percent_decode(bucket), percent_decode(key), version))
}

fn query_param(uri: &hyper::Uri, name: &str) -> Option<String> {
    uri.query()?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == name).then(|| percent_decode(v))
    })
}

#[async_trait::async_trait]
impl<S: KvStore + 'static> S3Access for Authorizer<S> {
    #[allow(clippy::too_many_lines)]
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let op = cx.s3_op().name().to_string();
        let (principal, key_id) = match cx.credentials() {
            Some(c) => match self.store.principal_of(&c.access_key) {
                Some(p) => (p, Some(c.access_key.clone())),
                None => return Err(s3_error!(InvalidAccessKeyId)),
            },
            None => (policy::ANONYMOUS.to_string(), None),
        };
        let bucket_name = cx.s3_path().get_bucket_name().map(str::to_string);
        let key = cx.s3_path().get_object_key().map(str::to_string);
        let uri = cx.uri().clone();
        let has_version = query_param(&uri, "versionId").is_some();
        let action = action_of(&op, has_version);
        let source_ip: Option<IpAddr> = cx
            .extensions_mut()
            .get::<AuditRecord>()
            .and_then(|a| a.fields().source.map(|s| s.ip()));
        let secure = uri.scheme_str() == Some("https")
            || (source_ip.is_some_and(|ip| self.trusted_proxies.contains(&ip))
                && cx
                    .headers()
                    .get("x-forwarded-proto")
                    .is_some_and(|v| v == "https"));
        let copy_source = cx
            .headers()
            .get("x-amz-copy-source")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let metadata_directive = cx
            .headers()
            .get("x-amz-metadata-directive")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let prefix = query_param(&uri, "prefix");
        let delimiter = query_param(&uri, "delimiter");

        let note = |cx: &mut S3AccessContext<'_>, decision: &str| {
            if let Some(a) = cx.extensions_mut().get::<AuditRecord>() {
                let a = a.clone();
                a.with(|f| {
                    f.principal.clone_from(&principal);
                    f.key_id.clone_from(&key_id);
                    f.operation.clone_from(&op);
                    f.action = action.map(str::to_string);
                    f.bucket.clone_from(&bucket_name);
                    f.key.clone_from(&key);
                    f.decision = Some(decision.to_string());
                });
            }
        };

        let outcome: Authz = match (op.as_str(), bucket_name.as_deref()) {
            ("ListBuckets", _) => {
                if principal == policy::ANONYMOUS {
                    Authz::Deny("anonymous-deny".into())
                } else {
                    Authz::Allow("authenticated".into())
                }
            }
            ("CreateBucket", _) => {
                if principal == policy::ANONYMOUS {
                    Authz::Deny("anonymous-deny".into())
                } else {
                    match &self.bucket_creation {
                        BucketCreation::Authenticated => Authz::Allow("authenticated".into()),
                        BucketCreation::Deny => Authz::Deny("bucket-creation-denied".into()),
                        BucketCreation::Principals(p) if p.contains(&principal) => {
                            Authz::Allow("bucket-creation-list".into())
                        }
                        BucketCreation::Principals(_) => {
                            Authz::Deny("bucket-creation-denied".into())
                        }
                    }
                }
            }
            (_, None) => Authz::Deny("no-bucket".into()),
            (_, Some(bucket_name)) => {
                let Some(action) = action else {
                    // Operations without a policy action are not
                    // implemented by the gateway; never authorized.
                    note(cx, "not-implemented");
                    return Err(s3_error!(NotImplemented));
                };
                let record = match bucket::get_bucket(self.meta.as_ref(), bucket_name).await {
                    Ok(b) => Some(b),
                    Err(arca_lake_metadata::MetaError::NoSuchBucket) => None,
                    Err(e) => {
                        tracing::warn!(error = %e, "bucket record read failed during authorization");
                        note(cx, "error-deny");
                        return Err(s3_error!(ServiceUnavailable, "metadata unavailable"));
                    }
                };
                let req = PolicyRequest {
                    principal: &principal,
                    action,
                    key: key.as_deref(),
                    prefix: prefix.as_deref(),
                    delimiter: delimiter.as_deref(),
                    source_ip,
                    secure_transport: secure,
                    metadata_directive: metadata_directive.as_deref(),
                    copy_source: copy_source.as_deref(),
                };
                if op == "DeleteObjects" {
                    // The keys are in the body: every entry is decided by
                    // the backend through `EntryAuthorizer`, as AWS
                    // authorizes `s3:DeleteObject(Version)` per key.
                    let Some(b) = record.clone() else {
                        note(cx, "no-such-bucket");
                        return Err(s3_error!(AccessDenied));
                    };
                    let policy = self.policy_of(&b);
                    let who = principal.clone();
                    let gate =
                        EntryAuthorizer(Arc::new(move |key: &str, version: Option<&str>| {
                            let action = if version.is_some() {
                                "s3:DeleteObjectVersion"
                            } else {
                                "s3:DeleteObject"
                            };
                            let req = PolicyRequest {
                                principal: &who,
                                action,
                                key: Some(key),
                                source_ip,
                                secure_transport: secure,
                                ..Default::default()
                            };
                            matches!(
                                decide_with(&who, action, &b, &policy, &req),
                                Authz::Allow(_)
                            )
                        }));
                    note(cx, "per-entry");
                    cx.extensions_mut().insert(gate);
                    cx.extensions_mut().insert(Principal(principal));
                    return Ok(());
                }
                let mut outcome = self.decide(&principal, action, record.as_ref(), &req);
                // A copy also reads its source: authorize `s3:GetObject`
                // (`Version`) on the source bucket, which may be another
                // tenant's (rule 7: a denied source is AccessDenied).
                if let (Authz::Allow(_), Some(src)) = (&outcome, copy_source.as_deref()) {
                    let Some((src_bucket, src_key, src_version)) = parse_copy_source(src) else {
                        note(cx, "bad-copy-source");
                        return Err(s3_error!(InvalidArgument, "malformed x-amz-copy-source"));
                    };
                    let src_action = if src_version.is_some() {
                        "s3:GetObjectVersion"
                    } else {
                        "s3:GetObject"
                    };
                    let src_record = match bucket::get_bucket(self.meta.as_ref(), &src_bucket).await
                    {
                        Ok(b) => Some(b),
                        Err(arca_lake_metadata::MetaError::NoSuchBucket) => None,
                        Err(e) => {
                            tracing::warn!(error = %e, "source bucket record read failed during authorization");
                            note(cx, "error-deny");
                            return Err(s3_error!(ServiceUnavailable, "metadata unavailable"));
                        }
                    };
                    let src_req = PolicyRequest {
                        principal: &principal,
                        action: src_action,
                        key: Some(&src_key),
                        source_ip,
                        secure_transport: secure,
                        ..Default::default()
                    };
                    if let Authz::Deny(why) =
                        self.decide(&principal, src_action, src_record.as_ref(), &src_req)
                    {
                        outcome = Authz::Deny(format!("source:{why}"));
                    }
                }
                outcome
            }
        };
        match outcome {
            Authz::Allow(why) => {
                note(cx, &why);
                cx.extensions_mut().insert(Principal(principal));
                Ok(())
            }
            Authz::Deny(why) => {
                note(cx, &why);
                Err(s3_error!(AccessDenied))
            }
        }
    }
}

/// Map an `S3Error` to the status code for the audit record.
#[must_use]
pub fn status_of(e: &S3Error) -> u16 {
    e.status_code().map_or(500, |s| s.as_u16())
}

/// Decide `action` for `principal` on bucket `b` with its parsed policy.
fn decide_with(
    principal: &str,
    action: &str,
    b: &BucketRecord,
    policy: &Result<Option<Arc<Policy>>, String>,
    req: &PolicyRequest<'_>,
) -> Authz {
    let is_owner = !b.owner.is_empty() && b.owner == principal;
    let policy = match policy {
        Ok(p) => p.clone(),
        Err(e) => {
            tracing::error!(bucket = %b.name, error = %e, "stored bucket policy failed to parse");
            // Rule 6: evaluation error denies, except the owner keeps the
            // right to repair the policy.
            if is_owner && OWNER_KEEPS.contains(&action) {
                return Authz::Allow("owner-override".into());
            }
            return Authz::Deny("error-deny".into());
        }
    };
    if let Some(p) = &policy {
        match p.evaluate(req) {
            Decision::Deny(i) => {
                if is_owner && OWNER_KEEPS.contains(&action) {
                    return Authz::Allow("owner-override".into());
                }
                return Authz::Deny(format!("deny:{}", p.sid(i).unwrap_or("")));
            }
            Decision::Allow(i) => {
                if is_owner {
                    return Authz::Allow("owner".into());
                }
                return Authz::Allow(format!("allow:{}", p.sid(i).unwrap_or("")));
            }
            Decision::NoMatch => {}
        }
    }
    if is_owner {
        return Authz::Allow("owner".into());
    }
    Authz::Deny(if principal == policy::ANONYMOUS {
        "anonymous-deny".into()
    } else {
        "default-deny".into()
    })
}

/// Files a key set was read from and a digest of their contents (change
/// detection only; nothing of it is kept but the hash).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stamp {
    files: Vec<std::path::PathBuf>,
    digest: u64,
}

impl Stamp {
    fn digest_of(files: &[std::path::PathBuf]) -> u64 {
        use std::hash::{Hash as _, Hasher as _};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for f in files {
            f.hash(&mut h);
            match std::fs::read(f) {
                Ok(bytes) => bytes.hash(&mut h),
                Err(e) => e.kind().hash(&mut h),
            }
        }
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_parses_utc() {
        let t = parse_rfc3339("2026-12-31T00:00:00Z").unwrap();
        let secs = t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        assert_eq!(secs, 1_798_675_200);
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00Z").unwrap(),
            SystemTime::UNIX_EPOCH
        );
        assert!(parse_rfc3339("2026-13-01T00:00:00Z").is_none());
        assert!(parse_rfc3339("2026-12-31T00:00:00+09:00").is_none());
    }

    #[test]
    fn credentials_file_loads_and_refuses_bad_states() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.toml");
        std::fs::write(
            &path,
            r#"
[[principal]]
name = "tenant-a"
[[principal.key]]
id = "AKIAONE"
secret = "s1"
[[principal.key]]
id = "AKIATWO"
secret = "s2"
state = "inactive"
[[principal.key]]
id = "AKIAOLD"
secret = "s3"
not_after = "2000-01-01T00:00:00Z"
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(
                CredentialStore::load(&path)
                    .unwrap_err()
                    .contains("world-readable")
            );
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let store = CredentialStore::load(&path).unwrap();
        assert_eq!(store.len(), 3);
        assert_eq!(store.principal_of("AKIAONE").as_deref(), Some("tenant-a"));
        assert_eq!(
            store.lookup("AKIATWO", SystemTime::now()).unwrap_err(),
            KeyRefusal::Inactive
        );
        assert_eq!(
            store.lookup("AKIAOLD", SystemTime::now()).unwrap_err(),
            KeyRefusal::Expired
        );
        assert_eq!(
            store.lookup("nope", SystemTime::now()).unwrap_err(),
            KeyRefusal::Unknown
        );
        // A broken rewrite keeps the old keys and counts a failure.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&path, "this is not toml [[").unwrap();
        assert!(!store.reload_if_changed());
        assert_eq!(
            store
                .reload_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(store.principal_of("AKIAONE").as_deref(), Some("tenant-a"));
        // Rotation: the new file drops key one and activates key two.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(
            &path,
            "[[principal]]\nname = \"tenant-a\"\n[[principal.key]]\nid = \"AKIATWO\"\nsecret = \"s2\"\n",
        )
        .unwrap();
        assert!(store.reload_if_changed());
        assert_eq!(store.principal_of("AKIAONE"), None);
        assert_eq!(store.principal_of("AKIATWO").as_deref(), Some("tenant-a"));
        // Unknown fields and duplicate ids are errors.
        std::fs::write(&path, "[[principal]]\nname = \"x\"\nbogus = 1\n").unwrap();
        assert!(CredentialStore::read_file(&path).is_err());
    }

    /// Rotating only a referenced secret file is a change (Phase 3.5 audit
    /// SEC-7: only the credentials file's mtime was watched, so the old
    /// secret stayed valid).
    #[test]
    fn rotating_a_secret_file_alone_reloads_the_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.toml");
        let secret = dir.path().join("one.secret");
        std::fs::write(&secret, "first-secret\n").unwrap();
        std::fs::write(
            &path,
            "[[principal]]\nname = \"tenant-a\"\n[[principal.key]]\nid = \"AKIAONE\"\nsecret_file = \"one.secret\"\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let store = CredentialStore::load(&path).unwrap();
        let secret_now = |s: &CredentialStore| {
            s.lookup("AKIAONE", SystemTime::now())
                .unwrap()
                .secret
                .expose()
                .to_string()
        };
        assert_eq!(secret_now(&store), "first-secret");
        assert!(!store.reload_if_changed(), "nothing changed");
        std::fs::write(&secret, "second-secret\n").unwrap();
        assert!(store.reload_if_changed());
        assert_eq!(secret_now(&store), "second-secret");
        assert!(!store.reload_if_changed());
    }

    #[test]
    fn query_values_are_percent_decoded() {
        assert_eq!(percent_decode("public%2Fa%20b"), "public/a b");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("bad%2"), "bad%2");
        let uri: hyper::Uri = "/b?list-type=2&prefix=public%2F&delimiter=%2F"
            .parse()
            .unwrap();
        assert_eq!(query_param(&uri, "prefix").as_deref(), Some("public/"));
        assert_eq!(query_param(&uri, "delimiter").as_deref(), Some("/"));
        assert_eq!(query_param(&uri, "versionId"), None);
    }

    #[test]
    fn copy_source_header_parses() {
        assert_eq!(
            parse_copy_source("/b/dir%2Fk?versionId=abc"),
            Some(("b".into(), "dir/k".into(), Some("abc".into())))
        );
        assert_eq!(
            parse_copy_source("b/k"),
            Some(("b".into(), "k".into(), None))
        );
        assert_eq!(parse_copy_source("/b"), None);
        assert_eq!(parse_copy_source("/b/"), None);
    }

    #[test]
    fn action_mapping_covers_implemented_operations() {
        assert_eq!(action_of("GetObject", false), Some("s3:GetObject"));
        assert_eq!(action_of("GetObject", true), Some("s3:GetObjectVersion"));
        assert_eq!(
            action_of("CompleteMultipartUpload", false),
            Some("s3:PutObject")
        );
        assert_eq!(action_of("PutObjectAcl", false), None);
    }
}
