//! FoundationDB backend for [`KvStore`].
//!
//! Thin mapping onto the `foundationdb` crate. Retry policy is left to
//! [`crate::transact`]: retryable commit errors become [`KvError::Conflict`];
//! an unknown commit outcome (`commit_unknown_result`) is **not** retried
//! here because the recipes above are not all idempotent with respect to
//! re-execution after an actual commit. That case surfaces as
//! [`KvError::CommitUnknown`] and is resolved by the caller (RFC-003 §5.8:
//! re-read the generation and decide).
//!
//! The client network thread can be started only once per process;
//! [`ensure_network`] does that lazily and keeps it running for the life of
//! the process.

use std::sync::OnceLock;

use foundationdb::api::NetworkAutoStop;
use foundationdb::{Database, FdbError, KeySelector, RangeOption, Transaction};

use crate::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};

static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();
static NETWORK_TLS: OnceLock<Option<FdbTls>> = OnceLock::new();

/// Client TLS for the metadata connection (ADR-0017): FoundationDB's own
/// network options, set once before the client network starts.
#[derive(Clone, PartialEq, Eq)]
pub struct FdbTls {
    /// Trust bundle (PEM).
    pub ca: std::path::PathBuf,
    /// Client certificate chain (PEM).
    pub cert: std::path::PathBuf,
    /// Client private key (PEM).
    pub key: std::path::PathBuf,
    /// FoundationDB peer verification string (`TLS_verify_peers`), e.g.
    /// `Check.Valid=1,S.subjectAltName>=URI:spiffe://<td>/fdb/`; `None`
    /// keeps FoundationDB's default (`Check.Valid=1`: chain and dates).
    pub verify_peers: Option<String>,
}

impl std::fmt::Debug for FdbTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbTls")
            .field("ca", &self.ca)
            .field("cert", &self.cert)
            .field("key", &self.key)
            .field("verify_peers", &self.verify_peers)
            .finish()
    }
}

/// Decide the metadata transport before the first connection: `Some`
/// enables client TLS, `None` declares plaintext. Must be called before
/// [`ensure_network`] (any `FdbKv::open`); a second call with a different
/// value is refused (the network starts once per process).
///
/// # Errors
///
/// Unreadable TLS files, or a conflicting earlier decision.
pub fn configure_network_tls(tls: Option<FdbTls>) -> Result<(), String> {
    if let Some(t) = &tls {
        for (what, p) in [("CA", &t.ca), ("certificate", &t.cert), ("key", &t.key)] {
            std::fs::metadata(p)
                .map_err(|e| format!("FoundationDB TLS {what} {}: {e}", p.display()))?;
        }
    }
    match NETWORK_TLS.set(tls) {
        Ok(()) => Ok(()),
        Err(v) if NETWORK_TLS.get() == Some(&v) => Ok(()),
        Err(_) => Err("FoundationDB network TLS already configured differently".into()),
    }
}

/// Whether metadata connections use TLS in this process.
#[must_use]
pub fn network_tls_enabled() -> bool {
    NETWORK_TLS.get().is_some_and(Option::is_some)
}

/// Coordinators of a cluster file and whether each is a `:tls` address.
///
/// # Errors
///
/// An unreadable or malformed cluster file.
pub fn cluster_file_tls(cluster_file: &str) -> Result<Vec<(String, bool)>, String> {
    let text = std::fs::read_to_string(cluster_file).map_err(|e| format!("{cluster_file}: {e}"))?;
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .ok_or_else(|| format!("{cluster_file}: empty cluster file"))?;
    let (_, coords) = line
        .split_once('@')
        .ok_or_else(|| format!("{cluster_file}: no '@' in the connection string"))?;
    Ok(coords
        .split(',')
        .map(|c| (c.trim().to_string(), c.trim().ends_with(":tls")))
        .collect())
}

/// Refuse a cluster file whose transport does not match the configured
/// one: with TLS every coordinator must be `:tls`; without TLS none may be
/// (a mixed file belongs to a migration, which this process must not
/// paper over).
///
/// # Errors
///
/// The mismatch.
pub fn check_cluster_file_transport(cluster_file: &str, tls: bool) -> Result<(), String> {
    let coords = cluster_file_tls(cluster_file)?;
    let n_tls = coords.iter().filter(|(_, t)| *t).count();
    match (tls, n_tls) {
        (true, n) if n == coords.len() => Ok(()),
        (false, 0) => Ok(()),
        (true, _) => Err(format!(
            "FoundationDB TLS is configured but {} of {} coordinators in {cluster_file} are not ':tls' addresses",
            coords.len() - n_tls,
            coords.len()
        )),
        (false, _) => Err(format!(
            "{cluster_file} lists ':tls' coordinators but no FoundationDB TLS is configured (--fdb-tls-*)"
        )),
    }
}

/// Whether [`ensure_network`] started the client network in this process.
#[must_use]
pub fn network_started() -> bool {
    NETWORK.get().is_some()
}

/// Default per-transaction timeout (overridable with `ARCA_FDB_TX_TIMEOUT_MS`).
pub const DEFAULT_TX_TIMEOUT_MS: i32 = 10_000;

/// Start the FoundationDB client network thread if it is not running.
///
/// The guard is kept in a process-wide static, so the network thread runs
/// until the process exits.
///
/// # Panics
///
/// If the client library cannot start or refuses the TLS options (the
/// files were checked by [`configure_network_tls`]).
#[allow(unsafe_code)]
pub fn ensure_network() {
    NETWORK.get_or_init(|| {
        let tls = NETWORK_TLS.get_or_init(|| None).clone();
        let mut b = foundationdb::api::FdbApiBuilder::default()
            .build()
            .expect("foundationdb API to be initialized");
        if let Some(t) = tls {
            use foundationdb::options::NetworkOption as O;
            let s = |p: &std::path::Path| p.display().to_string();
            b = b
                .set_option(O::TLSCaPath(s(&t.ca)))
                .and_then(|b| b.set_option(O::TLSCertPath(s(&t.cert))))
                .and_then(|b| b.set_option(O::TLSKeyPath(s(&t.key))))
                .expect("FoundationDB TLS network options");
            if let Some(v) = t.verify_peers {
                b = b
                    .set_option(O::TLSVerifyPeers(v.into_bytes()))
                    .expect("FoundationDB TLS verify peers");
            }
        }
        // SAFETY: `boot` is unsafe because the network must be started at
        // most once per process and stopped before exit. `OnceLock` ensures
        // a single start; the guard lives in a static, so the network runs
        // until process exit, which the client library tolerates for
        // short-lived processes such as tests and tools.
        unsafe { b.boot() }.expect("fdb network running")
    });
}

/// A FoundationDB-backed store.
pub struct FdbKv {
    db: Database,
}

impl std::fmt::Debug for FdbKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbKv").finish_non_exhaustive()
    }
}

impl FdbKv {
    /// Open the database described by `cluster_file` (or the client's default
    /// cluster file when `None`). Starts the network thread if needed.
    ///
    /// # Errors
    ///
    /// Returns a backend error if the database handle cannot be created.
    pub fn open(cluster_file: Option<&str>) -> Result<Self, KvError> {
        let timeout_ms = std::env::var("ARCA_FDB_TX_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_TX_TIMEOUT_MS);
        Self::open_with_timeout(cluster_file, timeout_ms)
    }

    /// Like [`FdbKv::open`], with an explicit per-transaction timeout. Every
    /// read and commit fails with a backend error after `timeout_ms` instead
    /// of waiting for the cluster to come back (RFC-011 §6: operations
    /// fail explicitly when the store has lost its quorum).
    ///
    /// # Errors
    ///
    /// Returns a backend error if the database handle cannot be created.
    pub fn open_with_timeout(cluster_file: Option<&str>, timeout_ms: i32) -> Result<Self, KvError> {
        if let Some(cf) = cluster_file {
            // Fail closed on a transport mismatch instead of letting the
            // client library time out (or silently mix transports).
            let tls = NETWORK_TLS.get().is_some_and(Option::is_some);
            check_cluster_file_transport(cf, tls).map_err(KvError::Backend)?;
        }
        ensure_network();
        let db = Database::new(cluster_file).map_err(map_err)?;
        db.set_option(foundationdb::options::DatabaseOption::TransactionTimeout(
            timeout_ms,
        ))
        .map_err(map_err)?;
        Ok(Self { db })
    }

    /// Delete every key in `range` outside the transaction abstraction.
    /// Intended for test setup and teardown.
    ///
    /// # Errors
    ///
    /// Backend errors.
    pub async fn clear_range_blocking(&self, range: &KeyRange) -> Result<(), KvError> {
        let trx = self.db.create_trx().map_err(map_err)?;
        trx.clear_range(&range.begin, &range.end);
        trx.commit().await.map_err(|e| map_commit_err(&e))?;
        Ok(())
    }
}

impl KvStore for FdbKv {
    type Tx = FdbTx;

    fn begin(&self) -> impl std::future::Future<Output = Result<FdbTx, KvError>> + Send {
        std::future::ready(
            self.db
                .create_trx()
                .map(|trx| FdbTx { trx })
                .map_err(map_err),
        )
    }

    fn limits(&self) -> Limits {
        Limits::FOUNDATIONDB
    }
}

/// A FoundationDB transaction.
#[derive(Debug)]
pub struct FdbTx {
    trx: Transaction,
}

impl Txn for FdbTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        let v = self.trx.get(key, false).await.map_err(map_err)?;
        Ok(v.map(|s| s.to_vec()))
    }

    async fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        let v = self.trx.get(key, true).await.map_err(map_err)?;
        Ok(v.map(|s| s.to_vec()))
    }

    fn atomic_add(
        &mut self,
        key: &[u8],
        delta: i64,
    ) -> impl std::future::Future<Output = Result<(), KvError>> + Send {
        self.trx.atomic_op(
            key,
            &delta.to_le_bytes(),
            foundationdb::options::MutationType::Add,
        );
        std::future::ready(Ok(()))
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        if range.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut opt = RangeOption {
            begin: KeySelector::first_greater_or_equal(range.begin.clone()),
            end: KeySelector::first_greater_or_equal(range.end.clone()),
            limit: Some(limit.min(i32::MAX as usize)),
            reverse,
            ..RangeOption::default()
        };
        let mut iteration = 1;
        loop {
            let values = self
                .trx
                .get_range(&opt, iteration, false)
                .await
                .map_err(map_err)?;
            for kv in &values {
                out.push((kv.key().to_vec(), kv.value().to_vec()));
                if out.len() >= limit {
                    return Ok(out);
                }
            }
            if !values.more() {
                return Ok(out);
            }
            match opt.next_range(&values) {
                Some(next) => {
                    opt = next;
                    opt.limit = Some((limit - out.len()).min(i32::MAX as usize));
                }
                None => return Ok(out),
            }
            iteration += 1;
        }
    }

    async fn get_many(&mut self, keys: &[Key]) -> Result<Vec<Option<Value>>, KvError> {
        let trx = &self.trx;
        let values = futures::future::try_join_all(keys.iter().map(|k| trx.get(k, false)))
            .await
            .map_err(map_err)?;
        Ok(values.into_iter().map(|v| v.map(|s| s.to_vec())).collect())
    }

    async fn get_first_many(
        &mut self,
        ranges: &[KeyRange],
    ) -> Result<Vec<Option<(Key, Value)>>, KvError> {
        let trx = &self.trx;
        let firsts = futures::future::try_join_all(ranges.iter().map(|r| async move {
            if r.is_empty() {
                return Ok(None);
            }
            let opt = RangeOption {
                begin: KeySelector::first_greater_or_equal(r.begin.clone()),
                end: KeySelector::first_greater_or_equal(r.end.clone()),
                limit: Some(1),
                ..RangeOption::default()
            };
            let values = trx.get_range(&opt, 1, false).await?;
            Ok::<_, FdbError>(
                values
                    .iter()
                    .next()
                    .map(|kv| (kv.key().to_vec(), kv.value().to_vec())),
            )
        }))
        .await
        .map_err(map_err)?;
        Ok(firsts)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.trx.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.trx.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.trx.clear_range(&range.begin, &range.end);
    }

    async fn commit(self) -> Result<u64, KvError> {
        let committed = self.trx.commit().await.map_err(|e| map_commit_err(&e))?;
        let version = committed.committed_version().map_err(map_err)?;
        Ok(u64::try_from(version).unwrap_or(0))
    }
}

/// Errors before the commit: nothing was sent for commit (R-72).
fn map_err(e: FdbError) -> KvError {
    crate::fdb_errors::classify(
        e.code(),
        &e.to_string(),
        crate::fdb_errors::Phase::Operation,
    )
}

/// Errors of the commit itself: unknown outcome unless the code proves the
/// commit was rejected (a timeout cancels the transaction, which may still
/// commit; R-72, `fdb_errors`).
fn map_commit_err(e: &foundationdb::TransactionCommitError) -> KvError {
    let e = FdbError::from_code(e.code());
    crate::fdb_errors::classify(e.code(), &e.to_string(), crate::fdb_errors::Phase::Commit)
}
