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

/// Default per-transaction timeout (overridable with `ARCA_FDB_TX_TIMEOUT_MS`).
pub const DEFAULT_TX_TIMEOUT_MS: i32 = 10_000;

/// Start the FoundationDB client network thread if it is not running.
///
/// The guard is kept in a process-wide static, so the network thread runs
/// until the process exits.
#[allow(unsafe_code)]
pub fn ensure_network() {
    NETWORK.get_or_init(|| {
        // SAFETY: `boot` is unsafe because the network must be started at
        // most once per process and stopped before exit. `OnceLock` ensures
        // a single start; the guard lives in a static, so the network runs
        // until process exit, which the client library tolerates for
        // short-lived processes such as tests and tools.
        unsafe { foundationdb::boot() }
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

/// FoundationDB error codes we map specially.
mod codes {
    pub const TRANSACTION_TIMED_OUT: i32 = 1031;
    pub const TRANSACTION_TOO_LARGE: i32 = 2101;
    pub const KEY_TOO_LARGE: i32 = 2102;
    pub const VALUE_TOO_LARGE: i32 = 2103;
}

fn map_err(e: FdbError) -> KvError {
    let limits = Limits::FOUNDATIONDB;
    match e.code() {
        codes::TRANSACTION_TOO_LARGE => KvError::TooLarge {
            what: "transaction",
            limit: limits.max_transaction_bytes,
            actual: 0,
        },
        codes::KEY_TOO_LARGE => KvError::TooLarge {
            what: "key",
            limit: limits.max_key_bytes,
            actual: 0,
        },
        codes::VALUE_TOO_LARGE => KvError::TooLarge {
            what: "value",
            limit: limits.max_value_bytes,
            actual: 0,
        },
        // A timed-out transaction is not retried blindly: with the cluster
        // unavailable every retry would wait the full timeout again.
        codes::TRANSACTION_TIMED_OUT => KvError::Backend(format!("transaction timed out: {e}")),
        _ if e.is_maybe_committed() => KvError::CommitUnknown(e.to_string()),
        _ if e.is_retryable() => KvError::Conflict,
        _ => KvError::Backend(e.to_string()),
    }
}

fn map_commit_err(e: &foundationdb::TransactionCommitError) -> KvError {
    map_err(FdbError::from_code(e.code()))
}
