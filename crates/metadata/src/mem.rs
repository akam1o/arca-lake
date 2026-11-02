//! In-memory [`KvStore`] with FoundationDB-like semantics.
//!
//! - Snapshot reads at the version current when the transaction began.
//! - Optimistic conflict detection: a commit fails if any key (or range) the
//!   transaction read was written by a commit after its snapshot version.
//! - Size limits are enforced at commit, as the FoundationDB client does.
//! - Every operation yields to the executor once so that concurrent
//!   transactions in tests interleave at realistic points.
//!
//! Not intended for production use. Transactions share the committed map
//! through an `Arc`; a commit copies it only while another transaction still
//! holds a snapshot, and updates it in place otherwise.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use crate::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};

#[derive(Debug, Default)]
struct State {
    version: u64,
    data: Arc<BTreeMap<Key, Value>>,
    /// Version of the last write (set or clear) per key.
    modified: BTreeMap<Key, u64>,
}

/// In-memory store.
#[derive(Debug, Clone)]
pub struct MemKv {
    state: Arc<Mutex<State>>,
    limits: Limits,
}

impl Default for MemKv {
    fn default() -> Self {
        Self::new(Limits::FOUNDATIONDB)
    }
}

impl MemKv {
    /// Create an empty store enforcing `limits`.
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            limits,
        }
    }

    /// Current commit version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .version
    }

    /// Number of keys stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .data
            .len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of all keys with `prefix` (for assertions in tests).
    #[must_use]
    pub fn dump_prefix(&self, prefix: &[u8]) -> Vec<(Key, Value)> {
        let r = KeyRange::prefix(prefix);
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .data
            .range(r.begin..r.end)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl KvStore for MemKv {
    type Tx = MemTx;

    async fn begin(&self) -> Result<MemTx, KvError> {
        yield_once().await;
        let (read_version, snapshot) = {
            let s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            (s.version, Arc::clone(&s.data))
        };
        Ok(MemTx {
            state: Arc::clone(&self.state),
            limits: self.limits,
            read_version,
            snapshot,
            reads: Vec::new(),
            writes: BTreeMap::new(),
            adds: BTreeMap::new(),
            range_clears: Vec::new(),
            affected_bytes: 0,
            pending_error: None,
        })
    }

    fn limits(&self) -> Limits {
        self.limits
    }
}

/// In-memory transaction.
#[derive(Debug)]
pub struct MemTx {
    state: Arc<Mutex<State>>,
    limits: Limits,
    read_version: u64,
    snapshot: Arc<BTreeMap<Key, Value>>,
    reads: Vec<KeyRange>,
    /// `None` means cleared.
    writes: BTreeMap<Key, Option<Value>>,
    /// Atomic additions applied against the committed value at commit.
    adds: BTreeMap<Key, i64>,
    range_clears: Vec<KeyRange>,
    affected_bytes: usize,
    pending_error: Option<KvError>,
}

impl MemTx {
    /// Value as this transaction sees it: own writes, then range clears,
    /// then the snapshot.
    fn view(&self, key: &[u8]) -> Option<Value> {
        let base = if let Some(w) = self.writes.get(key) {
            w.clone()
        } else if self.range_clears.iter().any(|r| r.contains(key)) {
            None
        } else {
            self.snapshot.get(key).cloned()
        };
        match self.adds.get(key) {
            Some(delta) => Some(add_le(base.as_deref(), *delta)),
            None => base,
        }
    }

    fn note_write(&mut self, key: &[u8], value_len: usize) {
        if key.len() > self.limits.max_key_bytes {
            self.pending_error.get_or_insert(KvError::TooLarge {
                what: "key",
                limit: self.limits.max_key_bytes,
                actual: key.len(),
            });
        }
        if value_len > self.limits.max_value_bytes {
            self.pending_error.get_or_insert(KvError::TooLarge {
                what: "value",
                limit: self.limits.max_value_bytes,
                actual: value_len,
            });
        }
        self.affected_bytes += key.len() + value_len;
    }
}

/// `i64` little-endian addition on an optional stored value.
fn add_le(base: Option<&[u8]>, delta: i64) -> Value {
    let cur = base
        .and_then(|b| <[u8; 8]>::try_from(b).ok())
        .map_or(0, i64::from_le_bytes);
    cur.wrapping_add(delta).to_le_bytes().to_vec()
}

impl Txn for MemTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        yield_once().await;
        self.reads.push(KeyRange::point(key));
        self.affected_bytes += key.len();
        Ok(self.view(key))
    }

    async fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        yield_once().await;
        self.affected_bytes += key.len();
        Ok(self.view(key))
    }

    fn atomic_add(
        &mut self,
        key: &[u8],
        delta: i64,
    ) -> impl Future<Output = Result<(), KvError>> + Send {
        self.note_write(key, 8);
        *self.adds.entry(key.to_vec()).or_insert(0) += delta;
        std::future::ready(Ok(()))
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        yield_once().await;
        self.reads.push(range.clone());
        self.affected_bytes += range.begin.len() + range.end.len();
        if range.is_empty() {
            return Ok(Vec::new());
        }
        // Merge snapshot and own writes within the range.
        let mut merged: BTreeMap<Key, Value> = self
            .snapshot
            .range(range.begin.clone()..range.end.clone())
            .filter(|(k, _)| !self.range_clears.iter().any(|r| r.contains(k)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, w) in self.writes.range(range.begin.clone()..range.end.clone()) {
            match w {
                Some(v) => {
                    merged.insert(k.clone(), v.clone());
                }
                None => {
                    merged.remove(k);
                }
            }
        }
        let out: Vec<(Key, Value)> = if reverse {
            merged.into_iter().rev().take(limit).collect()
        } else {
            merged.into_iter().take(limit).collect()
        };
        Ok(out)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.note_write(key, value.len());
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }

    fn clear(&mut self, key: &[u8]) {
        self.note_write(key, 0);
        self.writes.insert(key.to_vec(), None);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.affected_bytes += range.begin.len() + range.end.len();
        // Own earlier writes inside the range are superseded by the clear.
        let inside: Vec<Key> = self
            .writes
            .range(range.begin.clone()..range.end.clone())
            .map(|(k, _)| k.clone())
            .collect();
        for k in inside {
            self.writes.remove(&k);
        }
        self.range_clears.push(range.clone());
    }

    async fn commit(self) -> Result<u64, KvError> {
        yield_once().await;
        let MemTx {
            state,
            limits,
            read_version,
            snapshot,
            reads,
            writes,
            adds,
            range_clears,
            affected_bytes,
            pending_error,
        } = self;
        // Release this transaction's snapshot first: with no other
        // transaction open, the commit then updates the map in place.
        drop(snapshot);
        if let Some(e) = pending_error {
            return Err(e);
        }
        if affected_bytes > limits.max_transaction_bytes {
            return Err(KvError::TooLarge {
                what: "transaction",
                limit: limits.max_transaction_bytes,
                actual: affected_bytes,
            });
        }
        let mut s = state.lock().unwrap_or_else(PoisonError::into_inner);
        // Read-only transactions cannot conflict and need no version.
        if writes.is_empty() && range_clears.is_empty() && adds.is_empty() {
            return Ok(s.version);
        }
        for r in &reads {
            if s.modified
                .range(r.begin.clone()..r.end.clone())
                .any(|(_, &v)| v > read_version)
            {
                return Err(KvError::Conflict);
            }
        }
        s.version += 1;
        let version = s.version;
        let State { data, modified, .. } = &mut *s;
        let data = Arc::make_mut(data);
        for r in &range_clears {
            let keys: Vec<Key> = data
                .range(r.begin.clone()..r.end.clone())
                .map(|(k, _)| k.clone())
                .collect();
            for k in keys {
                data.remove(&k);
                modified.insert(k, version);
            }
        }
        for (k, w) in writes {
            match w {
                Some(v) => {
                    data.insert(k.clone(), v);
                }
                None => {
                    data.remove(&k);
                }
            }
            modified.insert(k, version);
        }
        // Atomic additions see the value as committed by everyone else.
        for (k, delta) in adds {
            let next = add_le(data.get(&k).map(Vec::as_slice), delta);
            data.insert(k.clone(), next);
            modified.insert(k, version);
        }
        Ok(version)
    }
}

/// A future that returns `Pending` exactly once, so that concurrent tasks in
/// a test executor interleave at every store operation.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn yield_once() -> YieldOnce {
    YieldOnce(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transact;

    fn run<T>(f: impl Future<Output = T>) -> T {
        futures::executor::block_on(f)
    }

    #[test]
    fn read_your_writes_and_commit() {
        let kv = MemKv::default();
        run(async {
            let mut tx = kv.begin().await.unwrap();
            assert_eq!(tx.get(b"a").await.unwrap(), None);
            tx.set(b"a", b"1");
            assert_eq!(tx.get(b"a").await.unwrap(), Some(b"1".to_vec()));
            tx.commit().await.unwrap();
            let mut tx = kv.begin().await.unwrap();
            assert_eq!(tx.get(b"a").await.unwrap(), Some(b"1".to_vec()));
        });
    }

    #[test]
    fn write_write_without_read_does_not_conflict_but_read_write_does() {
        let kv = MemKv::default();
        run(async {
            let mut t1 = kv.begin().await.unwrap();
            let mut t2 = kv.begin().await.unwrap();
            t1.set(b"k", b"1");
            t2.set(b"k", b"2");
            t1.commit().await.unwrap();
            // Blind write: no conflict (last writer wins), as in FoundationDB.
            t2.commit().await.unwrap();

            let mut t3 = kv.begin().await.unwrap();
            let mut t4 = kv.begin().await.unwrap();
            let _ = t3.get(b"k").await.unwrap();
            t3.set(b"k", b"3");
            t4.set(b"k", b"4");
            t4.commit().await.unwrap();
            assert_eq!(t3.commit().await.unwrap_err(), KvError::Conflict);
        });
    }

    #[test]
    fn range_read_conflicts_with_insert_in_range() {
        let kv = MemKv::default();
        run(async {
            let mut t1 = kv.begin().await.unwrap();
            let mut t2 = kv.begin().await.unwrap();
            let r = KeyRange::prefix(b"p/");
            assert_eq!(t1.get_range(&r, 10, false).await.unwrap(), Vec::new());
            t1.set(b"count", b"0");
            t2.set(b"p/x", b"1");
            t2.commit().await.unwrap();
            assert_eq!(t1.commit().await.unwrap_err(), KvError::Conflict);
        });
    }

    #[test]
    fn limits_surface_at_commit() {
        let kv = MemKv::new(Limits {
            max_key_bytes: 4,
            max_value_bytes: 8,
            max_transaction_bytes: 100,
        });
        run(async {
            let mut tx = kv.begin().await.unwrap();
            tx.set(b"toolong", b"v");
            assert!(matches!(
                tx.commit().await,
                Err(KvError::TooLarge { what: "key", .. })
            ));
            let mut tx = kv.begin().await.unwrap();
            tx.set(b"k", &[0u8; 9]);
            assert!(matches!(
                tx.commit().await,
                Err(KvError::TooLarge { what: "value", .. })
            ));
            let mut tx = kv.begin().await.unwrap();
            for i in 0..20u8 {
                tx.set(&[i], &[0u8; 8]);
            }
            assert!(matches!(
                tx.commit().await,
                Err(KvError::TooLarge {
                    what: "transaction",
                    ..
                })
            ));
        });
    }

    #[test]
    fn clear_range_and_reverse_range_read() {
        let kv = MemKv::default();
        run(async {
            let mut tx = kv.begin().await.unwrap();
            for i in 0..5u8 {
                tx.set(&[b'a', i], &[i]);
            }
            tx.set(b"b", b"x");
            tx.commit().await.unwrap();
            let mut tx = kv.begin().await.unwrap();
            tx.clear_range(&KeyRange::prefix(b"a"));
            assert_eq!(
                tx.get_range(&KeyRange::prefix(b"a"), 10, false)
                    .await
                    .unwrap(),
                Vec::new()
            );
            tx.set(b"a\x09", b"new");
            tx.commit().await.unwrap();
            let mut tx = kv.begin().await.unwrap();
            let all = tx
                .get_range(&KeyRange::new(vec![0], vec![0xFF]), 10, true)
                .await
                .unwrap();
            assert_eq!(all.len(), 2);
            assert_eq!(all[0].0, b"b");
            assert_eq!(all[1].0, b"a\x09");
        });
    }

    #[test]
    fn transact_retries_conflicts() {
        let kv = MemKv::default();
        run(async {
            // Two concurrent increments must both apply.
            let inc = async || {
                transact(&kv, (), |tx, ()| {
                    Box::pin(async move {
                        let n = tx
                            .get(b"n")
                            .await?
                            .map_or(0u64, |v| u64::from_le_bytes(v.try_into().unwrap()));
                        tx.set(b"n", &(n + 1).to_le_bytes());
                        Ok(())
                    })
                })
                .await
            };
            let (a, b) = futures::join!(inc(), inc());
            a.unwrap();
            b.unwrap();
            let mut tx = kv.begin().await.unwrap();
            let n = tx.get(b"n").await.unwrap().unwrap();
            assert_eq!(u64::from_le_bytes(n.try_into().unwrap()), 2);
        });
    }
}
