//! A [`KvStore`] wrapper that fences every transaction with a leadership
//! token (RFC-011 §5.3).
//!
//! Each transaction reads the leader record before committing and fails
//! with [`KvError::Fenced`] unless the holder and token still match. Because
//! the read is part of the transaction, a change of leader that commits
//! after the read but before this commit makes the commit conflict and
//! retry, where the check then fails. An old leader can therefore never
//! commit a metadata change after a new leader took over, however late its
//! clock or its scheduler is.
//!
//! Storage node RPCs are not fenced by this token; they are fenced by the
//! group epoch (RFC-004 §5.4), and every decision that leads to a node RPC
//! is made in a fenced transaction first (RFC-008 §6.4).

use std::sync::Arc;

use crate::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};
use crate::ops::leader::Leadership;
use crate::ops::leader::check_in_tx;

/// A fenced store.
pub struct FencedKv<S> {
    inner: Arc<S>,
    leadership: Leadership,
}

impl<S: std::fmt::Debug> std::fmt::Debug for FencedKv<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FencedKv")
            .field("leadership", &self.leadership)
            .finish_non_exhaustive()
    }
}

impl<S> FencedKv<S> {
    /// Wrap `inner` so that every transaction requires `leadership`.
    pub fn new(inner: Arc<S>, leadership: Leadership) -> Self {
        Self { inner, leadership }
    }

    /// The leadership this store is fenced to.
    pub fn leadership(&self) -> &Leadership {
        &self.leadership
    }

    /// The wrapped store.
    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }
}

impl<S: KvStore> KvStore for FencedKv<S> {
    type Tx = FencedTx<S::Tx>;

    async fn begin(&self) -> Result<Self::Tx, KvError> {
        Ok(FencedTx {
            inner: self.inner.begin().await?,
            leadership: self.leadership.clone(),
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

/// A transaction that verifies the leadership token at commit.
pub struct FencedTx<T> {
    inner: T,
    leadership: Leadership,
}

impl<T: std::fmt::Debug> std::fmt::Debug for FencedTx<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FencedTx")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<T: Txn> Txn for FencedTx<T> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get(key).await
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        self.inner.get_range(range, limit, reverse).await
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.inner.clear_range(range);
    }

    async fn commit(mut self) -> Result<u64, KvError> {
        let ok = check_in_tx(&mut self.inner, &self.leadership)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))?;
        if !ok {
            return Err(KvError::Fenced(format!(
                "leadership token {} of {} is no longer current",
                self.leadership.token, self.leadership.holder
            )));
        }
        self.inner.commit().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemKv;
    use crate::ops::leader;

    #[test]
    fn fenced_store_stops_committing_after_a_takeover() {
        futures::executor::block_on(async {
            let base = Arc::new(MemKv::default());
            let a = leader::try_acquire(base.as_ref(), "a", 1_000, 100)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(a.token, 1);
            assert!(
                leader::try_acquire(base.as_ref(), "b", 1_050, 100)
                    .await
                    .unwrap()
                    .is_none(),
                "lease still valid"
            );
            let fenced_a = FencedKv::new(Arc::clone(&base), a.clone());
            let mut tx = fenced_a.begin().await.unwrap();
            tx.set(b"x", b"1");
            tx.commit().await.unwrap();

            // Lease expires; b takes over with a new token.
            let b = leader::try_acquire(base.as_ref(), "b", 1_200, 100)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(b.token, 2);
            assert!(!leader::renew(base.as_ref(), &a, 1_210, 100).await.unwrap());
            let mut tx = fenced_a.begin().await.unwrap();
            tx.set(b"x", b"2");
            assert!(matches!(tx.commit().await, Err(KvError::Fenced(_))));
            let mut tx = base.begin().await.unwrap();
            assert_eq!(tx.get(b"x").await.unwrap(), Some(b"1".to_vec()));

            // The new leader commits; release expires its lease at once.
            let fenced_b = FencedKv::new(Arc::clone(&base), b.clone());
            let mut tx = fenced_b.begin().await.unwrap();
            tx.set(b"x", b"3");
            tx.commit().await.unwrap();
            assert!(leader::release(base.as_ref(), &b).await.unwrap());
            let c = leader::try_acquire(base.as_ref(), "c", 1_201, 100)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(c.token, 3);
        });
    }
}
