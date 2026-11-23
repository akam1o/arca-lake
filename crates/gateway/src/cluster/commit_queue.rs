//! Per-key commit queue (ADR-0041, R-112).
//!
//! Commits that publish to one (bucket, key) through this gateway take
//! turns, first come first served: at most one is in flight per key, so
//! they no longer conflict with each other in the metadata store. A turn
//! is held until the write's outcome is decided (an unknown commit is
//! resolved under it). Commits from other gateways still conflict; the
//! transaction loop then waits a jittered, growing delay before its next
//! attempt ([`JitterBackoff`]). The store's conflict check stays the
//! authority on order: the queue only avoids attempts that would lose.
//!
//! A turn not granted within the wait bound, or a queue already holding
//! its maximum of waiters, is refused before any commit attempt was made:
//! the caller answers `SlowDown` (not applied) and aborts its pending
//! generation. The table is bounded: when it holds its maximum of keys, a
//! commit for another key proceeds without a turn (its transactions still
//! back off after conflicts). An entry is removed when nobody holds or
//! waits for it; dropping a waiting or holding future (a client gone)
//! releases its place.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arca_lake_metadata::kv::ConflictBackoff;

/// Configuration of the commit queue.
#[derive(Debug, Clone)]
pub struct CommitQueueConfig {
    /// Off: every commit proceeds at once (the 0.1.0 behaviour), with no
    /// backoff between conflicts.
    pub enabled: bool,
    /// Commits waiting for one key at most (besides the one holding the
    /// turn); one more is refused.
    pub max_waiters: usize,
    /// Longest wait for a turn (W); a longer one is refused.
    pub wait_bound: Duration,
    /// Keys in the table at most; beyond, a commit proceeds without a turn.
    pub max_keys: usize,
    /// Backoff after the first conflict at most; doubled per conflict.
    pub backoff_base: Duration,
    /// Upper bound of one backoff.
    pub backoff_cap: Duration,
}

impl Default for CommitQueueConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_waiters: 256,
            wait_bound: Duration::from_secs(2),
            max_keys: 16_384,
            backoff_base: Duration::from_millis(2),
            backoff_cap: Duration::from_millis(50),
        }
    }
}

/// Why a turn was refused (no commit attempt was made).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// The key's queue holds its maximum of waiters.
    Full,
    /// No turn within the wait bound.
    WaitBound,
}

/// Counters of the commit queue.
#[derive(Debug, Default)]
pub struct CommitQueueStats {
    turns: AtomicU64,
    waited: AtomicU64,
    wait_us: AtomicU64,
    refused_full: AtomicU64,
    refused_wait: AtomicU64,
    bypassed: AtomicU64,
    conflicts: AtomicU64,
    backoff_us: AtomicU64,
}

/// A copy of [`CommitQueueStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitQueueStatsSnapshot {
    /// Turns granted.
    pub turns: u64,
    /// Turns that had to wait for another commit on the key.
    pub waited: u64,
    /// Total wait of granted turns, microseconds.
    pub wait_us: u64,
    /// Refused: the key's queue was full.
    pub refused_full: u64,
    /// Refused: no turn within the wait bound.
    pub refused_wait: u64,
    /// Commits that proceeded without a turn (table full).
    pub bypassed: u64,
    /// Conflicts of queued commits' transactions (backoffs taken).
    pub conflicts: u64,
    /// Total backoff, microseconds.
    pub backoff_us: u64,
    /// Keys in the table now.
    pub keys: u64,
}

type Table = Arc<Mutex<HashMap<(String, String), Entry>>>;

struct Entry {
    lock: Arc<tokio::sync::Mutex<()>>,
    /// Holder and waiters.
    users: usize,
}

/// The per-key commit queue of one gateway.
pub struct CommitQueue {
    cfg: CommitQueueConfig,
    table: Table,
    stats: Arc<CommitQueueStats>,
}

/// Leaves the table when dropped: the last user removes the entry.
struct Registration {
    table: Table,
    key: Option<(String, String)>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let mut t = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(e) = t.get_mut(&key) {
            e.users -= 1;
            if e.users == 0 {
                t.remove(&key);
            }
        }
    }
}

/// A commit's turn on its key; dropping it lets the next waiter in.
pub struct Turn {
    // Declared first: released before the registration leaves the table,
    // so an entry is never removed while its lock is held.
    _guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    _registration: Registration,
}

impl CommitQueue {
    /// An empty queue.
    #[must_use]
    pub fn new(cfg: CommitQueueConfig) -> Self {
        Self {
            cfg,
            table: Arc::default(),
            stats: Arc::default(),
        }
    }

    /// Wait for the turn to commit to `key` of `bucket`.
    ///
    /// # Errors
    ///
    /// [`Refused`] when the key's queue is full or no turn came within the
    /// wait bound; no commit may then be attempted.
    pub async fn turn(&self, bucket: &str, key: &str) -> Result<Turn, Refused> {
        let none = || Turn {
            _guard: None,
            _registration: Registration {
                table: Arc::clone(&self.table),
                key: None,
            },
        };
        if !self.cfg.enabled {
            return Ok(none());
        }
        let k = (bucket.to_owned(), key.to_owned());
        let lock = {
            let mut t = self.table.lock().unwrap_or_else(PoisonError::into_inner);
            let len = t.len();
            match t.get_mut(&k) {
                Some(e) if e.users > self.cfg.max_waiters => {
                    self.stats.refused_full.fetch_add(1, Relaxed);
                    return Err(Refused::Full);
                }
                Some(e) => {
                    e.users += 1;
                    Arc::clone(&e.lock)
                }
                None if len >= self.cfg.max_keys => {
                    self.stats.bypassed.fetch_add(1, Relaxed);
                    return Ok(none());
                }
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    t.insert(
                        k.clone(),
                        Entry {
                            lock: Arc::clone(&lock),
                            users: 1,
                        },
                    );
                    lock
                }
            }
        };
        let registration = Registration {
            table: Arc::clone(&self.table),
            key: Some(k),
        };
        let started = std::time::Instant::now();
        let guard = if let Ok(g) = Arc::clone(&lock).try_lock_owned() {
            g
        } else {
            self.stats.waited.fetch_add(1, Relaxed);
            let Ok(g) = tokio::time::timeout(self.cfg.wait_bound, lock.lock_owned()).await else {
                self.stats.refused_wait.fetch_add(1, Relaxed);
                return Err(Refused::WaitBound);
            };
            g
        };
        self.stats.turns.fetch_add(1, Relaxed);
        self.stats.wait_us.fetch_add(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Relaxed,
        );
        Ok(Turn {
            _guard: Some(guard),
            _registration: registration,
        })
    }

    /// The backoff the transactions of queued commits wait on; `None` when
    /// the queue is off (immediate retries, as before).
    #[must_use]
    pub fn backoff(&self) -> Option<JitterBackoff> {
        self.cfg.enabled.then(|| JitterBackoff {
            base: self.cfg.backoff_base,
            cap: self.cfg.backoff_cap,
            stats: Arc::clone(&self.stats),
        })
    }

    /// Counters, with the number of keys in the table now.
    #[must_use]
    pub fn stats(&self) -> CommitQueueStatsSnapshot {
        let s = &self.stats;
        CommitQueueStatsSnapshot {
            turns: s.turns.load(Relaxed),
            waited: s.waited.load(Relaxed),
            wait_us: s.wait_us.load(Relaxed),
            refused_full: s.refused_full.load(Relaxed),
            refused_wait: s.refused_wait.load(Relaxed),
            bypassed: s.bypassed.load(Relaxed),
            conflicts: s.conflicts.load(Relaxed),
            backoff_us: s.backoff_us.load(Relaxed),
            keys: self
                .table
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len() as u64,
        }
    }
}

/// Full-jitter exponential backoff between conflicts: after the n-th, a
/// uniform wait in `[0, min(cap, base * 2^(n-1))]`.
pub struct JitterBackoff {
    base: Duration,
    cap: Duration,
    stats: Arc<CommitQueueStats>,
}

/// The wait after the `conflicts`-th conflict for a uniform `random`.
fn backoff_delay(conflicts: u32, base: Duration, cap: Duration, random: u64) -> Duration {
    let ceiling = base
        .saturating_mul(1u32 << conflicts.saturating_sub(1).min(16))
        .min(cap);
    let ceiling_us = u64::try_from(ceiling.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(random % (ceiling_us + 1))
}

impl ConflictBackoff for JitterBackoff {
    fn after_conflict(&self, conflicts: u32) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let random = u64::from_le_bytes(
            uuid::Uuid::new_v4().as_bytes()[..8]
                .try_into()
                .unwrap_or_default(),
        );
        let d = backoff_delay(conflicts, self.base, self.cap, random);
        self.stats.conflicts.fetch_add(1, Relaxed);
        self.stats
            .backoff_us
            .fetch_add(u64::try_from(d.as_micros()).unwrap_or(u64::MAX), Relaxed);
        Box::pin(tokio::time::sleep(d))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(max_waiters: usize, wait_bound: Duration, max_keys: usize) -> CommitQueue {
        CommitQueue::new(CommitQueueConfig {
            max_waiters,
            wait_bound,
            max_keys,
            ..CommitQueueConfig::default()
        })
    }

    #[tokio::test]
    async fn waiters_are_bounded_per_key() {
        let q = queue(2, Duration::from_secs(60), 16);
        let holder = q.turn("b", "k").await.unwrap();
        let w1 = q.turn("b", "k");
        let w2 = q.turn("b", "k");
        tokio::pin!(w1, w2);
        assert!(futures::poll!(&mut w1).is_pending());
        assert!(futures::poll!(&mut w2).is_pending());
        assert_eq!(q.turn("b", "k").await.err(), Some(Refused::Full));
        // Another key is not affected.
        drop(q.turn("b", "other").await.unwrap());
        drop(holder);
        drop(w1.await.unwrap());
        drop(w2.await.unwrap());
        let s = q.stats();
        assert_eq!((s.refused_full, s.turns, s.waited, s.keys), (1, 4, 2, 0));
    }

    #[tokio::test]
    async fn turns_are_granted_in_arrival_order() {
        let q = queue(16, Duration::from_secs(60), 16);
        let order = Arc::new(Mutex::new(Vec::new()));
        let holder = q.turn("b", "k").await.unwrap();
        let mut waiters = Vec::new();
        for i in 0..8 {
            let order = Arc::clone(&order);
            let q = &q;
            let mut f = Box::pin(async move {
                let t = q.turn("b", "k").await.unwrap();
                order.lock().unwrap().push(i);
                tokio::task::yield_now().await;
                drop(t);
            });
            assert!(futures::poll!(&mut f).is_pending());
            waiters.push(f);
        }
        drop(holder);
        futures::future::join_all(waiters).await;
        assert_eq!(*order.lock().unwrap(), (0..8).collect::<Vec<_>>());
        assert_eq!(q.stats().keys, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wait_beyond_the_bound_is_refused() {
        let q = queue(16, Duration::from_millis(100), 16);
        let holder = q.turn("b", "k").await.unwrap();
        let started = tokio::time::Instant::now();
        assert_eq!(q.turn("b", "k").await.err(), Some(Refused::WaitBound));
        assert!(started.elapsed() >= Duration::from_millis(100));
        drop(holder);
        let s = q.stats();
        assert_eq!((s.refused_wait, s.keys), (1, 0));
        // Granted within the bound.
        let holder = q.turn("b", "k").await.unwrap();
        let w = q.turn("b", "k");
        tokio::pin!(w);
        assert!(futures::poll!(&mut w).is_pending());
        tokio::time::advance(Duration::from_millis(50)).await;
        drop(holder);
        drop(w.await.unwrap());
        assert_eq!(q.stats().refused_wait, 1);
    }

    #[tokio::test]
    async fn a_dropped_waiter_or_holder_releases_its_place() {
        let q = queue(1, Duration::from_secs(60), 16);
        let holder = q.turn("b", "k").await.unwrap();
        {
            let w = q.turn("b", "k");
            tokio::pin!(w);
            assert!(futures::poll!(&mut w).is_pending());
            // The queue is full while it waits.
            assert_eq!(q.turn("b", "k").await.err(), Some(Refused::Full));
        }
        // Its place is free again once dropped (client gone).
        let w = q.turn("b", "k");
        tokio::pin!(w);
        assert!(futures::poll!(&mut w).is_pending());
        drop(holder);
        let t = w.await.unwrap();
        assert_eq!(q.stats().keys, 1);
        drop(t);
        assert_eq!(q.stats().keys, 0);
    }

    #[tokio::test]
    async fn a_cancelled_holder_hands_the_turn_on() {
        // A holder cancelled mid-commit (its task aborted, a client gone).
        let q = Arc::new(queue(1, Duration::from_secs(60), 16));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task = {
            let q = Arc::clone(&q);
            tokio::spawn(async move {
                let _t = q.turn("b", "k").await.unwrap();
                tx.send(()).unwrap();
                std::future::pending::<()>().await;
            })
        };
        rx.await.unwrap();
        let w = q.turn("b", "k");
        tokio::pin!(w);
        assert!(futures::poll!(&mut w).is_pending());
        task.abort();
        let _ = task.await;
        drop(w.await.unwrap());
        assert_eq!(q.stats().keys, 0);
    }

    #[tokio::test]
    async fn a_full_table_lets_new_keys_through_unqueued() {
        let q = queue(16, Duration::from_secs(60), 2);
        let a = q.turn("b", "a").await.unwrap();
        let b = q.turn("b", "b").await.unwrap();
        // Two turns at once on a third key: neither waits.
        let c1 = q.turn("b", "c").await.unwrap();
        let c2 = q.turn("b", "c").await.unwrap();
        let s = q.stats();
        assert_eq!((s.bypassed, s.keys), (2, 2));
        drop((a, b, c1, c2));
        assert_eq!(q.stats().keys, 0);
    }

    #[tokio::test]
    async fn off_grants_every_turn_at_once_without_backoff() {
        let q = CommitQueue::new(CommitQueueConfig {
            enabled: false,
            ..CommitQueueConfig::default()
        });
        let a = q.turn("b", "k").await.unwrap();
        let b = q.turn("b", "k").await.unwrap();
        assert!(q.backoff().is_none());
        drop((a, b));
        assert_eq!(q.stats(), CommitQueueStatsSnapshot::default());
    }

    #[test]
    fn backoff_grows_to_its_cap_with_full_jitter() {
        let base = Duration::from_millis(2);
        let cap = Duration::from_millis(50);
        let max = |n| backoff_delay(n, base, cap, u64::MAX - 1);
        assert!(max(1) <= Duration::from_millis(2));
        assert!(max(3) <= Duration::from_millis(8));
        assert!(max(10) <= cap && max(31) <= cap && max(u32::MAX) <= cap);
        assert_eq!(backoff_delay(1, base, cap, 0), Duration::ZERO);
        assert_eq!(
            backoff_delay(3, base, cap, 8_000),
            Duration::from_millis(8),
            "the ceiling itself is reachable"
        );
        assert_eq!(backoff_delay(9, base, cap, 50_000), cap);
    }
}
