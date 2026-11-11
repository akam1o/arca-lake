//! Repair queue pagination (Phase 3.11, M4 of the Phase 3.10 audit): the
//! stripe repair scheduler used to read its whole task queue in one
//! transaction every pass, beyond FoundationDB's limits under widespread
//! damage. Now a pass scans a window of the queue page by page from a
//! cursor kept in metadata, keeps only the tasks it will run, and wraps.
//!
//! Checked: no read of more than `TASK_PAGE_MAX` entries in one
//! transaction; at most `tasks_per_pass` tasks per pass and `scan_window`
//! entries scanned; every task reached (none starved, none lost) with
//! tasks added and removed meanwhile; a failure mid-scan loses nothing and
//! the next pass (another leader) resumes from the persisted cursor.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arca_lake_controller::integrity::{RepairWorkerOptions, coders, repair_pass};
use arca_lake_metadata::kv::{Key, KeyRange, Limits, Value};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::integrity::{self as itasks, Finding};
use arca_lake_metadata::{KvError, KvStore, Txn};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms};

/// Counts rows per range read; fails the `fail_at`-th range read (0: never).
struct Counting {
    inner: Arc<MemKv>,
    max_rows: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    fail_at: Arc<AtomicUsize>,
}

struct CountingTx {
    inner: <MemKv as KvStore>::Tx,
    max_rows: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    fail_at: Arc<AtomicUsize>,
}

impl KvStore for Counting {
    type Tx = CountingTx;

    async fn begin(&self) -> Result<CountingTx, KvError> {
        Ok(CountingTx {
            inner: self.inner.begin().await?,
            max_rows: Arc::clone(&self.max_rows),
            reads: Arc::clone(&self.reads),
            fail_at: Arc::clone(&self.fail_at),
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

impl Txn for CountingTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get(key).await
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        let n = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_at.load(Ordering::SeqCst) == n {
            return Err(KvError::Backend("injected: read failed mid-scan".into()));
        }
        let rows = self.inner.get_range(range, limit, reverse).await?;
        if range
            .begin
            .starts_with(&arca_lake_metadata::keys::repair_tasks_range().begin)
        {
            self.max_rows.fetch_max(rows.len(), Ordering::SeqCst);
        }
        Ok(rows)
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

    async fn commit(self) -> Result<u64, KvError> {
        self.inner.commit().await
    }
}

fn counting(c: &TestCluster) -> Counting {
    Counting {
        inner: Arc::clone(&c.meta),
        max_rows: Arc::default(),
        reads: Arc::default(),
        fail_at: Arc::default(),
    }
}

fn opts() -> RepairWorkerOptions {
    RepairWorkerOptions {
        tasks_per_pass: 128,
        scan_window: 700,
        rate_bytes_per_s: 0,
        ..RepairWorkerOptions::default()
    }
}

/// `n` tasks for chunks no generation holds (each is dropped when run),
/// chunk ids from `base`.
async fn enqueue(c: &TestCluster, group: u64, base: u32, n: u32) {
    for i in base..base + n {
        let mut chunk = [0u8; 16];
        chunk[..4].copy_from_slice(&i.to_be_bytes());
        itasks::enqueue(
            c.meta.as_ref(),
            group,
            chunk,
            Finding::MissingShard,
            &[0],
            now_ms(),
        )
        .await
        .unwrap();
    }
}

async fn depth(c: &TestCluster) -> u64 {
    itasks::queue_stats(c.meta.as_ref()).await.unwrap().total
}

/// A large queue: bounded pages, bounded passes, every task reached; tasks
/// added before the cursor meanwhile are reached after the wrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_queue_is_read_in_bounded_pages_and_every_task_is_reached() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
    })
    .await;
    let group = c.groups().await.remove(0).id;
    enqueue(&c, group, 1_000, 2_500).await;
    assert_eq!(depth(&c).await, 2_500);
    let store = counting(&c);
    let o = opts();
    let mut passes = 0;
    let mut added = false;
    while depth(&c).await > 0 {
        let r = repair_pass(&store, &c.clients, &coders(), &o, None, None, now_ms())
            .await
            .unwrap();
        assert!(r.examined <= o.tasks_per_pass, "{r:?}");
        assert!(r.scanned <= o.scan_window, "{r:?}");
        passes += 1;
        // Behind the cursor: chunk ids below every one queued.
        if passes == 3 && !added {
            enqueue(&c, group, 0, 200).await;
            added = true;
        }
        assert!(
            passes <= 40,
            "the queue did not drain: {} left",
            depth(&c).await
        );
    }
    let max = store.max_rows.load(Ordering::SeqCst);
    assert!(
        max <= itasks::TASK_PAGE_MAX,
        "a transaction read {max} queue entries"
    );
    // 2,700 tasks at 128 per pass: at least 22 passes.
    assert!(passes >= 22, "{passes}");
    c.shutdown().await;
}

/// A read failing mid-scan fails that pass only: the cursor is not moved,
/// no task is lost, and the next pass (another leader: a new store handle)
/// resumes from the persisted cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_mid_scan_loses_nothing_and_the_next_leader_resumes_from_the_cursor() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
    })
    .await;
    let group = c.groups().await.remove(0).id;
    enqueue(&c, group, 0, 1_500).await;
    let o = opts();
    // First leader: one pass; the cursor moves past its window.
    let first = counting(&c);
    repair_pass(&first, &c.clients, &coders(), &o, None, None, now_ms())
        .await
        .unwrap();
    let cursor = itasks::scan_cursor(c.meta.as_ref()).await.unwrap();
    assert!(cursor.is_some(), "the cursor is persisted");
    // A pass failing at its second page read.
    let failing = counting(&c);
    failing.fail_at.store(3, Ordering::SeqCst);
    let left = depth(&c).await;
    assert!(
        repair_pass(&failing, &c.clients, &coders(), &o, None, None, now_ms())
            .await
            .is_err()
    );
    assert_eq!(
        itasks::scan_cursor(c.meta.as_ref()).await.unwrap(),
        cursor,
        "a failed pass does not move the cursor"
    );
    assert_eq!(depth(&c).await, left, "nothing lost");
    // A new leader resumes there and drains the queue.
    let next = counting(&c);
    let r = repair_pass(&next, &c.clients, &coders(), &o, None, None, now_ms())
        .await
        .unwrap();
    assert!(r.examined > 0, "{r:?}");
    let mut passes = 0;
    while depth(&c).await > 0 {
        repair_pass(&next, &c.clients, &coders(), &o, None, None, now_ms())
            .await
            .unwrap();
        passes += 1;
        assert!(passes <= 30, "{} left", depth(&c).await);
    }
    c.shutdown().await;
}
