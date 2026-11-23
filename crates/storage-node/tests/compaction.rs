//! Background compaction in a running node (RFC-005 §5.6): dead space of
//! deleted shards is reclaimed while the node serves, and every surviving
//! shard reads back unchanged, also after a restart.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_storage_node::compactor::CompactionPolicy;
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

/// Give the node at `addr` an incarnation and open delete fence generation
/// 1 on it, and take both for `c` (what a controller leader does: a node
/// takes no shard write before it adopted an incarnation, ADR-0037; no
/// delete outside its fence, ADR-0023).
async fn open_fence(c: &NodeClients, addr: &str) {
    adopt_incarnation(c, addr).await;
    let pushed = c
        .push_delete_fence_with(
            addr,
            1,
            true,
            arca_lake_protocol::client::FencePush {
                incarnation: Some(INCARNATION),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(pushed, (1, true, false));
    c.set_delete_fence(Some(1));
}

/// The incarnation [`open_fence`] gives a node.
const INCARNATION: [u8; 16] = [7; 16];

/// Give the node at `addr` [`INCARNATION`] and stamp `c`'s writes with it.
async fn adopt_incarnation(c: &NodeClients, addr: &str) {
    c.health_with(
        addr,
        arca_lake_protocol::storage::v1::HealthRequest {
            incarnation: Bytes::from_static(&INCARNATION),
            incarnation_changed_ms: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.set_incarnation(Some(INCARNATION));
}

fn shard(i: u32) -> ShardRef {
    let mut chunk_id = [0u8; 16];
    chunk_id[..4].copy_from_slice(&i.to_le_bytes());
    ShardRef {
        group_id: 1,
        chunk_id,
        shard_index: 0,
    }
}

fn data(i: u32) -> Bytes {
    Bytes::from(vec![u8::try_from(i % 251).unwrap(); 64 << 10])
}

/// Listings of the store a size is retried over at most (see [`seg_bytes`]).
const SCAN_ATTEMPTS: usize = 8;

/// Bytes of the store's segment files (`seg-*`), from one listing in which
/// every listed segment could still be read. Compaction deletes segments
/// while the node serves, so a segment listed and then gone before its size
/// is read (`NotFound`) starts the listing again, at most
/// [`SCAN_ATTEMPTS`] times; any other error, and a segment vanishing in
/// every listing, fails the test.
fn seg_bytes(dir: &std::path::Path) -> u64 {
    seg_bytes_with(dir, std::fs::DirEntry::metadata)
}

/// [`seg_bytes`] with the size read by `stat` (a test interleaves a
/// deletion there).
fn seg_bytes_with(
    dir: &std::path::Path,
    mut stat: impl FnMut(&std::fs::DirEntry) -> std::io::Result<std::fs::Metadata>,
) -> u64 {
    for _ in 0..SCAN_ATTEMPTS {
        let mut total = 0;
        let mut vanished = false;
        for entry in std::fs::read_dir(dir).expect("list the store directory") {
            let entry = entry.expect("read a store directory entry");
            if !entry.file_name().to_string_lossy().starts_with("seg-") {
                continue;
            }
            match stat(&entry) {
                Ok(m) => total += m.len(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    vanished = true;
                    break;
                }
                Err(e) => panic!("size of {}: {e}", entry.path().display()),
            }
        }
        if !vanished {
            return total;
        }
    }
    panic!("segment files vanished during each of {SCAN_ATTEMPTS} listings");
}

fn config(dir: &std::path::Path, listen: std::net::SocketAddr) -> NodeConfig {
    let mut cfg = NodeConfig::new(listen, dir);
    cfg.store.sync = arca_lake_storage::SyncPolicy::Never;
    cfg.store.segment_max_bytes = 1 << 20;
    cfg.compaction = CompactionPolicy {
        interval: Duration::from_millis(100),
        min_dead_fraction: 0.5,
        max_segments: 4,
        max_copy_bytes: 64 << 20,
    };
    cfg
}

#[tokio::test]
async fn deleted_space_is_reclaimed_while_serving_and_survivors_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let node = serve(config(dir.path(), "127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = node.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    open_fence(&c, &addr).await;
    for i in 0..120 {
        c.put_shard(&addr, shard(i), 0, data(i), 16 << 10)
            .await
            .unwrap();
    }
    let before = seg_bytes(dir.path());
    // Delete four of every five shards.
    for i in (0..120).filter(|i| i % 5 != 0) {
        c.delete_shard(&addr, shard(i), 0).await.unwrap();
    }
    let stats = std::sync::Arc::clone(node.service().compaction_stats());
    let start = Instant::now();
    while seg_bytes(dir.path()) >= before / 2 {
        assert!(start.elapsed() < Duration::from_secs(20), "no compaction");
        // Reads during compaction never fail or see wrong data.
        for i in (0..120).step_by(5) {
            assert_eq!(c.get_shard(&addr, shard(i), 0, 0).await.unwrap(), data(i));
        }
    }
    assert_eq!(stats.errors.load(Ordering::Relaxed), 0);
    assert!(stats.compactions.load(Ordering::Relaxed) > 0);
    assert!(stats.reclaimed_bytes.load(Ordering::Relaxed) > 0);
    // Settled: idle passes do not keep rewriting segments whose tombstones
    // are still needed (the churn found by the soak test).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let settled = stats.compactions.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        stats.compactions.load(Ordering::Relaxed),
        settled,
        "compaction churns while idle"
    );
    assert_eq!(
        stats.backlog_segments.load(Ordering::Relaxed),
        0,
        "compaction backlog left"
    );
    for i in 0..120 {
        let r = c.get_shard(&addr, shard(i), 0, 0).await;
        if i % 5 == 0 {
            assert_eq!(r.unwrap(), data(i));
        } else {
            assert_eq!(r.unwrap_err().code(), tonic::Code::NotFound, "{i}");
        }
    }
    // A restart rebuilds the same state from the compacted segments.
    let listen = node.local_addr;
    node.shutdown().await;
    let node = serve(config(dir.path(), listen)).await.unwrap();
    let c = NodeClients::new(Duration::from_secs(5));
    for i in 0..120 {
        let r = c.get_shard(&addr, shard(i), 0, 0).await;
        if i % 5 == 0 {
            assert_eq!(r.unwrap(), data(i));
        } else {
            assert_eq!(
                r.unwrap_err().code(),
                tonic::Code::NotFound,
                "deleted {i} resurrected"
            );
        }
    }
    node.shutdown().await;
}

/// Two segment files of 10 and 20 bytes in a fresh directory.
fn two_segments() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("seg-1"), [0u8; 10]).unwrap();
    std::fs::write(dir.path().join("seg-2"), [0u8; 20]).unwrap();
    std::fs::write(dir.path().join("index"), [0u8; 5]).unwrap();
    dir
}

/// The race the size had (CI, a flaky failure): a segment listed, then
/// deleted by compaction before its size is read, answers `NotFound`.
#[test]
fn a_listed_segment_can_vanish_before_its_size_is_read() {
    let dir = two_segments();
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(Result::unwrap)
        .collect();
    std::fs::remove_file(dir.path().join("seg-1")).unwrap();
    let e = entries
        .iter()
        .find(|e| e.file_name() == "seg-1")
        .unwrap()
        .metadata()
        .unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
}

/// A segment deleted between the listing and its size: listed again, the
/// size is that of the segments left.
#[test]
fn a_segment_vanishing_during_the_listing_is_listed_again() {
    let dir = two_segments();
    let mut deleted = false;
    let bytes = seg_bytes_with(dir.path(), |e| {
        if e.file_name() == "seg-1" && !deleted {
            deleted = true;
            std::fs::remove_file(e.path()).unwrap();
        }
        e.metadata()
    });
    assert!(deleted, "the interleaving did not happen");
    assert_eq!(bytes, 20);
    assert_eq!(seg_bytes(dir.path()), 20);
}

/// Any other error is a failure, not a reason to list again.
#[test]
#[should_panic(expected = "permission denied")]
fn other_errors_reading_a_size_fail() {
    let dir = two_segments();
    seg_bytes_with(dir.path(), |_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        ))
    });
}

/// Segments vanishing in every listing end the test, bounded.
#[test]
#[should_panic(expected = "vanished during each of 8 listings")]
fn listings_are_bounded() {
    let dir = two_segments();
    seg_bytes_with(dir.path(), |_| {
        Err(std::io::Error::from(std::io::ErrorKind::NotFound))
    });
}
