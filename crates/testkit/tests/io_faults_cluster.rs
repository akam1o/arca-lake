//! Storage I/O faults seen from the cluster (R-53, Phase 3.3 §3): an
//! injected device error on the nodes reaches the S3 client and the
//! controller as an error, never as a success, never as corruption. The
//! hook is process-wide (every in-process node), so the tests take the
//! hook's lock for the whole test (held across awaits on purpose).

#![allow(clippy::too_many_lines, clippy::await_holding_lock)]

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arca_lake_controller::integrity::{ScrubOptions, coders, scrub_group_now};
use arca_lake_storage::faults::{self, FaultOutcome, IoOp, set_io_hook};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3 as _;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "iofaults";

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn scrub_opts() -> ScrubOptions {
    ScrubOptions {
        page: 64,
        page_bytes: 0,
        rate_bytes_per_s: 0,
        interval_ms: 1,
        concurrency: 1,
        adaptive: None,
        manifest_roots_per_pass: 0,
    }
}

#[tokio::test]
async fn device_errors_reach_clients_and_the_controller_as_errors_not_corruption() {
    let _serial = faults::serial();
    let c = cluster().await;
    let data = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), 1));
    s3::put(
        &c.backend,
        BUCKET,
        "kept",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();

    // Disk full on every node: the PUT fails and is not visible; the
    // existing object still reads; space returns and PUTs work.
    set_io_hook(Some(Arc::new(|op| match op {
        IoOp::Write { .. } | IoOp::Create { .. } => {
            FaultOutcome::Error(std::io::Error::new(ErrorKind::StorageFull, "ENOSPC"))
        }
        _ => FaultOutcome::None,
    })));
    let e = s3::put(
        &c.backend,
        BUCKET,
        "full",
        Bytes::from(random_bytes(4096, 2)),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    eprintln!("PUT on full disks: {e}");
    assert_eq!(*e.code(), s3s::S3ErrorCode::SlowDown, "{e:?}");
    assert!(s3::head(&c.backend, BUCKET, "full").await.is_err());
    // The same failure through the HTTP gateway: an explicit, retryable
    // 503 SlowDown (the nodes answer out of space as a capacity refusal,
    // Phase 3.6) whose message carries no node address (they are logged
    // on the gateway).
    let sdk = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(format!("http://{}", c.gateway_addr))
            .force_path_style(true)
            .credentials_provider(aws_credential_types::Credentials::new(
                c.credentials.0.clone(),
                c.credentials.1.clone(),
                None,
                None,
                "test",
            ))
            .build(),
    );
    let err = sdk
        .put_object()
        .bucket(BUCKET)
        .key("full-http")
        .body(aws_sdk_s3::primitives::ByteStream::from(random_bytes(
            4096, 4,
        )))
        .send()
        .await
        .unwrap_err();
    let svc = err.into_service_error();
    let msg = svc.meta().message().unwrap_or_default().to_string();
    eprintln!("HTTP PUT on full disks: {:?} {msg}", svc.meta().code());
    assert_eq!(svc.meta().code(), Some("SlowDown"));
    assert!(msg.contains("insufficient capacity"), "{msg}");
    assert!(
        msg.contains("<node>") && !msg.contains("127.0.0.1"),
        "{msg}"
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, data);
    set_io_hook(None);
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        Bytes::from(random_bytes(4096, 3)),
        &PutOptions::default(),
    )
    .await
    .unwrap();

    // EIO on one shard read: the GET is served from the other shards
    // (degraded read), and no node quarantines anything.
    let left = Arc::new(AtomicUsize::new(1));
    let l = Arc::clone(&left);
    set_io_hook(Some(Arc::new(move |op| match op {
        IoOp::Read { len, .. } if *len > 4096 && l.load(Ordering::SeqCst) > 0 => {
            l.fetch_sub(1, Ordering::SeqCst);
            FaultOutcome::Error(std::io::Error::other("EIO"))
        }
        _ => FaultOutcome::None,
    })));
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, data);
    assert_eq!(left.load(Ordering::SeqCst), 0, "the fault fired");
    set_io_hook(None);
    for i in 0..c.nodes.len() {
        assert_eq!(c.node_store(i).quarantine_stats().0, 0, "node {i}");
    }

    // EIO on every read during a scrub: the controller counts errors and
    // records no finding (an I/O error is not corruption); a clean scrub
    // afterwards finds nothing either.
    set_io_hook(Some(Arc::new(|op| match op {
        IoOp::Read { len, .. } if *len > 4096 => FaultOutcome::Error(std::io::Error::other("EIO")),
        _ => FaultOutcome::None,
    })));
    let group = c.groups().await[0].id;
    let r = scrub_group_now(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        group,
        &scrub_opts(),
        now_ms(),
    )
    .await
    .unwrap();
    set_io_hook(None);
    eprintln!("scrub under EIO: {r:?}");
    assert_eq!(r.findings, [0; 8], "{r:?}");
    assert!(r.errors > 0, "{r:?}");
    for i in 0..c.nodes.len() {
        assert_eq!(c.node_store(i).quarantine_stats().0, 0, "node {i}");
    }
    let r = scrub_group_now(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        group,
        &scrub_opts(),
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!((r.findings, r.errors), ([0; 8], 0), "{r:?}");
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, data);
    c.shutdown().await;
}

/// Phase 3.8 failure matrix: GC's tombstone writes fail with ENOSPC on
/// every node. The generation is deferred, not lost and not half
/// forgotten: its metadata stays until its shards are gone, live objects
/// read throughout, and once space returns the next pass collects it.
#[tokio::test]
async fn gc_tombstones_hitting_enospc_are_deferred_and_collected_once_space_returns() {
    let _serial = faults::serial();
    let c = cluster().await;
    let data = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), 2));
    for k in ["kept", "gone"] {
        s3::put(&c.backend, BUCKET, k, data.clone(), &PutOptions::default())
            .await
            .unwrap();
    }
    let before = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "gone").await.unwrap();
    // Tombstones are the only small appends here: fail every write.
    set_io_hook(Some(Arc::new(|op| {
        if matches!(op, IoOp::Write { .. } | IoOp::Create { .. }) {
            FaultOutcome::Error(std::io::Error::new(ErrorKind::StorageFull, "ENOSPC"))
        } else {
            FaultOutcome::None
        }
    })));
    let r = c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(r.collected, 0, "{r:?}");
    assert!(r.deferred > 0, "{r:?}");
    assert_eq!(c.total_shards().await, before, "nothing deleted");
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, data);
    set_io_hook(None);
    let mut collected = 0;
    for _ in 0..3 {
        collected += c.gc(now_ms() + 3_600_000, 0).await.collected;
    }
    assert!(collected > 0);
    assert_eq!(c.total_shards().await, before / 2);
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, data);
    c.shutdown().await;
}

/// What the gate does to a shard write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Gate {
    #[default]
    Open,
    /// Hold until opened.
    Hold,
    /// Delay each by this much.
    Delay(std::time::Duration),
}

/// Holds or delays shard writes on every node and records how many are
/// held, for the failed-write settle tests.
#[derive(Default)]
struct WriteGate {
    mode: std::sync::Mutex<Gate>,
    opened: std::sync::Condvar,
    /// Writes inside the hook now.
    held: AtomicUsize,
    /// Writes that entered the hook since it was armed.
    entered: AtomicUsize,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}

/// Opens the gate and removes the hook when dropped, also on a failed
/// assertion: a held store would otherwise keep the runtime from ending.
struct Installed(Arc<WriteGate>);

impl Drop for Installed {
    fn drop(&mut self) {
        self.0.open();
        set_io_hook(None);
    }
}

impl WriteGate {
    fn install(self: &Arc<Self>) -> Installed {
        let gate = Arc::clone(self);
        set_io_hook(Some(Arc::new(move |op| {
            if matches!(op, IoOp::Write { .. }) {
                gate.pass();
            }
            FaultOutcome::None
        })));
        Installed(Arc::clone(self))
    }

    /// Runs on the node's blocking store thread.
    fn pass(&self) {
        let mut mode = self.mode.lock().unwrap();
        let how = *mode;
        if how == Gate::Open {
            return;
        }
        self.held.fetch_add(1, Ordering::SeqCst);
        self.entered.fetch_add(1, Ordering::SeqCst);
        if let Some(w) = self.waker.lock().unwrap().take() {
            w.wake();
        }
        if let Gate::Delay(d) = how {
            drop(mode);
            // Models a slow device: the store completes after `d`.
            std::thread::sleep(d);
        } else {
            while *mode != Gate::Open {
                mode = self.opened.wait(mode).unwrap();
            }
        }
        self.held.fetch_sub(1, Ordering::SeqCst);
    }

    fn arm(&self, how: Gate) {
        self.entered.store(0, Ordering::SeqCst);
        *self.mode.lock().unwrap() = how;
    }

    fn open(&self) {
        *self.mode.lock().unwrap() = Gate::Open;
        self.opened.notify_all();
    }
}

/// A PUT body of one full chunk that then fails as a body whose SHA-256
/// does not match its signature does, once a shard write of that chunk
/// has reached a node: a refused write with a placement in flight.
struct RefusedBody {
    chunk: Option<Bytes>,
    gate: Arc<WriteGate>,
    failed: bool,
}

impl futures::Stream for RefusedBody {
    type Item = Result<Bytes, s3s::stream::upload_stream::UploadStreamError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        if let Some(c) = self.chunk.take() {
            return Poll::Ready(Some(Ok(c)));
        }
        if self.failed {
            return Poll::Ready(None);
        }
        *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.gate.entered.load(Ordering::SeqCst) == 0 {
            return Poll::Pending;
        }
        self.failed = true;
        Poll::Ready(Some(Err(
            s3s::stream::upload_stream::UploadStreamError::Sha256Mismatch,
        )))
    }
}

async fn refused_put(c: &TestCluster, key: &str, gate: &Arc<WriteGate>) -> s3s::S3Error {
    let body = RefusedBody {
        chunk: Some(Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), 7))),
        gate: Arc::clone(gate),
        failed: false,
    };
    let input = s3s::dto::PutObjectInput {
        bucket: BUCKET.into(),
        key: key.into(),
        body: Some(s3s::dto::StreamingBlob::wrap(body)),
        ..Default::default()
    };
    c.backend
        .put_object(s3::request(input))
        .await
        .map(|_| ())
        .unwrap_err()
}

/// No shard of a refused write is stored after its reply: the reply waits
/// for the placement in flight (a slow store of 300 ms on every node), and
/// GC plus reconciliation right after it leave the baseline.
#[tokio::test]
async fn a_refused_write_answers_after_its_placements_in_flight() {
    let _serial = faults::serial();
    let c = cluster().await;
    s3::put(
        &c.backend,
        BUCKET,
        "kept",
        Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), 1)),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let baseline = c.total_shards().await;
    let gate = Arc::new(WriteGate::default());
    let installed = gate.install();
    gate.arm(Gate::Delay(std::time::Duration::from_millis(300)));
    let e = refused_put(&c, "refused", &gate).await;
    assert_eq!(e.code().as_str(), "XAmzContentSHA256Mismatch", "{e:?}");
    assert_eq!(
        gate.held.load(Ordering::SeqCst),
        0,
        "a shard store of the refused write was still running at its reply"
    );
    drop(installed);
    c.gc(now_ms(), 0).await;
    c.reconcile(now_ms() + 600_000, 0).await;
    assert_eq!(
        c.total_shards().await,
        baseline,
        "orphans of the refused write"
    );
    assert_eq!(c.backend.put_stats().settle_timeouts, 0);
    assert!(s3::head(&c.backend, BUCKET, "refused").await.is_err());
    c.shutdown().await;
}

/// A node that does not complete its store does not hold the reply of a
/// refused write beyond the settle deadline: the placement is aborted (and
/// counted), the client gets its 400.
#[tokio::test]
async fn a_stalled_node_does_not_hold_a_refused_write_beyond_the_deadline() {
    let _serial = faults::serial();
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        failed_write_settle: std::time::Duration::from_millis(200),
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let gate = Arc::new(WriteGate::default());
    let installed = gate.install();
    gate.arm(Gate::Hold);
    let started = std::time::Instant::now();
    // Without the deadline the reply would wait for the held stores
    // forever; the bound here only reports that as a failure.
    let e = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        refused_put(&c, "stalled", &gate),
    )
    .await
    .expect("the reply was held by the stalled node");
    eprintln!("refused write answered after {:?}", started.elapsed());
    assert_eq!(e.code().as_str(), "XAmzContentSHA256Mismatch", "{e:?}");
    assert!(
        gate.held.load(Ordering::SeqCst) > 0,
        "the stores are still held"
    );
    assert_eq!(c.backend.put_stats().settle_timeouts, 1);
    drop(installed);
    assert!(s3::head(&c.backend, BUCKET, "stalled").await.is_err());
    c.shutdown().await;
}
