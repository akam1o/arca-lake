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
