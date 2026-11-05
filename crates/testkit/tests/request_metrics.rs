//! S3 request metrics name the operation on a gateway without an
//! authorizer (Phase 3.4 soak: every request was counted as
//! `op="unknown"`), and such a gateway still refuses unsigned requests.

use std::sync::Arc;

use arca_lake_gateway::requests::S3Stats;
use arca_lake_gateway::{GatewayConfig, memory::MemoryBackend, serve};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[tokio::test]
async fn requests_are_counted_by_operation_and_unsigned_ones_refused() {
    let stats = Arc::new(S3Stats::default());
    let cfg = GatewayConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        credentials: Some(("metrics-ak".into(), "metrics-secret-key-0000".into())),
        store: None,
        audit: false,
        stats: Some(Arc::clone(&stats)),
    };
    let gw = serve(cfg, MemoryBackend::new()).await.unwrap();
    let addr = gw.local_addr;
    let c = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://{addr}"))
            .force_path_style(true)
            .credentials_provider(Credentials::new(
                "metrics-ak",
                "metrics-secret-key-0000",
                None,
                None,
                "test",
            ))
            .build(),
    );
    c.create_bucket().bucket("metrics-bkt").send().await.unwrap();
    c.put_object()
        .bucket("metrics-bkt")
        .key("k")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    c.get_object().bucket("metrics-bkt").key("k").send().await.unwrap();
    assert!(
        c.get_object()
            .bucket("metrics-bkt")
            .key("missing")
            .send()
            .await
            .is_err()
    );

    // An unsigned request is refused, as without an access hook.
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /metrics-bkt/k HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let out = String::from_utf8_lossy(&out);
    assert!(out.starts_with("HTTP/1.1 403"), "{out}");

    let mut text = String::new();
    stats.render(&mut text);
    for series in [
        "op=\"CreateBucket\",status=\"2xx\"} 1",
        "op=\"PutObject\",status=\"2xx\"} 1",
        "op=\"GetObject\",status=\"2xx\"} 1",
        "op=\"GetObject\",status=\"4xx\"} 2",
    ] {
        assert!(
            text.contains(&format!("arca_gateway_s3_requests_total{{{series}")),
            "{series} missing in:\n{text}"
        );
    }
    assert!(!text.contains("op=\"unknown\""), "{text}");
    gw.shutdown().await;
}
