//! An access policy is never installed without authentication: s3s skips
//! the access hook when no authentication is configured, so the policy
//! would allow every request (Phase 3.5 audit SEC-3).

use arca_lake_gateway::{AllowAll, GatewayConfig, memory::MemoryBackend, serve, serve_with_access};

#[tokio::test]
async fn an_access_policy_without_credentials_is_refused() {
    let cfg = GatewayConfig::new("127.0.0.1:0".parse().unwrap(), None);
    let r = serve_with_access(cfg, MemoryBackend::new(), Some(AllowAll)).await;
    assert_eq!(
        r.err().map(|e| e.kind()),
        Some(std::io::ErrorKind::InvalidInput)
    );
    // An anonymous gateway without a policy still starts.
    let cfg = GatewayConfig::new("127.0.0.1:0".parse().unwrap(), None);
    serve(cfg, MemoryBackend::new())
        .await
        .unwrap()
        .shutdown()
        .await;
    let cfg = GatewayConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        Some(("ak".into(), "sk-0000000000".into())),
    );
    serve_with_access(cfg, MemoryBackend::new(), Some(AllowAll))
        .await
        .unwrap()
        .shutdown()
        .await;
}
