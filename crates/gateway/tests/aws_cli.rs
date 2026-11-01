//! Smoke test of the gateway with the AWS CLI.
//!
//! Skips when `aws` is not on `PATH`. Credentials are generated per run and
//! never written anywhere but the environment of the child processes.

#![allow(clippy::too_many_lines)]

use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;

use arca_lake_gateway::memory::MemoryBackend;
use arca_lake_gateway::{GatewayConfig, serve};
use tokio::process::Command;

struct Cli {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

impl Cli {
    async fn run(&self, args: &[&str]) -> (bool, String, String) {
        let out = Command::new("aws")
            .args([
                "--endpoint-url",
                &self.endpoint,
                "--output",
                "json",
                "--no-cli-pager",
            ])
            .args(args)
            .env("AWS_ACCESS_KEY_ID", &self.access_key)
            .env("AWS_SECRET_ACCESS_KEY", &self.secret_key)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("AWS_PROFILE")
            .stdin(Stdio::null())
            .output()
            .await
            .expect("spawn aws");
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    async fn ok(&self, args: &[&str]) -> String {
        let (ok, stdout, stderr) = self.run(args).await;
        assert!(ok, "aws {args:?} failed:\n{stderr}");
        stdout
    }

    async fn json(&self, args: &[&str]) -> serde_json::Value {
        let out = self.ok(args).await;
        serde_json::from_str(&out).unwrap_or_else(|e| panic!("aws {args:?}: bad json {e}: {out}"))
    }
}

async fn aws_available() -> bool {
    Command::new("aws")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok()
}

fn random_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 56) as u8
        })
        .collect()
}

#[tokio::test]
async fn aws_cli_smoke() {
    if !aws_available().await {
        eprintln!("aws CLI not found; skipping");
        return;
    }
    let access_key = format!("ARCATEST{}", uuid::Uuid::new_v4().simple());
    let secret_key = uuid::Uuid::new_v4().simple().to_string();
    let server = serve(
        GatewayConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            credentials: Some((access_key.clone(), secret_key.clone())),
        },
        MemoryBackend::new(),
    )
    .await
    .unwrap();
    let cli = Cli {
        endpoint: format!("http://{}", server.local_addr),
        access_key,
        secret_key,
    };

    // Buckets.
    cli.ok(&["s3api", "create-bucket", "--bucket", "bucket-one"])
        .await;
    let buckets = cli.json(&["s3api", "list-buckets"]).await;
    assert_eq!(buckets["Buckets"][0]["Name"], "bucket-one");
    cli.ok(&["s3api", "head-bucket", "--bucket", "bucket-one"])
        .await;
    let (ok, _, _) = cli.run(&["s3api", "head-bucket", "--bucket", "nope"]).await;
    assert!(!ok, "head-bucket on a missing bucket must fail");

    // Objects via the high-level `s3 cp` (uses the CLI's default checksum and
    // transfer behaviour) and via s3api.
    let dir = tempfile::tempdir().unwrap();
    let payload = random_bytes(3 << 20, 7);
    let src = dir.path().join("src.bin");
    tokio::fs::write(&src, &payload).await.unwrap();
    cli.ok(&[
        "s3",
        "cp",
        src.to_str().unwrap(),
        "s3://bucket-one/dir/obj.bin",
    ])
    .await;

    let head = cli
        .json(&[
            "s3api",
            "head-object",
            "--bucket",
            "bucket-one",
            "--key",
            "dir/obj.bin",
        ])
        .await;
    assert_eq!(head["ContentLength"], serde_json::json!(payload.len()));
    let etag = head["ETag"].as_str().unwrap().trim_matches('"').to_string();
    assert_eq!(etag.len(), 32, "single-part ETag is an MD5 hex digest");

    let dst = dir.path().join("dst.bin");
    cli.ok(&[
        "s3",
        "cp",
        "s3://bucket-one/dir/obj.bin",
        dst.to_str().unwrap(),
    ])
    .await;
    assert_eq!(tokio::fs::read(&dst).await.unwrap(), payload);

    // Range GET.
    let part = dir.path().join("part.bin");
    cli.ok(&[
        "s3api",
        "get-object",
        "--bucket",
        "bucket-one",
        "--key",
        "dir/obj.bin",
        "--range",
        "bytes=100-199",
        part.to_str().unwrap(),
    ])
    .await;
    assert_eq!(tokio::fs::read(&part).await.unwrap(), &payload[100..200]);

    // Small object with metadata and content type via s3api put-object.
    let small = dir.path().join("small.txt");
    tokio::fs::write(&small, b"hello").await.unwrap();
    cli.ok(&[
        "s3api",
        "put-object",
        "--bucket",
        "bucket-one",
        "--key",
        "top.txt",
        "--body",
        small.to_str().unwrap(),
        "--content-type",
        "text/plain",
        "--metadata",
        "owner=test",
    ])
    .await;
    let head = cli
        .json(&[
            "s3api",
            "head-object",
            "--bucket",
            "bucket-one",
            "--key",
            "top.txt",
        ])
        .await;
    assert_eq!(head["ContentType"], "text/plain");
    assert_eq!(head["Metadata"]["owner"], "test");

    // Conditional create: second attempt must fail.
    let (ok, _, stderr) = cli
        .run(&[
            "s3api",
            "put-object",
            "--bucket",
            "bucket-one",
            "--key",
            "top.txt",
            "--body",
            small.to_str().unwrap(),
            "--if-none-match",
            "*",
        ])
        .await;
    assert!(
        !ok && stderr.contains("PreconditionFailed"),
        "expected PreconditionFailed, got: {stderr}"
    );

    // Listing.
    let list = cli
        .json(&[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "bucket-one",
            "--delimiter",
            "/",
        ])
        .await;
    assert_eq!(list["Contents"][0]["Key"], "top.txt");
    assert_eq!(list["CommonPrefixes"][0]["Prefix"], "dir/");
    let list = cli
        .json(&[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "bucket-one",
            "--prefix",
            "dir/",
        ])
        .await;
    assert_eq!(list["Contents"][0]["Key"], "dir/obj.bin");
    // The CLI paginates and merges pages itself, dropping per-page scalars
    // such as KeyCount, so only the merged list is checked.
    assert_eq!(list["Contents"].as_array().unwrap().len(), 1);

    // Pagination through the CLI (it follows continuation tokens itself).
    for i in 0..5 {
        let k = format!("page/{i}");
        cli.ok(&[
            "s3api",
            "put-object",
            "--bucket",
            "bucket-one",
            "--key",
            &k,
            "--body",
            small.to_str().unwrap(),
        ])
        .await;
    }
    let paged = cli
        .json(&[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "bucket-one",
            "--prefix",
            "page/",
            "--page-size",
            "2",
        ])
        .await;
    assert_eq!(paged["Contents"].as_array().unwrap().len(), 5);

    // Delete and cleanup.
    cli.ok(&[
        "s3api",
        "delete-object",
        "--bucket",
        "bucket-one",
        "--key",
        "dir/obj.bin",
    ])
    .await;
    let (ok, _, _) = cli
        .run(&[
            "s3api",
            "head-object",
            "--bucket",
            "bucket-one",
            "--key",
            "dir/obj.bin",
        ])
        .await;
    assert!(!ok, "deleted object must be gone");
    let (ok, _, _) = cli
        .run(&["s3api", "delete-bucket", "--bucket", "bucket-one"])
        .await;
    assert!(!ok, "non-empty bucket must not be deletable");
    cli.ok(&["s3", "rm", "s3://bucket-one/", "--recursive"])
        .await;
    cli.ok(&["s3api", "delete-bucket", "--bucket", "bucket-one"])
        .await;

    // Wrong secret is rejected.
    let bad = Cli {
        secret_key: "not-the-secret".into(),
        ..cli
    };
    let (ok, _, stderr) = bad.run(&["s3api", "list-buckets"]).await;
    assert!(!ok && stderr.contains("SignatureDoesNotMatch"), "{stderr}");

    server.shutdown().await;
    let _ = Path::new("");
}
