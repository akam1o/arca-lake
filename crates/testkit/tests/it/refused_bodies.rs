//! How S3 clients see a write refused for capacity (Phase 3.7, R-84): the
//! gateway answers `503 SlowDown` with `Retry-After` at the request's
//! header when the tier refuses new data. A client that asks for
//! `100-continue` (AWS CLI v2 and boto3 for PUT and UploadPart) never
//! sends the body; for one that sends it at once (the AWS SDK for Rust),
//! the gateway reads and drops up to the drain bound before answering, so
//! that the client can read the `503`; beyond it the connection is closed
//! after the answer and the client sees a closed connection (retryable).
//!
//! One JSON line per client, operation and size; the assertions are the
//! guarantees: a refused write never succeeds and is never visible, the
//! refusal within the drain bound is a `SlowDown`, and the connection is
//! usable again afterwards.

#![allow(clippy::too_many_lines)]

use std::time::Instant;

use arca_lake_metadata::ops::admission::{self, AdmissionRecord, TierAdmission};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;

const MIB: usize = 1 << 20;
const BUCKET: &str = "refused";

async fn force(c: &TestCluster, state: u8) {
    let rec = AdmissionRecord {
        computed_ms: now_ms(),
        ttl_ms: 3_600_000,
        gateways: 1,
        tiers: vec![TierAdmission {
            tier: "default".into(),
            state,
            reason: "test".into(),
            ..Default::default()
        }],
        leader_token: u64::MAX,
        incarnation: c.incarnation(),
        ..Default::default()
    };
    admission::publish(c.meta.as_ref(), &rec).await.unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
}

fn sdk(c: &TestCluster, attempts: u32) -> Client {
    let (ak, sk) = &c.credentials;
    Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://{}", c.gateway_addr))
            .force_path_style(true)
            .credentials_provider(Credentials::new(ak, sk, None, None, "test"))
            .retry_config(
                aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(attempts),
            )
            .build(),
    )
}

/// The outcome of a refused write as the client reports it.
fn sdk_outcome<O, E, R>(r: &Result<O, aws_sdk_s3::error::SdkError<E, R>>) -> String
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug,
    R: std::fmt::Debug,
{
    match r {
        Ok(_) => "accepted".into(),
        Err(aws_sdk_s3::error::SdkError::ServiceError(e)) => {
            format!("s3:{}", e.err().code().unwrap_or("?"))
        }
        Err(aws_sdk_s3::error::SdkError::DispatchFailure(_)) => "connection".into(),
        Err(e) => format!(
            "other:{}",
            format!("{e:?}").chars().take(60).collect::<String>()
        ),
    }
}

async fn python_available() -> bool {
    tokio::process::Command::new("python3")
        .args(["-c", "import boto3"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success())
}

/// boto3 `put_object` (one attempt): its error code, or "connection".
async fn boto3_put(c: &TestCluster, key: &str, path: &std::path::Path) -> String {
    let (ak, sk) = &c.credentials;
    let script = r"
import sys, boto3, botocore
from botocore.config import Config
s3 = boto3.client('s3', endpoint_url=sys.argv[1], aws_access_key_id=sys.argv[2],
    aws_secret_access_key=sys.argv[3], region_name='us-east-1',
    config=Config(retries={'max_attempts': 1, 'mode': 'standard'}, s3={'addressing_style': 'path'}))
try:
    with open(sys.argv[6], 'rb') as f:
        s3.put_object(Bucket=sys.argv[4], Key=sys.argv[5], Body=f)
    print('accepted')
except botocore.exceptions.ClientError as e:
    print('s3:' + e.response['Error']['Code'])
except Exception as e:
    print('connection:' + type(e).__name__)
";
    let out = tokio::process::Command::new("python3")
        .args(["-c", script])
        .arg(format!("http://{}", c.gateway_addr))
        .args([ak, sk, BUCKET, key])
        .arg(path)
        .output()
        .await
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_writes_refused_for_capacity_reach_s3_clients_as_slowdown() {
    let c = TestCluster::start(ClusterOptions::rs(3, 2, 1, MIB as u64, 64 << 10)).await;
    let client = sdk(&c, 1);
    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cli = c.aws_cli();
    let has_cli = arca_lake_testkit::s3::AwsCli::available().await;
    let has_boto3 = python_available().await;
    force(&c, 3).await;
    for mib in [1usize, 8, 32, 64] {
        let data = random_bytes(mib * MIB, mib as u64);
        let path = dir.path().join(format!("{mib}.bin"));
        std::fs::write(&path, &data).unwrap();
        // AWS SDK for Rust: no 100-continue.
        let key = format!("sdk-{mib}");
        let t = Instant::now();
        let r = client
            .put_object()
            .bucket(BUCKET)
            .key(&key)
            .body(ByteStream::from(data.clone()))
            .send()
            .await;
        let sdk = sdk_outcome(&r);
        let sdk_ms = t.elapsed().as_millis();
        assert_ne!(sdk, "accepted");
        if mib <= 16 {
            assert_eq!(sdk, "s3:SlowDown", "within the drain bound");
        }
        if let Err(aws_sdk_s3::error::SdkError::ServiceError(e)) = &r {
            assert_eq!(e.raw().status().as_u16(), 503);
            assert!(
                e.raw().headers().get("retry-after").is_some(),
                "Retry-After missing"
            );
        }
        // The client's connection pool works afterwards.
        client.head_bucket().bucket(BUCKET).send().await.unwrap();
        // AWS CLI v2 (100-continue).
        let cli_out = if has_cli {
            let (ok, _, err) = cli
                .run(&[
                    "s3api",
                    "put-object",
                    "--bucket",
                    BUCKET,
                    "--key",
                    &format!("cli-{mib}"),
                    "--body",
                    path.to_str().unwrap(),
                ])
                .await;
            assert!(!ok);
            if err.contains("SlowDown") {
                "s3:SlowDown".to_string()
            } else {
                format!(
                    "other:{}",
                    err.lines()
                        .last()
                        .unwrap_or("")
                        .chars()
                        .take(80)
                        .collect::<String>()
                )
            }
        } else {
            "absent".into()
        };
        if has_cli {
            assert_eq!(cli_out, "s3:SlowDown", "{cli_out}");
        }
        let boto = if has_boto3 {
            boto3_put(&c, &format!("boto3-{mib}"), &path).await
        } else {
            "absent".into()
        };
        if has_boto3 {
            assert_eq!(boto, "s3:SlowDown");
        }
        println!(
            "{}",
            serde_json::json!({"mib": mib, "rust_sdk": sdk, "rust_sdk_ms": sdk_ms, "aws_cli_v2": cli_out, "boto3": boto})
        );
        for k in [
            format!("sdk-{mib}"),
            format!("cli-{mib}"),
            format!("boto3-{mib}"),
        ] {
            assert!(
                client
                    .head_object()
                    .bucket(BUCKET)
                    .key(&k)
                    .send()
                    .await
                    .is_err(),
                "{k} visible"
            );
        }
    }
    // A multipart upload's large part, refused, then accepted on retry once
    // the tier admits again: the upload completes.
    force(&c, 0).await;
    let up = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .send()
        .await
        .unwrap();
    let id = up.upload_id().unwrap().to_string();
    force(&c, 3).await;
    let part = random_bytes(8 * MIB, 99);
    let r = client
        .upload_part()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from(part.clone()))
        .send()
        .await;
    assert_eq!(sdk_outcome(&r), "s3:SlowDown");
    force(&c, 0).await;
    let p = client
        .upload_part()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from(part.clone()))
        .send()
        .await
        .unwrap();
    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(&id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(p.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let got = client
        .get_object()
        .bucket(BUCKET)
        .key("mp")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes().len(),
        8 * MIB
    );
    c.shutdown().await;
}
