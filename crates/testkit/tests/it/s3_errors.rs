//! S3 error codes of rejected requests through SigV4-signed clients, on
//! the cluster and the memory backends (0.2.0 three-host validation, P1
//! and P2): a body that does not match its signed `x-amz-content-sha256`
//! is `400 XAmzContentSHA256Mismatch` and changes nothing; re-creating
//! one's own bucket is `409 BucketAlreadyOwnedByYou`, another principal's
//! `409 BucketAlreadyExists`.

use std::net::SocketAddr;

use arca_lake_gateway::memory::MemoryBackend;
use arca_lake_gateway::{GatewayConfig, serve};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut;
use aws_sdk_s3::config::{
    ConfigBag, Intercept, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    RuntimeComponents,
};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

const MIB: usize = 1 << 20;

/// Replaces the body of a signed request with `body` (same length) after
/// signing, as a corrupting proxy or a buggy client would. Checks that
/// the request carries a signed payload hash, so that the replacement is
/// a payload mismatch and not an unsigned or streaming upload.
#[derive(Debug)]
struct Tamper {
    body: Vec<u8>,
}

impl Intercept for Tamper {
    fn name(&self) -> &'static str {
        "Tamper"
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let req = context.request_mut();
        let hash = req
            .headers()
            .get("x-amz-content-sha256")
            .unwrap_or_default()
            .to_string();
        assert!(
            hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "not a signed payload hash: {hash:?}"
        );
        assert!(req.headers().get("authorization").is_some());
        *req.body_mut() = SdkBody::from(self.body.clone());
        Ok(())
    }
}

fn client(endpoint: &str, ak: &str, sk: &str, tamper: Option<Vec<u8>>) -> Client {
    let mut cfg = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new(ak, sk, None, None, "test"))
        // No checksum trailer: the body is sent as is, with its SHA-256
        // in the signed `x-amz-content-sha256`.
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled());
    if let Some(body) = tamper {
        cfg = cfg.interceptor(Tamper { body });
    }
    Client::from_conf(cfg.build())
}

fn status_code<E: ProvideErrorMetadata + std::fmt::Debug>(
    e: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
) -> (u16, String) {
    match e {
        SdkError::ServiceError(s) => (
            s.raw().status().as_u16(),
            s.err().code().unwrap_or("").to_string(),
        ),
        other => (0, format!("{other:?}")),
    }
}

async fn cluster() -> TestCluster {
    TestCluster::start(ClusterOptions {
        groups: 2,
        authorize: true,
        principals: vec![(
            "AKIATENANTB0000000000".into(),
            "secret-b".repeat(4),
            "tenant-b".into(),
        )],
        ..ClusterOptions::rs(3, 2, 1, MIB as u64, 64 << 10)
    })
    .await
}

/// A signed PUT and UploadPart whose bodies were replaced after signing:
/// `400 XAmzContentSHA256Mismatch`, the earlier object and the upload are
/// unchanged. With `cluster`, the refused writes' pending generations are
/// aborted and their shards reclaimed: a GC pass now (not after a lease
/// expiry) and the node reconciliation of shards placed before the end of
/// the body but not yet recorded (RFC-004 §5.10) bring the shard count
/// back to that before the refused writes.
#[allow(clippy::too_many_lines)]
async fn payload_mismatch(endpoint: &str, ak: &str, sk: &str, cluster: Option<&TestCluster>) {
    let shards = || async {
        match cluster {
            Some(c) => {
                c.gc(now_ms(), 0).await;
                c.reconcile(now_ms() + 600_000, 0).await;
                Some(c.total_shards().await)
            }
            None => None,
        }
    };
    let good = client(endpoint, ak, sk, None);
    let bucket = "mismatch";
    good.create_bucket().bucket(bucket).send().await.unwrap();
    // Larger than a chunk: chunks are placed before the end of the body,
    // where the mismatch is found.
    let a = random_bytes(3 * MIB + 17, 1);
    let b = random_bytes(a.len(), 2);
    good.put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(a.clone()))
        .send()
        .await
        .unwrap();
    let baseline = shards().await;
    let bad = client(endpoint, ak, sk, Some(b.clone()));
    let err = bad
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(a.clone()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err), (400, "XAmzContentSHA256Mismatch".into()));
    let err = bad
        .put_object()
        .bucket(bucket)
        .key("new")
        .body(ByteStream::from(a.clone()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err), (400, "XAmzContentSHA256Mismatch".into()));
    let got = good
        .get_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), a);
    let err = good
        .head_object()
        .bucket(bucket)
        .key("new")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err).0, 404);
    assert_eq!(shards().await, baseline, "shards of refused PUTs remain");
    if cluster.is_none() {
        return;
    }

    let up = good
        .create_multipart_upload()
        .bucket(bucket)
        .key("mp")
        .send()
        .await
        .unwrap();
    let id = up.upload_id().unwrap();
    let err = bad
        .upload_part()
        .bucket(bucket)
        .key("mp")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from(a.clone()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err), (400, "XAmzContentSHA256Mismatch".into()));
    let parts = good
        .list_parts()
        .bucket(bucket)
        .key("mp")
        .upload_id(id)
        .send()
        .await
        .unwrap();
    assert!(parts.parts().is_empty(), "{parts:?}");
    assert_eq!(shards().await, baseline, "shards of a refused part remain");
    // The same part sent intact is accepted and completes.
    let part = good
        .upload_part()
        .bucket(bucket)
        .key("mp")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from(a.clone()))
        .send()
        .await
        .unwrap();
    good.complete_multipart_upload()
        .bucket(bucket)
        .key("mp")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let got = good
        .get_object()
        .bucket(bucket)
        .key("mp")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), a);
}

#[tokio::test]
async fn signed_payload_mismatch_is_400_on_the_cluster_backend() {
    let c = cluster().await;
    let endpoint = format!("http://{}", c.gateway_addr);
    payload_mismatch(&endpoint, &c.credentials.0, &c.credentials.1, Some(&c)).await;
}

async fn memory_gateway() -> (arca_lake_gateway::ServerHandle, String, (String, String)) {
    let creds = (
        "AKIAMEMORY0000000000".to_string(),
        "memory-secret".repeat(3),
    );
    let cfg = GatewayConfig::new(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        Some(creds.clone()),
    );
    let handle = serve(cfg, MemoryBackend::new()).await.unwrap();
    let endpoint = format!("http://{}", handle.local_addr);
    (handle, endpoint, creds)
}

#[tokio::test]
async fn signed_payload_mismatch_is_400_on_the_memory_backend() {
    let (handle, endpoint, (ak, sk)) = memory_gateway().await;
    payload_mismatch(&endpoint, &ak, &sk, None).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn recreating_a_bucket_names_its_owner() {
    let c = cluster().await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let a = client(&endpoint, &c.credentials.0, &c.credentials.1, None);
    let b = client(
        &endpoint,
        "AKIATENANTB0000000000",
        &"secret-b".repeat(4),
        None,
    );
    a.create_bucket().bucket("owned").send().await.unwrap();
    let err = a.create_bucket().bucket("owned").send().await.unwrap_err();
    assert_eq!(status_code(&err), (409, "BucketAlreadyOwnedByYou".into()));
    let err = b.create_bucket().bucket("owned").send().await.unwrap_err();
    assert_eq!(status_code(&err), (409, "BucketAlreadyExists".into()));
    // Nothing changed: the bucket is still tenant-a's alone.
    let names = |out: aws_sdk_s3::operation::list_buckets::ListBucketsOutput| {
        out.buckets()
            .iter()
            .map(|x| x.name().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(names(a.list_buckets().send().await.unwrap()), ["owned"]);
    assert_eq!(names(b.list_buckets().send().await.unwrap()), [""; 0]);

    let (handle, endpoint, (ak, sk)) = memory_gateway().await;
    let m = client(&endpoint, &ak, &sk, None);
    m.create_bucket().bucket("owned").send().await.unwrap();
    let err = m.create_bucket().bucket("owned").send().await.unwrap_err();
    assert_eq!(status_code(&err), (409, "BucketAlreadyOwnedByYou".into()));
    handle.shutdown().await;
}
