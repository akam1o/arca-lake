//! `x-amz-checksum-*` of the XML request bodies through SigV4-signed
//! clients (RFC-007 §5.7): a value that does not match the body is `400
//! BadDigest`, a malformed one `400 InvalidRequest`, and the operation is
//! not applied, on DeleteObjects, PutBucketPolicy, PutBucketVersioning and
//! CreateBucket; CompleteMultipartUpload's header stays the object's
//! checksum.

use arca_lake_gateway::checksum;
use arca_lake_metadata::records::ChecksumAlgorithm;
use arca_lake_testkit::{ClusterOptions, TestCluster};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::interceptors::{
    BeforeTransmitInterceptorContextMut, BeforeTransmitInterceptorContextRef,
};
use aws_sdk_s3::config::{
    ConfigBag, Intercept, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    RuntimeComponents,
};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    VersioningConfiguration,
};

const MIB: usize = 1 << 20;
const BUCKET: &str = "xmlcs";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ALGOS: [(ChecksumAlgorithm, &str); 5] = [
    (ChecksumAlgorithm::Crc32, "x-amz-checksum-crc32"),
    (ChecksumAlgorithm::Crc32c, "x-amz-checksum-crc32c"),
    (ChecksumAlgorithm::Crc64Nvme, "x-amz-checksum-crc64nvme"),
    (ChecksumAlgorithm::Sha1, "x-amz-checksum-sha1"),
    (ChecksumAlgorithm::Sha256, "x-amz-checksum-sha256"),
];

/// What a checksum header carries.
#[derive(Debug, Clone, Copy)]
enum Value {
    /// The checksum of the body.
    Right,
    /// The checksum of other bytes.
    Wrong,
    /// Not base64 of the algorithm's length.
    Malformed,
}

/// Replaces the SDK's checksum headers with `name` before signing, its
/// value computed from the serialized body; checks that the signed request
/// carries exactly that checksum header.
#[derive(Debug, Clone)]
struct SetChecksum {
    algo: ChecksumAlgorithm,
    name: &'static str,
    value: Value,
}

impl Intercept for SetChecksum {
    fn name(&self) -> &'static str {
        "SetChecksum"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let req = context.request_mut();
        let body = req.body().bytes().expect("an in-memory XML body").to_vec();
        let value = match self.value {
            Value::Right => checksum::encode_b64(&checksum::digest(self.algo, &body)),
            Value::Wrong => checksum::encode_b64(&checksum::digest(self.algo, b"other bytes")),
            Value::Malformed => "AAAA".to_string(),
        };
        let names: Vec<String> = req
            .headers()
            .iter()
            .map(|(k, _)| k.to_string())
            .filter(|k| k.starts_with("x-amz-checksum-") || k == "x-amz-sdk-checksum-algorithm")
            .collect();
        for n in names {
            req.headers_mut().remove(n);
        }
        req.headers_mut().insert(self.name, value);
        Ok(())
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let h = context.request().headers();
        assert!(h.get("authorization").is_some(), "unsigned request");
        let checksums: Vec<_> = h
            .iter()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with("x-amz-checksum-"))
            .collect();
        assert_eq!(
            checksums,
            [self.name],
            "the SDK changed the checksum headers"
        );
        Ok(())
    }
}

fn set(algo: ChecksumAlgorithm, name: &'static str, value: Value) -> SetChecksum {
    SetChecksum { algo, name, value }
}

fn client(endpoint: &str, ak: &str, sk: &str) -> Client {
    let cfg = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new(ak, sk, None, None, "test"))
        .request_checksum_calculation(RequestChecksumCalculation::WhenSupported)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .build();
    Client::from_conf(cfg)
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

fn refused(value: Value) -> (u16, String) {
    match value {
        Value::Wrong => (400, "BadDigest".into()),
        Value::Malformed => (400, "InvalidRequest".into()),
        Value::Right => unreachable!(),
    }
}

async fn exists(s3: &Client, key: &str) -> bool {
    s3.head_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .is_ok()
}

async fn cluster() -> TestCluster {
    TestCluster::start(ClusterOptions {
        groups: 2,
        authorize: true,
        ..ClusterOptions::rs(3, 2, 1, MIB as u64, 64 << 10)
    })
    .await
}

#[tokio::test]
async fn xml_body_checksums_are_verified() {
    let c = cluster().await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let s3 = client(&endpoint, &c.credentials.0, &c.credentials.1);
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();

    // The SDK's own checksums (CRC32 for DeleteObjects and
    // PutBucketPolicy) are accepted.
    s3.put_object()
        .bucket(BUCKET)
        .key("k")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();
    let delete = || {
        s3.delete_objects().bucket(BUCKET).delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("k").build().unwrap())
                .build()
                .unwrap(),
        )
    };
    delete().send().await.unwrap();
    assert!(!exists(&s3, "k").await);

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::xmlcs/*"}]}"#;
    let versioning = || {
        s3.put_bucket_versioning()
            .bucket(BUCKET)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
    };
    for (algo, name) in ALGOS {
        for value in [Value::Wrong, Value::Malformed] {
            let ctx = format!("{name} {value:?}");
            // DeleteObjects: not deleted.
            s3.put_object()
                .bucket(BUCKET)
                .key("k")
                .body(ByteStream::from_static(b"data"))
                .send()
                .await
                .unwrap();
            let err = delete()
                .customize()
                .interceptor(set(algo, name, value))
                .send()
                .await
                .unwrap_err();
            assert_eq!(status_code(&err), refused(value), "{ctx}");
            assert!(exists(&s3, "k").await, "{ctx}: deleted anyway");
            // PutBucketPolicy: not stored.
            let err = s3
                .put_bucket_policy()
                .bucket(BUCKET)
                .policy(policy)
                .customize()
                .interceptor(set(algo, name, value))
                .send()
                .await
                .unwrap_err();
            assert_eq!(status_code(&err), refused(value), "{ctx}");
            let err = s3
                .get_bucket_policy()
                .bucket(BUCKET)
                .send()
                .await
                .unwrap_err();
            assert_eq!(status_code(&err).1, "NoSuchBucketPolicy", "{ctx}");
            // PutBucketVersioning: unchanged.
            let err = versioning()
                .customize()
                .interceptor(set(algo, name, value))
                .send()
                .await
                .unwrap_err();
            assert_eq!(status_code(&err), refused(value), "{ctx}");
            let v = s3
                .get_bucket_versioning()
                .bucket(BUCKET)
                .send()
                .await
                .unwrap();
            assert_eq!(v.status(), None, "{ctx}");
            // CreateBucket with a configuration body: not created.
            let err = s3
                .create_bucket()
                .bucket("xmlcs-new")
                .create_bucket_configuration(
                    aws_sdk_s3::types::CreateBucketConfiguration::builder().build(),
                )
                .customize()
                .interceptor(set(algo, name, value))
                .send()
                .await
                .unwrap_err();
            assert_eq!(status_code(&err), refused(value), "{ctx}");
            assert!(
                s3.head_bucket().bucket("xmlcs-new").send().await.is_err(),
                "{ctx}: created anyway"
            );
        }
        // The right value of every algorithm is accepted.
        delete()
            .customize()
            .interceptor(set(algo, name, Value::Right))
            .send()
            .await
            .unwrap();
        assert!(!exists(&s3, "k").await, "{name}");
    }
    s3.put_bucket_policy()
        .bucket(BUCKET)
        .policy(policy)
        .customize()
        .interceptor(set(
            ChecksumAlgorithm::Sha256,
            "x-amz-checksum-sha256",
            Value::Right,
        ))
        .send()
        .await
        .unwrap();
    s3.get_bucket_policy().bucket(BUCKET).send().await.unwrap();
    versioning()
        .customize()
        .interceptor(set(
            ChecksumAlgorithm::Crc32c,
            "x-amz-checksum-crc32c",
            Value::Right,
        ))
        .send()
        .await
        .unwrap();
    let v = s3
        .get_bucket_versioning()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(v.status(), Some(&BucketVersioningStatus::Enabled));
}

/// CompleteMultipartUpload's `x-amz-checksum-*` is the object's checksum
/// (compared by the backend with the one derived from the parts), not the
/// checksum of its XML body: the right object checksum completes.
#[tokio::test]
async fn complete_checksum_header_is_the_objects() {
    let c = cluster().await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let s3 = client(&endpoint, &c.credentials.0, &c.credentials.1);
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    let data = arca_lake_testkit::random_bytes(1000, 1);
    let up = s3
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
        .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
        .send()
        .await
        .unwrap();
    let id = up.upload_id().unwrap();
    let part = s3
        .upload_part()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(id)
        .part_number(1)
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();
    let full = checksum::encode_b64(&checksum::digest(ChecksumAlgorithm::Crc32, &data));
    s3.complete_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(id)
        .checksum_crc32(&full)
        .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .set_checksum_crc32(part.checksum_crc32().map(str::to_string))
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(exists(&s3, "mp").await);
}
