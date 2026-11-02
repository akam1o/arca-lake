//! S3 compatibility through real clients against the in-process cluster
//! gateway (RFC-007 §11 A1/A2, RFC-012 §11): the AWS SDK for Rust (always),
//! the AWS CLI and the AWS SDK for Python (when installed), and the
//! authorization rules over HTTP with two principals and anonymous
//! requests.

#![allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty
)]

use std::collections::HashMap;
use std::path::Path;

use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Region, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::RequestId as _;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload,
    CompletedPart, Delete, MetadataDirective, ObjectIdentifier, VersioningConfiguration,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const MIB: usize = 1 << 20;

fn md5_hex(data: &[u8]) -> String {
    use s3s::crypto::{Checksum as _, Md5};
    let mut m = Md5::new();
    m.update(data);
    arca_lake_gateway::checksum::hex(m.finalize().as_ref())
}

fn client(endpoint: &str, ak: &str, sk: &str) -> Client {
    let cfg = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new(ak, sk, None, None, "test"))
        // SDK defaults: CRC32 trailers on uploads, validation on reads.
        .request_checksum_calculation(RequestChecksumCalculation::WhenSupported)
        .response_checksum_validation(ResponseChecksumValidation::WhenSupported)
        .build();
    Client::from_conf(cfg)
}

fn code<E: ProvideErrorMetadata + std::fmt::Debug, R: std::fmt::Debug>(
    e: &SdkError<E, R>,
) -> String {
    match e {
        SdkError::ServiceError(s) => s.err().code().unwrap_or("").to_string(),
        other => format!("{other:?}"),
    }
}

fn status<E>(e: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>) -> u16
where
    E: std::fmt::Debug,
{
    match e {
        SdkError::ServiceError(s) => s.raw().status().as_u16(),
        SdkError::ResponseError(r) => r.raw().status().as_u16(),
        _ => 0,
    }
}

async fn cluster(authorize: bool) -> TestCluster {
    TestCluster::start(ClusterOptions {
        groups: 2,
        authorize,
        principals: vec![(
            "AKIATENANTB0000000000".into(),
            "secret-b".repeat(4),
            "tenant-b".into(),
        )],
        ..ClusterOptions::rs(3, 2, 1, MIB as u64, 64 << 10)
    })
    .await
}

// ---- AWS SDK for Rust -----------------------------------------------------

#[tokio::test]
async fn rust_sdk_conformance() {
    let c = cluster(false).await;
    let s3 = client(
        &format!("http://{}", c.gateway_addr),
        &c.credentials.0,
        &c.credentials.1,
    );
    let bucket = "sdk-rust";
    s3.create_bucket().bucket(bucket).send().await.unwrap();

    // PUT with the SDK's default CRC32 trailer; GET validates the CRC32 we
    // return (checksum mode on by default in the SDK for validation).
    let data = random_bytes(3 * MIB + 321, 1);
    let put = s3
        .put_object()
        .bucket(bucket)
        .key("single")
        .content_type("application/x-test")
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(put.e_tag().unwrap().trim_matches('"'), md5_hex(&data));
    assert!(
        put.checksum_crc32().is_some(),
        "trailer checksum stored and echoed"
    );
    let got = s3
        .get_object()
        .bucket(bucket)
        .key("single")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some("application/x-test"));
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), data);
    // Explicit SHA256 and a wrong value.
    let put = s3
        .put_object()
        .bucket(bucket)
        .key("sha")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();
    let sha = put.checksum_sha256().unwrap().to_string();
    let head = s3
        .head_object()
        .bucket(bucket)
        .key("sha")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head.checksum_sha256(), Some(sha.as_str()));
    let err = s3
        .put_object()
        .bucket(bucket)
        .key("bad")
        .checksum_sha256("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "BadDigest");
    assert_eq!(
        code(
            &s3.head_object()
                .bucket(bucket)
                .key("bad")
                .send()
                .await
                .unwrap_err()
        ),
        "NotFound",
        "nothing was published by the rejected PUT"
    );
    // Range.
    let r = s3
        .get_object()
        .bucket(bucket)
        .key("single")
        .range("bytes=1048570-1048585")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.body.collect().await.unwrap().into_bytes(),
        &data[1_048_570..1_048_586]
    );

    // Multipart with SHA1 composite checksum.
    let parts = [
        random_bytes(MIB + MIB / 2, 2),
        random_bytes(MIB, 3),
        random_bytes(77, 4),
    ];
    let mpu = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key("mp")
        .checksum_algorithm(ChecksumAlgorithm::Sha1)
        .send()
        .await
        .unwrap();
    let uid = mpu.upload_id().unwrap().to_string();
    let mut completed = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        let up = s3
            .upload_part()
            .bucket(bucket)
            .key("mp")
            .upload_id(&uid)
            .part_number(i as i32 + 1)
            .checksum_algorithm(ChecksumAlgorithm::Sha1)
            .body(ByteStream::from(p.clone()))
            .send()
            .await
            .unwrap();
        assert_eq!(up.e_tag().unwrap().trim_matches('"'), md5_hex(p));
        completed.push(
            CompletedPart::builder()
                .part_number(i as i32 + 1)
                .e_tag(up.e_tag().unwrap())
                .checksum_sha1(up.checksum_sha1().unwrap())
                .build(),
        );
    }
    let listed = s3
        .list_parts()
        .bucket(bucket)
        .key("mp")
        .upload_id(&uid)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.parts().len(), 3);
    let uploads = s3
        .list_multipart_uploads()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(uploads.uploads().len(), 1);
    let done = s3
        .complete_multipart_upload()
        .bucket(bucket)
        .key("mp")
        .upload_id(&uid)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let etags: Vec<String> = parts.iter().map(|p| md5_hex(p)).collect();
    assert_eq!(
        done.e_tag().unwrap().trim_matches('"'),
        arca_lake_gateway::checksum::composite_etag(&etags)
    );
    assert!(done.checksum_sha1().unwrap().ends_with("-3"));
    let whole = parts.concat();
    let got = s3
        .get_object()
        .bucket(bucket)
        .key("mp")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), whole);
    let p2 = s3
        .get_object()
        .bucket(bucket)
        .key("mp")
        .part_number(2)
        .send()
        .await
        .unwrap();
    assert_eq!(p2.parts_count(), Some(3));
    assert_eq!(p2.body.collect().await.unwrap().into_bytes(), parts[1]);
    // Abort of a second upload.
    let uid2 = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key("gone")
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_string();
    s3.upload_part()
        .bucket(bucket)
        .key("gone")
        .upload_id(&uid2)
        .part_number(1)
        .body(ByteStream::from(random_bytes(1000, 5)))
        .send()
        .await
        .unwrap();
    s3.abort_multipart_upload()
        .bucket(bucket)
        .key("gone")
        .upload_id(&uid2)
        .send()
        .await
        .unwrap();
    assert!(
        s3.list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap()
            .uploads()
            .is_empty()
    );
    let err = s3
        .list_parts()
        .bucket(bucket)
        .key("gone")
        .upload_id(&uid2)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NoSuchUpload");

    // Versioning, markers, version-specific operations.
    s3.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        s3.get_bucket_versioning()
            .bucket(bucket)
            .send()
            .await
            .unwrap()
            .status(),
        Some(&BucketVersioningStatus::Enabled)
    );
    let v1 = s3
        .put_object()
        .bucket(bucket)
        .key("v")
        .body(ByteStream::from_static(b"one"))
        .send()
        .await
        .unwrap()
        .version_id()
        .unwrap()
        .to_string();
    let v2 = s3
        .put_object()
        .bucket(bucket)
        .key("v")
        .body(ByteStream::from_static(b"two"))
        .send()
        .await
        .unwrap()
        .version_id()
        .unwrap()
        .to_string();
    let old = s3
        .get_object()
        .bucket(bucket)
        .key("v")
        .version_id(&v1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        old.body.collect().await.unwrap().into_bytes().as_ref(),
        b"one"
    );
    let d = s3
        .delete_object()
        .bucket(bucket)
        .key("v")
        .send()
        .await
        .unwrap();
    assert_eq!(d.delete_marker(), Some(true));
    let marker = d.version_id().unwrap().to_string();
    let err = s3
        .get_object()
        .bucket(bucket)
        .key("v")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NoSuchKey");
    let lv = s3
        .list_object_versions()
        .bucket(bucket)
        .prefix("v")
        .send()
        .await
        .unwrap();
    let ids: Vec<&str> = lv
        .versions()
        .iter()
        .map(|v| v.version_id().unwrap())
        .collect();
    assert_eq!(ids, vec![v2.as_str(), v1.as_str()]);
    assert_eq!(lv.delete_markers().len(), 1);
    assert!(lv.delete_markers()[0].is_latest().unwrap());
    s3.delete_object()
        .bucket(bucket)
        .key("v")
        .version_id(&marker)
        .send()
        .await
        .unwrap();
    let cur = s3
        .get_object()
        .bucket(bucket)
        .key("v")
        .send()
        .await
        .unwrap();
    assert_eq!(cur.version_id(), Some(v2.as_str()));
    let etag = cur.e_tag().unwrap().to_string();
    assert_eq!(
        cur.body.collect().await.unwrap().into_bytes().as_ref(),
        b"two"
    );
    // Conditional reads and writes.
    let err = s3
        .get_object()
        .bucket(bucket)
        .key("v")
        .if_none_match(&etag)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 304);
    let err = s3
        .get_object()
        .bucket(bucket)
        .key("v")
        .if_match("\"0000\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "PreconditionFailed");
    let err = s3
        .put_object()
        .bucket(bucket)
        .key("v")
        .if_none_match("*")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "PreconditionFailed");
    // CopyObject of a version with REPLACE metadata; conditional copy.
    let cp = s3
        .copy_object()
        .bucket(bucket)
        .key("v-copy")
        .copy_source(format!("{bucket}/v?versionId={v1}"))
        .metadata_directive(MetadataDirective::Replace)
        .metadata("k", "v")
        .send()
        .await
        .unwrap();
    assert_eq!(cp.copy_source_version_id(), Some(v1.as_str()));
    let got = s3
        .get_object()
        .bucket(bucket)
        .key("v-copy")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.metadata().unwrap().get("k").map(String::as_str),
        Some("v")
    );
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes().as_ref(),
        b"one"
    );
    let err = s3
        .copy_object()
        .bucket(bucket)
        .key("v-copy2")
        .copy_source(format!("{bucket}/v"))
        .copy_source_if_none_match(&etag)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "PreconditionFailed");
    // DeleteObjects.
    s3.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .unwrap();
    for k in ["d/1", "d/2", "d/3"] {
        s3.put_object()
            .bucket(bucket)
            .key(k)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }
    let del = s3
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("d/1").build().unwrap())
                .objects(ObjectIdentifier::builder().key("d/2").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(del.deleted().len(), 2);
    let listed = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix("d/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = listed.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, vec!["d/3"]);
    c.shutdown().await;
}

// ---- authorization over HTTP (RFC-012 §5.3.2) ------------------------------

/// Raw anonymous HTTP request; returns the status code.
async fn anonymous(addr: std::net::SocketAddr, method: &str, path: &str) -> u16 {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(
        format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let head = String::from_utf8_lossy(&buf);
    head.split_whitespace().nth(1).unwrap().parse().unwrap()
}

#[tokio::test]
async fn authorization_rules_over_http() {
    let c = cluster(true).await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let a = client(&endpoint, &c.credentials.0, &c.credentials.1);
    let b = client(&endpoint, "AKIATENANTB0000000000", &"secret-b".repeat(4));
    let bucket = "tenant-a-bucket";
    a.create_bucket().bucket(bucket).send().await.unwrap();
    for k in ["private/x", "public/x", "public/secret1", "www/index.html"] {
        a.put_object()
            .bucket(bucket)
            .key(k)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
    }
    // Rule 1/7: tenant-b is denied everything on the bucket, with AccessDenied.
    assert_eq!(
        code(&b.head_bucket().bucket(bucket).send().await.unwrap_err()),
        ""
    );
    assert_eq!(
        status(&b.head_bucket().bucket(bucket).send().await.unwrap_err()),
        403
    );
    assert_eq!(
        code(
            &b.get_object()
                .bucket(bucket)
                .key("private/x")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert_eq!(
        code(&b.list_objects_v2().bucket(bucket).send().await.unwrap_err()),
        "AccessDenied"
    );
    assert_eq!(
        code(
            &b.put_object()
                .bucket(bucket)
                .key("new")
                .body(ByteStream::from_static(b"x"))
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert_eq!(
        code(&b.delete_bucket().bucket(bucket).send().await.unwrap_err()),
        "AccessDenied"
    );
    // A missing bucket is also AccessDenied for non-owners (no existence leak).
    assert_eq!(
        code(
            &b.get_object()
                .bucket("nope")
                .key("x")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    // Unknown access key: InvalidAccessKeyId.
    let stranger = client(&endpoint, "AKIAUNKNOWN0000000000", "whatever-secret-value");
    assert_eq!(
        code(&stranger.list_buckets().send().await.unwrap_err()),
        "InvalidAccessKeyId"
    );
    // Rule 3: ListBuckets is owner-scoped.
    b.create_bucket()
        .bucket("tenant-b-bucket")
        .send()
        .await
        .unwrap();
    let names = |out: &aws_sdk_s3::operation::list_buckets::ListBucketsOutput| {
        out.buckets()
            .iter()
            .map(|x| x.name().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(&a.list_buckets().send().await.unwrap()),
        vec![bucket.to_string()]
    );
    assert_eq!(
        names(&b.list_buckets().send().await.unwrap()),
        vec!["tenant-b-bucket".to_string()]
    );
    // Anonymous: denied without a policy.
    assert_eq!(
        anonymous(c.gateway_addr, "GET", &format!("/{bucket}/www/index.html")).await,
        403
    );
    assert_eq!(anonymous(c.gateway_addr, "GET", "/").await, 403);

    // Rule 5: unsupported elements are rejected at registration.
    let bad = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectAcl","Resource":"*"}]}"#;
    let err = a
        .put_bucket_policy()
        .bucket(bucket)
        .policy(bad)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "MalformedPolicy");
    assert_eq!(
        code(
            &a.get_bucket_policy()
                .bucket(bucket)
                .send()
                .await
                .unwrap_err()
        ),
        "NoSuchBucketPolicy"
    );
    // A policy: tenant-b may read public/* and list with prefix public/;
    // nobody may read public/secret*; anonymous may read www/*.
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[
          {{"Sid":"b-read","Effect":"Allow","Principal":{{"AWS":"tenant-b"}},"Action":["s3:GetObject"],"Resource":"arn:aws:s3:::{bucket}/public/*"}},
          {{"Sid":"b-list","Effect":"Allow","Principal":{{"AWS":"tenant-b"}},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::{bucket}","Condition":{{"StringLike":{{"s3:prefix":"public/*"}}}}}},
          {{"Sid":"no-secret","Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/public/secret*"}},
          {{"Sid":"www","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::{bucket}/www/*"}}
        ]}}"#
    );
    a.put_bucket_policy()
        .bucket(bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();
    assert_eq!(
        a.get_bucket_policy()
            .bucket(bucket)
            .send()
            .await
            .unwrap()
            .policy()
            .unwrap(),
        policy.as_str()
    );
    // Rule 7 for non-owner is not an existence leak within a granted prefix:
    // a missing key under public/ is NoSuchKey.
    assert!(
        b.get_object()
            .bucket(bucket)
            .key("public/x")
            .send()
            .await
            .is_ok()
    );
    assert_eq!(
        code(
            &b.get_object()
                .bucket(bucket)
                .key("public/missing")
                .send()
                .await
                .unwrap_err()
        ),
        "NoSuchKey"
    );
    assert_eq!(
        code(
            &b.get_object()
                .bucket(bucket)
                .key("private/x")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert!(
        b.list_objects_v2()
            .bucket(bucket)
            .prefix("public/")
            .send()
            .await
            .is_ok()
    );
    assert_eq!(
        code(
            &b.list_objects_v2()
                .bucket(bucket)
                .prefix("private/")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert_eq!(
        code(&b.list_objects_v2().bucket(bucket).send().await.unwrap_err()),
        "AccessDenied"
    );
    // Rule 2: explicit Deny wins, for tenant-b and for the owner.
    assert_eq!(
        code(
            &b.get_object()
                .bucket(bucket)
                .key("public/secret1")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert_eq!(
        code(
            &a.get_object()
                .bucket(bucket)
                .key("public/secret1")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert!(
        a.get_object()
            .bucket(bucket)
            .key("public/x")
            .send()
            .await
            .is_ok(),
        "owner otherwise unrestricted"
    );
    // Anonymous through the policy.
    assert_eq!(
        anonymous(c.gateway_addr, "GET", &format!("/{bucket}/www/index.html")).await,
        200
    );
    assert_eq!(
        anonymous(c.gateway_addr, "GET", &format!("/{bucket}/private/x")).await,
        403
    );
    assert_eq!(
        anonymous(c.gateway_addr, "PUT", &format!("/{bucket}/www/new")).await,
        403
    );
    // Rule 4: a Deny of everything applies to the owner, except the policy
    // operations, so the owner can repair the lock-out.
    let lockout = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::{bucket}","arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    a.put_bucket_policy()
        .bucket(bucket)
        .policy(&lockout)
        .send()
        .await
        .unwrap();
    assert_eq!(
        code(
            &a.get_object()
                .bucket(bucket)
                .key("public/x")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert_eq!(
        code(&a.list_objects_v2().bucket(bucket).send().await.unwrap_err()),
        "AccessDenied"
    );
    assert!(
        a.get_bucket_policy().bucket(bucket).send().await.is_ok(),
        "owner-override"
    );
    a.delete_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    assert!(
        a.get_object()
            .bucket(bucket)
            .key("public/x")
            .send()
            .await
            .is_ok()
    );
    assert_eq!(
        code(
            &b.get_object()
                .bucket(bucket)
                .key("public/x")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    // Multipart and copy actions map to PutObject / GetObject.
    assert_eq!(
        code(
            &b.create_multipart_upload()
                .bucket(bucket)
                .key("mp")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    assert_eq!(
        code(
            &b.copy_object()
                .bucket("tenant-b-bucket")
                .key("stolen")
                .copy_source(format!("{bucket}/public/x"))
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied",
        "copy source is authorized on the source bucket"
    );
    // Every response carries a request id.
    let out = a.head_bucket().bucket(bucket).send().await.unwrap();
    assert!(out.request_id().is_some_and(|r| !r.is_empty()));
    c.shutdown().await;
}

// ---- AWS CLI ----------------------------------------------------------------

#[tokio::test]
async fn aws_cli_conformance() {
    if !arca_lake_testkit::s3::AwsCli::available().await {
        eprintln!("aws CLI not found; skipping");
        return;
    }
    let c = cluster(false).await;
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config");
    std::fs::write(
        &config,
        "[default]\ns3 =\n  multipart_threshold = 5MB\n  multipart_chunksize = 5MB\n",
    )
    .unwrap();
    let mut cli = c.aws_cli();
    cli.config_file = Some(config);
    let bucket = "cli-compat";
    cli.ok(&["s3api", "create-bucket", "--bucket", bucket])
        .await;
    // `aws s3 cp` above the threshold is a multipart upload (3 parts).
    let big = random_bytes(12 * MIB + 7, 1);
    let big_path = dir.path().join("big.bin");
    std::fs::write(&big_path, &big).unwrap();
    cli.ok(&[
        "s3",
        "cp",
        big_path.to_str().unwrap(),
        &format!("s3://{bucket}/big"),
    ])
    .await;
    let head = cli
        .json(&["s3api", "head-object", "--bucket", bucket, "--key", "big"])
        .await;
    assert_eq!(head["ContentLength"].as_u64().unwrap() as usize, big.len());
    assert!(head["ETag"].as_str().unwrap().contains("-3"), "{head}");
    let out_path = dir.path().join("big.out");
    cli.ok(&[
        "s3",
        "cp",
        &format!("s3://{bucket}/big"),
        out_path.to_str().unwrap(),
    ])
    .await;
    assert_eq!(std::fs::read(&out_path).unwrap(), big);
    // s3api multipart with explicit parts, list-parts, list-multipart-uploads,
    // complete; then a second upload aborted.
    let mpu = cli
        .json(&[
            "s3api",
            "create-multipart-upload",
            "--bucket",
            bucket,
            "--key",
            "mp",
            "--checksum-algorithm",
            "CRC32C",
        ])
        .await;
    let uid = mpu["UploadId"].as_str().unwrap().to_string();
    let mut parts_json = Vec::new();
    for (i, seed) in [(1, 10u64), (2, 11), (3, 12)] {
        let p = random_bytes(if i == 3 { 999 } else { 5 * MIB }, seed);
        let path = dir.path().join(format!("part{i}"));
        std::fs::write(&path, &p).unwrap();
        let r = cli
            .json(&[
                "s3api",
                "upload-part",
                "--bucket",
                bucket,
                "--key",
                "mp",
                "--upload-id",
                &uid,
                "--part-number",
                &i.to_string(),
                "--body",
                path.to_str().unwrap(),
                "--checksum-algorithm",
                "CRC32C",
            ])
            .await;
        assert_eq!(r["ETag"].as_str().unwrap().trim_matches('"'), md5_hex(&p));
        parts_json.push(format!(
            r#"{{"PartNumber":{i},"ETag":{},"ChecksumCRC32C":{}}}"#,
            r["ETag"], r["ChecksumCRC32C"]
        ));
    }
    let listed = cli
        .json(&[
            "s3api",
            "list-parts",
            "--bucket",
            bucket,
            "--key",
            "mp",
            "--upload-id",
            &uid,
        ])
        .await;
    assert_eq!(listed["Parts"].as_array().unwrap().len(), 3);
    let uploads = cli
        .json(&["s3api", "list-multipart-uploads", "--bucket", bucket])
        .await;
    assert_eq!(uploads["Uploads"].as_array().unwrap().len(), 1);
    let spec = format!(r#"{{"Parts":[{}]}}"#, parts_json.join(","));
    let done = cli
        .json(&[
            "s3api",
            "complete-multipart-upload",
            "--bucket",
            bucket,
            "--key",
            "mp",
            "--upload-id",
            &uid,
            "--multipart-upload",
            &spec,
        ])
        .await;
    assert!(done["ETag"].as_str().unwrap().ends_with("-3\""), "{done}");
    assert!(
        done["ChecksumCRC32C"].as_str().unwrap().ends_with("-3"),
        "{done}"
    );
    let (ok, _, err) = cli
        .run(&[
            "s3api",
            "abort-multipart-upload",
            "--bucket",
            bucket,
            "--key",
            "mp",
            "--upload-id",
            &uid,
        ])
        .await;
    assert!(!ok && err.contains("NoSuchUpload"), "{err}");
    let got = dir.path().join("mp.out");
    cli.ok(&[
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        "mp",
        "--checksum-mode",
        "ENABLED",
        got.to_str().unwrap(),
    ])
    .await;
    assert_eq!(std::fs::read(&got).unwrap().len(), 10 * MIB + 999);
    let uid2 = cli
        .json(&[
            "s3api",
            "create-multipart-upload",
            "--bucket",
            bucket,
            "--key",
            "ab",
        ])
        .await["UploadId"]
        .as_str()
        .unwrap()
        .to_string();
    cli.ok(&[
        "s3api",
        "abort-multipart-upload",
        "--bucket",
        bucket,
        "--key",
        "ab",
        "--upload-id",
        &uid2,
    ])
    .await;
    let uploads = cli
        .json(&["s3api", "list-multipart-uploads", "--bucket", bucket])
        .await;
    assert!(uploads.get("Uploads").is_none(), "{uploads}");
    // Versioning.
    cli.ok(&[
        "s3api",
        "put-bucket-versioning",
        "--bucket",
        bucket,
        "--versioning-configuration",
        "Status=Enabled",
    ])
    .await;
    let v = cli
        .json(&["s3api", "get-bucket-versioning", "--bucket", bucket])
        .await;
    assert_eq!(v["Status"], "Enabled");
    let small = dir.path().join("small");
    std::fs::write(&small, b"one").unwrap();
    let v1 = cli
        .json(&[
            "s3api",
            "put-object",
            "--bucket",
            bucket,
            "--key",
            "v",
            "--body",
            small.to_str().unwrap(),
        ])
        .await["VersionId"]
        .as_str()
        .unwrap()
        .to_string();
    std::fs::write(&small, b"two").unwrap();
    cli.ok(&[
        "s3api",
        "put-object",
        "--bucket",
        bucket,
        "--key",
        "v",
        "--body",
        small.to_str().unwrap(),
    ])
    .await;
    let d = cli
        .json(&["s3api", "delete-object", "--bucket", bucket, "--key", "v"])
        .await;
    assert_eq!(d["DeleteMarker"], true);
    let lv = cli
        .json(&[
            "s3api",
            "list-object-versions",
            "--bucket",
            bucket,
            "--prefix",
            "v",
        ])
        .await;
    assert_eq!(lv["Versions"].as_array().unwrap().len(), 2);
    assert_eq!(lv["DeleteMarkers"].as_array().unwrap().len(), 1);
    let old = dir.path().join("old");
    cli.ok(&[
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        "v",
        "--version-id",
        &v1,
        old.to_str().unwrap(),
    ])
    .await;
    assert_eq!(std::fs::read(&old).unwrap(), b"one");
    let (ok, _, err) = cli
        .run(&["s3api", "head-object", "--bucket", bucket, "--key", "v"])
        .await;
    assert!(!ok && err.contains("404"), "{err}");
    // copy-object and delete-objects.
    let cp = cli
        .json(&[
            "s3api",
            "copy-object",
            "--bucket",
            bucket,
            "--key",
            "v-copy",
            "--copy-source",
            &format!("{bucket}/v?versionId={v1}"),
        ])
        .await;
    assert_eq!(cp["CopySourceVersionId"], v1);
    let del = cli
        .json(&[
            "s3api",
            "delete-objects",
            "--bucket",
            bucket,
            "--delete",
            r#"{"Objects":[{"Key":"v-copy"},{"Key":"missing"}],"Quiet":false}"#,
        ])
        .await;
    assert_eq!(del["Deleted"].as_array().unwrap().len(), 2);
    c.shutdown().await;
}

// ---- AWS SDK for Python (boto3) ----------------------------------------------

#[tokio::test]
async fn boto3_conformance() {
    let have = tokio::process::Command::new("python3")
        .args(["-c", "import boto3"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success());
    if !have {
        eprintln!("python3 with boto3 not found; skipping");
        return;
    }
    let c = cluster(false).await;
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/compat/boto3_compat.py");
    let out = tokio::process::Command::new("python3")
        .arg(&script)
        .arg(format!("http://{}", c.gateway_addr))
        .arg(&c.credentials.0)
        .arg(&c.credentials.1)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .envs(HashMap::<String, String>::new())
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "boto3 scenarios failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    c.shutdown().await;
}
