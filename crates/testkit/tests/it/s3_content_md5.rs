//! `Content-MD5` through SigV4-signed clients on the cluster and the memory
//! backends (RFC-007 §5.7): a malformed value is `400 InvalidDigest`, a
//! value that does not match the body `400 BadDigest`, and nothing is
//! committed, for single and multi-chunk bodies, `aws-chunked` uploads,
//! UploadPart, and the XML bodies of DeleteObjects and PutBucketPolicy.

#![allow(clippy::too_many_lines)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use arca_lake_gateway::memory::MemoryBackend;
use arca_lake_gateway::{GatewayConfig, serve};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
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
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};

const MIB: usize = 1 << 20;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Sets `Content-MD5` before signing and records the request's
/// `x-amz-content-sha256` (to tell `aws-chunked` uploads from plain ones).
#[derive(Debug, Clone)]
struct SetMd5 {
    value: String,
    payload: Arc<Mutex<Vec<String>>>,
}

impl Intercept for SetMd5 {
    fn name(&self) -> &'static str {
        "SetMd5"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        context
            .request_mut()
            .headers_mut()
            .insert("content-md5", self.value.clone());
        Ok(())
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let h = context.request().headers();
        assert_eq!(h.get("content-md5"), Some(self.value.as_str()));
        assert!(h.get("authorization").is_some(), "unsigned request");
        self.payload.lock().unwrap().push(
            h.get("x-amz-content-sha256")
                .unwrap_or_default()
                .to_string(),
        );
        Ok(())
    }
}

fn md5(value: &str) -> SetMd5 {
    SetMd5 {
        value: value.to_string(),
        payload: Arc::default(),
    }
}

fn b64_md5(data: &[u8]) -> String {
    use s3s::crypto::{Checksum as _, Md5};
    let mut m = Md5::new();
    m.update(data);
    arca_lake_gateway::checksum::encode_b64(m.finalize().as_ref())
}

/// `chunked`: the SDK's default CRC32 trailer, which makes PUT and
/// UploadPart `aws-chunked`; otherwise a plain signed body.
fn client(endpoint: &str, ak: &str, sk: &str, chunked: bool) -> Client {
    let calc = if chunked {
        RequestChecksumCalculation::WhenSupported
    } else {
        RequestChecksumCalculation::WhenRequired
    };
    let cfg = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new(ak, sk, None, None, "test"))
        .request_checksum_calculation(calc)
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

fn bad_digest() -> (u16, String) {
    (400, "BadDigest".into())
}

fn invalid_digest() -> (u16, String) {
    (400, "InvalidDigest".into())
}

/// The body as the SDK sends it: in memory (a signed payload with the
/// checksum, if any, as a header), or streamed from a file, which the SDK
/// sends `aws-chunked` with a CRC32 trailer when checksums are on.
async fn body(data: &[u8], streamed: bool) -> (ByteStream, Option<tempfile::TempDir>) {
    if !streamed {
        return (ByteStream::from(data.to_vec()), None);
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("body");
    std::fs::write(&path, data).unwrap();
    (ByteStream::from_path(&path).await.unwrap(), Some(dir))
}

async fn put(s3: &Client, key: &str, data: &[u8], header: SetMd5) -> Result<String, (u16, String)> {
    let (body, _dir) = body(data, key.starts_with("chunked")).await;
    s3.put_object()
        .bucket("md5")
        .key(key)
        .body(body)
        .customize()
        .interceptor(header)
        .send()
        .await
        .map(|o| o.e_tag().unwrap_or("").trim_matches('"').to_string())
        .map_err(|e| status_code(&e))
}

async fn get(s3: &Client, key: &str) -> Option<Vec<u8>> {
    let out = s3.get_object().bucket("md5").key(key).send().await.ok()?;
    Some(out.body.collect().await.unwrap().into_bytes().to_vec())
}

/// PUT: the matching digest is accepted, a wrong or malformed one refused
/// with nothing stored, over a plain and an `aws-chunked` body.
async fn put_checks(s3: &Client, chunked: bool, cluster: Option<&TestCluster>) {
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
    let tag = if chunked { "chunked" } else { "plain" };
    // Several chunks (1 MiB on the cluster), and a small object.
    for (i, len) in [3 * MIB + 17, 1000].into_iter().enumerate() {
        let key = format!("{tag}/{i}");
        let a = random_bytes(len, 1);
        let b = random_bytes(len, 2);
        let header = md5(&b64_md5(&a));
        let etag = put(s3, &key, &a, header.clone()).await.unwrap();
        assert_eq!(
            arca_lake_gateway::checksum::unhex(&etag)
                .map(|d| arca_lake_gateway::checksum::encode_b64(&d)),
            Some(b64_md5(&a))
        );
        let payload = header.payload.lock().unwrap().clone();
        assert_eq!(
            payload.iter().all(|p| p.starts_with("STREAMING-")),
            chunked,
            "{payload:?}"
        );
        let baseline = shards().await;
        // B sent with A's digest: over an existing key and a new one.
        assert_eq!(
            put(s3, &key, &b, md5(&b64_md5(&a))).await,
            Err(bad_digest())
        );
        let fresh = format!("{key}-new");
        assert_eq!(
            put(s3, &fresh, &b, md5(&b64_md5(&a))).await,
            Err(bad_digest())
        );
        // Surrounding whitespace is ignored; unpadded base64 is malformed.
        let padded = b64_md5(&a);
        put(s3, &key, &a, md5(&format!(" {padded} ")))
            .await
            .unwrap();
        let unpadded = padded.trim_end_matches('=').to_string();
        for bad in ["not base64!", "AAAA", unpadded.as_str()] {
            assert_eq!(
                put(s3, &fresh, &b, md5(bad)).await,
                Err(invalid_digest()),
                "{bad:?}"
            );
        }
        assert_eq!(get(s3, &key).await, Some(a.clone()));
        assert_eq!(get(s3, &fresh).await, None);
        assert_eq!(shards().await, baseline, "shards of refused PUTs remain");
    }
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
async fn content_md5_on_put_and_upload_part_cluster() {
    let c = cluster().await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let (ak, sk) = (&c.credentials.0, &c.credentials.1);
    client(&endpoint, ak, sk, false)
        .create_bucket()
        .bucket("md5")
        .send()
        .await
        .unwrap();
    for chunked in [false, true] {
        let s3 = client(&endpoint, ak, sk, chunked);
        put_checks(&s3, chunked, Some(&c)).await;

        // UploadPart: a refused part is not recorded; the intact one
        // completes.
        let key = format!("mp-{chunked}");
        let a = random_bytes(3 * MIB + 5, 3);
        let b = random_bytes(a.len(), 4);
        let id = s3
            .create_multipart_upload()
            .bucket("md5")
            .key(&key)
            .send()
            .await
            .unwrap()
            .upload_id()
            .unwrap()
            .to_string();
        let part = |data: Vec<u8>, header: SetMd5| {
            let s3 = s3.clone();
            let (key, id) = (key.clone(), id.clone());
            async move {
                let (body, _dir) = body(&data, chunked).await;
                s3.upload_part()
                    .bucket("md5")
                    .key(&key)
                    .upload_id(&id)
                    .part_number(1)
                    .body(body)
                    .customize()
                    .interceptor(header)
                    .send()
                    .await
            }
        };
        let shards = || async {
            c.gc(now_ms(), 0).await;
            c.reconcile(now_ms() + 600_000, 0).await;
            c.total_shards().await
        };
        let baseline = shards().await;
        let err = part(b.clone(), md5(&b64_md5(&a))).await.unwrap_err();
        assert_eq!(status_code(&err), bad_digest());
        let err = part(b.clone(), md5("AAAA")).await.unwrap_err();
        assert_eq!(status_code(&err), invalid_digest());
        let parts = s3
            .list_parts()
            .bucket("md5")
            .key(&key)
            .upload_id(&id)
            .send()
            .await
            .unwrap();
        assert!(parts.parts().is_empty(), "{parts:?}");
        assert_eq!(shards().await, baseline, "shards of refused parts remain");
        let ok = part(a.clone(), md5(&b64_md5(&a))).await.unwrap();
        let complete = || {
            s3.complete_multipart_upload()
                .bucket("md5")
                .key(&key)
                .upload_id(&id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(
                            CompletedPart::builder()
                                .part_number(1)
                                .e_tag(ok.e_tag().unwrap())
                                .set_checksum_crc32(ok.checksum_crc32().map(str::to_string))
                                .build(),
                        )
                        .build(),
                )
        };
        // CompleteMultipartUpload's XML body with a wrong or malformed
        // digest: not completed, the part still listed.
        for (header, expected) in [
            (md5(&b64_md5(b"something else")), bad_digest()),
            (md5("AAAA"), invalid_digest()),
        ] {
            let err = complete()
                .customize()
                .interceptor(header)
                .send()
                .await
                .unwrap_err();
            assert_eq!(status_code(&err), expected);
            assert_eq!(get(&s3, &key).await, None, "completed anyway");
            let parts = s3
                .list_parts()
                .bucket("md5")
                .key(&key)
                .upload_id(&id)
                .send()
                .await
                .unwrap();
            assert_eq!(parts.parts().len(), 1);
        }
        complete().send().await.unwrap();
        assert_eq!(get(&s3, &key).await, Some(a));
    }
}

#[tokio::test]
async fn content_md5_on_xml_bodies_cluster() {
    let c = cluster().await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let s3 = client(&endpoint, &c.credentials.0, &c.credentials.1, false);
    s3.create_bucket().bucket("md5").send().await.unwrap();
    put(&s3, "k", b"data", md5(&b64_md5(b"data")))
        .await
        .unwrap();
    let delete = || {
        s3.delete_objects().bucket("md5").delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("k").build().unwrap())
                .build()
                .unwrap(),
        )
    };
    let err = delete()
        .customize()
        .interceptor(md5(&b64_md5(b"something else")))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err), bad_digest());
    let err = delete()
        .customize()
        .interceptor(md5("AAAA"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err), invalid_digest());
    assert_eq!(
        get(&s3, "k").await,
        Some(b"data".to_vec()),
        "deleted anyway"
    );

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::md5/*"}]}"#;
    let err = s3
        .put_bucket_policy()
        .bucket("md5")
        .policy(policy)
        .customize()
        .interceptor(md5(&b64_md5(b"something else")))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err), bad_digest());
    let err = s3
        .get_bucket_policy()
        .bucket("md5")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        status_code(&err).1,
        "NoSuchBucketPolicy",
        "policy stored anyway"
    );
    s3.put_bucket_policy()
        .bucket("md5")
        .policy(policy)
        .customize()
        .interceptor(md5(&b64_md5(policy.as_bytes())))
        .send()
        .await
        .unwrap();
    s3.get_bucket_policy().bucket("md5").send().await.unwrap();
}

#[tokio::test]
async fn content_md5_on_put_memory() {
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
    client(&endpoint, &creds.0, &creds.1, false)
        .create_bucket()
        .bucket("md5")
        .send()
        .await
        .unwrap();
    for chunked in [false, true] {
        put_checks(
            &client(&endpoint, &creds.0, &creds.1, chunked),
            chunked,
            None,
        )
        .await;
    }
    handle.shutdown().await;
}

/// Appends `?policy` to the request's URI before signing: an object PUT
/// whose query names a bucket subresource.
#[derive(Debug)]
struct PolicyQuery;

impl Intercept for PolicyQuery {
    fn name(&self) -> &'static str {
        "PolicyQuery"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let req = context.request_mut();
        let uri = req.uri().to_string();
        let sep = if uri.contains('?') { '&' } else { '?' };
        req.set_uri(format!("{uri}{sep}policy"))?;
        Ok(())
    }
}

/// Raw anonymous request with a body; returns the status and the body.
async fn anonymous(addr: SocketAddr, head: &str, body: &[u8]) -> (u16, String) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(
        format!(
            "{head}\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, text)
}

#[tokio::test]
async fn content_md5_routing_and_authentication_cluster() {
    let c = cluster().await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let (ak, sk) = (&c.credentials.0, &c.credentials.1);
    let s3 = client(&endpoint, ak, sk, false);
    s3.create_bucket().bucket("md5").send().await.unwrap();
    put(&s3, "k", b"data", md5(&b64_md5(b"data")))
        .await
        .unwrap();

    // An object PUT with a bucket subresource in its query and an
    // aws-chunked body is an object PUT: checked by the backend over the
    // decoded body, accepted with its digest.
    let chunked = client(&endpoint, ak, sk, true);
    let data = random_bytes(2 * MIB + 3, 9);
    let header = md5(&b64_md5(&data));
    let (b, _dir) = body(&data, true).await;
    chunked
        .put_object()
        .bucket("md5")
        .key("q")
        .body(b)
        .customize()
        .interceptor(PolicyQuery)
        .interceptor(header.clone())
        .send()
        .await
        .unwrap();
    assert!(
        header
            .payload
            .lock()
            .unwrap()
            .iter()
            .all(|p| p.starts_with("STREAMING-"))
    );
    assert_eq!(get(&s3, "q").await, Some(data));
    let err = s3
        .get_bucket_policy()
        .bucket("md5")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err).1, "NoSuchBucketPolicy");

    // Unauthenticated: AccessDenied, not a digest error (the body is not
    // read before authorization), and no policy stored.
    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::md5/*"}]}"#;
    for value in [b64_md5(b"something else"), "AAAA".to_string()] {
        let (status, text) = anonymous(
            c.gateway_addr,
            &format!("PUT /md5?policy HTTP/1.1\r\nContent-MD5: {value}"),
            policy,
        )
        .await;
        assert_eq!(status, 403, "{text}");
        assert!(text.contains("AccessDenied"), "{text}");
    }
    let err = s3
        .get_bucket_policy()
        .bucket("md5")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err).1, "NoSuchBucketPolicy");
    // DeleteObjects is authorized per entry (RFC-012), after its body is
    // read: an anonymous one with a wrong digest is refused for the digest
    // before any entry is considered; nothing is deleted either way.
    let xml = b"<Delete><Object><Key>k</Key></Object></Delete>";
    // With the right digest, every entry is AccessDenied (200).
    for (value, status, code) in [
        (b64_md5(b"something else"), 400, "<Code>BadDigest</Code>"),
        (b64_md5(xml), 200, "<Code>AccessDenied</Code><Key>k</Key>"),
    ] {
        let (got, text) = anonymous(
            c.gateway_addr,
            &format!("POST /md5?delete HTTP/1.1\r\nContent-MD5: {value}"),
            xml,
        )
        .await;
        assert_eq!(got, status, "{text}");
        assert!(text.contains(code), "{text}");
    }
    assert_eq!(get(&s3, "k").await, Some(b"data".to_vec()));

    // A malformed digest on a large XML body is answered, not cut off:
    // the client reads the 400 after sending the whole body.
    let objects: Vec<_> = (0..1000)
        .map(|i| {
            ObjectIdentifier::builder()
                .key(format!("{i:0>200}"))
                .build()
                .unwrap()
        })
        .collect();
    let unpadded = b64_md5(b"x").trim_end_matches('=').to_string();
    let err = s3
        .delete_objects()
        .bucket("md5")
        .delete(
            Delete::builder()
                .set_objects(Some(objects))
                .build()
                .unwrap(),
        )
        .customize()
        .interceptor(md5(&unpadded))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status_code(&err), invalid_digest());
    assert_eq!(get(&s3, "k").await, Some(b"data".to_vec()));
}
