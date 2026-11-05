//! Security and audit (RFC-012 S-A5, S-A6, S-A9; rotation under load;
//! tenant isolation) over HTTP with the AWS SDK.

#![allow(clippy::too_many_lines)]

use std::io::Write as _;
use std::sync::{Arc, Mutex};

use arca_lake_gateway::auth::CredentialStore;
use arca_lake_metadata::ops::bucket;
use arca_lake_testkit::{ClusterOptions, TestCluster};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Region, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;

const KEY_B: &str = "AKIATENANTB0000000000";
const SECRET_B: &str = "secret-b-secret-b-secret-b-secret-b";

fn client(endpoint: &str, ak: &str, sk: &str) -> Client {
    let cfg = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new(ak, sk, None, None, "test"))
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
        other => panic!("{other:?}"),
    }
}

async fn cluster_with(opts: ClusterOptions) -> TestCluster {
    TestCluster::start(ClusterOptions {
        authorize: true,
        ..opts
    })
    .await
}

fn two_tenants() -> ClusterOptions {
    ClusterOptions {
        principals: vec![(KEY_B.into(), SECRET_B.into(), "tenant-b".into())],
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 64 << 10)
    }
}

#[tokio::test]
async fn corrupt_stored_policy_denies_everyone_but_the_owner_repairs_it_and_unsupported_elements_are_refused()
 {
    let c = cluster_with(two_tenants()).await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let a = client(&endpoint, &c.credentials.0, &c.credentials.1);
    let b = client(&endpoint, KEY_B, SECRET_B);
    let bucket = "policy-bucket";
    a.create_bucket().bucket(bucket).send().await.unwrap();
    a.put_object()
        .bucket(bucket)
        .key("public/x")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();
    let allow_b = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Sid":"b","Effect":"Allow","Principal":{{"AWS":"tenant-b"}},"Action":["s3:GetObject"],"Resource":"arn:aws:s3:::{bucket}/public/*"}}]}}"#
    );
    a.put_bucket_policy()
        .bucket(bucket)
        .policy(&allow_b)
        .send()
        .await
        .unwrap();
    b.get_object()
        .bucket(bucket)
        .key("public/x")
        .send()
        .await
        .unwrap();

    // S-A5: every unsupported element is refused with MalformedPolicy and
    // the stored policy is untouched.
    for bad in [
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","NotAction":"s3:GetObject","Resource":"*"}]}"#,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"tenant-b"},"Action":"s3:GetObject","Resource":"*","Condition":{"StringLike":{"aws:UserAgent":"x"}}}]}"#,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"x"},"Action":"s3:GetObject","Resource":"*"}]}"#,
        r#"{"Version":"2012-10-17","Id":"x","Statement":[]}"#,
        r#"{"Version":"2008-10-17","Statement":[]}"#,
        "{not json",
    ] {
        let err = a
            .put_bucket_policy()
            .bucket(bucket)
            .policy(bad)
            .send()
            .await
            .unwrap_err();
        assert_eq!(code(&err), "MalformedPolicy", "{bad}: {err:?}");
    }
    let stored = bucket::get_bucket(c.meta.as_ref(), bucket)
        .await
        .unwrap()
        .policy;
    assert_eq!(stored.as_deref(), Some(allow_b.as_str()));

    // S-A6: a policy corrupted in storage (bypassing the gateway's
    // validation) denies non-owners and the owner's data operations; the
    // owner keeps Get/Put/DeleteBucketPolicy and can replace it.
    bucket::set_policy(c.meta.as_ref(), bucket, Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","NotAction":"s3:*","Resource":"*"}]}"#))
        .await
        .unwrap();
    let err = b
        .get_object()
        .bucket(bucket)
        .key("public/x")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "AccessDenied");
    let err = a
        .get_object()
        .bucket(bucket)
        .key("public/x")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        code(&err),
        "AccessDenied",
        "the owner's data access is denied too (rule 6)"
    );
    let got = a.get_bucket_policy().bucket(bucket).send().await.unwrap();
    assert!(
        got.policy().unwrap().contains("NotAction"),
        "the owner can inspect the corrupt document"
    );
    a.put_bucket_policy()
        .bucket(bucket)
        .policy(&allow_b)
        .send()
        .await
        .unwrap();
    a.get_object()
        .bucket(bucket)
        .key("public/x")
        .send()
        .await
        .unwrap();
    b.get_object()
        .bucket(bucket)
        .key("public/x")
        .send()
        .await
        .unwrap();
    // Corrupt again, then the owner deletes the policy: default rules apply.
    bucket::set_policy(c.meta.as_ref(), bucket, Some("{not json"))
        .await
        .unwrap();
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
    a.delete_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    a.get_object()
        .bucket(bucket)
        .key("public/x")
        .send()
        .await
        .unwrap();
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
    c.shutdown().await;
}

/// Captures every `arca.audit` line emitted on this thread's subscriber.
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Sink {
    fn lines(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .map(str::to_string)
            .collect()
    }
}

/// The process-wide audit sink: one subscriber per test binary.
fn audit_sink() -> Sink {
    static SINK: std::sync::OnceLock<Sink> = std::sync::OnceLock::new();
    SINK.get_or_init(|| {
        let sink = Sink::default();
        let writer = sink.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new("arca.audit=info"))
            .with_ansi(false)
            .json()
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("first subscriber");
        sink
    })
    .clone()
}

#[tokio::test]
async fn audit_log_has_one_record_per_request_and_no_secret_material() {
    let sink = audit_sink();

    let c = cluster_with(two_tenants()).await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let a = client(&endpoint, &c.credentials.0, &c.credentials.1);
    let b = client(&endpoint, KEY_B, SECRET_B);
    let stranger = client(
        &endpoint,
        "AKIAUNKNOWN0000000000",
        "stranger-secret-value-000",
    );
    let bucket = "audited";
    let mut requests = 0u32;
    a.create_bucket().bucket(bucket).send().await.unwrap();
    requests += 1;
    a.put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from_static(b"payload"))
        .send()
        .await
        .unwrap();
    requests += 1;
    a.get_object().bucket(bucket).key("k").send().await.unwrap();
    requests += 1;
    assert_eq!(
        code(
            &b.get_object()
                .bucket(bucket)
                .key("k")
                .send()
                .await
                .unwrap_err()
        ),
        "AccessDenied"
    );
    requests += 1;
    assert_eq!(
        code(&stranger.list_buckets().send().await.unwrap_err()),
        "InvalidAccessKeyId"
    );
    requests += 1;
    a.put_bucket_policy()
        .bucket(bucket)
        .policy(format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Sid":"deny-b","Effect":"Deny","Principal":{{"AWS":"tenant-b"}},"Action":"s3:*","Resource":["arn:aws:s3:::{bucket}","arn:aws:s3:::{bucket}/*"]}}]}}"#
        ))
        .send()
        .await
        .unwrap();
    requests += 1;
    assert_eq!(
        code(&b.head_bucket().bucket(bucket).send().await.unwrap_err()),
        ""
    );
    requests += 1;
    a.delete_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    requests += 1;
    // Anonymous (unsigned) request.
    let resp = reqwest_get(&format!("{endpoint}/{bucket}/k")).await;
    assert_eq!(resp, 403);
    requests += 1;
    let secret_a = c.credentials.1.clone();
    c.shutdown().await;

    // Other tests of this binary share the sink: keep this test's requests
    // (its bucket, its stranger key).
    let lines: Vec<String> = sink
        .lines()
        .into_iter()
        .filter(|l| l.contains("arca.audit"))
        .filter(|l| {
            l.contains(&format!("\"bucket\":\"{bucket}\""))
                || l.contains("\"key_id\":\"AKIAUNKNOWN0000000000\"")
        })
        .collect();
    assert_eq!(
        u32::try_from(lines.len()).unwrap(),
        requests,
        "one record per request: {lines:#?}"
    );
    // Automated scan (S-A9): no secret key, no signature, no authorization
    // header material, no credential scope in any record.
    let secrets = [
        secret_a.as_str(),
        SECRET_B,
        "stranger-secret-value-000",
        "AWS4-HMAC-SHA256",
        "Signature=",
        "X-Amz-Signature",
        "X-Amz-Credential",
        "Authorization",
        "x-amz-security-token",
    ];
    for l in &lines {
        for s in secrets {
            assert!(!l.contains(s), "audit record leaks {s:?}: {l}");
        }
        for field in [
            "\"request_id\":",
            "\"principal\":",
            "\"key_id\":",
            "\"operation\":",
            "\"decision\":",
            "\"status\":",
        ] {
            assert!(l.contains(field), "audit record lacks {field}: {l}");
        }
    }
    let ids: std::collections::HashSet<String> = lines
        .iter()
        .map(|l| {
            let i = l.find("\"request_id\":\"").unwrap() + 14;
            l[i..i + 32].to_string()
        })
        .collect();
    assert_eq!(ids.len(), lines.len(), "request ids are unique");
    // Decisions and principals are recorded as designed.
    assert!(
        lines
            .iter()
            .any(|l| l.contains("\"principal\":\"tenant-a\"")
                && l.contains("\"decision\":\"owner\"")),
        "{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("\"principal\":\"tenant-b\"")
                && l.contains("\"decision\":\"default-deny\"")),
        "{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("\"principal\":\"tenant-b\"")
                && l.contains("\"decision\":\"deny:deny-b\"")),
        "{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("\"key_id\":\"AKIAUNKNOWN0000000000\"")
                && l.contains("\"status\":403")),
        "{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("\"principal\":\"anonymous\"")),
        "{lines:#?}"
    );
}

/// Plain unsigned GET; returns the status code.
async fn reqwest_get(url: &str) -> u16 {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let url = url.strip_prefix("http://").unwrap();
    let (host, path) = url.split_once('/').unwrap();
    let mut s = tokio::net::TcpStream::connect(host).await.unwrap();
    s.write_all(
        format!("GET /{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out.split_whitespace().nth(1).unwrap().parse().unwrap()
}

fn write_credentials(path: &std::path::Path, keys: &[(&str, &str, &str)]) {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut text = String::from("[[principal]]\nname = \"tenant-a\"\n");
    for (id, secret, state) in keys {
        use std::fmt::Write as _;
        let _ = write!(
            text,
            "[[principal.key]]\nid = \"{id}\"\nsecret = \"{secret}\"\nstate = \"{state}\"\n"
        );
    }
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .unwrap();
    f.write_all(text.as_bytes()).unwrap();
    f.sync_all().unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

#[tokio::test]
async fn credential_rotation_under_concurrent_requests_never_fails_a_valid_key_nor_accepts_a_retired_one()
 {
    const K1: &str = "AKIAROTATE10000000000";
    const S1: &str = "rotation-secret-one-rotation-secret-one";
    const K2: &str = "AKIAROTATE20000000000";
    const S2: &str = "rotation-secret-two-rotation-secret-two";
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("credentials.toml");
    write_credentials(&path, &[(K1, S1, "active")]);
    let store = Arc::new(CredentialStore::load(&path).unwrap());
    let c = cluster_with(ClusterOptions {
        credential_store: Some(Arc::clone(&store)),
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 64 << 10)
    })
    .await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let k1 = client(&endpoint, K1, S1);
    let k2 = client(&endpoint, K2, S2);
    let bucket = "rotating";
    k1.create_bucket().bucket(bucket).send().await.unwrap();
    assert_eq!(
        code(&k2.list_buckets().send().await.unwrap_err()),
        "InvalidAccessKeyId"
    );

    // A batch of concurrent PUT/GET with both keys straddling each rotation
    // step; every outcome must be 200 or an explicit InvalidAccessKeyId,
    // never anything else, and every successful PUT is readable.
    let batch = |k1: Client, k2: Client, tag: &'static str| async move {
        let mut handles = Vec::new();
        for i in 0..12u32 {
            let (cl, key) = if i % 2 == 0 {
                (k1.clone(), K1)
            } else {
                (k2.clone(), K2)
            };
            handles.push(tokio::spawn(async move {
                let obj = format!("{tag}-{i}");
                let r = cl
                    .put_object()
                    .bucket("rotating")
                    .key(&obj)
                    .body(ByteStream::from(obj.clone().into_bytes()))
                    .send()
                    .await;
                let outcome = match &r {
                    Ok(_) => "ok".to_string(),
                    Err(e) => code(e),
                };
                (key, obj, outcome)
            }));
        }
        let mut out = Vec::new();
        for h in handles {
            out.push(h.await.unwrap());
        }
        out
    };
    let check = |results: &[(&str, String, String)], k1_ok: &[&str], k2_ok: &[&str]| {
        for (key, obj, outcome) in results {
            let allowed = if *key == K1 { k1_ok } else { k2_ok };
            assert!(
                allowed.contains(&outcome.as_str()),
                "{key} {obj}: {outcome} not in {allowed:?}"
            );
        }
    };
    // Step 1: add K2 while a batch runs.
    let b1 = tokio::spawn(batch(k1.clone(), k2.clone(), "s1"));
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    write_credentials(&path, &[(K1, S1, "active"), (K2, S2, "active")]);
    assert!(store.reload_if_changed());
    let r1 = b1.await.unwrap();
    check(&r1, &["ok"], &["ok", "InvalidAccessKeyId"]);
    // Both keys sign now.
    check(
        &batch(k1.clone(), k2.clone(), "both").await,
        &["ok"],
        &["ok"],
    );
    // Step 2: retire K1 while a batch runs.
    let b2 = tokio::spawn(batch(k1.clone(), k2.clone(), "s2"));
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    write_credentials(&path, &[(K1, S1, "inactive"), (K2, S2, "active")]);
    assert!(store.reload_if_changed());
    let r2 = b2.await.unwrap();
    check(&r2, &["ok", "InvalidAccessKeyId"], &["ok"]);
    // Only K2 signs now; an invalid file keeps that set.
    check(
        &batch(k1.clone(), k2.clone(), "after").await,
        &["InvalidAccessKeyId"],
        &["ok"],
    );
    std::fs::write(&path, "this is not toml [[").unwrap();
    assert!(!store.reload_if_changed());
    check(
        &batch(k1.clone(), k2.clone(), "broken").await,
        &["InvalidAccessKeyId"],
        &["ok"],
    );
    // Every successful PUT is readable with the live key.
    for (_, obj, outcome) in r1.iter().chain(&r2) {
        if outcome == "ok" {
            let got = k2
                .get_object()
                .bucket(bucket)
                .key(obj)
                .send()
                .await
                .unwrap()
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes();
            assert_eq!(got.as_ref(), obj.as_bytes());
        }
    }
    c.shutdown().await;
}

#[tokio::test]
async fn tenants_cannot_observe_each_others_buckets_objects_uploads_or_versions() {
    let c = cluster_with(two_tenants()).await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let a = client(&endpoint, &c.credentials.0, &c.credentials.1);
    let b = client(&endpoint, KEY_B, SECRET_B);
    a.create_bucket().bucket("a-bucket").send().await.unwrap();
    b.create_bucket().bucket("b-bucket").send().await.unwrap();
    a.put_object()
        .bucket("a-bucket")
        .key("secret-name")
        .body(ByteStream::from_static(b"a's data"))
        .metadata("owner-note", "confidential")
        .send()
        .await
        .unwrap();
    let up = a
        .create_multipart_upload()
        .bucket("a-bucket")
        .key("secret-upload")
        .send()
        .await
        .unwrap();
    // Bucket names: ListBuckets is owner-scoped.
    let names: Vec<String> = b
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets()
        .iter()
        .map(|x| x.name().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["b-bucket".to_string()]);
    // Object names, metadata, uploads and versions of a-bucket: denied, and
    // the error carries no key name or metadata.
    macro_rules! denied {
        ($r:expr) => {{
            let err = $r.await.unwrap_err();
            assert_eq!(status(&err), 403, "{err:?}");
            let text = format!("{err:?}");
            assert!(!text.contains("secret-name"), "{text}");
            assert!(!text.contains("secret-upload"), "{text}");
            assert!(!text.contains("confidential"), "{text}");
            assert!(!text.contains("a's data"), "{text}");
        }};
    }
    denied!(b.list_objects_v2().bucket("a-bucket").send());
    denied!(b.list_object_versions().bucket("a-bucket").send());
    denied!(b.list_multipart_uploads().bucket("a-bucket").send());
    denied!(b.head_object().bucket("a-bucket").key("secret-name").send());
    denied!(b.get_object().bucket("a-bucket").key("secret-name").send());
    denied!(
        b.list_parts()
            .bucket("a-bucket")
            .key("secret-upload")
            .upload_id(up.upload_id().unwrap())
            .send()
    );
    denied!(
        b.abort_multipart_upload()
            .bucket("a-bucket")
            .key("secret-upload")
            .upload_id(up.upload_id().unwrap())
            .send()
    );
    // Copy from a-bucket into b-bucket is authorized on the source too.
    denied!(
        b.copy_object()
            .bucket("b-bucket")
            .key("stolen")
            .copy_source("a-bucket/secret-name")
            .send()
    );
    assert_eq!(
        code(
            &b.head_object()
                .bucket("b-bucket")
                .key("stolen")
                .send()
                .await
                .unwrap_err()
        ),
        "NotFound",
        "nothing was copied"
    );
    // A prefix-scoped Allow reveals only the prefix: a listing outside it
    // and a listing without a prefix are denied, not filtered.
    a.put_object()
        .bucket("a-bucket")
        .key("public/p")
        .body(ByteStream::from_static(b"p"))
        .send()
        .await
        .unwrap();
    a.put_bucket_policy()
        .bucket("a-bucket")
        .policy(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"tenant-b"},"Action":["s3:ListBucket"],"Resource":"arn:aws:s3:::a-bucket","Condition":{"StringLike":{"s3:prefix":"public/*"}}},{"Effect":"Allow","Principal":{"AWS":"tenant-b"},"Action":["s3:GetObject"],"Resource":"arn:aws:s3:::a-bucket/public/*"}]}"#)
        .send()
        .await
        .unwrap();
    let keys: Vec<String> = b
        .list_objects_v2()
        .bucket("a-bucket")
        .prefix("public/")
        .send()
        .await
        .unwrap()
        .contents()
        .iter()
        .map(|o| o.key().unwrap().to_string())
        .collect();
    assert_eq!(keys, vec!["public/p".to_string()]);
    denied!(b.list_objects_v2().bucket("a-bucket").send());
    denied!(
        b.list_objects_v2()
            .bucket("a-bucket")
            .prefix("secret")
            .send()
    );
    denied!(b.get_object().bucket("a-bucket").key("secret-name").send());
    c.shutdown().await;
}

/// `aws:SecureTransport` cannot be satisfied by a client-sent
/// `x-forwarded-proto` on the gateway's plaintext listener (Phase 3.5 audit
/// SEC-2): only a configured TLS-terminating proxy is believed.
#[tokio::test]
async fn a_forged_forwarded_proto_header_does_not_satisfy_secure_transport() {
    let c = cluster_with(two_tenants()).await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let a = client(&endpoint, &c.credentials.0, &c.credentials.1);
    let b = client(&endpoint, KEY_B, SECRET_B);
    let bucket = "tls-only";
    a.create_bucket().bucket(bucket).send().await.unwrap();
    a.put_object()
        .bucket(bucket)
        .key("x")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Sid":"tls","Effect":"Allow","Principal":{{"AWS":"tenant-b"}},"Action":["s3:GetObject"],"Resource":"arn:aws:s3:::{bucket}/*","Condition":{{"Bool":{{"aws:SecureTransport":"true"}}}}}}]}}"#
    );
    a.put_bucket_policy()
        .bucket(bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();
    let plain = b.get_object().bucket(bucket).key("x").send().await;
    assert_eq!(status(&plain.unwrap_err()), 403, "plaintext is not secure");
    let forged = b
        .get_object()
        .bucket(bucket)
        .key("x")
        .customize()
        .mutate_request(|r| {
            r.headers_mut().insert("x-forwarded-proto", "https");
        })
        .send()
        .await;
    assert_eq!(
        status(&forged.unwrap_err()),
        403,
        "a client-sent x-forwarded-proto is not believed"
    );
    c.shutdown().await;
}

/// DeleteObjects is authorized per entry (Phase 3.5 audit SEC-1): an
/// explicit object-scoped deny holds inside a batch, `s3:DeleteObject` does
/// not grant deleting a version, and a grant on objects allows the batch.
#[tokio::test]
async fn batch_delete_is_authorized_per_key_and_per_version() {
    use aws_sdk_s3::types::{
        BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration,
    };
    let c = cluster_with(two_tenants()).await;
    let endpoint = format!("http://{}", c.gateway_addr);
    let a = client(&endpoint, &c.credentials.0, &c.credentials.1);
    let b = client(&endpoint, KEY_B, SECRET_B);
    let bucket = "batch-delete";
    a.create_bucket().bucket(bucket).send().await.unwrap();
    a.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let mut versions = std::collections::HashMap::new();
    for k in ["private/x", "public/y", "public/z"] {
        let o = a
            .put_object()
            .bucket(bucket)
            .key(k)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        versions.insert(k, o.version_id().unwrap().to_string());
    }
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Sid":"keep","Effect":"Deny","Principal":"*","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":"arn:aws:s3:::{bucket}/private/*"}},{{"Sid":"b","Effect":"Allow","Principal":{{"AWS":"tenant-b"}},"Action":["s3:DeleteObject"],"Resource":"arn:aws:s3:::{bucket}/public/*"}}]}}"#
    );
    a.put_bucket_policy()
        .bucket(bucket)
        .policy(&policy)
        .send()
        .await
        .unwrap();
    let ident = |k: &str, v: Option<&str>| {
        ObjectIdentifier::builder()
            .key(k)
            .set_version_id(v.map(str::to_string))
            .build()
            .unwrap()
    };
    let errors_of = |o: &aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput| {
        let mut e: Vec<(String, String)> = o
            .errors()
            .iter()
            .map(|e| {
                (
                    e.key().unwrap_or("").to_string(),
                    e.code().unwrap_or("").to_string(),
                )
            })
            .collect();
        e.sort();
        e
    };
    // The owner: the explicit deny on private/ holds inside the batch.
    let out = a
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(ident("private/x", Some(&versions["private/x"])))
                .objects(ident("public/y", Some(&versions["public/y"])))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        errors_of(&out),
        vec![("private/x".into(), "AccessDenied".into())]
    );
    a.get_object()
        .bucket(bucket)
        .key("private/x")
        .version_id(&versions["private/x"])
        .send()
        .await
        .expect("the denied version survives");
    // Tenant b may delete public objects, not their versions.
    let out = b
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(ident("public/z", Some(&versions["public/z"])))
                .objects(ident("public/z", None))
                .objects(ident("private/x", None))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        errors_of(&out),
        vec![
            ("private/x".into(), "AccessDenied".into()),
            ("public/z".into(), "AccessDenied".into())
        ]
    );
    a.get_object()
        .bucket(bucket)
        .key("public/z")
        .version_id(&versions["public/z"])
        .send()
        .await
        .expect("s3:DeleteObject does not delete a version");
    assert!(
        a.get_object()
            .bucket(bucket)
            .key("public/z")
            .send()
            .await
            .is_err(),
        "the unversioned delete placed a marker"
    );
    c.shutdown().await;
}
