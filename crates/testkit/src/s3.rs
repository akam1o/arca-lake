//! Driving a backend directly through the [`S3`] trait (no HTTP), and an
//! AWS CLI runner for end-to-end checks through the gateway.

use std::process::Stdio;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::{Extensions, HeaderMap, Method, Uri};
use s3s::dto::{
    CreateBucketInput, DeleteObjectInput, ETag, ETagCondition, GetObjectInput, HeadObjectInput,
    PutObjectInput, Range, StreamingBlob,
};
use s3s::{S3, S3Request, S3Result, s3_error};
use tokio::process::Command;

/// Wrap an operation input in a request with no HTTP context.
#[must_use]
pub fn request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::default(),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// PUT options.
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// `If-None-Match: *`.
    pub if_none_match_any: bool,
    /// `If-Match: <etag>`.
    pub if_match: Option<String>,
    /// Content type.
    pub content_type: Option<String>,
}

/// Create a bucket.
///
/// # Errors
///
/// The S3 error.
pub async fn create_bucket(s3: &impl S3, bucket: &str) -> S3Result<()> {
    s3.create_bucket(request(CreateBucketInput {
        bucket: bucket.to_string(),
        ..Default::default()
    }))
    .await?;
    Ok(())
}

/// PUT an object; returns its ETag.
///
/// # Errors
///
/// The S3 error.
pub async fn put(
    s3: &impl S3,
    bucket: &str,
    key: &str,
    data: Bytes,
    opts: &PutOptions,
) -> S3Result<String> {
    let input = PutObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body: Some(StreamingBlob::from_bytes(data)),
        if_none_match: opts.if_none_match_any.then_some(ETagCondition::Any),
        if_match: opts
            .if_match
            .clone()
            .map(|e| ETagCondition::ETag(ETag::Strong(e))),
        content_type: opts.content_type.clone(),
        ..Default::default()
    };
    let out = s3.put_object(request(input)).await?.output;
    Ok(etag_string(out.e_tag.as_ref()))
}

fn etag_string(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => String::new(),
    }
}

/// GET a whole object; returns `(bytes, etag)`. A stream error after the
/// headers (for example an unreadable chunk) is returned as an error.
///
/// # Errors
///
/// The S3 error, or an internal error if the body stream fails.
pub async fn get(s3: &impl S3, bucket: &str, key: &str) -> S3Result<(Bytes, String)> {
    let out = s3
        .get_object(request(GetObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        }))
        .await?
        .output;
    let etag = etag_string(out.e_tag.as_ref());
    Ok((collect(out.body).await?, etag))
}

/// GET bytes `first..=last`.
///
/// # Errors
///
/// As [`get`].
pub async fn get_range(
    s3: &impl S3,
    bucket: &str,
    key: &str,
    first: u64,
    last: u64,
) -> S3Result<Bytes> {
    let out = s3
        .get_object(request(GetObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            range: Some(Range::Int {
                first,
                last: Some(last),
            }),
            ..Default::default()
        }))
        .await?
        .output;
    collect(out.body).await
}

/// Collect a streaming body into memory.
///
/// # Errors
///
/// Stream errors.
pub async fn collect(body: Option<StreamingBlob>) -> S3Result<Bytes> {
    let Some(mut body) = body else {
        return Ok(Bytes::new());
    };
    let mut buf = BytesMut::new();
    while let Some(item) = body.next().await {
        match item {
            Ok(b) => buf.extend_from_slice(&b),
            Err(e) => return Err(s3_error!(InternalError, "body stream failed: {e}")),
        }
    }
    Ok(buf.freeze())
}

/// HEAD; returns `(size, etag)`.
///
/// # Errors
///
/// The S3 error.
pub async fn head(s3: &impl S3, bucket: &str, key: &str) -> S3Result<(u64, String)> {
    let out = s3
        .head_object(request(HeadObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        }))
        .await?
        .output;
    let size = out
        .content_length
        .and_then(|n| u64::try_from(n).ok())
        .unwrap_or(0);
    Ok((size, etag_string(out.e_tag.as_ref())))
}

/// DELETE.
///
/// # Errors
///
/// The S3 error.
pub async fn delete(s3: &impl S3, bucket: &str, key: &str) -> S3Result<()> {
    s3.delete_object(request(DeleteObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        ..Default::default()
    }))
    .await?;
    Ok(())
}

/// AWS CLI v2 runner against a gateway endpoint. Credentials are passed
/// through the environment of the child process only.
#[derive(Debug, Clone)]
pub struct AwsCli {
    /// `http://host:port`.
    pub endpoint: String,
    /// Access key.
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
}

impl AwsCli {
    /// Whether `aws` is on `PATH`.
    pub async fn available() -> bool {
        Command::new("aws")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok()
    }

    /// Run a command; returns `(success, stdout, stderr)`.
    ///
    /// # Panics
    ///
    /// If the process cannot be spawned.
    pub async fn run(&self, args: &[&str]) -> (bool, String, String) {
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

    /// Run a command that must succeed; returns stdout.
    ///
    /// # Panics
    ///
    /// If the command fails.
    pub async fn ok(&self, args: &[&str]) -> String {
        let (ok, stdout, stderr) = self.run(args).await;
        assert!(ok, "aws {args:?} failed:\n{stderr}");
        stdout
    }

    /// Run a command and parse its JSON output.
    ///
    /// # Panics
    ///
    /// If the command fails or prints invalid JSON.
    pub async fn json(&self, args: &[&str]) -> serde_json::Value {
        let out = self.ok(args).await;
        serde_json::from_str(&out).unwrap_or_else(|e| panic!("aws {args:?}: bad json {e}: {out}"))
    }
}
