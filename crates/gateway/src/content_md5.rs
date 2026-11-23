//! `Content-MD5` of the XML request bodies (RFC-007 §5.7).
//!
//! `s3s` reads the whole body of DeleteObjects, CompleteMultipartUpload,
//! PutBucketPolicy, PutBucketVersioning and CreateBucket and hands the
//! backend the parsed document only, so their `Content-MD5` is checked
//! here, on the bytes as they are read. PutObject and UploadPart check it
//! in the backend over the decoded body (`aws-chunked` included).

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use s3s::crypto::{Checksum as _, Md5};
use s3s::path::S3Path;
use s3s::stream::{ByteStream, RemainingLength};
use s3s::{Body, S3Error, StdError, s3_error};

/// Why a body was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The header is not base64 of 16 bytes.
    Malformed,
    /// The body's MD5 differs.
    Mismatch,
}

impl Refusal {
    /// The S3 error answered instead of the operation's.
    pub(crate) fn error(self) -> S3Error {
        match self {
            Self::Malformed => {
                s3_error!(InvalidDigest, "the Content-MD5 you specified is not valid")
            }
            Self::Mismatch => s3_error!(
                BadDigest,
                "the Content-MD5 you specified did not match what was received"
            ),
        }
    }
}

/// Whether `Content-MD5` of this request is checked here: the operations
/// with an XML body that the gateway implements, recognized as `s3s`
/// routes them (percent-decoded path-style path, decoded query names, so
/// `?%64elete` is `?delete` and `/b%2Fk` an object). PUT only on a bucket
/// path (PutBucketPolicy, PutBucketVersioning, CreateBucket): an object
/// PUT carries object data, which the backend checks over the decoded
/// body, whatever its query. Never an `aws-chunked` body, whose wire bytes
/// are not the content. Virtual-hosted-style addressing (not served
/// today) would need the bucket from the `Host` header here.
pub(crate) fn checked_here(
    method: &http::Method,
    uri: &http::Uri,
    headers: &http::HeaderMap,
) -> bool {
    let chunked = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("STREAMING-"))
        || headers
            .get_all(http::header::CONTENT_ENCODING)
            .iter()
            .any(|v| v.to_str().is_ok_and(|v| v.contains("aws-chunked")));
    if chunked {
        return false;
    }
    let Ok(path) = urlencoding::decode(uri.path()) else {
        return false;
    };
    let Ok(path) = s3s::path::parse_path_style(&path) else {
        return false;
    };
    let names: Vec<_> = uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|kv| urlencoding::decode(kv.split('=').next().unwrap_or("")).ok())
        .collect();
    let has = |name: &str| names.iter().any(|n| n == name);
    match (method, &path) {
        (&http::Method::POST, S3Path::Bucket { .. }) => has("delete"),
        (&http::Method::POST, S3Path::Object { .. }) => has("uploadId"),
        (&http::Method::PUT, S3Path::Bucket { .. }) => true,
        _ => false,
    }
}

/// `inner`, refusing at its end when its MD5 is not `expected`; the
/// refusal is also recorded in `outcome` for the response.
pub(crate) struct Md5Checked {
    inner: Body,
    md5: Option<Md5>,
    expected: Option<[u8; 16]>,
    outcome: Arc<OnceLock<Refusal>>,
}

impl Md5Checked {
    /// Wrap `inner` for the `Content-MD5` value `header`.
    pub(crate) fn new(inner: Body, header: &str, outcome: Arc<OnceLock<Refusal>>) -> Self {
        Self {
            inner,
            md5: Some(Md5::new()),
            expected: crate::checksum::content_md5(Some(header)).ok().flatten(),
            outcome,
        }
    }

    fn refuse(&self, r: Refusal) -> Poll<Option<Result<Bytes, StdError>>> {
        let _ = self.outcome.set(r);
        Poll::Ready(Some(Err(Box::new(r.error()))))
    }
}

impl Stream for Md5Checked {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.md5.is_none() {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(b))) => {
                if let Some(md5) = self.md5.as_mut() {
                    md5.update(&b);
                }
                Poll::Ready(Some(Ok(b)))
            }
            Poll::Ready(None) => {
                // A malformed value too is answered at the end of the body,
                // as a mismatch is: the client has sent it all and reads the
                // answer (the body is bounded by `s3s`'s XML size limit).
                if self.expected.is_none() {
                    self.md5 = None;
                    return self.refuse(Refusal::Malformed);
                }
                let digest = self.md5.take().map(Md5::finalize);
                let matches = matches!((&digest, &self.expected), (Some(d), Some(e)) if d.as_ref() == e.as_slice());
                if matches {
                    Poll::Ready(None)
                } else {
                    self.refuse(Refusal::Mismatch)
                }
            }
            other => other,
        }
    }
}

impl ByteStream for Md5Checked {
    fn remaining_length(&self) -> RemainingLength {
        self.inner.remaining_length()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;

    use super::*;

    fn b64_md5(data: &[u8]) -> String {
        let mut m = Md5::new();
        m.update(data);
        crate::checksum::encode_b64(m.finalize().as_ref())
    }

    async fn read(header: &str, data: &'static [u8]) -> (Vec<u8>, Option<Refusal>) {
        let outcome = Arc::new(OnceLock::new());
        let mut s = Md5Checked::new(
            Body::from(Bytes::from_static(data)),
            header,
            Arc::clone(&outcome),
        );
        let mut out = Vec::new();
        while let Some(r) = s.next().await {
            match r {
                Ok(b) => out.extend_from_slice(&b),
                Err(_) => break,
            }
        }
        (out, outcome.get().copied())
    }

    #[tokio::test]
    async fn the_body_passes_only_with_its_md5() {
        let body = b"<Delete><Object><Key>k</Key></Object></Delete>";
        assert_eq!(read(&b64_md5(body), body).await, (body.to_vec(), None));
        assert_eq!(
            read(&b64_md5(b"other"), body).await.1,
            Some(Refusal::Mismatch)
        );
        assert_eq!(read("not base64!", body).await.1, Some(Refusal::Malformed));
        assert_eq!(read("AAAA", body).await.1, Some(Refusal::Malformed));
        assert_eq!(Refusal::Mismatch.error().code().as_str(), "BadDigest");
        assert_eq!(Refusal::Malformed.error().code().as_str(), "InvalidDigest");
    }

    #[test]
    fn the_xml_operations_are_checked_here() {
        let plain = http::HeaderMap::new();
        let c = |m: &str, u: &str| checked_here(&m.parse().unwrap(), &u.parse().unwrap(), &plain);
        assert!(c("POST", "/bkt?delete"));
        assert!(c("POST", "/bkt?delete="));
        assert!(c("POST", "/bkt?%64elete"));
        assert!(c("POST", "/bkt/k?uploadId=x"));
        assert!(c("PUT", "/bkt?policy"));
        assert!(c("PUT", "/bkt?versioning"));
        assert!(c("PUT", "/bkt"));
        assert!(c("PUT", "/bkt/"));
        // Bodies the backend checks while it streams them, whatever the
        // query or the encoding of the path.
        assert!(!c("PUT", "/bkt/k"));
        assert!(!c("PUT", "/bkt/k?policy"));
        assert!(!c("PUT", "/bkt%2Fk?policy"));
        assert!(!c("PUT", "/bkt/k?x-id=PutObject"));
        assert!(!c("PUT", "/bkt/k?partNumber=1&uploadId=x"));
        assert!(!c("POST", "/bkt/k?uploads"));
        assert!(!c("POST", "/bkt"));
        assert!(!c("GET", "/bkt?delete"));
        // Not routable by s3s: left to it.
        assert!(!c("PUT", "/b?policy"));
        // An aws-chunked body is never hashed here.
        let mut chunked = http::HeaderMap::new();
        chunked.insert(
            "x-amz-content-sha256",
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER".parse().unwrap(),
        );
        assert!(!checked_here(
            &http::Method::PUT,
            &"/bkt?policy".parse().unwrap(),
            &chunked
        ));
        let mut encoded = http::HeaderMap::new();
        encoded.insert(
            http::header::CONTENT_ENCODING,
            "aws-chunked".parse().unwrap(),
        );
        assert!(!checked_here(
            &http::Method::POST,
            &"/bkt?delete".parse().unwrap(),
            &encoded
        ));
    }

    #[test]
    fn header_values() {
        let digest = crate::checksum::content_md5(Some(&b64_md5(b"x"))).unwrap();
        assert!(digest.is_some());
        // Surrounding whitespace is ignored; unpadded base64 is malformed.
        let padded = b64_md5(b"x");
        assert_eq!(
            crate::checksum::content_md5(Some(&format!(" {padded}\t"))).unwrap(),
            digest
        );
        let unpadded = padded.trim_end_matches('=');
        assert_eq!(unpadded.len(), 22);
        assert_eq!(
            crate::checksum::content_md5(Some(unpadded))
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidDigest"
        );
    }
}
