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
/// with an XML body that the gateway implements (path-style addressing).
pub(crate) fn checked_here(method: &http::Method, uri: &http::Uri) -> bool {
    let has = |name: &str| {
        uri.query()
            .unwrap_or("")
            .split('&')
            .any(|kv| kv.split('=').next() == Some(name))
    };
    let bucket_only = uri
        .path()
        .trim_start_matches('/')
        .trim_end_matches('/')
        .find('/')
        .is_none();
    match *method {
        http::Method::POST => has("delete") || has("uploadId"),
        http::Method::PUT => has("policy") || has("versioning") || bucket_only,
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
        if self.expected.is_none() {
            return self.refuse(Refusal::Malformed);
        }
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
        let c = |m: &str, u: &str| checked_here(&m.parse().unwrap(), &u.parse().unwrap());
        assert!(c("POST", "/b?delete"));
        assert!(c("POST", "/b/k?uploadId=x"));
        assert!(c("PUT", "/b?policy"));
        assert!(c("PUT", "/b?versioning"));
        assert!(c("PUT", "/b"));
        assert!(c("PUT", "/b/"));
        // Bodies the backend checks while it streams them.
        assert!(!c("PUT", "/b/k"));
        assert!(!c("PUT", "/b/k?x-id=PutObject"));
        assert!(!c("PUT", "/b/k?partNumber=1&uploadId=x"));
        assert!(!c("POST", "/b/k?uploads"));
        assert!(!c("GET", "/b?delete"));
    }
}
