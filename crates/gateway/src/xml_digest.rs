//! `Content-MD5` and `x-amz-checksum-*` of the XML request bodies
//! (RFC-007 §5.7).
//!
//! `s3s` reads the whole body of DeleteObjects, CompleteMultipartUpload,
//! PutBucketPolicy, PutBucketVersioning and CreateBucket and hands the
//! backend the parsed document only, so their digests are checked here,
//! on the bytes as they are read. PutObject and UploadPart check theirs in
//! the backend over the decoded body (`aws-chunked` included).

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use arca_lake_metadata::records::ChecksumAlgorithm;
use bytes::Bytes;
use futures::Stream;
use s3s::crypto::{Checksum as _, Md5};
use s3s::path::S3Path;
use s3s::stream::{ByteStream, RemainingLength};
use s3s::{Body, S3Error, StdError, s3_error};

use crate::checksum::{self, Hasher};

/// The checksum headers of a body, by algorithm.
const CHECKSUMS: [(&str, ChecksumAlgorithm); 5] = [
    ("x-amz-checksum-crc32", ChecksumAlgorithm::Crc32),
    ("x-amz-checksum-crc32c", ChecksumAlgorithm::Crc32c),
    ("x-amz-checksum-crc64nvme", ChecksumAlgorithm::Crc64Nvme),
    ("x-amz-checksum-sha1", ChecksumAlgorithm::Sha1),
    ("x-amz-checksum-sha256", ChecksumAlgorithm::Sha256),
];

/// Checksum headers `s3s` knows of whose algorithms the gateway does not
/// implement (refused as on PutObject).
const UNSUPPORTED: [&str; 5] = [
    "x-amz-checksum-sha512",
    "x-amz-checksum-md5",
    "x-amz-checksum-xxhash64",
    "x-amz-checksum-xxhash3",
    "x-amz-checksum-xxhash128",
];

/// Why a body was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// `Content-MD5` is not base64 of 16 bytes.
    Malformed,
    /// The body's MD5 differs.
    Mismatch,
    /// An `x-amz-checksum-*` value is not base64 of the algorithm's length.
    ChecksumMalformed(ChecksumAlgorithm),
    /// The body's checksum differs.
    ChecksumMismatch(ChecksumAlgorithm),
    /// A checksum algorithm the gateway does not implement.
    ChecksumUnsupported,
    /// Inconsistent digest headers (`400 InvalidRequest`).
    Invalid(&'static str),
}

impl Refusal {
    /// The S3 error answered instead of the operation's (as PutObject
    /// answers the same header).
    pub(crate) fn error(self) -> S3Error {
        match self {
            Self::Malformed => {
                s3_error!(InvalidDigest, "the Content-MD5 you specified is not valid")
            }
            Self::Mismatch => s3_error!(
                BadDigest,
                "the Content-MD5 you specified did not match what was received"
            ),
            Self::ChecksumMalformed(a) => s3_error!(
                InvalidRequest,
                "value for x-amz-checksum-{} header is invalid",
                a.name().to_ascii_lowercase()
            ),
            Self::ChecksumMismatch(a) => s3_error!(
                BadDigest,
                "the {} you specified did not match the calculated checksum",
                a.name()
            ),
            Self::ChecksumUnsupported => s3_error!(
                NotImplemented,
                "only CRC32, CRC32C, CRC64NVME, SHA1 and SHA256 checksums are supported"
            ),
            Self::Invalid(m) => S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, m),
        }
    }
}

/// Which digests of a request's body are checked here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// `Content-MD5` and `x-amz-checksum-*`.
    All,
    /// `Content-MD5` only: on CompleteMultipartUpload `x-amz-checksum-*`
    /// is the object's checksum, which the backend compares with the
    /// derived one.
    Md5,
}

/// Which digests of this request's body are checked here, if any: the
/// operations with an XML body that the gateway implements, recognized as
/// `s3s` routes them (percent-decoded path-style path, decoded query names,
/// so `?%64elete` is `?delete` and `/b%2Fk` an object). PUT only on a
/// bucket path (PutBucketPolicy, PutBucketVersioning, CreateBucket): an
/// object PUT carries object data, which the backend checks over the
/// decoded body, whatever its query. Virtual-hosted-style addressing (not
/// served today) would need the bucket from the `Host` header here.
pub(crate) fn checked_here(method: &http::Method, uri: &http::Uri) -> Option<Scope> {
    let path = urlencoding::decode(uri.path()).ok()?;
    let path = s3s::path::parse_path_style(&path).ok()?;
    let names: Vec<_> = uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|kv| urlencoding::decode(kv.split('=').next().unwrap_or("")).ok())
        .collect();
    let has = |name: &str| names.iter().any(|n| n == name);
    match (method, &path) {
        (&http::Method::POST, S3Path::Bucket { .. }) if has("delete") => Some(Scope::All),
        (&http::Method::POST, S3Path::Object { .. }) if has("uploadId") => Some(Scope::Md5),
        (&http::Method::PUT, S3Path::Bucket { .. }) => Some(Scope::All),
        _ => None,
    }
}

/// Whether the body is `aws-chunked` (its wire bytes are not the content).
fn aws_chunked(headers: &http::HeaderMap) -> bool {
    headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("STREAMING-"))
        || headers
            .get_all(http::header::CONTENT_ENCODING)
            .iter()
            .any(|v| v.to_str().is_ok_and(|v| v.contains("aws-chunked")))
}

/// The one value of header `name`, if any; an error if it is repeated.
fn single<'h>(headers: &'h http::HeaderMap, name: &str) -> Result<Option<&'h str>, Refusal> {
    let mut all = headers.get_all(name).iter();
    let first = all.next();
    if all.next().is_some() {
        return Err(Refusal::Invalid("a digest or checksum header is repeated"));
    }
    Ok(first.map(|v| v.to_str().unwrap_or("")))
}

/// What a body must hash to, from its headers. A header error is kept
/// and answered at the end of the body.
#[derive(Default)]
pub(crate) struct Expected {
    /// The first header error, in the order PutObject checks them.
    refusal: Option<Refusal>,
    md5: Option<[u8; 16]>,
    checksum: Option<(ChecksumAlgorithm, Vec<u8>)>,
}

impl Expected {
    /// The digests `headers` ask for within `scope`; `None` if none.
    pub(crate) fn from_headers(headers: &http::HeaderMap, scope: Scope) -> Option<Self> {
        let mut e = Self::default();
        if let Err(r) = e.read(headers, scope) {
            e.refusal = Some(r);
        }
        (e.refusal.is_some() || e.md5.is_some() || e.checksum.is_some()).then_some(e)
    }

    fn read(&mut self, headers: &http::HeaderMap, scope: Scope) -> Result<(), Refusal> {
        // AWS accepts aws-chunked bodies on PutObject and UploadPart only;
        // here the wire bytes would be hashed, not the content.
        if aws_chunked(headers) {
            return Err(Refusal::Invalid(
                "aws-chunked bodies are supported on PutObject and UploadPart only",
            ));
        }
        if let Some(v) = single(headers, "content-md5")? {
            self.md5 = Some(
                checksum::content_md5(Some(v))
                    .ok()
                    .flatten()
                    .ok_or(Refusal::Malformed)?,
            );
        }
        if scope == Scope::Md5 {
            return Ok(());
        }
        if UNSUPPORTED.iter().any(|n| headers.contains_key(*n)) {
            return Err(Refusal::ChecksumUnsupported);
        }
        let mut present = Vec::new();
        for (name, a) in CHECKSUMS {
            if let Some(v) = single(headers, name)? {
                present.push((a, v));
            }
        }
        if present.len() > 1 {
            return Err(Refusal::Invalid(
                "expecting a single x-amz-checksum-* header",
            ));
        }
        if let Some((a, v)) = present.first() {
            let d = checksum::decode_b64(*a, v).ok_or(Refusal::ChecksumMalformed(*a))?;
            self.checksum = Some((*a, d));
        }
        // An algorithm named without its value, or another one than the
        // value header's, is refused as AWS does.
        for name in ["x-amz-sdk-checksum-algorithm", "x-amz-checksum-algorithm"] {
            let Some(v) = single(headers, name)? else {
                continue;
            };
            let Some(named) = ChecksumAlgorithm::parse(v.trim()) else {
                return Err(
                    if ["SHA512", "MD5", "XXHASH64", "XXHASH3", "XXHASH128"]
                        .contains(&v.trim().to_ascii_uppercase().as_str())
                    {
                        Refusal::ChecksumUnsupported
                    } else {
                        Refusal::Invalid("the checksum algorithm header is invalid")
                    },
                );
            };
            match &self.checksum {
                None => {
                    return Err(Refusal::Invalid(
                        "a checksum algorithm is specified, but no corresponding x-amz-checksum-* header was found",
                    ));
                }
                Some((a, _)) if *a != named => {
                    return Err(Refusal::Invalid(
                        "the checksum algorithm header names another algorithm than the x-amz-checksum-* header",
                    ));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

/// `inner`, refusing at its end when it does not hash to what is
/// expected; the refusal is also recorded in `outcome` for the response.
pub(crate) struct DigestChecked {
    inner: Body,
    expected: Expected,
    md5: Option<Md5>,
    hasher: Option<Hasher>,
    done: bool,
    outcome: Arc<OnceLock<Refusal>>,
}

impl DigestChecked {
    /// Wrap `inner`.
    pub(crate) fn new(inner: Body, expected: Expected, outcome: Arc<OnceLock<Refusal>>) -> Self {
        let md5 = expected.md5.is_some().then(Md5::new);
        let hasher = expected.checksum.as_ref().map(|(a, _)| Hasher::new(*a));
        Self {
            inner,
            expected,
            md5,
            hasher,
            done: false,
            outcome,
        }
    }

    /// The refusal at the end of the body, if any: header errors first,
    /// then mismatches.
    fn verdict(&mut self) -> Option<Refusal> {
        if let Some(r) = self.expected.refusal {
            return Some(r);
        }
        if let (Some(e), Some(m)) = (&self.expected.md5, self.md5.take())
            && m.finalize().as_ref() != e.as_slice()
        {
            return Some(Refusal::Mismatch);
        }
        if let (Some((a, e)), Some(h)) = (&self.expected.checksum, self.hasher.take())
            && h.finalize() != *e
        {
            return Some(Refusal::ChecksumMismatch(*a));
        }
        None
    }
}

impl Stream for DigestChecked {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(b))) => {
                if let Some(m) = self.md5.as_mut() {
                    m.update(&b);
                }
                if let Some(h) = self.hasher.as_mut() {
                    h.update(&b);
                }
                Poll::Ready(Some(Ok(b)))
            }
            // Header errors too are answered at the end of the body, as a
            // mismatch is: the client has sent it all and reads the answer
            // (the body is bounded by `s3s`'s XML size limit).
            Poll::Ready(None) => {
                self.done = true;
                match self.verdict() {
                    None => Poll::Ready(None),
                    Some(r) => {
                        let _ = self.outcome.set(r);
                        Poll::Ready(Some(Err(Box::new(r.error()))))
                    }
                }
            }
            other => other,
        }
    }
}

impl ByteStream for DigestChecked {
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

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    async fn read_with(
        pairs: &[(&str, &str)],
        scope: Scope,
        data: &'static [u8],
    ) -> (Vec<u8>, Option<Refusal>) {
        let outcome = Arc::new(OnceLock::new());
        let expected = Expected::from_headers(&headers(pairs), scope).unwrap();
        let mut s = DigestChecked::new(
            Body::from(Bytes::from_static(data)),
            expected,
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

    async fn read(header: &str, data: &'static [u8]) -> (Vec<u8>, Option<Refusal>) {
        read_with(&[("content-md5", header)], Scope::All, data).await
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

    #[tokio::test]
    async fn the_body_passes_only_with_its_checksum() {
        let body = b"<Delete><Object><Key>k</Key></Object></Delete>";
        for (name, a) in CHECKSUMS {
            let good = checksum::encode_b64(&checksum::digest(a, body));
            let bad = checksum::encode_b64(&checksum::digest(a, b"other"));
            assert_eq!(
                read_with(&[(name, &good)], Scope::All, body).await,
                (body.to_vec(), None)
            );
            assert_eq!(
                read_with(&[(name, &bad)], Scope::All, body).await.1,
                Some(Refusal::ChecksumMismatch(a))
            );
            assert_eq!(
                read_with(&[(name, "AAAA")], Scope::All, body).await.1,
                Some(Refusal::ChecksumMalformed(a))
            );
            // Together with Content-MD5: both must match.
            assert_eq!(
                read_with(
                    &[(name, &bad), ("content-md5", &b64_md5(body))],
                    Scope::All,
                    body
                )
                .await
                .1,
                Some(Refusal::ChecksumMismatch(a))
            );
        }
        let crc = checksum::encode_b64(&checksum::digest(ChecksumAlgorithm::Crc32, body));
        let sha = checksum::encode_b64(&checksum::digest(ChecksumAlgorithm::Sha1, body));
        assert_eq!(
            read_with(
                &[
                    ("x-amz-checksum-crc32", &crc),
                    ("x-amz-checksum-sha1", &sha)
                ],
                Scope::All,
                body
            )
            .await
            .1,
            Some(Refusal::Invalid(
                "expecting a single x-amz-checksum-* header"
            ))
        );
        assert_eq!(
            read_with(&[("x-amz-checksum-sha512", "AAAA")], Scope::All, body)
                .await
                .1,
            Some(Refusal::ChecksumUnsupported)
        );
        // CompleteMultipartUpload: the header is the object's checksum.
        assert!(
            Expected::from_headers(&headers(&[("x-amz-checksum-crc32", "AAAA")]), Scope::Md5)
                .is_none()
        );
        // Not a value: nothing to check.
        assert!(
            Expected::from_headers(
                &headers(&[("x-amz-checksum-type", "FULL_OBJECT")]),
                Scope::All
            )
            .is_none()
        );
        for (r, code) in [
            (
                Refusal::ChecksumMismatch(ChecksumAlgorithm::Crc32),
                "BadDigest",
            ),
            (
                Refusal::ChecksumMalformed(ChecksumAlgorithm::Crc32),
                "InvalidRequest",
            ),
            (Refusal::Invalid("x"), "InvalidRequest"),
            (Refusal::ChecksumUnsupported, "NotImplemented"),
        ] {
            assert_eq!(r.error().code().as_str(), code);
        }
    }

    #[test]
    fn the_xml_operations_are_checked_here() {
        let scope = |m: &str, u: &str| checked_here(&m.parse().unwrap(), &u.parse().unwrap());
        let c = |m: &str, u: &str| scope(m, u).is_some();
        assert_eq!(scope("POST", "/bkt?delete"), Some(Scope::All));
        assert_eq!(scope("PUT", "/bkt?policy"), Some(Scope::All));
        assert_eq!(scope("POST", "/bkt/k?uploadId=x"), Some(Scope::Md5));
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
        // An aws-chunked body is refused here (AWS: PutObject and
        // UploadPart only), digest headers or not, on every scope.
        let chunked =
            Refusal::Invalid("aws-chunked bodies are supported on PutObject and UploadPart only");
        for (h, v) in [
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            ("content-encoding", "aws-chunked"),
        ] {
            for scope in [Scope::All, Scope::Md5] {
                let e = Expected::from_headers(&headers(&[(h, v)]), scope).unwrap();
                assert_eq!(e.refusal, Some(chunked), "{h}: {v}");
            }
        }
    }

    #[test]
    fn inconsistent_digest_headers_are_refused() {
        let body = b"<Delete/>";
        let crc = checksum::encode_b64(&checksum::digest(ChecksumAlgorithm::Crc32, body));
        let refusal = |pairs: &[(&str, &str)], scope| {
            Expected::from_headers(&headers(pairs), scope).and_then(|e| e.refusal)
        };
        let repeated = Some(Refusal::Invalid("a digest or checksum header is repeated"));
        // A repeated value header, also with the same value twice.
        assert_eq!(
            refusal(
                &[
                    ("x-amz-checksum-crc32", &crc),
                    ("x-amz-checksum-crc32", &crc)
                ],
                Scope::All
            ),
            repeated
        );
        let md5 = b64_md5(body);
        assert_eq!(
            refusal(&[("content-md5", &md5), ("content-md5", &md5)], Scope::All),
            repeated
        );
        assert_eq!(
            refusal(&[("content-md5", &md5), ("content-md5", &md5)], Scope::Md5),
            repeated
        );
        // The SDK's pair, in either case of the name: accepted.
        for name in ["CRC32", "crc32"] {
            for header in ["x-amz-sdk-checksum-algorithm", "x-amz-checksum-algorithm"] {
                assert_eq!(
                    refusal(
                        &[(header, name), ("x-amz-checksum-crc32", &crc)],
                        Scope::All
                    ),
                    None
                );
            }
        }
        // An algorithm without its value, another one, an unknown or an
        // unsupported one.
        assert!(matches!(
            refusal(&[("x-amz-sdk-checksum-algorithm", "CRC32")], Scope::All),
            Some(Refusal::Invalid(_))
        ));
        assert!(matches!(
            refusal(
                &[
                    ("x-amz-sdk-checksum-algorithm", "SHA256"),
                    ("x-amz-checksum-crc32", &crc)
                ],
                Scope::All
            ),
            Some(Refusal::Invalid(_))
        ));
        assert!(matches!(
            refusal(
                &[
                    ("x-amz-checksum-algorithm", "FOO"),
                    ("x-amz-checksum-crc32", &crc)
                ],
                Scope::All
            ),
            Some(Refusal::Invalid(_))
        ));
        assert_eq!(
            refusal(
                &[
                    ("x-amz-sdk-checksum-algorithm", "SHA512"),
                    ("x-amz-checksum-crc32", &crc)
                ],
                Scope::All
            ),
            Some(Refusal::ChecksumUnsupported)
        );
        assert_eq!(
            refusal(
                &[
                    ("x-amz-sdk-checksum-algorithm", "CRC32"),
                    ("x-amz-sdk-checksum-algorithm", "CRC32"),
                    ("x-amz-checksum-crc32", &crc)
                ],
                Scope::All
            ),
            repeated
        );
        // CompleteMultipartUpload: its checksum headers are the object's.
        assert!(
            Expected::from_headers(
                &headers(&[("x-amz-sdk-checksum-algorithm", "CRC32")]),
                Scope::Md5
            )
            .is_none()
        );
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
