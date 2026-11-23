//! S3 errors for a request body that cannot be read.
//!
//! `s3s` verifies a signed body while it is read: the payload SHA-256 of
//! `x-amz-content-sha256`, the declared length, and the chunk signatures
//! and framing of `aws-chunked` bodies. A failed check is the client's
//! request, not a server fault, and is answered with the code AWS uses;
//! only a failure of the transport itself stays `InternalError`.

use http::StatusCode;
use s3s::stream::aws_chunked_stream::AwsChunkedStreamError;
use s3s::stream::upload_stream::UploadStreamError;
use s3s::{S3Error, S3ErrorCode, StdError, s3_error};

/// The S3 error for `e`, an error of a request body stream.
pub(crate) fn body_error(e: &StdError) -> S3Error {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e.as_ref());
    while let Some(err) = cur {
        if let Some(u) = err.downcast_ref::<UploadStreamError>() {
            match u {
                UploadStreamError::Underlying(_) => {}
                UploadStreamError::Sha256Mismatch => return sha256_mismatch(),
                other => return S3Error::with_message(other.to_s3_error_code(), other.to_string()),
            }
        }
        if let Some(c) = err.downcast_ref::<AwsChunkedStreamError>()
            && !matches!(c, AwsChunkedStreamError::Underlying(_))
        {
            return S3Error::with_message(c.to_s3_error_code(), c.to_string());
        }
        cur = err.source();
    }
    s3_error!(InternalError, "body read failed: {e}")
}

/// `400 XAmzContentSHA256Mismatch`, as AWS S3 answers a body whose SHA-256
/// differs from the signed `x-amz-content-sha256` (`s3s` has no such code
/// and suggests `BadDigest`, which AWS uses for `Content-MD5`).
fn sha256_mismatch() -> S3Error {
    let code =
        S3ErrorCode::from_bytes(b"XAmzContentSHA256Mismatch").unwrap_or(S3ErrorCode::BadDigest);
    let mut err = S3Error::with_message(
        code,
        "The provided 'x-amz-content-sha256' header does not match what was computed.",
    );
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(e: impl std::error::Error + Send + Sync + 'static) -> StdError {
        Box::new(e)
    }

    #[test]
    fn verification_failures_are_client_errors() {
        let e = body_error(&boxed(UploadStreamError::Sha256Mismatch));
        assert_eq!(e.code().as_str(), "XAmzContentSHA256Mismatch");
        assert_eq!(e.status_code(), Some(StatusCode::BAD_REQUEST));
        for (err, code) in [
            (boxed(UploadStreamError::LengthMismatch), "IncompleteBody"),
            (boxed(UploadStreamError::Incomplete), "IncompleteBody"),
            (
                boxed(AwsChunkedStreamError::SignatureMismatch),
                "SignatureDoesNotMatch",
            ),
            (boxed(AwsChunkedStreamError::FormatError), "IncompleteBody"),
            (boxed(AwsChunkedStreamError::Incomplete), "IncompleteBody"),
            (
                boxed(AwsChunkedStreamError::ChunkDataTooLarge(2, 1)),
                "EntityTooLarge",
            ),
        ] {
            let e = body_error(&err);
            assert_eq!(e.code().as_str(), code, "{err}");
            let status = e.status_code();
            assert!(
                status.is_some_and(|s| s.is_client_error()),
                "{err}: {status:?}"
            );
        }
    }

    #[test]
    fn nested_verification_failures_are_found() {
        // A chunk signature failure under the payload-hash wrapper.
        let inner = UploadStreamError::Underlying(boxed(AwsChunkedStreamError::SignatureMismatch));
        assert_eq!(
            body_error(&boxed(inner)).code().as_str(),
            "SignatureDoesNotMatch"
        );
    }

    #[test]
    fn transport_failures_stay_internal_errors() {
        let io = || std::io::Error::other("connection reset");
        for err in [
            boxed(io()),
            boxed(UploadStreamError::Underlying(boxed(io()))),
            boxed(AwsChunkedStreamError::Underlying(boxed(io()))),
        ] {
            let e = body_error(&err);
            assert_eq!(*e.code(), S3ErrorCode::InternalError, "{err}");
            assert!(e.message().unwrap_or("").contains("body read failed"));
        }
    }
}
