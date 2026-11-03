//! Error messages leave the gateway without internal addresses (Phase 3.3
//! §8, RFC-012): an S3 error that would name a storage node (`host:port`,
//! `a.b.c.d:port`, `[v6]:port`) is logged in full on the gateway and
//! returned to the client with every address replaced by `<node>`.

use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteBucketPolicyInput, DeleteBucketPolicyOutput, DeleteObjectInput,
    DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, GetBucketLocationInput,
    GetBucketLocationOutput, GetBucketPolicyInput, GetBucketPolicyOutput, GetBucketVersioningInput,
    GetBucketVersioningOutput, GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput,
    HeadObjectInput, HeadObjectOutput, ListBucketsInput, ListBucketsOutput,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListObjectVersionsInput,
    ListObjectVersionsOutput, ListObjectsV2Input, ListObjectsV2Output, ListPartsInput,
    ListPartsOutput, PutBucketPolicyInput, PutBucketPolicyOutput, PutBucketVersioningInput,
    PutBucketVersioningOutput, PutObjectInput, PutObjectOutput, UploadPartCopyInput,
    UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3Error, S3Request, S3Response, S3Result};

/// Replace every network address in `msg` with `<node>`.
#[must_use]
pub fn redact_addresses(msg: &str) -> String {
    let b = msg.as_bytes();
    let mut out = String::with_capacity(msg.len());
    let mut i = 0;
    let host = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_');
    while i < b.len() {
        // A candidate starts at a host character (or `[` for IPv6) that
        // does not continue a previous word.
        let starts = (host(b[i]) || b[i] == b'[') && (i == 0 || !host(b[i - 1]));
        if starts {
            let mut j = i;
            if b[i] == b'[' {
                while j < b.len() && b[j] != b']' {
                    j += 1;
                }
                j = (j + 1).min(b.len());
            } else {
                while j < b.len() && host(b[j]) {
                    j += 1;
                }
            }
            let name = &msg[i..j];
            if j < b.len() && b[j] == b':' {
                let mut k = j + 1;
                while k < b.len() && b[k].is_ascii_digit() {
                    k += 1;
                }
                let digits = k - j - 1;
                let looks_like_host = name.starts_with('[')
                    || name.contains('.')
                    || name.eq_ignore_ascii_case("localhost");
                if (1..=5).contains(&digits) && looks_like_host && (k == b.len() || !host(b[k])) {
                    out.push_str("<node>");
                    i = k;
                    continue;
                }
            }
            out.push_str(name);
            i = j;
            continue;
        }
        let ch = msg[i..].chars().next().unwrap_or(' ');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn redact(op: &'static str, mut e: S3Error) -> S3Error {
    if let Some(m) = e.message() {
        let r = redact_addresses(m);
        if r != m {
            tracing::warn!(op, code = ?e.code(), error = %m, "S3 error (addresses redacted for the client)");
            e.set_message(r);
        }
    }
    e
}

/// An S3 service whose error messages carry no internal addresses.
#[derive(Debug, Clone)]
pub struct Redacted<B>(pub B);

macro_rules! delegate {
    ($($m:ident: $i:ident => $o:ident),* $(,)?) => {
        #[async_trait::async_trait]
        impl<B: S3 + Send + Sync + 'static> S3 for Redacted<B> {
            $(
                async fn $m(&self, req: S3Request<$i>) -> S3Result<S3Response<$o>> {
                    self.0.$m(req).await.map_err(|e| redact(stringify!($m), e))
                }
            )*
        }
    };
}

delegate!(
    create_bucket: CreateBucketInput => CreateBucketOutput,
    delete_bucket: DeleteBucketInput => DeleteBucketOutput,
    head_bucket: HeadBucketInput => HeadBucketOutput,
    get_bucket_location: GetBucketLocationInput => GetBucketLocationOutput,
    list_buckets: ListBucketsInput => ListBucketsOutput,
    get_bucket_versioning: GetBucketVersioningInput => GetBucketVersioningOutput,
    put_bucket_versioning: PutBucketVersioningInput => PutBucketVersioningOutput,
    get_bucket_policy: GetBucketPolicyInput => GetBucketPolicyOutput,
    put_bucket_policy: PutBucketPolicyInput => PutBucketPolicyOutput,
    delete_bucket_policy: DeleteBucketPolicyInput => DeleteBucketPolicyOutput,
    put_object: PutObjectInput => PutObjectOutput,
    get_object: GetObjectInput => GetObjectOutput,
    head_object: HeadObjectInput => HeadObjectOutput,
    delete_object: DeleteObjectInput => DeleteObjectOutput,
    delete_objects: DeleteObjectsInput => DeleteObjectsOutput,
    copy_object: CopyObjectInput => CopyObjectOutput,
    list_objects_v2: ListObjectsV2Input => ListObjectsV2Output,
    list_object_versions: ListObjectVersionsInput => ListObjectVersionsOutput,
    create_multipart_upload: CreateMultipartUploadInput => CreateMultipartUploadOutput,
    upload_part: UploadPartInput => UploadPartOutput,
    upload_part_copy: UploadPartCopyInput => UploadPartCopyOutput,
    complete_multipart_upload: CompleteMultipartUploadInput => CompleteMultipartUploadOutput,
    abort_multipart_upload: AbortMultipartUploadInput => AbortMultipartUploadOutput,
    list_parts: ListPartsInput => ListPartsOutput,
    list_multipart_uploads: ListMultipartUploadsInput => ListMultipartUploadsOutput,
);

#[cfg(test)]
mod tests {
    use super::redact_addresses;

    #[test]
    fn addresses_are_replaced_and_ordinary_text_is_kept() {
        assert_eq!(
            redact_addresses(
                "node 10.0.0.7:9100: code: 'Internal', message: \"I/O error: ENOSPC\""
            ),
            "node <node>: code: 'Internal', message: \"I/O error: ENOSPC\""
        );
        assert_eq!(
            redact_addresses(
                "failed: node-a.example.internal:9100 and [fd00::1]:9101, localhost:80"
            ),
            "failed: <node> and <node>, <node>"
        );
        let plain = "chunk 0 unreadable: 1 of 2 shards; crc 0badf00d; part 3: size 5 at 10:30";
        assert_eq!(redact_addresses(plain), plain);
    }
}
