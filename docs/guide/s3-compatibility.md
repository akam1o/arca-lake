# S3 compatibility

The normative matrix, with the test that verifies each line, is
[RFC-007 §5.1](../rfcs/RFC-007-s3-gateway-compatibility.md) and its
acceptance table (§11). This page is a summary.

| Status | Operations |
| --- | --- |
| Implemented, verified | Buckets: create, delete, head, list, location, versioning, bucket policy (subset). Objects: PUT (incl. `aws-chunked`), GET (range, `partNumber`, `versionId`), HEAD, DELETE, DeleteObjects, CopyObject, ListObjectsV2, ListObjectVersions. Multipart: create, UploadPart, UploadPartCopy, complete, abort, ListParts, ListMultipartUploads. Conditional requests (`If-Match`, `If-None-Match`, `If-(Un)Modified-Since`). Checksums (CRC32, CRC32C, CRC64NVME, SHA1, SHA256; composite and full-object). SigV4 header and presigned authentication |
| Not implemented (`NotImplemented`) | Lifecycle, tagging, object ACLs, GetObjectAttributes, ListObjects v1 |
| Not planned (`NotImplemented`) | Bucket ACL, CORS, website, logging, notifications, replication, Object Lock, encryption configuration, accelerate, request payment, RestoreObject, SelectObjectContent |

Documented differences from AWS S3 are listed in RFC-007 §5.1.1. Notable
for clients:

- A write refused for capacity is `503 SlowDown` with `Retry-After`
  (ADR-0020); SDKs retry it. Reads, deletes, aborts and multipart
  completion are never refused for capacity.
- `409 ConditionalRequestConflict` is never returned; conditional writes
  are serialized by FoundationDB.
- The minimum part size is 5 MiB by default (`--multipart-min-part-bytes`).
- `Content-MD5` is verified (`400 BadDigest` on a mismatch, `400
  InvalidDigest` if malformed; nothing is stored). DeleteObjects does not
  require it.
