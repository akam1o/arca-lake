#!/usr/bin/env python3
"""S3 compatibility scenarios driven by the AWS SDK for Python (boto3).

Usage: boto3_compat.py ENDPOINT ACCESS_KEY SECRET_KEY

Run by `crates/testkit/tests/s3_compat.rs` against an in-process cluster
gateway; exits non-zero on the first failed assertion. Covers single and
multipart uploads (with the SDK's default checksums and explicit SHA256),
ETags, versioning and delete markers, conditional reads, CopyObject,
DeleteObjects and listing (RFC-007 §11, A2).
"""

import hashlib
import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def main() -> int:
    endpoint, access_key, secret_key = sys.argv[1:4]
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )
    bucket = "boto3-compat"
    s3.create_bucket(Bucket=bucket)

    # Single PUT with the SDK's default checksum (trailer) and an explicit SHA256.
    data = os.urandom(3 * 1024 * 1024 + 123)
    put = s3.put_object(Bucket=bucket, Key="single", Body=data, ContentType="application/x-test")
    assert put["ETag"].strip('"') == hashlib.md5(data).hexdigest(), put["ETag"]
    got = s3.get_object(Bucket=bucket, Key="single", ChecksumMode="ENABLED")
    assert got["Body"].read() == data
    assert got["ContentType"] == "application/x-test"
    put = s3.put_object(Bucket=bucket, Key="sha", Body=data, ChecksumAlgorithm="SHA256")
    assert "ChecksumSHA256" in put, put
    head = s3.head_object(Bucket=bucket, Key="sha", ChecksumMode="ENABLED")
    assert head["ChecksumSHA256"] == put["ChecksumSHA256"]
    # Ranged GET.
    part = s3.get_object(Bucket=bucket, Key="single", Range="bytes=1048570-1048585")["Body"].read()
    assert part == data[1048570:1048586], len(part)

    # Multipart with an explicit SHA256 (composite; pure-Python in botocore) and the transfer API.
    parts = [os.urandom(5 * 1024 * 1024), os.urandom(5 * 1024 * 1024), os.urandom(1234)]
    mpu = s3.create_multipart_upload(Bucket=bucket, Key="mp", ChecksumAlgorithm="SHA256")
    uid = mpu["UploadId"]
    assert mpu.get("ChecksumType") == "COMPOSITE", mpu
    etags = []
    for i, p in enumerate(parts, start=1):
        r = s3.upload_part(Bucket=bucket, Key="mp", UploadId=uid, PartNumber=i, Body=p, ChecksumAlgorithm="SHA256")
        etags.append({"PartNumber": i, "ETag": r["ETag"], "ChecksumSHA256": r["ChecksumSHA256"]})
    listed = s3.list_parts(Bucket=bucket, Key="mp", UploadId=uid)["Parts"]
    assert [p["PartNumber"] for p in listed] == [1, 2, 3]
    uploads = s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
    assert [u["UploadId"] for u in uploads] == [uid]
    done = s3.complete_multipart_upload(Bucket=bucket, Key="mp", UploadId=uid, MultipartUpload={"Parts": etags})
    whole = b"".join(parts)
    expected_etag = hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest() + "-3"
    assert done["ETag"].strip('"') == expected_etag, done["ETag"]
    assert done["ChecksumSHA256"].endswith("-3"), done
    got = s3.get_object(Bucket=bucket, Key="mp", ChecksumMode="ENABLED")
    assert got["Body"].read() == whole
    assert got["ETag"].strip('"') == expected_etag
    assert s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []) == []
    try:
        s3.abort_multipart_upload(Bucket=bucket, Key="mp", UploadId=uid)
        raise AssertionError("abort after complete must fail")
    except ClientError as e:
        assert e.response["Error"]["Code"] == "NoSuchUpload", e.response
    # Transfer manager multipart (automatic chunking) round trip.
    big = os.urandom(12 * 1024 * 1024 + 7)
    from boto3.s3.transfer import TransferConfig

    cfg = TransferConfig(multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024)
    import io

    s3.upload_fileobj(io.BytesIO(big), bucket, "big", Config=cfg)
    out = io.BytesIO()
    s3.download_fileobj(bucket, "big", out, Config=cfg)
    assert out.getvalue() == big
    # Abort collects: a second upload aborted leaves no listing.
    uid2 = s3.create_multipart_upload(Bucket=bucket, Key="aborted")["UploadId"]
    s3.upload_part(Bucket=bucket, Key="aborted", UploadId=uid2, PartNumber=1, Body=os.urandom(1024))
    s3.abort_multipart_upload(Bucket=bucket, Key="aborted", UploadId=uid2)
    assert s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []) == []

    # Versioning.
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    assert s3.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"
    v1 = s3.put_object(Bucket=bucket, Key="v", Body=b"one")["VersionId"]
    v2 = s3.put_object(Bucket=bucket, Key="v", Body=b"two")["VersionId"]
    assert v1 != v2
    assert s3.get_object(Bucket=bucket, Key="v", VersionId=v1)["Body"].read() == b"one"
    d = s3.delete_object(Bucket=bucket, Key="v")
    assert d["DeleteMarker"] is True
    try:
        s3.get_object(Bucket=bucket, Key="v")
        raise AssertionError("expected 404 after a delete marker")
    except ClientError as e:
        assert e.response["Error"]["Code"] == "NoSuchKey"
        assert e.response["ResponseMetadata"]["HTTPHeaders"].get("x-amz-delete-marker") == "true"
    lv = s3.list_object_versions(Bucket=bucket, Prefix="v")
    assert [x["VersionId"] for x in lv["Versions"]] == [v2, v1]
    assert len(lv["DeleteMarkers"]) == 1 and lv["DeleteMarkers"][0]["IsLatest"]
    s3.delete_object(Bucket=bucket, Key="v", VersionId=lv["DeleteMarkers"][0]["VersionId"])
    assert s3.get_object(Bucket=bucket, Key="v")["Body"].read() == b"two"
    # Conditional reads.
    etag = s3.head_object(Bucket=bucket, Key="v")["ETag"]
    try:
        s3.get_object(Bucket=bucket, Key="v", IfNoneMatch=etag)
        raise AssertionError("expected 304")
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 304
    try:
        s3.get_object(Bucket=bucket, Key="v", IfMatch='"0000"')
        raise AssertionError("expected 412")
    except ClientError as e:
        assert e.response["Error"]["Code"] == "PreconditionFailed"
    # Conditional PUT.
    try:
        s3.put_object(Bucket=bucket, Key="v", Body=b"x", IfNoneMatch="*")
        raise AssertionError("expected 412")
    except ClientError as e:
        assert e.response["Error"]["Code"] == "PreconditionFailed"
    # CopyObject with a version and REPLACE metadata.
    c = s3.copy_object(
        Bucket=bucket,
        Key="v-copy",
        CopySource={"Bucket": bucket, "Key": "v", "VersionId": v1},
        MetadataDirective="REPLACE",
        Metadata={"k": "v"},
    )
    assert c["CopySourceVersionId"] == v1
    got = s3.get_object(Bucket=bucket, Key="v-copy")
    assert got["Body"].read() == b"one" and got["Metadata"] == {"k": "v"}
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    # DeleteObjects.
    for k in ["d/1", "d/2", "d/3"]:
        s3.put_object(Bucket=bucket, Key=k, Body=b"x")
    r = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "d/1"}, {"Key": "d/2"}, {"Key": "missing"}]})
    assert sorted(x["Key"] for x in r["Deleted"]) == ["d/1", "d/2", "missing"], r
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix="d/")["Contents"]]
    assert keys == ["d/3"], keys
    print("boto3 compat: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
