from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

from .log import log

if TYPE_CHECKING:
    from types_boto3_s3 import Client as S3Client


@dataclass
class S3Connection:
    bucket: str | None = None
    """Name of S3 bucket."""
    prefix: str | None = None
    """Prefix path of s3 objects."""


# Retry configuration for S3 requests. botocore's default `legacy` retry mode
# does not retry TLS handshake / certificate validation errors (SSLError), so a
# single flaky connection can abort an otherwise long upload. `standard` mode
# treats ConnectionError / HTTPClientError as transient and retries them with
# exponential backoff, jitter and a retry quota.
_S3_RETRY_CONFIG = Config(retries={"mode": "standard", "total_max_attempts": 10})

# Public read endpoint for the `public-datasets` bucket. Used when neither
# `--endpoint-url` nor the `AWS_ENDPOINT_URL` environment variable is set, so
# unsigned reads of the public bucket work out of the box instead of hitting
# AWS and getting `AccessDenied`.
DEFAULT_ENDPOINT_URL = "https://web.s3.wisc.edu/"


def make_client(*, sign: bool = False, endpoint_url: str | None = None) -> S3Client:
    """Create an S3 client for read-only listing/opening.

    Requests are unsigned by default, which is what public buckets need; pass
    `sign=True` to use the standard boto3 credential chain. `endpoint_url`
    defaults to the `AWS_ENDPOINT_URL` environment variable, and then to
    `DEFAULT_ENDPOINT_URL`.
    """
    config = _S3_RETRY_CONFIG if sign else _S3_RETRY_CONFIG.merge(Config(signature_version=botocore.UNSIGNED))
    endpoint_url = endpoint_url or os.environ.get("AWS_ENDPOINT_URL") or DEFAULT_ENDPOINT_URL
    return boto3.client("s3", config=config, endpoint_url=endpoint_url)


def resolve_prefix(value: str, bucket: str | None) -> tuple[str, str]:
    """Resolve a prefix argument to a `(bucket, prefix)` pair.

    `value` may be a full `s3://bucket/prefix` URI (any `s3`/`s3n`/`s3a`
    scheme), in which case `bucket` is ignored, or a bare prefix that is then
    combined with `bucket`.
    """
    parts = urlsplit(value)
    if parts.scheme in {"s3", "s3n", "s3a"}:
        if not parts.netloc:
            raise ValueError(f"S3 URI without a bucket: {value!r}")
        return parts.netloc, parts.path.lstrip("/").rstrip("/")
    if "://" in value:
        raise ValueError(f"Unsupported URI scheme in {value!r}; expected an s3:// URI.")
    if bucket is None:
        raise ValueError(f"{value!r} is a bare prefix; pass --bucket or use an s3:// URI.")
    return bucket, value.strip("/")


@dataclass(frozen=True)
class S3Object:
    """An object under a prefix, with the key needed to read it back.

    `list_objects` returns these keyed by the object's path *relative* to the
    prefix, but the full `key` is kept alongside because an S3 prefix is a plain
    string match: a prefix like `.../pano` also matches the sibling
    `.../pano_001.zip`, whose relative path (`_001.zip`) cannot be turned back
    into a key by joining it onto the prefix.
    """

    key: str
    size: int
    etag: str


def list_objects(*, s3_client: S3Client, bucket: str, prefix: str) -> dict[str, S3Object]:
    """List every object under `prefix`, keyed by its path relative to `prefix`.

    Directory-marker keys (those ending in `/`) are skipped, and the surrounding
    quotes are stripped from each ETag.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    objects: dict[str, S3Object] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            relative = key[len(prefix) :].lstrip("/") if prefix else key
            if not relative:
                continue
            objects[relative] = S3Object(key=key, size=obj["Size"], etag=obj.get("ETag", "").strip('"'))
    return objects


def check_exists(*, s3_client: S3Client, conn: S3Connection, key: str | os.PathLike) -> int:
    if conn.bucket is None:
        raise ValueError("Bucket name not specified!")
    try:
        return s3_client.head_object(Bucket=conn.bucket, Key=str(Path(conn.prefix or "") / key))["ContentLength"]
    except ClientError as e:
        # A missing object simply means it does not exist yet. Any other error
        # (e.g. 403, bad credentials, wrong bucket/region) must NOT be silently
        # treated as "absent", otherwise the upload would proceed as though it
        # were safe to (over)write a key it may not actually own.
        response = e.response or {}
        error_code = str(response.get("Error", {}).get("Code", ""))
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return 0
        raise


def upload_file(
    src: str | os.PathLike,
    dst: str | os.PathLike,
    *,
    s3_client: S3Client,
    conn: S3Connection,
    public: bool = True,
    callback: Callable | None = None,
) -> None:
    if conn.bucket is None:
        raise ValueError("Bucket name not specified!")
    try:
        log.info(f"Uploading {src} as {dst}...")
        extra_args = {"ACL": "public-read"} if public else {}
        s3_client.upload_file(
            str(src),
            conn.bucket,
            str(Path(conn.prefix or "") / dst),
            ExtraArgs=extra_args,
            # boto3 invokes `callback(bytes_transferred)` intermittently during
            # the transfer, which drives the per-chunk upload progress bar.
            Callback=callback,
        )
    except ClientError as e:
        log.error(f"Failed to upload {src} to {dst}.")
        log.error(e)
