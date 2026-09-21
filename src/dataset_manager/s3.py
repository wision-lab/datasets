from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

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
