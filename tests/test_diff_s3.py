"""Tests for the S3 operand of `dataset_manager.commands.diff_s3`, against a fake client.

No network and no credentials: `FakeS3` implements the two client calls `diff_s3` makes
(`list_objects_v2` through a paginator, and the ranged `get_object` that `smart_open`
issues when `remote_archive_members` lists an archive), so the real collector and the
real `remote_archive_members` run against in-memory bytes.

Scoped to what differs between a prefix and a local directory: a loose object's ETag
becomes its checksum only when it is a plain MD5, archive members are placed under the
archive's parent directory, and a compressed tar is reported rather than downloaded.
"""

import hashlib
import io
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from dataset_manager.archives import remote_archive_members
from dataset_manager.commands.diff_s3 import (
    Fingerprint,
    Prefix,
    _collect_local_files,
    _collect_virtual_files,
    _differs,
)

if TYPE_CHECKING:
    from types_boto3_s3 import Client as S3Client


def md5_etag(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


class _Body(io.BytesIO):
    """The subset of botocore's `StreamingBody` smart_open's reader uses."""

    def close(self) -> None:  # `BytesIO.close` would make the object unusable
        pass

    def release_conn(self) -> None:
        pass


class _Paginator:
    def __init__(self, client: FakeS3) -> None:
        self._client = client

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, object]]:
        objects = self._client.objects(Bucket, Prefix)
        return [{"Contents": objects}] if objects else [{}]


class FakeS3:
    """An in-memory S3 endpoint holding `bucket -> {key: bytes}`."""

    def __init__(self, bucket: str, objects: dict[str, bytes]) -> None:
        self.bucket = bucket
        self._objects = dict(objects)
        self.etags = {key: md5_etag(data) for key, data in objects.items()}
        self.get_object_keys: list[str] = []

    def objects(self, bucket: str, prefix: str) -> list[dict[str, object]]:
        assert bucket == self.bucket
        return [
            {"Key": key, "Size": len(data), "ETag": f'"{self.etags[key]}"'}
            for key, data in sorted(self._objects.items())
            if key.startswith(prefix)
        ]

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator(self)

    def get_object(self, *, Bucket: str, Key: str, Range: str | None = None) -> dict[str, object]:
        assert Bucket == self.bucket
        self.get_object_keys.append(Key)
        data = self._objects[Key]
        start, stop = 0, len(data) - 1
        partial = False
        if Range:
            # botocore hands the range through verbatim: "bytes=start-stop"
            first, _, last = Range.removeprefix("bytes=").partition("-")
            start = int(first) if first else 0
            stop = int(last) if last else len(data) - 1
            partial = True
        body = data[start : stop + 1]
        response: dict[str, object] = {
            "Body": _Body(body),
            "ContentLength": len(data),
            "ResponseMetadata": {
                "RetryAttempts": 0,
                # A ranged GET answers 206 (and reports the range); an unranged one 200.
                "HTTPStatusCode": 206 if partial else 200,
            },
        }
        if partial:
            response["ContentRange"] = f"bytes {start}-{start + len(body) - 1}/{len(data)}"
        return response


@pytest.fixture
def prefix() -> Prefix:
    return Prefix(bucket="test-bucket", prefix="data")


def collect(fake: FakeS3, prefix: Prefix) -> dict[str, Fingerprint]:
    return _collect_virtual_files(s3_client=cast("S3Client", fake), location=prefix, workers=2).files


def test_loose_object_etag_is_the_checksum_when_it_is_a_plain_md5(prefix: Prefix) -> None:
    payload = b"hello"
    fake = FakeS3("test-bucket", {"data/a.txt": payload})
    fingerprint = collect(fake, prefix)["a.txt"]
    assert (fingerprint.kind, fingerprint.size, fingerprint.checksum) == ("file", 5, md5_etag(payload))


def test_multipart_etag_leaves_the_checksum_unset(prefix: Prefix) -> None:
    """A multipart ETag is not an MD5 of the bytes, so it must not be compared as one."""
    fake = FakeS3("test-bucket", {"data/big.bin": b"x" * 16})
    fake.etags["data/big.bin"] = "cf0427245a2d0f7f08805889a6d44993-123"
    fingerprint = collect(fake, prefix)["big.bin"]
    assert fingerprint.checksum is None


def test_archive_members_land_under_the_archive_parent_directory(prefix: Prefix) -> None:
    archive = zip_bytes({"1.jpg": b"image-one", "2.jpg": b"image-two-longer"})
    fake = FakeS3("test-bucket", {"data/a/x/part_0.zip": archive})

    files = collect(fake, prefix)
    assert sorted(files) == ["a/x/1.jpg", "a/x/2.jpg"]
    assert files["a/x/1.jpg"].kind == "zip"
    assert files["a/x/1.jpg"].size == 9
    assert files["a/x/1.jpg"].checksum is not None, "a zip member carries the central-directory CRC32"

    # Member data is never fetched: only the archive itself is ever read, and always
    # through ranged GETs (the member list comes from the central directory).
    assert set(fake.get_object_keys) == {"data/a/x/part_0.zip"}


def test_a_local_copy_of_the_same_tree_produces_the_same_virtual_keys(tmp_path: Path, prefix: Prefix) -> None:
    """The S3 operand and a local extracted copy agree key-for-key, which is the whole point.

    Both sides are compared through `_differs` here so the pairing (a zip member against
    an extracted loose file) is covered end to end, not just the key derivation.
    """
    archive = zip_bytes({"1.jpg": b"image-one", "2.jpg": b"image-two-longer"})
    fake = FakeS3(
        "test-bucket",
        {"data/a/x/part_0.zip": archive, "data/b/y.txt": b"other"},
    )
    extracted = tmp_path / "extracted"
    (extracted / "a" / "x").mkdir(parents=True)
    (extracted / "a" / "x" / "1.jpg").write_bytes(b"image-one")
    (extracted / "a" / "x" / "2.jpg").write_bytes(b"image-two-longer")
    (extracted / "b").mkdir()
    (extracted / "b" / "y.txt").write_bytes(b"other")

    source = collect(fake, prefix)
    target = _collect_local_files(path=extracted, workers=2).files
    assert sorted(source) == sorted(target)
    assert not [path for path in source if _differs(source[path], target[path])]


def test_a_compressed_tar_is_reported_not_downloaded(prefix: Prefix, caplog: pytest.LogCaptureFixture) -> None:
    """A `.tar.gz` is classified as unlistable, not attempted and then failed.

    Both paths leave the object opaque, so the distinguishing observable is which
    message is logged: the compressed-tar report, never the "failed to read as an
    archive" fallback that a download attempt would produce.
    """
    fake = FakeS3("test-bucket", {"data/part.tar.gz": b"\x1f\x8b not really gzip"})
    with caplog.at_level("WARNING"):
        files = collect(fake, prefix)
    assert files["part.tar.gz"].kind == "file", "an unlistable archive stays opaque"
    assert fake.get_object_keys == [], "listing it would require decompressing (and downloading) the whole stream"
    assert "is a compressed tar" in caplog.text
    assert "as an archive" not in caplog.text, "the tar must be classified, not attempted and failed"


def test_remote_archive_members_reports_a_zip_frame(prefix: Prefix) -> None:
    archive = zip_bytes({"a/x/1.jpg": b"image-one", "a/x/2.jpg": b"image-two-longer"})
    fake = FakeS3("test-bucket", {"data/pack.zip": archive})
    members = remote_archive_members(s3_client=cast("S3Client", fake), bucket="test-bucket", key="data/pack.zip")
    assert sorted(members) == ["a/x/1.jpg", "a/x/2.jpg"]
    assert members["a/x/1.jpg"][0] == 9
    assert isinstance(members["a/x/1.jpg"][1], int), "a zip member carries a central-directory CRC32"


def test_a_corrupt_archive_falls_back_to_a_loose_object(prefix: Prefix) -> None:
    """An object that will not open as an archive stays in the diff by its ETag."""
    fake = FakeS3("test-bucket", {"data/broken.zip": b"not a zip at all"})
    files = collect(fake, prefix)
    assert files["broken.zip"].kind == "file", "a failed archive must not disappear from the comparison"
    assert files["broken.zip"].size == 16
