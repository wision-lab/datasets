from __future__ import annotations

import os
import tarfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import smart_open

from .sizes import _bytes_to_str

if TYPE_CHECKING:
    from types_boto3_s3 import Client as S3Client

# Archives whose member list can be read by seeking alone, so the payload is
# never downloaded. Tar has no central directory, so listing it walks the
# 512-byte headers and seeks past each member's data; that is many small ranged
# GETs, unlike zip's single central-directory read.
SEEKABLE_ARCHIVE_SUFFIXES = (".zip", ".tar")

# Compressed tar variants are a single compressed stream: listing their members
# would require decompressing (and therefore downloading) the whole object, so
# they are reported and treated as opaque files instead.
COMPRESSED_TAR_SUFFIXES = (".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".tar.zst", ".tzst")

# Size of the buffers files are streamed into archives with. Also the granularity
# at which per-chunk compression progress is reported.
_ZIP_BUFFER_SIZE = 1 << 20  # 1 MiB

# Size of each individual read from the source. Deliberately much smaller than
# `_ZIP_BUFFER_SIZE`: some NAS/FUSE mounts reject reads larger than their
# advertised `max_read` with `OSError: [Errno 22] Invalid argument`. The source is
# opened unbuffered so this is exactly the size handed to the OS: a buffered
# reader would coalesce reads up to `io.DEFAULT_BUFFER_SIZE` (128 KiB on CPython
# 3.14), which is what the previous `archive.write(...)` path effectively did.
_ZIP_READ_SIZE = 1 << 16  # 64 KiB


def write_zip_stream(
    archive: zipfile.ZipFile,
    src: Path,
    *,
    arcname: str | os.PathLike,
    on_bytes: Callable[[int], None] | None = None,
) -> None:
    """Add `src` to `archive` under `arcname`, streaming it in fixed-size buffers.

    Equivalent to `ZipFile.write` (same LZMA compression, mtime and permission
    metadata) except that `on_bytes` is invoked with the number of raw bytes
    written so far, so callers can report progress that advances even within a
    single large file.

    Reads are issued in `_ZIP_READ_SIZE` blocks rather than `_ZIP_BUFFER_SIZE`
    ones: some NAS/FUSE mounts answer reads larger than their `max_read` with
    `OSError: [Errno 22]`. The source is opened unbuffered, so `_ZIP_READ_SIZE` is
    exactly the size handed to the OS, and `on_bytes` is only called once per
    `_ZIP_BUFFER_SIZE` of accumulated input to keep progress updates cheap.

    Note:
        `ZipInfo.from_file` defaults to `ZIP_STORED`, so the archive's compression
        must be copied onto the entry explicitly (as `ZipFile.write` does), else
        the data would be stored uncompressed. Zip64 is enabled automatically for
        large files because `from_file` records the source size.
    """
    zinfo = zipfile.ZipInfo.from_file(src, arcname)
    zinfo.compress_type = archive.compression
    zinfo._compresslevel = archive.compresslevel  # type: ignore[attr-defined]
    pending = 0
    try:
        with open(src, "rb", buffering=0) as fileobj, archive.open(zinfo, "w") as dest:
            while chunk := fileobj.read(_ZIP_READ_SIZE):
                dest.write(chunk)
                if on_bytes is not None:
                    pending += len(chunk)
                    if pending >= _ZIP_BUFFER_SIZE:
                        on_bytes(pending)
                        pending = 0
            # Report the tail, which is almost never an exact multiple of the buffer.
            if on_bytes is not None and pending:
                on_bytes(pending)
    except OSError as error:
        # Name the offending file: the traceback would otherwise only point at
        # this function, which is useless when thousands of files are archived.
        error.add_note(f"while archiving {src} ({_bytes_to_str(zinfo.file_size)})")
        raise


def archive_suffix(key: str) -> str | None:
    """Classify `key` by archive suffix.

    Returns a member of `SEEKABLE_ARCHIVE_SUFFIXES` for an archive whose members
    can be listed without downloading, a member of `COMPRESSED_TAR_SUFFIXES` for
    an archive that cannot, or None for a plain object.
    """
    name = key.lower()
    for suffix in (*SEEKABLE_ARCHIVE_SUFFIXES, *COMPRESSED_TAR_SUFFIXES):
        if name.endswith(suffix):
            return suffix
    return None


def remote_archive_members(*, s3_client: S3Client, bucket: str, key: str) -> dict[str, tuple[int, int | None]]:
    """Read a remote archive's member list, returning member metadata.

    Only archive metadata is fetched (ranged GETs through `smart_open`): zip
    members come from the central directory, tar members from walking the headers
    and seeking past each payload. Member data is never downloaded. Returns a
    mapping of `member_name -> (uncompressed_size, crc32)`, where the CRC is None
    for tar, which stores no per-member checksum. Directory entries are skipped.
    """
    suffix = archive_suffix(key)
    with smart_open.open(
        f"s3://{bucket}/{key}",
        "rb",
        transport_params={"client": s3_client, "defer_seek": True},
        compression="disable",
    ) as fileobj:
        if suffix == ".zip":
            with zipfile.ZipFile(fileobj) as archive:
                return {
                    info.filename: (info.file_size, info.CRC)
                    for info in archive.infolist()
                    if not info.filename.endswith("/")
                }
        if suffix == ".tar":
            # "r:" forces the uncompressed, seekable reader (auto-detection would
            # try to decompress the raw stream).
            with tarfile.open(fileobj=fileobj, mode="r:") as archive:
                return {info.name: (info.size, None) for info in archive if not info.isdir()}
    raise ValueError(f"{key!r} is not a seekable archive")
