from __future__ import annotations

import errno
import os
import tarfile
import time
import zipfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import smart_open

from .log import log
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

# Every archive suffix `archive_suffix` classifies: the seekable ones, whose member
# lists can be read by seeking, plus the compressed tars, which cannot be listed
# without decompressing the whole stream.
ARCHIVE_SUFFIXES = (*SEEKABLE_ARCHIVE_SUFFIXES, *COMPRESSED_TAR_SUFFIXES)

# Size of the buffers files are streamed into archives with. Also the granularity
# at which per-chunk compression progress is reported.
_ZIP_BUFFER_SIZE = 1 << 20  # 1 MiB

# Size of each individual read from the source. Deliberately smaller than
# `_ZIP_BUFFER_SIZE`: some NAS/FUSE mounts reject reads larger than their
# advertised `max_read` with `OSError: [Errno 22] Invalid argument`. The source is
# opened unbuffered so this is exactly the size handed to the OS: a buffered
# reader would coalesce reads up to `io.DEFAULT_BUFFER_SIZE` (128 KiB on CPython
# 3.14), which is what the previous `archive.write(...)` path effectively did.
_ZIP_READ_SIZE = 1 << 16  # 64 KiB

# Lower bound for the adaptive read size below. FUSE's own `max_read` defaults to
# this and no real mount advertises less, so a source that still answers `EINVAL`
# at this size is failing for some other reason and the error is raised.
_ZIP_MIN_READ_SIZE = 1 << 13  # 8 KiB

# Errors that mean "this read did not happen, but the file is fine": a connection
# drop or a server-side reboot makes the gvfs/NetworkManager FUSE layer surface
# `EIO`/`ESTALE` on a read of an otherwise healthy file. Anything else (`EINVAL`
# on a small read, `ENOENT`, `EACCES`, ...) is a real failure and propagates.
_TRANSIENT_READ_ERRNOS = frozenset({errno.EIO, errno.ESTALE})

# Per-read retry budget. The failures these cover last seconds at most (a
# reconnect), and a multi-TB archive issues millions of reads, so the budget is
# deliberately small: retries delay the error surfaced for a genuinely dead
# source far less than they extend the run when a mount flaps repeatedly.
_ZIP_READ_RETRIES = 4
_ZIP_READ_BACKOFF_S = 0.5


def _read_block(fileobj: Any, size: int) -> bytes:
    """Read up to `size` bytes, retrying the transient failures of a FUSE mount.

    Raises the original `OSError` once the retry budget for that single read is
    exhausted, so a dead mount still fails the chunk instead of stalling it.
    """
    for attempt in range(_ZIP_READ_RETRIES + 1):
        try:
            return fileobj.read(size)
        except OSError as error:
            if error.errno not in _TRANSIENT_READ_ERRNOS or attempt == _ZIP_READ_RETRIES:
                raise
            log.debug(f"Retrying read of {size} bytes after {error}")
            time.sleep(_ZIP_READ_BACKOFF_S * 2**attempt)
    raise AssertionError("unreachable")


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

    Reads start at `_ZIP_READ_SIZE` and are halved (down to `_ZIP_MIN_READ_SIZE`)
    whenever the source rejects one with `OSError: [Errno 22]`, which is how some
    NAS/FUSE mounts answer reads larger than the `max_read` they advertise. The
    reduced size is kept for the whole archive: mounts that reject a size do so
    consistently, so every later read would otherwise pay the same round trip to
    rediscover it. Transient errors (`EIO`/`ESTALE`, i.e. a FUSE mount whose
    connection blipped) are retried a few times before giving up. The source is
    opened unbuffered so the size handed to `read` is exactly the size handed to
    the OS: a buffered reader would coalesce reads up to `io.DEFAULT_BUFFER_SIZE`
    (128 KiB on CPython 3.14), which is what the previous `archive.write(...)`
    path effectively did.

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
            for chunk in _read_blocks(fileobj, src=src):
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


def _read_blocks(fileobj: Any, *, src: Path) -> Iterator[bytes]:
    """Yield `src`'s contents in blocks, shrinking the block size when required.

    `EINVAL` on a read means the FUSE mount refused a request of that size, not
    that the file is unreadable: halving the request until the mount accepts it
    keeps such a source archivable instead of failing the whole chunk. Any other
    error is either retried (transient) or raised, via `_read_block`.
    """
    size = _ZIP_READ_SIZE
    while True:
        try:
            chunk = _read_block(fileobj, size)
        except OSError as error:
            if error.errno != errno.EINVAL or size <= _ZIP_MIN_READ_SIZE:
                raise
            size //= 2
            log.warning(
                f"{src} rejected a {_bytes_to_str(size * 2)} read ({error}); retrying with {_bytes_to_str(size)} blocks."
            )
            continue
        if not chunk:
            return
        yield chunk


def archive_suffix(key: str) -> str | None:
    """Classify `key` by archive suffix.

    Returns a member of `SEEKABLE_ARCHIVE_SUFFIXES` for an archive whose members
    can be listed without downloading, a member of `COMPRESSED_TAR_SUFFIXES` for
    an archive that cannot, or None for a plain object.
    """
    name = key.lower()
    for suffix in ARCHIVE_SUFFIXES:
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
