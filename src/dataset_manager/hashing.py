"""Block-wise hashing of binary streams, shared by the commands that need it."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import IO

# Size of the blocks a file is hashed in, so a multi-GB file never lands in memory.
HASH_BLOCK_SIZE = 1 << 20  # 1 MiB


class HashCancelled(Exception):
    """Raised when `md5_stream` was asked to stop before reaching the end.

    Distinct from an IO error, and never accompanied by a digest: an aborted hash
    must not be comparable or reportable as if it were complete.
    """


def md5_stream(
    fileobj: IO[bytes],
    *,
    block_size: int = HASH_BLOCK_SIZE,
    cancelled: Callable[[], bool] | None = None,
    on_bytes: Callable[[int], None] | None = None,
    on_progress: Callable[[int], None] | None = None,
    source: IO[bytes] | None = None,
) -> tuple[str, int]:
    """MD5 and byte length of a binary stream, read in fixed-size blocks.

    Takes an open stream rather than a path so a decompressed gzip member is
    hashed by the same code path as a plain file: `gzip.GzipFile` is a binary
    stream and its reads already re-block internally. The stream is not closed and
    its position is not restored; the caller owns it. `readinto` reuses one
    buffer, so the peak extra allocation is `block_size` regardless of file size.
    An empty stream hashes in one call and never hits the filesystem.

    `cancelled` is polled once per block, before the block is hashed; when it
    returns True, `HashCancelled` is raised and no digest is produced. A caller
    that has already read part of the stream therefore cannot mistake a partial
    hash for a final one.

    `on_bytes` is called with each block's length: the bytes hashed, which for a
    gzip member is its *decompressed* size. It is called after the digest update,
    so a caller that sees the callback has seen the bytes counted.

    `on_progress` is the alternative progress signal for compressed files: it is
    called with the number of bytes consumed from `source`, the underlying file,
    so a bar can show "bytes read / bytes in the file" against the stored size.
    Pass `source` (the raw file under a `gzip.GzipFile`) whenever `on_progress` is
    given; without it, `fileobj` is measured directly.
    """
    size = 0
    hasher = hashlib.md5()
    underlying = source if source is not None else fileobj
    with memoryview(bytearray(block_size)) as buffer:
        # `readinto` exists on both `FileIO` and `GzipFile` at runtime, but
        # `IO[bytes]` does not declare it in typeshed, so the parameter is typed
        # loosely and asserted here rather than narrowing the accepted streams.
        while chunk := fileobj.readinto(buffer):  # type: ignore[attr-defined]
            if cancelled is not None and cancelled():
                raise HashCancelled("hashing stopped early")
            hasher.update(buffer[:chunk])
            size += chunk
            if on_bytes is not None:
                on_bytes(chunk)
            if on_progress is not None:
                on_progress(_position(underlying))
    return hasher.hexdigest(), size


def _position(fileobj: IO[bytes]) -> int:
    """Current offset of a raw file object, or 0 if it cannot be told.

    A buffered reader's `tell()` subtracts its own buffer, so it reports progress
    the file itself has made rather than what the reader has pulled; that is what
    a "bytes read / bytes in the file" bar wants. Falls back to 0 rather than
    raising, because progress reporting must never break a hash.
    """
    try:
        return fileobj.tell()
    except OSError, ValueError:
        return 0
