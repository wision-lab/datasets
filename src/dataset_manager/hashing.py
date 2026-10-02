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

    `on_bytes` is called with each block's length, which is what drives a
    per-file progress bar. It is called after the digest update, so a caller that
    sees the callback has seen the bytes counted.
    """
    size = 0
    hasher = hashlib.md5()
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
    return hasher.hexdigest(), size
