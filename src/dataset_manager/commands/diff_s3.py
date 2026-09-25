from __future__ import annotations

import hashlib
import os
import posixpath
import re
import tarfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from natsort import natsorted
from rich.status import Status

from ..app import app
from ..archives import (
    COMPRESSED_TAR_SUFFIXES,
    SEEKABLE_ARCHIVE_SUFFIXES,
    archive_suffix,
    remote_archive_members,
)
from ..log import log
from ..paths import sanitize_name
from ..s3 import list_objects, make_client, resolve_prefix
from ..sizes import _bytes_to_str

if TYPE_CHECKING:
    from types_boto3_s3 import Client as S3Client

# A loose object's ETag is the MD5 of its bytes only for single-part uploads;
# multipart ETags are `md5-of-md5s-N` and depend on the part sizes, so the same
# bytes uploaded to two prefixes can carry different ETags. Only a plain
# 32-hex-character MD5 is therefore safe to compare across prefixes.
_MD5_ETAG_RE = re.compile(r"^[0-9a-f]{32}$")

# Local files that `upload` never archives, so the S3 side cannot have them: excluded
# from the local walk rather than reported as one-sided noise.
_IGNORED_LOCAL_NAMES = frozenset({".DS_Store", "Thumbs.db"})

# Size of the blocks a local file is hashed in, so a multi-GB file never lands in memory.
_HASH_BLOCK_SIZE = 1 << 20  # 1 MiB


@dataclass(frozen=True)
class Prefix:
    """A resolved S3 location."""

    bucket: str
    prefix: str

    def __str__(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}" if self.prefix else f"s3://{self.bucket}"


@dataclass(frozen=True)
class Fingerprint:
    """Identity of one virtual file, used for the changed/identical decision."""

    kind: str  # "file" (loose object) or "zip" (zip member)
    size: int
    checksum: str | None = None  # MD5 for loose files, CRC32 for zip members

    def __str__(self) -> str:
        size = _bytes_to_str(self.size)
        if self.checksum is None:
            return f"{size}"
        label = "crc32" if self.kind in {"zip", "tar"} else "md5"
        return f"{size} ({label} {self.checksum})"


@dataclass(frozen=True)
class LocalRoot:
    """A resolved local directory."""

    path: Path

    def __str__(self) -> str:
        return str(self.path)


@dataclass
class VirtualTree:
    """The extracted file tree of one prefix, plus the counts used in the summary."""

    files: dict[str, Fingerprint] = field(default_factory=dict)
    objects: int = 0
    nested_archives: int = 0
    compressed_archives: int = 0


def _differs(a: Fingerprint, b: Fingerprint) -> bool:
    """Decide whether two fingerprints for the same virtual path differ.

    Compared by size and checksum when both sides carry a checksum of the same kind,
    and by size alone otherwise. The kinds differ for an S3 operand against a local one
    whenever only one side expanded an archive at that path: a zip member carries the
    archive's CRC32 while the extracted local copy carries a whole-file MD5, and a tar
    member carries no checksum at all. Those pairings are decided by size, so an
    unchanged extracted copy still reports identical and a size change still reports a
    difference; a content edit that keeps the size is not detectable across checksum
    kinds.
    """
    if a.checksum is None or b.checksum is None or a.kind != b.kind:
        return a.size != b.size
    return (a.size, a.checksum) != (b.size, b.checksum)


def _member_key(parent: str, member: str) -> str | None:
    """Virtual key of an archive member, joined onto the archive's parent directory.

    Shared by the S3 and local collectors so the two sides cannot normalize a member
    differently: `/` separators are unified, and empty, `.` and `..` components, plus a
    leading `/`, are dropped. An archive built on Windows or with a leading-slash
    arcname would otherwise produce keys only one side recognizes, and every such member
    would be reported as present on one side only.

    Returns None for a member that normalizes to nothing (or to the parent directory
    itself), which is a directory-like entry the member lists already skip; keeping it
    would file the member under the archive's own key.
    """
    parts = [part for part in member.replace("\\", "/").split("/") if part not in {"", ".", ".."}]
    if not parts:
        return None
    return posixpath.join(parent, *parts)


def _local_key(path: Path, root: Path) -> str | None:
    """Path of a local file relative to `root`, with `/` separators.

    `Path.resolve` collapses `.` and `..` lexically (and without requiring the
    components to exist), so a dataset extracted with `7z x`, whose member names can
    carry a leading `./`, maps to the same key as the S3 member it came from. Returns
    None when `path` does not live under `root`.
    """
    try:
        return "/".join(path.resolve().relative_to(root.resolve()).parts)
    except ValueError:
        return None


def _collect_virtual_files(*, s3_client: S3Client, location: Prefix, workers: int) -> VirtualTree:
    """Build the virtual extracted tree of one prefix.

    Loose objects become leaves at their key path. Zip and tar objects are
    expanded by reading their member lists; each member is placed under the
    archive's parent directory, mirroring `7z x archive -o$(dirname archive)` and
    the arcnames written by `upload`. Compressed tars cannot be listed without a
    full download, so they are reported and kept as opaque files.
    """
    objects = list_objects(s3_client=s3_client, bucket=location.bucket, prefix=location.prefix)
    tree = VirtualTree(objects=len(objects))

    suffixes = {key: archive_suffix(key) for key in objects}
    archive_keys = [key for key, suffix in suffixes.items() if suffix in SEEKABLE_ARCHIVE_SUFFIXES]
    compressed_keys = [key for key, suffix in suffixes.items() if suffix in COMPRESSED_TAR_SUFFIXES]
    for key in compressed_keys:
        tree.compressed_archives += 1
        log.warning(
            f"{location.bucket}/{objects[key].key} is a compressed tar; listing it would require a full "
            "download, so it is treated as a plain file."
        )

    manifests: dict[str, dict[str, tuple[int, int | None]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for key in archive_keys:
            full_key = objects[key].key
            futures[
                executor.submit(remote_archive_members, s3_client=s3_client, bucket=location.bucket, key=full_key)
            ] = (
                key,
                full_key,
            )
        for future in as_completed(futures):
            key, full_key = futures[future]
            try:
                manifests[key] = future.result()
            except Exception as error:  # noqa: BLE001 - a bad archive must not abort the whole diff
                log.warning(
                    f"Failed to read {location.bucket}/{full_key} as an archive ({error}); treating it as a plain file."
                )

    for key, manifest in manifests.items():
        kind = "tar" if suffixes[key] == ".tar" else "zip"
        parent = posixpath.dirname(key)
        has_nested = False
        for member, (size, crc) in manifest.items():
            virtual_key = _member_key(parent, member)
            if virtual_key is None:
                continue
            if archive_suffix(member) is not None:
                has_nested = True
            tree.files[virtual_key] = Fingerprint(kind=kind, size=size, checksum=None if crc is None else str(crc))
        if has_nested:
            tree.nested_archives += 1
            log.warning(
                f"{location.bucket}/{objects[key].key} contains nested archive member(s); not recursing into them."
            )

    for key, obj in objects.items():
        if key in manifests:
            continue
        checksum = obj.etag if _MD5_ETAG_RE.match(obj.etag) else None
        tree.files[key] = Fingerprint(kind="file", size=obj.size, checksum=checksum)

    return tree


def _local_archive_members(path: Path) -> dict[str, tuple[int, int | None]]:
    """Read a local archive's member list, returning member metadata.

    The filesystem counterpart of `remote_archive_members`: zip members come from the
    central directory and tar members from walking the headers, both by seeking, so no
    member data is read. Returns `member_name -> (uncompressed_size, crc32)`, where the
    CRC is None for tar, which stores no per-member checksum. Directory entries are
    skipped.
    """
    suffix = archive_suffix(path.name)
    with open(path, "rb") as fileobj:
        if suffix == ".zip":
            with zipfile.ZipFile(fileobj) as archive:
                return {
                    info.filename: (info.file_size, info.CRC)
                    for info in archive.infolist()
                    if not info.filename.endswith("/")
                }
        if suffix == ".tar":
            # "r:" forces the uncompressed, seekable reader (auto-detection would try to
            # decompress the raw stream).
            with tarfile.open(fileobj=fileobj, mode="r:") as archive:
                return {info.name: (info.size, None) for info in archive if not info.isdir()}
    raise ValueError(f"{path} is not a seekable archive")


def _local_file_fingerprint(path: Path) -> Fingerprint | None:
    """Fingerprint a local file: size plus the MD5 of its contents.

    Returns None for a `_IGNORED_LOCAL_NAMES` entry, which therefore never enters the
    comparison: `upload` never archives such a file, so no S3 object can match it, and
    keeping it would only ever report it as one-sided noise. The MD5 is computed over
    the whole file in fixed-size blocks, so a multi-GB file never lands in memory; an
    empty file hashes in a single call and never hits the filesystem.
    """
    if path.name in _IGNORED_LOCAL_NAMES:
        return None
    size = 0
    hasher = hashlib.md5()
    with open(path, "rb", buffering=0) as fileobj, memoryview(bytearray(_HASH_BLOCK_SIZE)) as buffer:
        while chunk := fileobj.readinto(buffer):
            hasher.update(buffer[:chunk])
            size += chunk
    return Fingerprint(kind="file", size=size, checksum=hasher.hexdigest())


def _collect_local_files(*, path: Path, workers: int) -> VirtualTree:
    """Build the virtual extracted tree of a local directory.

    Mirrors `_collect_virtual_files`: seekable archives are expanded by reading their
    member lists from the filesystem (nothing is decompressed), compressed tars are
    reported and kept as opaque files, and every other file is a loose file. S3 has no
    directory entries, so directories are not part of the comparison.
    """
    path = path.resolve()
    entries: dict[str, Path] = {}
    archives: dict[str, Path] = {}
    compressed_keys: list[str] = []
    for dirpath, dirnames, filenames in os.walk(path):
        dirnames.sort()
        for name in sorted(filenames):
            source = Path(dirpath) / name
            relative = _local_key(source, path)
            if relative is None:
                continue
            entries[relative] = source
            suffix = archive_suffix(name)
            if suffix in SEEKABLE_ARCHIVE_SUFFIXES:
                archives[relative] = source
            elif suffix in COMPRESSED_TAR_SUFFIXES:
                compressed_keys.append(relative)

    tree = VirtualTree(objects=len(entries))
    for relative in sorted(compressed_keys):
        tree.compressed_archives += 1
        log.warning(
            f"{entries[relative]} is a compressed tar; listing it would require decompressing "
            "it, so it is treated as a plain file."
        )

    manifests: dict[str, dict[str, tuple[int, int | None]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_local_archive_members, archives[relative]): relative for relative in archives}
        for future in as_completed(futures):
            relative = futures[future]
            try:
                manifests[relative] = future.result()
            except Exception as error:  # noqa: BLE001 - a bad archive must not abort the whole diff
                log.warning(f"Failed to read {archives[relative]} as an archive ({error}); treating it as a plain file.")

    for relative, manifest in manifests.items():
        kind = "tar" if archive_suffix(relative) == ".tar" else "zip"
        parent = posixpath.dirname(relative)
        has_nested = False
        for member, (size, crc) in manifest.items():
            virtual_key = _member_key(parent, member)
            if virtual_key is None:
                continue
            tree.files[virtual_key] = Fingerprint(kind=kind, size=size, checksum=None if crc is None else str(crc))
            if archive_suffix(member) is not None:
                has_nested = True
        if has_nested:
            tree.nested_archives += 1
            log.warning(f"{archives[relative]} contains nested archive member(s); not recursing into them.")

    for relative, source in entries.items():
        if relative in manifests:
            continue
        try:
            fingerprint = _local_file_fingerprint(source)
        except OSError as error:
            log.warning(f"Failed to fingerprint {source} ({error}); skipping it.")
            continue
        if fingerprint is None:
            continue
        tree.files[relative] = fingerprint

    return tree


def _resolve_operand(value: str, bucket: str | None) -> Prefix | LocalRoot:
    """Resolve a diff operand: an S3 prefix or a local directory.

    An `s3`/`s3n`/`s3a` URI is always S3. Any other value that names an existing local
    directory is a local operand, even when `--bucket` is given: a path like `/data/x`
    cannot be a prefix, so letting `--bucket` claim it would only ever diff the wrong
    thing. Everything else is resolved as a prefix by `resolve_prefix`, which requires
    `--bucket` for a bare word and rejects a bare word with no `--bucket` rather than
    implying a local directory that does not exist.
    """
    if urlsplit(value).scheme in {"s3", "s3n", "s3a"}:
        return Prefix(*resolve_prefix(value, bucket))
    candidate = Path(value)
    if candidate.is_dir():
        return LocalRoot(path=candidate.resolve())
    if "://" in value:
        return Prefix(*resolve_prefix(value, bucket))
    if bucket is None:
        raise ValueError(
            f"{value!r} is neither an s3:// URI nor an existing local directory; give --bucket to read it as a prefix."
        )
    return Prefix(*resolve_prefix(value, bucket))


def _print_section(title: str, lines: list[str], *, summary_only: bool) -> None:
    if not lines:
        return
    print(f"{title} ({len(lines)}):")
    if not summary_only:
        for line in lines:
            print(f"  {line}")
    print()


@app.command
def diff_s3(
    source: str,
    target: str,
    /,
    bucket: str | None = None,
    sign: bool = False,
    workers: int = 8,
    summary_only: bool = False,
    fail_on_diff: bool = False,
    normalize: bool = False,
    endpoint_url: str | None = None,
) -> None:
    """Diff the extracted file trees of two S3 prefixes or local directories.

    Loose objects are compared directly; `.zip` and uncompressed `.tar` objects
    are expanded by reading their member lists over ranged requests (the archives
    are never downloaded), and their members are placed under the archive's parent
    directory, mirroring `7z x archive -o$(dirname archive)`. Nested archive
    members are reported but not recursed into, and compressed tars (`.tar.gz`
    and friends) are reported as unexpandable rather than downloaded.

    A local directory operand is walked and its `.zip`/uncompressed `.tar` files are
    expanded from the filesystem the same way. Loose local files are compared by their
    real size and MD5 against the object's ETag when that ETag is a plain MD5, and
    `.DS_Store`/`Thumbs.db` (which `upload` never archives) are excluded.

    Args:
        source (str): First operand, either an `s3://bucket/prefix`, a bare prefix
            (then `--bucket` is required), or an existing local directory.
        target (str): Second operand, same conventions as `source`.
        bucket (str, optional): Bucket used for any bare-prefix argument.
        sign (bool, optional): Sign requests with the standard boto3 credential
            chain. Reads are unsigned by default, which is what public buckets
            need. Only affects S3 operands.
        workers (int, optional): Number of archives whose member lists are read
            concurrently. Default 8.
        summary_only (bool, optional): Print only the counts, not the file lists.
        fail_on_diff (bool, optional): Exit with status 1 if any file is missing
            on either side or differs, so the command can gate a verification
            step in a script. Default False.
        normalize (bool, optional): Apply `dm sanitize-paths`' Windows/SMB-safe
            conversion to both sides before comparing, so an old prefix whose
            names contain e.g. `:` matches its repackaged, sanitized copy.
        endpoint_url (str, optional): Override the S3 endpoint. Defaults to the
            `AWS_ENDPOINT_URL` variable, then to the public web endpoint
            (`https://web.s3.wisc.edu/`). Only affects S3 operands.
    """
    if workers < 1:
        raise ValueError("Argument `workers` must be at least 1.")

    source_root = _resolve_operand(source, bucket)
    target_root = _resolve_operand(target, bucket)

    s3_client = make_client(sign=sign, endpoint_url=endpoint_url)

    with Status("Building virtual trees...", spinner="bouncingBall"):
        if isinstance(source_root, LocalRoot):
            source_tree = _collect_local_files(path=source_root.path, workers=workers)
        else:
            source_tree = _collect_virtual_files(s3_client=s3_client, location=source_root, workers=workers)
        if isinstance(target_root, LocalRoot):
            target_tree = _collect_local_files(path=target_root.path, workers=workers)
        else:
            target_tree = _collect_virtual_files(s3_client=s3_client, location=target_root, workers=workers)

    print(f"Source: {source_root}  ({source_tree.objects} objects, {len(source_tree.files)} files)")
    print(f"Target: {target_root}  ({target_tree.objects} objects, {len(target_tree.files)} files)")
    print()

    # `--normalize` maps both sides through the same Windows/SMB-safe conversion
    # used by `dm sanitize-paths`, so an old prefix whose names contain e.g. `:`
    # lines up with its sanitized copy.
    source_files = (
        {sanitize_name(path): fp for path, fp in source_tree.files.items()} if normalize else source_tree.files
    )
    target_files = (
        {sanitize_name(path): fp for path, fp in target_tree.files.items()} if normalize else target_tree.files
    )

    only_source = natsorted(source_files.keys() - target_files.keys())
    only_target = natsorted(target_files.keys() - source_files.keys())
    changed = natsorted(
        path for path in source_files.keys() & target_files.keys() if _differs(source_files[path], target_files[path])
    )
    identical = len(source_files.keys() & target_files.keys()) - len(changed)

    _print_section("Only in source", list(only_source), summary_only=summary_only)
    _print_section("Only in target", list(only_target), summary_only=summary_only)
    _print_section(
        "Changed",
        [f"{path}  [{source_files[path]} -> {target_files[path]}]" for path in changed],
        summary_only=summary_only,
    )

    nested = source_tree.nested_archives + target_tree.nested_archives
    compressed = source_tree.compressed_archives + target_tree.compressed_archives
    print(
        f"Summary: {identical} identical, {len(only_source)} only in source, "
        f"{len(only_target)} only in target, {len(changed)} changed, "
        f"{nested} nested archive(s) skipped, {compressed} compressed archive(s) not expanded."
    )

    if fail_on_diff and (only_source or only_target or changed):
        raise SystemExit(1)
