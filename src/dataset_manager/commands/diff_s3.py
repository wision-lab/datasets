from __future__ import annotations

import posixpath
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

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
        label = "md5" if self.kind == "file" else "crc32"
        return f"{size} ({label} {self.checksum})"


@dataclass
class VirtualTree:
    """The extracted file tree of one prefix, plus the counts used in the summary."""

    files: dict[str, Fingerprint] = field(default_factory=dict)
    objects: int = 0
    nested_archives: int = 0
    compressed_archives: int = 0


def _differs(a: Fingerprint, b: Fingerprint) -> bool:
    """Decide whether two fingerprints for the same virtual path differ.

    Fingerprints are compared by size and checksum when both sides carry a
    checksum of the same kind (zip members by CRC32, loose files by MD5). Any
    other pairing - tar members (no checksum), a missing MD5, or a loose file
    against an archive member - falls back to size only, since the checksums are
    not comparable.
    """
    if a.kind == b.kind and a.checksum is not None and b.checksum is not None:
        return (a.size, a.checksum) != (b.size, b.checksum)
    return a.size != b.size


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
            member = member.lstrip("/")
            if archive_suffix(member) is not None:
                has_nested = True
            tree.files[posixpath.join(parent, member)] = Fingerprint(
                kind=kind, size=size, checksum=None if crc is None else str(crc)
            )
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
    """Diff the extracted file trees of two S3 prefixes.

    Loose objects are compared directly; `.zip` and uncompressed `.tar` objects
    are expanded by reading their member lists over ranged requests (the archives
    are never downloaded), and their members are placed under the archive's parent
    directory, mirroring `7z x archive -o$(dirname archive)`. Nested archive
    members are reported but not recursed into, and compressed tars (`.tar.gz`
    and friends) are reported as unexpandable rather than downloaded.

    Args:
        source (str): First prefix, either `s3://bucket/prefix` or a bare prefix
            (then `--bucket` is required).
        target (str): Second prefix, same conventions as `source`.
        bucket (str, optional): Bucket used for any bare-prefix argument.
        sign (bool, optional): Sign requests with the standard boto3 credential
            chain. Reads are unsigned by default, which is what public buckets
            need.
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
            (`https://web.s3.wisc.edu/`).
    """
    if workers < 1:
        raise ValueError("Argument `workers` must be at least 1.")

    source_location = Prefix(*resolve_prefix(source, bucket))
    target_location = Prefix(*resolve_prefix(target, bucket))

    s3_client = make_client(sign=sign, endpoint_url=endpoint_url)

    with Status("Building virtual trees...", spinner="bouncingBall"):
        source_tree = _collect_virtual_files(s3_client=s3_client, location=source_location, workers=workers)
        target_tree = _collect_virtual_files(s3_client=s3_client, location=target_location, workers=workers)

    print(f"Source: {source_location}  ({source_tree.objects} objects, {len(source_tree.files)} files)")
    print(f"Target: {target_location}  ({target_tree.objects} objects, {len(target_tree.files)} files)")
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
