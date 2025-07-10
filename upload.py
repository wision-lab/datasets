# NOTE: For internal use only, this script is used to upload datasets to our S3 bucket, 
#   and will not work without proper auth tokens (i.e in ~/.aws/credentials).
#
#   Further, the AWS_ENDPOINT_URL environment variable should point
#   to https://web.s3.wisc.edu/ for public access and to https://campus.s3.wisc.edu/
#   for private access.

from __future__ import annotations

import itertools
import logging
import os
import re
import sys
import tempfile
import zipfile
from functools import partial
from pathlib import Path
from typing_extensions import Annotated, List, TypeAlias

import boto3
import more_itertools as mitertools
import questionary
import tyro
from botocore.exceptions import ClientError
from natsort import natsort_key, natsorted
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.traceback import install
from treelib import Tree

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logging.getLogger("PIL").setLevel(logging.WARNING)
log = logging.getLogger("rich")
install(suppress=[tyro])

_SIZE_SYMBOLS = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
_SIZE_BOUNDS = [(1024**i, sym) for i, sym in enumerate(_SIZE_SYMBOLS)]
_SIZE_DICT = {sym: val for val, sym in _SIZE_BOUNDS}
_SIZE_RANGES = list(zip(_SIZE_BOUNDS, _SIZE_BOUNDS[1:]))


def _bytes_from_str(size: str | List[str]) -> int:
    # Based on https://stackoverflow.com/a/60708339
    if isinstance(size, list):
        assert len(size) == 1
        return _bytes_from_str(size[0])
    try:
        return int(size)
    except ValueError:
        size = size.upper()
        if not re.match(r" ", size):
            symbols = "".join(_SIZE_SYMBOLS)
            size = re.sub(rf"([{symbols}]?B)", r" \1", size)
        number, unit = [string.strip() for string in size.split()]
        return int(float(number) * _SIZE_DICT[unit[0]])


def _bytes_to_str(nbytes: int, ndigits: int = 1) -> str:
    # Based on https://boltons.readthedocs.io/en/latest/strutils.html#boltons.strutils.bytes2human
    abs_bytes = abs(nbytes)
    for (size, symbol), (next_size, _) in _SIZE_RANGES:
        if abs_bytes <= next_size:
            break
    hnbytes = float(nbytes) / size
    return f"{hnbytes:.{ndigits}f}{symbol}"


MemSize: TypeAlias = Annotated[
    int,
    tyro.constructors.PrimitiveConstructorSpec(
        nargs=1,
        metavar="BYTES",
        instance_from_str=_bytes_from_str,
        is_instance=lambda instance: isinstance(instance, int),
        str_from_instance=_bytes_to_str,
    ),
]


def check_exists(*, s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=str(bucket), Key=str(key))
    except ClientError as e:
        return int(e.response["Error"]["Code"]) != 404
    return True


def upload_file(src, dst, *, s3_client, bucket, public=True, dry=False):
    try:
        if dry:
            log.info(f"Would have uploaded {src} to {dst}.")
        else:
            log.info(f"Uploading {src} as {dst}...")
            extra_args = {"ACL": "public-read"} if public else {}
            s3_client.upload_file(str(src), str(bucket), str(dst), ExtraArgs=extra_args)
    except ClientError as e:
        log.error(f"Failed to upload {src} to {dst}.")
        log.error(e)


class PathData:
    def __init__(self, *, path=None, size=None):
        self.path = path
        self.size = size

    def __str__(self):
        return _bytes_to_str(self.size or 0)


class ZipData:
    def __init__(self, *, subtree=None, size=None, filename=None):
        self.subtree = subtree
        self.size = size
        self.filename = filename

    def __str__(self):
        return _bytes_to_str(self.size or 0)


def is_not_hidden(path):
    return not path.name.startswith(".")


def is_not_dunder(path):
    return not path.name.startswith("_")


def is_not_match(path, exclude_patterns=None):
    return not any(path.match(exclude_pattern) for exclude_pattern in (exclude_patterns or []))


def populate_filesize(*, node=None, tree=None):
    if node is None:
        node = tree.get_node(tree.root)

    for child in tree.children(node.identifier):
        populate_filesize(node=child, tree=tree)

    if node.data.size is None:
        node.data.size = sum(c.data.size for c in tree.children(node.identifier))


def directory_tree(path, on_error=None, follow_symlinks=False, filter_fn=None):
    tree = Tree()
    skipped_dirs = set()
    path = Path(path).resolve()
    node = tree.create_node(tag=f"ROOT: {path}", data=PathData(path=path))
    path2id = {str(node.data.path): node.identifier}

    for dirpath, dirnames, filenames in Path(path).walk(on_error=on_error, follow_symlinks=follow_symlinks):
        if filter_fn is not None and (not filter_fn(dirpath) or dirpath.parent in skipped_dirs):
            skipped_dirs.add(dirpath)
            continue
        for dirname in dirnames:
            if filter_fn is None or filter_fn(dirpath / dirname):
                node = tree.create_node(
                    tag=f"📁 {dirname}", parent=path2id[str(dirpath.resolve())], data=PathData(path=dirpath / dirname)
                )
                path2id[str(node.data.path.resolve())] = node.identifier
        for filename in filenames:
            if filter_fn is None or filter_fn(dirpath / filename):
                node = tree.create_node(
                    tag=f"📄 {filename}",
                    parent=path2id[str(dirpath.resolve())],
                    data=PathData(path=dirpath / filename, size=(dirpath / filename).stat().st_size),
                )
                path2id[str(node.data.path.resolve())] = node.identifier
    populate_filesize(tree=tree)
    return tree


def split_into_chunks(*, chunk_size=_bytes_from_str("200MB"), node=None, tree=None):
    if node is None:
        node = tree.get_node(tree.root)
        tree = Tree(tree, deep=True)

    for child in tree.children(node.identifier):
        tree = split_into_chunks(chunk_size=chunk_size, node=child, tree=tree)

    if Path(node.data.path).is_dir():
        # Convert a dir (with no child zips) into a zip if it's bigger than chunk_size
        parent = tree.parent(node.identifier)
        is_toplevel = parent == tree.get_node(tree.root)
        has_zipped_child = any(c.data.path for c in tree.children(node.identifier))

        if is_toplevel or (node.data.size > chunk_size and not has_zipped_child):
            if node.data.size < chunk_size * 1.5:
                # Store as single zip
                child_tree = tree.remove_subtree(node.identifier)
                child_tree_root = child_tree.get_node(child_tree.root)

                tree.create_node(
                    tag=f"(ZIP {_bytes_to_str(node.data.size)}) {node.tag}",
                    identifier=node.identifier,
                    parent=parent,
                    data=ZipData(
                        size=child_tree_root.data.size,
                        subtree=child_tree,
                        filename=child_tree_root.data.path.relative_to(parent.data.path).with_suffix(".zip"),
                    ),
                )
            else:
                # Partition it into a few zips
                children = natsorted(
                    tree.children(node.identifier), key=lambda n: (n.data.path.is_file(), str(n.data.path))
                )
                cumulative_sizes = list(itertools.accumulate(c.data.size for c in children))
                groups_lengths = [
                    len(g)
                    for g in mitertools.split_when(cumulative_sizes, lambda x, y: x // chunk_size != y // chunk_size)
                ]
                children = iter(children)

                for i, group_size in enumerate(groups_lengths):
                    subtree = Tree()
                    subtree.add_node(node)
                    group_children = mitertools.take(group_size, children)

                    size = sum(p.data.size for p in group_children)
                    contents = ", ".join(str(p.data.path.relative_to(node.data.path)) for p in group_children)
                    tag = f"(ZIP #{i+1}/{len(groups_lengths)} {_bytes_to_str(size)}) {contents}"

                    for c in group_children:
                        sb = tree.remove_subtree(c.identifier)
                        subtree.paste(subtree.root, sb)

                    data = ZipData(
                        size=size, subtree=subtree, filename=Path(f"{node.data.path.stem}_{i+1:03}").with_suffix(".zip")
                    )
                    tree.create_node(tag=tag, parent=node, data=data)
    return tree


def build_ziptree(
    path: Path, /, chunk_size: MemSize = _bytes_from_str("10GB"), exclude: List[str] = [], follow_symlinks: bool = False
):
    # Create filesystem tree and split it into zip-sized chunks
    def path_filter(p):
        keep = is_not_hidden(p) and is_not_dunder(p) and is_not_match(p, exclude_patterns=exclude)
        if not keep:
            log.info(f"Excluding {p} ({_bytes_to_str(p.stat().st_size)})")
        return keep

    file_tree = directory_tree(path, filter_fn=path_filter, follow_symlinks=follow_symlinks)
    zipd_tree = split_into_chunks(chunk_size=chunk_size, tree=file_tree)

    # Validate format, nodes must be zips, dirs of zips of toplevel.
    for identifier in zipd_tree.expand_tree():
        node = zipd_tree.get_node(identifier)
        parent = zipd_tree.parent(identifier)

        if not (
            identifier == zipd_tree.root
            or isinstance(node.data, ZipData)
            or node.data.path.is_dir()
            or (parent.identifier == zipd_tree.root and node.data.path.is_file())
        ):
            raise ValueError("Zip tree is malformed!")
    return zipd_tree


def partition_and_upload(
    path: Path,
    /,
    bucket: str,
    prefix: str,
    chunk_size: MemSize = _bytes_from_str("10GB"),
    exclude: List[str] = [],
    tmp_dir: str | os.PathLike | None = None,
    follow_symlinks: bool = False,
    overwrite: bool = False,
    dry: bool = True,
):
    """Auto-partition dataset into archives and upload them to an S3 bucket.

    Args:
        path (Path): Directory to upload.
        bucket (str): Name of S3 bucket.
        prefix (str): Prefix path of s3 objects.
        chunk_size (MemSize, optional): Target size of archives (pre-compression).
            At most each (deflated) zip will be one and a half time this size.
        exclude (List[str], optional): Space separated list of path exclusion
            patterns. Warning something like "logs/" will match any path that
            contains logs. Internally uses `Path.match`.
        tmp_dir (str | os.PathLike | None, optional): Location of scratch dir
            used to build archives. Useful if the `chunk_size` is more than a
            few GBs. Defaults to OS default tmp directory.
        follow_symlinks (bool, optional): If true, symlinks will be followed.
            Careful, this can lead to infinite recursions!
        overwrite (bool, optional): If true, objects in the S3 bucket will be
            overwritten by new ones that share the same key, otherwise the
            conflicting uploads are skipped.
        dry (bool, optional): If true, do not actually upload anything.
    """
    # Build zip tree, validate, and request user input
    tree = build_ziptree(path, chunk_size=chunk_size, exclude=exclude, follow_symlinks=follow_symlinks)
    tree.show(key=lambda x: natsort_key(x.tag))

    if not questionary.confirm("Confirm zip partition?", default=False).ask():
        sys.exit(1)

    # Iter over tree, zip components and upload
    s3_client = boto3.client("s3")

    if not dry:
        if not questionary.confirm(
            "Not running in dry mode, this will upload artifacts to S3. Confirm?", default=False
        ).ask():
            sys.exit(1)
    public = questionary.confirm("Make uploaded artifacts public?", default=False).ask()
    upload = partial(upload_file, bucket=bucket, s3_client=s3_client, public=public, dry=dry)

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(elapsed_when_finished=True),
    ) as progress:
        task = progress.add_task("", total=tree.size())

        for identifier in tree.expand_tree(key=lambda n: natsort_key(n.tag)):
            node = tree.get_node(identifier)
            parent = tree.parent(identifier)

            if isinstance(node.data, ZipData):
                subtree = node.data.subtree
                subtree_root = subtree.get_node(subtree.root)
                relative_root = subtree_root.data.path.parent
                object_key = Path(prefix) / node.data.filename
                exists = check_exists(s3_client=s3_client, bucket=bucket, key=object_key)

                if not overwrite and exists:
                    log.info(f"Skipping {object_key} as objects with the same key exists in bucket.")
                else:
                    progress.update(task, description=f"Compressing {node.data.filename}")

                    with tempfile.TemporaryDirectory(dir=tmp_dir) as tmpdir:
                        with zipfile.ZipFile(Path(tmpdir) / node.data.filename, mode="w") as archive:
                            if not dry:
                                for n in node.data.subtree.all_nodes():
                                    archive.write(n.data.path, arcname=n.data.path.relative_to(relative_root))

                        progress.update(task, description=f"Uploading {node.data.filename}")
                        upload(Path(tmpdir) / node.data.filename, object_key)
                progress.update(task, advance=1)

            elif node.data.path.is_file() and parent.identifier == tree.root:
                relative_root = tree.get_node(tree.root).data.path
                object_key = Path(prefix) / node.data.path.relative_to(relative_root)
                exists = check_exists(s3_client=s3_client, bucket=bucket, key=object_key)

                if not overwrite and exists:
                    log.info(f"Skipping {object_key} as objects with the same key exists in bucket.")
                else:
                    progress.update(task, description=f"Uploading {node.data.path.relative_to(relative_root)}")
                    upload(node.data.path, object_key)
                progress.update(task, advance=1)

            else:
                progress.update(task, advance=1)


if __name__ == "__main__":
    if boto3.__version__ != "1.35.31":
        log.warning("Please use boto3==1.35.31 as other versions might fail to upload files!!")

    tyro.cli(partition_and_upload)
