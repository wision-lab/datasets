# NOTE: For internal use only, this script is used to upload datasets to our S3 bucket,
#   and will not work without proper auth tokens (i.e in ~/.aws/credentials).
#
#   Further, the AWS_ENDPOINT_URL environment variable should point
#   to https://web.s3.wisc.edu/ for public access and to https://campus.s3.wisc.edu/
#   for private access.

from __future__ import annotations

import functools
import itertools
import json
import logging
import os
import re
import sys
import tempfile
import zipfile
from fnmatch import fnmatch
from functools import partial
from pathlib import Path

import boto3
import more_itertools as mitertools
import questionary
import treelib
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
from rich.status import Status
from rich.traceback import install
from treelib import Node, Tree
from typing_extensions import TYPE_CHECKING, Annotated, Callable, TypeAlias, cast

if TYPE_CHECKING:
    from types_boto3_s3 import Client as S3Client

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO").upper(),
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


def _bytes_from_str(size: str | list[str]) -> int:
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


def check_exists(*, s3_client: S3Client, bucket: str, key: str | os.PathLike) -> bool:
    try:
        s3_client.head_object(Bucket=str(bucket), Key=str(key))
    except ClientError as e:
        return int(e.response["Error"]["Code"]) != 404
    return True


def upload_file(
    src: str | os.PathLike,
    dst: str | os.PathLike,
    *,
    s3_client: S3Client,
    bucket: str,
    public: bool = True,
    dry: bool = False,
) -> None:
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


def is_not_hidden(path: Path) -> bool:
    return not path.name.startswith(".")


def is_not_dunder(path: Path) -> bool:
    return not path.name.startswith("_")


def is_match(path: Path, patterns: list[str] | None = None) -> bool:
    return any(path.match(exclude_pattern) for exclude_pattern in (patterns or []))


def populate_filesize(
    *, tree: Tree, node: Node | None = None, refresh: bool = False
) -> None:
    if node is None:
        node = tree.get_node(tree.root)
        assert isinstance(node, Node)

    for child in tree.children(node.identifier):
        populate_filesize(tree=tree, node=child, refresh=refresh)

    if (node.data.path.is_dir() and refresh) or node.data.size is None:
        node.data.size = sum(c.data.size for c in tree.children(node.identifier))


def directory_tree(
    path: str | os.PathLike,
    on_error: Callable | None = None,
    follow_symlinks: bool = False,
    filter_fn: Callable | None = None,
) -> Tree:
    tree = Tree()
    skipped_dirs = set()
    path = Path(path).resolve()
    node = tree.create_node(tag=f"ROOT: {path}", data=PathData(path=path))
    path2id = {str(node.data.path): node.identifier}

    for dirpath, dirnames, filenames in Path(path).walk(
        on_error=on_error, follow_symlinks=follow_symlinks
    ):
        if filter_fn is not None and (
            not filter_fn(dirpath) or dirpath.parent in skipped_dirs
        ):
            skipped_dirs.add(dirpath)
            continue
        for dirname in dirnames:
            if filter_fn is None or filter_fn(dirpath / dirname):
                node = tree.create_node(
                    tag=f"📁 {dirname}",
                    parent=path2id[str(dirpath.resolve())],
                    data=PathData(path=dirpath / dirname),
                )
                path2id[str(node.data.path.resolve())] = node.identifier
        for filename in filenames:
            if filter_fn is None or filter_fn(dirpath / filename):
                node = tree.create_node(
                    tag=f"📄 {filename}",
                    parent=path2id[str(dirpath.resolve())],
                    data=PathData(
                        path=dirpath / filename,
                        size=(dirpath / filename).stat().st_size,
                    ),
                )
                path2id[str(node.data.path.resolve())] = node.identifier
    populate_filesize(tree=tree)
    return tree


def split_into_chunks(
    *,
    tree: Tree,
    chunk_size=_bytes_from_str("200MB"),
    node: Node | None = None,
    root_name: str | None = None,
) -> Tree:
    if node is None:
        node = tree.get_node(tree.root)
        tree = Tree(tree, deep=True)
        assert isinstance(node, Node)

    for child in tree.children(node.identifier):
        tree = split_into_chunks(
            tree=tree, chunk_size=chunk_size, node=child, root_name=root_name
        )

    if Path(node.data.path).is_dir():
        # Convert a dir (with no child zips) into a zip if it's bigger than chunk_size
        parent = tree.parent(node.identifier)
        is_root = parent is None
        has_zipped_child = any(
            isinstance(c.data, ZipData) for c in tree.children(node.identifier)
        )

        if (is_root or node.data.size > chunk_size) and not has_zipped_child:
            if node.data.size < chunk_size * 1.5:
                # Store as single zip
                child_tree = tree.remove_subtree(node.identifier)
                child_tree_root = child_tree.get_node(child_tree.root)
                filename = Path(
                    child_tree_root.data.path.stem if not is_root else root_name
                ).with_suffix(".zip")

                tree.create_node(
                    tag=f"(ZIP {_bytes_to_str(node.data.size)}) {node.tag if not is_root else root_name}",
                    identifier=node.identifier,
                    parent=parent,
                    data=ZipData(
                        size=child_tree_root.data.size,
                        subtree=child_tree,
                        filename=filename,
                    ),
                )
            else:
                # Partition it into a few zips
                children = natsorted(
                    tree.children(node.identifier),
                    key=lambda n: (n.data.path.is_file(), str(n.data.path)),
                )
                cumulative_sizes = list(
                    itertools.accumulate(c.data.size for c in children)
                )
                groups_lengths = [
                    len(g)
                    for g in mitertools.split_when(
                        cumulative_sizes,
                        lambda x, y: x // chunk_size != y // chunk_size,
                    )
                ]
                children_iter = iter(children)

                for i, group_size in enumerate(groups_lengths):
                    subtree = Tree()
                    subtree.add_node(node)
                    group_children = mitertools.take(group_size, children_iter)

                    size = sum(p.data.size for p in group_children)
                    contents = ", ".join(
                        str(p.data.path.relative_to(node.data.path))
                        for p in group_children
                    )
                    tag = f"(ZIP #{i+1}/{len(groups_lengths)} {_bytes_to_str(size)}) {contents}"

                    for c in group_children:
                        sb = tree.remove_subtree(c.identifier)
                        subtree.paste(cast(str, subtree.root), sb)

                    filename = Path(
                        f"{node.data.path.stem if not is_root else root_name}_{i+1:03}"
                    ).with_suffix(".zip")
                    data = ZipData(
                        size=size,
                        subtree=subtree,
                        filename=filename,
                    )
                    tree.create_node(tag=tag, parent=node, data=data)
    return tree


def partition_tree_by_fnmatches(*, tree: Tree, patterns: list[str]) -> list[Tree]:
    # Match on leaf node paths, ensure no overlap between sets
    matched_ids = [
        set(n.identifier for n in tree.leaves() if fnmatch(n.data.path, pattern))
        for pattern in patterns
    ]
    assert all(u.isdisjoint(v) for u, v in itertools.combinations(matched_ids, 2))

    # Get all unmatched ids
    all_ids = set(tree.nodes.keys())
    all_leaf_ids = set(n.identifier for n in tree.leaves())
    all_matched_ids = functools.reduce(set.union, matched_ids)
    unmatched_ids = all_leaf_ids - all_matched_ids

    # TODO: Super inefficient...
    subtrees = [Tree(tree, deep=False) for _ in range(len(patterns) + 1)]

    for subtree, matched in zip(subtrees, matched_ids + [unmatched_ids]):
        valid_ids = set(
            i for p in tree.paths_to_leaves() if p[-1] in matched for i in p
        )
        invalid_ids = all_ids - valid_ids

        for i in invalid_ids:
            try:
                subtree.remove_node(i)
            except treelib.exceptions.NodeIDAbsentError:
                pass

    subtree_node_ids = functools.reduce(
        set.union, (set(s.nodes.keys()) for s in subtrees)
    )
    assert all_ids == subtree_node_ids

    subtrees = {
        pattern: Tree(st, deep=True)
        for pattern, st in zip(patterns + [None], subtrees)
        if st.size()
    }

    for subtree in subtrees.values():
        populate_filesize(tree=subtree, refresh=True)

    return subtrees


def build_ziptree(
    path: Path,
    /,
    chunk_size: MemSize = _bytes_from_str("10GB"),
    exclude: list[str] = [],
    follow_symlinks: bool = False,
) -> list[Tree]:
    # Create filesystem tree and split it into zip-sized chunks
    def path_filter(p):
        keep = (
            is_not_hidden(p) and is_not_dunder(p) and not is_match(p, patterns=exclude)
        )
        if not keep:
            log.debug(f"Excluding {p} ({_bytes_to_str(p.stat().st_size)})")
        return keep

    with Status("Building Tree...", spinner="bouncingBall") as status:
        file_tree = directory_tree(
            path, filter_fn=path_filter, follow_symlinks=follow_symlinks
        )

    with Status("Partitioning Tree...", spinner="bouncingBall") as status:
        # subtrees = partition_tree_by_fnmatches(
        #     tree=file_tree,
        #     patterns=[
        #         "**/frames/**",
        #         "**/depths/**",
        #         "**/normals/**",
        #         "**/flows/**",
        #         "**/segmentations/**",
        #         "**/previews/**",
        #     ],
        # )
        subtrees = [file_tree]

    with Status("Splitting into chunks...", spinner="bouncingBall") as status:
        zip_trees = [
            split_into_chunks(tree=st, chunk_size=chunk_size) for st in subtrees
        ]

    for zip_tree in zip_trees:
        validate_ziptree(zip_tree)
    return zip_trees


def validate_ziptree(zip_tree: Tree) -> None:
    # Validate format, nodes must be zips, dirs of zips of toplevel.
    for identifier in zip_tree.expand_tree():
        parent = zip_tree.parent(identifier)
        node = zip_tree.get_node(identifier)
        assert isinstance(node, Node)

        if not (
            identifier == zip_tree.root
            or isinstance(node.data, ZipData)
            or node.data.path.is_dir()
            or (
                parent is not None
                and parent.identifier == zip_tree.root
                and node.data.path.is_file()
            )
        ):
            raise ValueError("Zip tree is malformed!")


def partition_and_upload(
    path: Path,
    /,
    bucket: str,
    prefix: str,
    chunk_size: MemSize = _bytes_from_str("10GB"),
    exclude: list[str] = [],
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
        exclude (list[str], optional): Space separated list of path exclusion
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
    trees = build_ziptree(
        path,
        chunk_size=chunk_size,
        exclude=exclude,
        follow_symlinks=follow_symlinks,
    )

    for tree in trees:
        tree.show(key=lambda x: natsort_key(x.tag))

    if not questionary.confirm("Confirm zip partition?", default=False).ask():
        sys.exit(1)

    # Iter over tree, zip components and upload
    s3_client = boto3.client("s3")

    if not dry:
        if not questionary.confirm(
            "Not running in dry mode, this will upload artifacts to S3. Confirm?",
            default=False,
        ).ask():
            sys.exit(1)
    public = questionary.confirm("Make uploaded artifacts public?", default=False).ask()
    upload = partial(
        upload_file, bucket=bucket, s3_client=s3_client, public=public, dry=dry
    )

    for i, tree in enumerate(trees):
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
                    exists = check_exists(
                        s3_client=s3_client, bucket=bucket, key=object_key
                    )

                    if not overwrite and exists:
                        log.info(
                            f"Skipping {object_key} as objects with the same key exists in bucket."
                        )
                    else:
                        progress.update(
                            task, description=f"Compressing {node.data.filename}"
                        )

                        with tempfile.TemporaryDirectory(dir=tmp_dir) as tmpdir:
                            with zipfile.ZipFile(
                                Path(tmpdir) / node.data.filename, mode="w"
                            ) as archive:
                                if not dry:
                                    for n in node.data.subtree.all_nodes():
                                        archive.write(
                                            n.data.path,
                                            arcname=n.data.path.relative_to(
                                                relative_root
                                            ),
                                        )

                            progress.update(
                                task, description=f"Uploading {node.data.filename}"
                            )
                            upload(Path(tmpdir) / node.data.filename, object_key)
                    progress.update(task, advance=1)

                elif node.data.path.is_file() and parent.identifier == tree.root:
                    relative_root = tree.get_node(tree.root).data.path
                    object_key = Path(prefix) / node.data.path.relative_to(
                        relative_root
                    )
                    exists = check_exists(
                        s3_client=s3_client, bucket=bucket, key=object_key
                    )

                    if not overwrite and exists:
                        log.info(
                            f"Skipping {object_key} as objects with the same key exists in bucket."
                        )
                    else:
                        progress.update(
                            task,
                            description=f"Uploading {node.data.path.relative_to(relative_root)}",
                        )
                        upload(node.data.path, object_key)
                    progress.update(task, advance=1)

                else:
                    progress.update(task, advance=1)


if __name__ == "__main__":
    if boto3.__version__ != "1.35.31":
        log.warning(
            "Please use boto3==1.35.31 as other versions might fail to upload files!!"
        )

    # tyro.cli(partition_and_upload)

    def path_filter(p):
        keep = is_not_hidden(p) and is_not_dunder(p) and not is_match(p, patterns=[])
        if not keep:
            log.debug(f"Excluding {p} ({_bytes_to_str(p.stat().st_size)})")
        return keep

    with Status("Building Tree...", spinner="bouncingBall") as status:
        file_tree = directory_tree(
            Path("/home/sjung/Downloads/010-00/"),
            filter_fn=path_filter,
            follow_symlinks=False,
        )

    # with Status("Partitioning Tree...", spinner="bouncingBall") as status:
    #     subtrees = partition_tree_by_fnmatches(
    #         tree=file_tree,
    #         patterns=[
    #             "**/frames/**",
    #             "**/depths/**",
    #             "**/normals/**",
    #             "**/flows/**",
    #             "**/segmentations/**",
    #             "**/previews/**",
    #         ],
    #     )

    # with Status("Splitting into chunks...", spinner="bouncingBall") as status:
    #     zip_trees = {
    #         k: split_into_chunks(tree=st, chunk_size=_bytes_from_str("5MB"), root_name=(k or "metadata").strip("*/"))
    #         for k, st in subtrees.items()
    #     }

    # for zt in zip_trees.values():
    #     zt.show()

    # print()
