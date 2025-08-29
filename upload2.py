# NOTE: For internal use only, this script is used to upload datasets to our S3 bucket,
#   and will not work without proper auth tokens (i.e in ~/.aws/credentials).
#
#   Further, the AWS_ENDPOINT_URL environment variable should point
#   to https://web.s3.wisc.edu/ for public access and to https://campus.s3.wisc.edu/
#   for private access.

from __future__ import annotations

import copy
import functools
import io
import itertools
import logging
import os
import re
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
import textwrap

import boto3
import more_itertools as mitertools
import tyro
from botocore.exceptions import ClientError
from natsort import natsorted
from nutree import Tree
from nutree.node import Node
from rich.logging import RichHandler
from rich.status import Status
from rich.traceback import install
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


@dataclass
class PathData:
    path: Path
    depth: int
    size: int | None = None
    grouped_children: list[Tree] = field(default_factory=list)

    def __str__(self):
        # subtree = "\n".join(', '.join(str(n.data) for n in t.children) for t in self.grouped_children)
        subtree = "\n".join(t.format(repr='{node.data}') for t in self.grouped_children)
        subtree = textwrap.indent(subtree, self.depth * "    ")

        return (
            f"{'📁' if self.path.is_dir() else '📄'} "
            f"{self.path.name} ({_bytes_to_str(self.size or 0)}) "
            f"{"\n" + subtree if self.grouped_children else ''}"
            # f"{len(self.grouped_children)}"
        ).strip()

    def __hash__(self):
        return hash((self.path, *self.grouped_children))

    @staticmethod
    def serialize_mapper(node, data):
        def serialize_tree(tree):
            with io.StringIO() as f:
                tree.save(f, mapper=PathData.serialize_mapper)
                f.seek(0)
                return f.read()
            
        data["data"] = (
            str(node.data.path),
            node.data.depth,
            node.data.size,
            [serialize_tree(t) for t in node.data.grouped_children],
        )
        return data

    @staticmethod
    def deserialize_mapper(parent, data):
        def deserialize_tree(data):
            with io.StringIO(data) as f:
                f.seek(0)
                return Tree.load(f, mapper=PathData.deserialize_mapper)
    
        path, depth, size, children = data["data"]
        return PathData(
            path=Path(path), depth=depth, size=size, grouped_children=[deserialize_tree(d) for d in children]
        )


def is_not_hidden(path: Path) -> bool:
    return not path.name.startswith(".")


def is_not_dunder(path: Path) -> bool:
    return not path.name.startswith("_")


def is_match(path: Path, patterns: list[str] | None = None) -> bool:
    return any(path.match(exclude_pattern) for exclude_pattern in (patterns or []))


def deepcopy(tree: Tree) -> Tree:
    # Create deepcopy through serialization to/from memory
    with io.StringIO() as f:
        tree.save(f, mapper=PathData.serialize_mapper)
        f.seek(0)

        return Tree.load(f, mapper=PathData.deserialize_mapper)


def populate_filesize(*, node: Node | Tree, refresh: bool = False) -> Node | Tree:
    if isinstance(node, Tree):
        root = node.first_child()

        if root is not None:
            populate_filesize(node=root, refresh=refresh)
        return node

    for child in node.children:
        populate_filesize(node=child, refresh=refresh)

    if (node.data.path.is_dir() and refresh) or node.data.size is None:
        node.data.size = sum(c.data.size for c in node.children)
    return node


def directory_tree(
    path: str | os.PathLike,
    on_error: Callable | None = None,
    follow_symlinks: bool = False,
    filter_fn: Callable | None = None,
) -> Tree:
    tree: Tree = Tree("Directory Listing")
    path = Path(path).resolve()
    root = tree.add(PathData(path=path, depth=1))
    path2node = {str(root.data.path.resolve()): root}
    skipped_dirs = set()

    for dirpath, dirnames, filenames in Path(path).walk(
        on_error=on_error, follow_symlinks=follow_symlinks
    ):
        parent = path2node[str(dirpath.resolve())]

        if filter_fn is not None and (
            not filter_fn(dirpath) or dirpath.parent in skipped_dirs
        ):
            skipped_dirs.add(dirpath)
            continue

        for dirname in dirnames:
            if filter_fn is None or filter_fn(dirpath / dirname):
                child_data = PathData(path=dirpath / dirname, depth=parent.data.depth+1)
                child = parent.add(child_data)
                path2node[str(child.data.path.resolve())] = child
        for filename in filenames:
            if filter_fn is None or filter_fn(dirpath / filename):
                child_data = PathData(
                    path=dirpath / filename,
                    depth=parent.data.depth+1,
                    size=(dirpath / filename).stat().st_size,
                )
                child = parent.add(child_data)
                path2node[str(child.data.path.resolve())] = child
    populate_filesize(node=root)
    return tree


def partition_tree_by_fnmatches(
    *, tree: Tree, patterns: list[str]
) -> dict[str | None, Tree]:
    matched_leafs = [
        {
            n.data.path: n
            for n in tree.find_all(
                match=lambda n: n.is_leaf() and fnmatch(n.data.path, pattern)
            )
        }
        for pattern in patterns
    ]
    matched_leafs += [
        {
            n.data.path: n
            for n in tree.find_all(
                match=lambda n: n.is_leaf()
                and not any(fnmatch(n.data.path, pattern) for pattern in patterns)
            )
        }
    ]
    assert all(
        set(u).isdisjoint(v) for u, v in itertools.combinations(matched_leafs, 2)
    )

    all_leafs = set(n.data.path for n in tree.find_all(match=lambda n: n.is_leaf()))
    all_matched_leafs = functools.reduce(set.union, (set(l) for l in matched_leafs))
    assert all_matched_leafs == all_leafs

    subtrees = [
        tree.filtered(
            lambda n: n.data.path in leafs
            or any(n.is_ancestor_of(l) for l in leafs.values())
        )
        for leafs in matched_leafs
    ]
    subtrees = [deepcopy(subtree) for subtree in subtrees]

    subtrees_dict = {
        pat: cast(Tree, populate_filesize(node=st, refresh=True))
        for pat, st in zip(patterns + [None], subtrees)
        if st.count
    }
    return subtrees_dict


def split_into_chunks(
    *,
    node: Node | Tree,
    chunk_size=_bytes_from_str("200MB"),
) -> Tree | None:
    if isinstance(node, Tree):
        root = node.first_child()

        if root is not None:
            split_into_chunks(node=root, chunk_size=chunk_size)
            return node
        return

    for child in node.children:
        split_into_chunks(node=child, chunk_size=chunk_size)

    has_grouped_children= any(
        c.data.grouped_children for c in node.children
    )
    
    if node.data.path.is_dir() and (node.data.size > chunk_size or node.is_top()):
        # Sort children with files first
        children = natsorted(
            filter(lambda n: not any(c.data.grouped_children for c in n.iterator(add_self=True)), node.children),
            # node.children,
            key=lambda n: (n.data.path.is_file(), str(n.data.path)),
        )

        cumulative_sizes = list(itertools.accumulate(c.data.size for c in children))
        groups_lengths = [
            len(g)
            for g in mitertools.split_when(
                cumulative_sizes,
                lambda x, y: x // chunk_size != y // chunk_size,
            )
        ]
        children_iter = iter(children)

        for i, group_size in enumerate(groups_lengths):        
            tree = Tree("Zip Folder")
            group_children = mitertools.take(group_size, children_iter)

            for child in group_children:
                child.copy_to(tree)
                child.remove()
            node.data.grouped_children.append(tree)


if __name__ == "__main__":
    if boto3.__version__ != "1.35.31":
        log.warning(
            "Please use boto3==1.35.31 as other versions might fail to upload files!!"
        )

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

    with Status("Partitioning Tree...", spinner="bouncingBall") as status:
        subtrees = partition_tree_by_fnmatches(
            tree=file_tree,
            patterns=[
                "**/frames/**",
                "**/depths/**",
                "**/normals/**",
                "**/flows/**",
                "**/segmentations/**",
                "**/previews/**",
            ],
        )

    # for st in subtrees.values():
    #     st.print(repr="{node.data}")

    tree = subtrees[None]
    # tree = subtrees["**/frames/**"]
    tree = split_into_chunks(node=tree, chunk_size=_bytes_from_str("10MB"))
    tree.print(repr="{node.data}")
