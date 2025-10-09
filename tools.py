# NOTE: For internal use only, this script is used to upload datasets to our S3 bucket,
#   and will not work without proper auth tokens (i.e in ~/.aws/credentials).
#
#   Further, the AWS_ENDPOINT_URL environment variable should point
#   to https://web.s3.wisc.edu/ for public access and to https://campus.s3.wisc.edu/
#   for private access.

from __future__ import annotations

import contextlib
import io
import itertools
import json
import logging
import os
import re
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from fnmatch import fnmatch
from functools import partial, reduce
from pathlib import Path

import boto3
import more_itertools as mitertools
import questionary
import tyro
from botocore.exceptions import ClientError
from natsort import natsorted
from nutree import SkipBranch, StopTraversal, Tree
from nutree.node import Node
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
from typing_extensions import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Protocol,
    TypeAlias,
    cast,
)
from tyro.extras import SubcommandApp

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
app = SubcommandApp()

_SIZE_SYMBOLS = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
_SIZE_BOUNDS = [(1024**i, sym) for i, sym in enumerate(_SIZE_SYMBOLS)]
_SIZE_DICT = {sym: val for val, sym in _SIZE_BOUNDS}
_SIZE_RANGES = list(zip(_SIZE_BOUNDS, _SIZE_BOUNDS[1:]))


class UpdateFn(Protocol):
    """Mirrors `rich.progress.Progress.update` with curried task-id argument"""

    def __call__(
        self,
        total: float | None = None,
        completed: float | None = None,
        advance: float | None = None,
        description: str | None = None,
        visible: bool | None = None,
        refresh: bool = False,
    ) -> None: ...


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


@dataclass
class S3Connection:
    bucket: str | None = None
    """Name of S3 bucket."""
    prefix: str | None = None
    """Prefix path of s3 objects."""


def check_exists(
    *, s3_client: S3Client, conn: S3Connection, key: str | os.PathLike
) -> bool:
    if conn.bucket is None:
        raise ValueError("Bucket name not specified!")
    try:
        s3_client.head_object(
            Bucket=conn.bucket, Key=str(Path(conn.prefix or "") / key)
        )
    except ClientError as e:
        return int(e.response["Error"]["Code"]) != 404
    return True


def upload_file(
    src: str | os.PathLike,
    dst: str | os.PathLike,
    *,
    s3_client: S3Client,
    conn: S3Connection,
    public: bool = True,
) -> None:
    if conn.bucket is None:
        raise ValueError("Bucket name not specified!")
    try:
        log.info(f"Uploading {src} as {dst}...")
        extra_args = {"ACL": "public-read"} if public else {}
        s3_client.upload_file(
            str(src),
            conn.bucket,
            str(Path(conn.prefix or "") / dst),
            ExtraArgs=extra_args,
        )
    except ClientError as e:
        log.error(f"Failed to upload {src} to {dst}.")
        log.error(e)


@dataclass
class PathData:
    path: Path
    is_dir: bool | None = None
    is_zip: bool = False
    has_zip_descendants: bool = False
    size: int | None = None

    def __post_init__(self):
        if self.is_dir is None:
            self.is_dir = self.path.is_dir()

    def __str__(self):
        icon = "💾" if self.is_zip else "📁" if self.is_dir else "📄"
        return f"{icon} {self.path.name} ({_bytes_to_str(self.size or 0)})"

    def __hash__(self):
        return hash((self.path, self.is_zip))

    @staticmethod
    def serialize_mapper(node, data):
        data["data"] = (
            str(node.data.path),
            node.data.is_dir,
            node.data.is_zip,
            node.data.has_zip_descendants,
            node.data.size,
        )
        return data

    @staticmethod
    def deserialize_mapper(parent, data):
        path, is_dir, is_zip, has_zip_descendants, size = data["data"]
        return PathData(
            path=Path(path),
            is_dir=is_dir,
            is_zip=is_zip,
            has_zip_descendants=has_zip_descendants,
            size=size,
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

    if (node.data.is_dir and refresh) or node.data.size is None:
        node.data.size = sum(c.data.size for c in node.children)
    return node


def directory_tree(
    path: str | os.PathLike,
    on_error: Callable | None = None,
    follow_symlinks: bool = False,
    filter_fn: Callable | None = None,
) -> Tree:
    path = Path(path).resolve()
    tree: Tree = Tree("Directory Listing")
    root = tree.add(PathData(path=path))
    path2node = {str(root.data.path.resolve()): root}
    skipped_dirs = set()

    if not path.exists():
        raise FileNotFoundError(f"Directory {path} does not exist!")

    for dirpath, dirnames, filenames in Path(path).walk(
        on_error=on_error, follow_symlinks=follow_symlinks
    ):
        if filter_fn is not None and (
            not filter_fn(dirpath) or dirpath.parent in skipped_dirs
        ):
            skipped_dirs.add(dirpath)
            continue
        
        parent = path2node[str(dirpath.resolve())]

        for dirname in dirnames:
            if filter_fn is None or filter_fn(dirpath / dirname):
                child_data = PathData(path=dirpath / dirname)
                child = parent.add(child_data)
                path2node[str(child.data.path.resolve())] = child
        for filename in filenames:
            if filter_fn is None or filter_fn(dirpath / filename):
                child_data = PathData(
                    path=dirpath / filename,
                    size=(dirpath / filename).stat().st_size,
                )
                child = parent.add(child_data)
                path2node[str(child.data.path.resolve())] = child
    populate_filesize(node=root)
    return tree


def partition_tree_by_fnmatches(
    *, tree: Tree, patterns: dict[str, str | None], default_groupname: str = "metadata"
) -> dict[str, Tree]:
    """Split tree into disjoint partitions based on pattern matches.

    Warning:
        Expects partitions to have disjoint set of leaf nodes (intermediate nodes
        can be shared), all nodes that do not match any pattern will be mapped to
        a separate subtree.

    Args:
        tree (Tree): The tree to partition.
        patterns (dict[str, str | None]): Mapping of pattern names to their expressions.
            A pattern expression of `None` will be matched to all leaf nodes that are
            otherwise not matched. If this wildcard is not present, it will be added and
            given a default name. Matches are computed with fnmatch on the leaf node paths.
        default_groupname (str, optional): Default name of non-matched pattern.

    Returns:
        dict[str, Tree]: Maps a pattern name to it's matching subtree
    """
    # Ensure there's a default group
    reverse_patterns = {v: k for k, v in patterns.items()}

    if len(patterns) != len(reverse_patterns):
        raise RuntimeError(f"Multiple partitions have the same pattern!")

    if None not in reverse_patterns:
        patterns[default_groupname] = None
    else:
        default_groupname = reverse_patterns[None]

    # Split all leaf node based on matches
    all_leafs = {n.data.path: n for n in tree.find_all(match=lambda n: n.is_leaf())}
    matched_leafs = {
        pattern_name: {
            n.data.path: n
            for n in filter(
                lambda n: fnmatch(n.data.path, pattern_expr), all_leafs.values()
            )
        }
        for pattern_name, pattern_expr in patterns.items()
        if pattern_name != default_groupname
    }
    matched_leafs[default_groupname] = {
        n.data.path: n
        for n in filter(
            lambda n: not any(
                fnmatch(n.data.path, pattern_expr)
                for pattern_expr in patterns.values()
                if pattern_expr
            ),
            all_leafs.values(),
        )
    }

    # Ensure no leaf node overlap and that we didn't miss any nodes
    assert all(
        set(u).isdisjoint(v)
        for u, v in itertools.combinations(matched_leafs.values(), 2)
    )
    all_matched_leafs = reduce(set.union, (set(l) for l in matched_leafs.values()))
    assert all_matched_leafs == set(all_leafs)

    # Filter tree into all subtrees, recalculate file sizes, deepcopy all and filter out empty trees
    subtrees = {
        pattern_name: tree.filtered(lambda n: n.data.path in leafs)
        for pattern_name, leafs in matched_leafs.items()
    }
    subtrees = {
        pattern_name: cast(Tree, populate_filesize(node=deepcopy(st), refresh=True))
        for pattern_name, st in subtrees.items()
        if st.count
    }
    for pattern_name, st in subtrees.items():
        st.name = pattern_name.title()
    return subtrees


def split_into_chunks(
    *, tree: Tree, chunk_size: int = _bytes_from_str("200MB"), min_zip_depth: int = 1
) -> Tree:
    def find_splits(
        *, node: Node, splits: dict[tuple[str | int, int], list[Node]]
    ) -> None:
        # First recurse and propagate the has_zip_descendants label up
        for child in node.children:
            find_splits(node=child, splits=splits)
            node.data.has_zip_descendants = (
                node.data.has_zip_descendants or child.data.has_zip_descendants
            )

        # Split node if too big, making sure to exclude children with zip descendants
        # if node.data.size > chunk_size or node.is_top():
        if node.data.size > chunk_size or node.depth() <= min_zip_depth:
            children = natsorted(
                filter(lambda n: not n.data.has_zip_descendants, node.children),
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
                group_children = mitertools.take(group_size, children_iter)
                splits[(node.data_id, i)] = group_children
                node.data.has_zip_descendants = True

    def validate_ziptree(node: Node, _memo: Any) -> SkipBranch | StopTraversal | None:
        if node.data.is_zip:
            if node.is_leaf():
                raise StopTraversal(RuntimeError(f"Found empty zipped node: {node}."))
            raise SkipBranch
        elif node.is_leaf():
            raise StopTraversal(RuntimeError(f"Found non-zipped leaf node: {node}."))
        return None

    if (root := tree.first_child()) is None:
        # Empty tree
        return tree

    splits: dict[tuple[str | int, int], list[Node]] = {}
    raw_files = set(
        str(n.data.path) for n in tree.find_all(match=lambda n: n.is_leaf())
    )

    # Find places for zips without modifying tree topology!
    find_splits(node=root, splits=splits)

    # Create new zipnodes for every zip
    for (data_id, i), group_children in splits.items():
        node = tree.find(data_id=data_id)
        assert isinstance(node, Node)

        zipnode_data = PathData(
            path=node.data.path.with_name(f"{node.data.path.stem}_{i}.zip"),
            size=sum(c.data.size for c in group_children),
            is_zip=True,
        )
        zipnode = node.up().add(zipnode_data)

        # Move over contents of zip file
        for c in group_children:
            c.move_to(zipnode)

        # Remove node once we've removed all children
        if not node.children:
            node.remove()

    # Ensure no files are missed
    zip_files = set(
        str(n.data.path) for n in tree.find_all(match=lambda n: n.is_leaf())
    )

    if diff := raw_files - zip_files:
        raise RuntimeError(f"Detected missing files in ziptree: {diff}")

    # Validate that there are no non zipped leaves
    if error := tree.visit(validate_ziptree):
        raise error
    return tree


@app.command
def show_tree(path: Path, /, full: bool = False) -> None:
    """Print out (zip) tree given it's path

    Args:
        path (Path): Path to saved tree (expects a json file).
        full (bool, optional): If true, also print contents of zip files.
    """
    tree: Tree = Tree.load(path, mapper=PathData.deserialize_mapper)
    tree.name = path.name 
    
    if not full:
        slim_tree = tree.filtered(
            lambda n: SkipBranch(and_self=False) if n.data.is_zip else True
        )
        slim_tree.name = tree.name
        slim_tree.print(repr="{node.data}")
    else:
        tree.print(repr="{node.data}")


@app.command
def upload(
    path: Path,
    /,
    s3: S3Connection = S3Connection(),
    chunk_size: MemSize = _bytes_from_str("10GB"),
    exclude: list[str] = [],
    tmp_dir: Path | None = None,
    keep: bool = False,
    partitions: Path | None = None,
    min_zip_depth: int = 1,
    follow_symlinks: bool = False,
    overwrite: bool = False,
) -> None:
    """Auto-partition dataset into archives and upload them to an S3 bucket.

    Args:
        path (Path): Directory to upload.
        chunk_size (MemSize, optional): Target size of archives (pre-compression).
            At most each (deflated) zip will be one and a half time this size.
        exclude (list[str], optional): Space separated list of path exclusion
            patterns. Warning something like "logs/" will match any path that
            contains logs. Internally uses `Path.match`.
        tmp_dir (Path, optional): Location of scratch dir
            used to build archives. Useful if the `chunk_size` is more than a
            few GBs. Defaults to OS default tmp directory.
        keep (bool, optional): If true, the temporary directory is kept. Useful for
            debugging or for making a local archive instead of using S3 if
            S3 connection parameters are not set.
        partitions (Path, optional): Path of json file containing partition names,
            their matching patterns and optionally a per-partition `min_zip_depth`.
        min_zip_depth (int, optional): Allow for zipping a node if it is
            shallower than this depth, were the root is at a depth of 1,
            even if it's less than `chunk_size`. Default 1 (only root node)
        follow_symlinks (bool, optional): If true, symlinks will be followed.
            Careful, this can lead to infinite recursions!
        overwrite (bool, optional): If true, objects in the S3 bucket will be
            overwritten by new ones that share the same key, otherwise the
            conflicting uploads are skipped.
    """
    if min_zip_depth <= 0:
        raise ValueError(f"Argument `min_zip_depth` must be at least 1.")

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
        if partitions:
            with open(partitions, "r") as f:
                partitions_dict = json.load(f)
        else:
            partitions_dict = {"": {"pattern": None}}

        patterns = {k: v["pattern"] for k, v in partitions_dict.items()}
        subtrees = partition_tree_by_fnmatches(tree=file_tree, patterns=patterns)

    with Status(
        f"Splitting into {_bytes_to_str(chunk_size)} chunks...", spinner="bouncingBall"
    ) as status:
        subtrees = {
            k: split_into_chunks(
                tree=st,
                chunk_size=chunk_size,
                min_zip_depth=partitions_dict[k].get("min_zip_depth", min_zip_depth),
            )
            for k, st in subtrees.items()
        }

    for prefix, tree in subtrees.items():
        slim_tree = tree.filtered(
            lambda n: SkipBranch(and_self=False) if n.data.is_zip else True
        )
        slim_tree.name = (prefix or "Zip Tree").title()
        slim_tree.print(repr="{node.data}")
        print()

    if not questionary.confirm("Confirm zip partition?", default=False).ask():
        sys.exit(1)

    # Confirm all s3 settings, ensure we don't accidentally upload anything
    if s3.bucket is not None and s3.prefix is not None:
        if not questionary.confirm(
            "Not running in local mode, this will upload artifacts to S3. Confirm?",
            default=False,
        ).ask():
            sys.exit(1)

        public = questionary.confirm(
            "Make uploaded artifacts public?", default=False
        ).ask()
        s3_client = boto3.client("s3")
        exists: Callable = partial(check_exists, s3_client=s3_client, conn=s3)
        upload: Callable = partial(
            upload_file, s3_client=s3_client, conn=s3, public=public
        )
    else:
        # Do not check existence if not uploading
        upload = lambda src, dst: log.info(f"Would have uploaded {src} to {dst}.")
        exists = lambda *args, **kwargs: False

    if tmp_dir:
        tmp_dir.mkdir(exist_ok=True, parents=True)
    if keep and tmp_dir:
        # Use user-supplied tmp_dir directly, do not delete anything from it
        context = partial(contextlib.nullcontext, enter_result=tmp_dir.resolve())
    elif keep and tmp_dir is None:
        # Use a single tmpdir, open context manager here and keep contents
        with tempfile.TemporaryDirectory(delete=False) as tmpdir:
            context = partial(contextlib.nullcontext, enter_result=tmpdir)
            log.info(f"Using tempdir {tmpdir}")
    elif not keep:
        # We're not keeping the tempdir, so we can safely re-enter into a new
        # temporary directory every time which helps keep it a manageable size
        context = partial(
            cast(Callable, tempfile.TemporaryDirectory),
            dir=tmp_dir.resolve() if tmp_dir is not None else None,
            delete=True,
        )

    # Callable used to visit tree, will zip up all descendant of a zip node and upload it
    def upload_ziptree(
        node: Node,
        memo: Any,
        *,
        prefix: str | None,
        context: Callable,
        update_fn: UpdateFn,
    ) -> SkipBranch | None:
        if not node.data.is_zip:
            return None

        object_key = Path(prefix or "") / node.data.path.relative_to(path.parent)
        update_fn(description=f"Compressing {node.data.path.name}")

        if not overwrite and exists(key=object_key):
            log.info(
                f"Skipping {object_key} as objects with the same key exists in bucket."
            )
            update_fn(advance=1)
            raise SkipBranch

        with context() as tmpdir:
            zip_path = Path(tmpdir) / object_key
            zip_path.parent.mkdir(exist_ok=True, parents=True)

            with zipfile.ZipFile(zip_path, mode="w") as archive:
                if (s3.bucket is not None or s3.prefix is not None) or keep:
                    for n in node.find_all(match=lambda n: n.is_leaf(), add_self=True):
                        archive.write(
                            n.data.path,
                            arcname=n.data.path.relative_to(node.data.path.parent),
                        )
            update_fn(description=f"Uploading {node.data.path.name}")
            upload(zip_path, object_key)
        update_fn(advance=1)
        raise SkipBranch

    for i, (prefix, tree) in enumerate(subtrees.items()):
        with Progress(
            TextColumn(
                f"({i+1}/{len(subtrees)}) " + "[progress.description]{task.description}"
            ),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(elapsed_when_finished=True),
        ) as progress:
            task = progress.add_task(
                "", total=len(tree.find_all(match=lambda n: n.data.is_zip))
            )
            update_fn = partial(progress.update, task)
            tree.visit(
                partial(
                    upload_ziptree, prefix=prefix, context=context, update_fn=update_fn
                )
            )

    # Save all subtrees for future inspection
    (path / "trees").mkdir(exist_ok=True, parents=True)
    for k, st in subtrees.items():
        st.save(
            path / "trees" / f"{k or 'tree'}.json",
            mapper=PathData.serialize_mapper,
            compression=True,
        )


if __name__ == "__main__":
    if boto3.__version__ != "1.35.31":
        log.warning(
            "Please use boto3==1.35.31 as other versions might fail to upload files!!"
        )
    app.cli()
