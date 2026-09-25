from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from nutree import Tree
from nutree.node import Node

from .log import log
from .sizes import _bytes_to_str


@dataclass
class PathData:
    path: Path
    is_dir: bool | None = None
    is_zip: bool = False
    has_zip_descendants: bool = False
    # A sibling file pulled into an ancestor's archive (`merge_ratio`); its parent
    # must not be archived separately.
    related: bool = False
    size: int | None = None
    zip_size: int | None = None

    def __post_init__(self):
        if self.is_dir is None:
            self.is_dir = self.path.is_dir()

    def __str__(self):
        icon = "💾" if self.is_zip else "📁" if self.is_dir else "📄"
        size = _bytes_to_str(self.size or 0)
        size += f" {(self.size or 0) / self.zip_size:.1f}x" if self.is_zip and self.zip_size else ""
        return f"{icon} {self.path.name} ({size})"

    def __hash__(self):
        return hash((self.path, self.is_zip))

    @staticmethod
    def serialize_mapper(node, data):
        data["data"] = (
            str(node.data.path),
            node.data.is_dir,
            node.data.is_zip,
            node.data.has_zip_descendants,
            node.data.related,
            node.data.size,
            node.data.zip_size,
        )
        return data

    @staticmethod
    def deserialize_mapper(parent, data):
        fields = data["data"]
        # Backwards compatibility: old files may have 5 fields (missing zip_size)
        # or 6 fields (missing `related`).
        if len(fields) == 5:
            path, is_dir, is_zip, has_zip_descendants, size = fields
            related, zip_size = False, None
        elif len(fields) == 6:
            path, is_dir, is_zip, has_zip_descendants, size, zip_size = fields
            related = False
        else:
            path, is_dir, is_zip, has_zip_descendants, related, size, zip_size = fields
        return PathData(
            path=Path(path),
            is_dir=is_dir,
            is_zip=is_zip,
            has_zip_descendants=has_zip_descendants,
            related=related,
            size=size,
            zip_size=zip_size,
        )


def is_not_hidden(path: Path) -> bool:
    return not path.name.startswith(".")


def is_not_dunder(path: Path) -> bool:
    return not path.name.startswith("_")


def is_match(path: Path, patterns: list[str] | None = None) -> bool:
    return any(path.match(exclude_pattern) for exclude_pattern in (patterns or []))


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


def drop_empty_dirs(*, node: Node) -> int:
    """Remove descendant directories that hold no files, deepest first.

    An empty directory is a leaf of the walked tree, so `upload` would write it into
    an archive and `write_zip_stream` would then try to open it as a file
    (`IsADirectoryError`). Nothing could ever match it on the other side either: S3
    stores no directory entries. The passed node itself is always kept, so a tree
    whose root holds no files stays representable. Returns the number of nodes
    removed.
    """
    removed = 0
    for child in list(node.children):
        if not child.data.is_dir:
            continue
        removed += drop_empty_dirs(node=child)
        if not child.children:
            child.remove()
            removed += 1
    return removed


def directory_tree(
    path: str | os.PathLike,
    on_error: Callable | None = None,
    follow_symlinks: bool = False,
    filter_fn: Callable | None = None,
) -> Tree:
    """Build the tree of a directory listing.

    Directories with no file below them are dropped (see `drop_empty_dirs`), because
    `upload` archives files only.
    """
    path = Path(path).resolve()
    tree: Tree = Tree("Directory Listing")
    root = tree.add(PathData(path=path))
    path2node = {str(root.data.path.resolve()): root}
    skipped_dirs: set[Path] = set()

    if not path.exists():
        raise FileNotFoundError(f"Directory {path} does not exist!")

    # Use os.scandir for a single-pass walk that avoids redundant stat() calls.
    # Path.walk() calls stat() internally, and then we were calling stat() again
    # for each file. os.scandir provides stat info directly from the directory entry.
    for dirpath, dirnames, filenames, dir_entries in _scandir_walk(
        path, on_error=on_error, follow_symlinks=follow_symlinks
    ):
        if filter_fn is not None and (not filter_fn(dirpath) or dirpath.parent in skipped_dirs):
            skipped_dirs.add(dirpath)
            continue

        parent = path2node[str(dirpath.resolve())]

        for dirname in dirnames:
            child_path = dirpath / dirname
            if filter_fn is None or filter_fn(child_path):
                child_data = PathData(path=child_path)
                child = parent.add(child_data)
                path2node[str(child.data.path.resolve())] = child
        for filename, entry in filenames:
            child_path = dirpath / filename
            if filter_fn is None or filter_fn(child_path):
                # Use stat from scandir entry to avoid redundant stat() call
                child_data = PathData(
                    path=child_path,
                    size=entry.stat(follow_symlinks=follow_symlinks).st_size,
                )
                child = parent.add(child_data)
                path2node[str(child.data.path.resolve())] = child
    removed = drop_empty_dirs(node=root)
    if removed:
        log.info(f"Ignoring empty directories under {path} ({removed} removed).")
    populate_filesize(node=root)
    return tree


def _scandir_walk(
    top: Path,
    on_error: Callable | None = None,
    follow_symlinks: bool = False,
) -> list[tuple[Path, list[str], list[tuple[str, os.DirEntry]], list[os.DirEntry]]]:
    """A replacement for Path.walk() that uses os.scandir to avoid redundant stat() calls.

    Returns a list of (dirpath, dirnames, filenames_with_entries, dir_entries) tuples,
    where filenames_with_entries is a list of (filename, DirEntry) pairs.
    """
    results: list[tuple[Path, list[str], list[tuple[str, os.DirEntry]], list[os.DirEntry]]] = []

    try:
        scandir_iter = os.scandir(top)
    except OSError as e:
        if on_error is not None:
            on_error(e)
        return results

    with scandir_iter as it:
        dirnames: list[str] = []
        filenames: list[tuple[str, os.DirEntry]] = []
        dir_entries: list[os.DirEntry] = []

        for entry in it:
            try:
                is_dir = entry.is_dir(follow_symlinks=follow_symlinks)
            except OSError:
                is_dir = False

            if is_dir:
                dirnames.append(entry.name)
                dir_entries.append(entry)
            else:
                filenames.append((entry.name, entry))

        results.append((top, dirnames, filenames, dir_entries))

    # Recurse into subdirectories
    for dirname, entry in zip(dirnames, dir_entries):
        sub_path = top / dirname
        try:
            sub_results = _scandir_walk(sub_path, on_error=on_error, follow_symlinks=follow_symlinks)
            results.extend(sub_results)
        except PermissionError as e:
            if on_error is not None:
                on_error(e)

    return results
