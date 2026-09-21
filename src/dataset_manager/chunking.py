from __future__ import annotations

import itertools
import logging
from collections.abc import Callable
from typing import Any, Literal, Protocol

import more_itertools as mitertools
from natsort import natsorted
from nutree import SkipBranch, StopTraversal, Tree
from nutree.node import Node

from .log import log
from .sizes import _bytes_from_str, _bytes_to_str
from .tree import PathData

# Names of the packing strategies offered by the `upload` command.
type ChunkStrategy = Literal["legacy", "greedy", "optimal"]

# Levels with more packable children than this are packed with the greedy
# heuristic instead of an exact MIP: bin packing is NP-hard and a single level
# can hold many thousands of files, where the model would be far too large.
_MIP_MAX_ITEMS = 200

# Safety net for a single MIP solve. The search already stops at the first
# feasible bin count, so this only guards against pathological instances; on
# timeout the greedy grouping is kept.
_MIP_MAX_SECONDS = 30.0


class ChunkStrategyFn(Protocol):
    """Common API of every `--strategy` implementation.

    A strategy takes the tree of a single partition and returns that tree with
    archive nodes (``PathData.is_zip``) inserted. It owns every decision about
    which nodes become archives, which files they contain and in which order, and
    at which level the archives are placed - so callers must not assume that an
    archive holds a node's children in tree order, nor that archive names or
    boundaries are stable across strategies.

    Implementations must uphold the invariants enforced by `_validate_ziptree`:
    every file of the input tree ends up inside exactly one archive, and no
    archive is empty.
    """

    def __call__(self, *, tree: Tree, chunk_size: int, min_zip_depth: int) -> Tree:
        """Chunk `tree` into archives of about `chunk_size` bytes.

        Args:
            tree (Tree): Partition subtree, modified in place.
            chunk_size (int): Target archive size (pre-compression), in bytes.
            min_zip_depth (int): Depth (root = 1) at or above which a node is
                archived even when it is smaller than `chunk_size`.

        Returns:
            Tree: `tree`, with archive nodes inserted.
        """
        ...


def _legacy_group_children(node: Node, children: list[Node], chunk_size: int) -> list[list[Node]]:
    """Group a node's children with the original bucket rule.

    A group is a maximal run of children whose running totals - measured from the
    node's first child - fall inside one ``chunk_size`` bucket, i.e. a split
    happens wherever ``cumulative // chunk_size`` changes.

    Args:
        node (Node): Node whose children are being packed.
        children (list[Node]): Eligible children, in the default packing order
            (directories first, then files, each natsorted by path).
        chunk_size (int): Target archive size (pre-compression), in bytes.

    Returns:
        list[list[Node]]: One list of children per archive, in creation order.

    Note:
        This is the packing `upload` used before `--strategy` existed. It is *not*
        bounded by `chunk_size`: the group that straddles a bucket boundary keeps
        filling that bucket, so an archive can reach ``chunk_size + size of its
        first child`` bytes, almost twice the target. Overshoots are reported
        with a warning, see below.
    """
    cumulative_sizes = list(itertools.accumulate(c.data.size or 0 for c in children))
    groups_lengths = [
        len(group) for group in mitertools.split_when(cumulative_sizes, lambda x, y: x // chunk_size != y // chunk_size)
    ]
    children_iter = iter(children)
    groups = [list(mitertools.take(n, children_iter)) for n in groups_lengths]

    # The bucket rule can overshoot `chunk_size` even when no single file does,
    # because the straddling group keeps filling its bucket. Report it rather than
    # letting it look like a bug (`greedy` provably cannot do this).
    for i, group in enumerate(groups):
        size = sum(c.data.size or 0 for c in group)
        if len(group) > 1 and size > chunk_size:
            biggest = max(group, key=lambda c: c.data.size or 0)
            log.warning(
                f"Archive {node.data.path.stem}_{i}.zip is {_bytes_to_str(size)}, "
                f"above the {_bytes_to_str(chunk_size)} target although no single "
                f"file exceeds it (legacy bucket packing; largest child: "
                f"{biggest.data.path} ({_bytes_to_str(biggest.data.size or 0)}))."
            )
    return groups


def _greedy_group_children(node: Node, children: list[Node], chunk_size: int) -> list[list[Node]]:
    """Group a node's children greedily into archives of at most `chunk_size`.

    Children are accumulated in the given order and a group is closed just before
    adding the child that would push it past `chunk_size`.

    Args:
        node (Node): Node whose children are being packed (unused here; part of
            the common per-node API, see `_split_in_place`).
        children (list[Node]): Eligible children, in the default packing order.
        chunk_size (int): Target archive size (pre-compression), in bytes.

    Returns:
        list[list[Node]]: One list of children per archive, in creation order.

    Note:
        A group only exceeds `chunk_size` when it holds a single child that is
        itself larger, because a group is closed before it can grow past the
        target. Such a child is always a file: leaves are never split, while a
        directory larger than `chunk_size` is split - or excluded from packing via
        `has_zip_descendants` - before its parent is packed.
    """
    groups: list[list[Node]] = []
    group: list[Node] = []
    group_size = 0
    for child in children:
        size = child.data.size or 0
        if group and group_size + size > chunk_size:
            groups.append(group)
            group, group_size = [], 0
        group.append(child)
        group_size += size
    if group:
        groups.append(group)
    return groups


def _solve_bin_packing(
    items: list[Node],
    chunk_size: int,
    *,
    lower_bound: int,
    upper_bound: int,
) -> list[list[Node]]:
    """Pack `items` into the fewest bins of capacity `chunk_size` using a MIP.

    Bin packing is NP-hard, so rather than minimizing the bin count directly this
    searches for the smallest feasible number of bins starting at `lower_bound`:
    the first feasible count is provably optimal, and each subproblem stays small
    when only a few bins are needed. `upper_bound` (the greedy bin count) caps the
    search.

    Args:
        items (list[Node]): Children to pack, each no larger than `chunk_size`.
        chunk_size (int): Bin capacity (pre-compression size), in bytes.
        lower_bound (int): Lower bound on the number of bins, ``ceil(total /
            chunk_size)`` (at least 1 when `items` is non-empty).
        upper_bound (int): Known feasible number of bins, from greedy packing.

    Returns:
        list[list[Node]]: One list of items per bin, in the given order.

    Raises:
        RuntimeError: If no feasible packing is found up to `upper_bound`.
    """
    # python-mip logs an INFO banner when it is imported; keep it off the CLI.
    logging.getLogger("mip").setLevel(logging.WARNING)

    # Imported lazily so the native solver is only loaded for `--strategy optimal`.
    from mip import BINARY, Model, OptimizationStatus, xsum

    sizes = [item.data.size or 0 for item in items]
    n = len(items)

    for bins in range(lower_bound, upper_bound + 1):
        model = Model(solver_name="cbc")
        model.verbose = 0
        model.max_seconds = _MIP_MAX_SECONDS

        # x[i][j] == 1 when item i is placed in bin j. Symmetry breaking: item i
        # may only use bin j <= i (item 0 is fixed to bin 0), so interchangeable
        # bins cannot multiply the search space.
        x: list[list[Any]] = [
            [model.add_var(var_type=BINARY) if j <= i else None for j in range(bins)] for i in range(n)
        ]
        for i in range(n):
            model += xsum(x[i][j] for j in range(min(i, bins - 1) + 1)) == 1
        for j in range(bins):
            model += xsum(sizes[i] * x[i][j] for i in range(n) if x[i][j] is not None) <= chunk_size

        model.optimize()
        if model.status in (OptimizationStatus.OPTIMAL, OptimizationStatus.FEASIBLE):
            groups: list[list[Node]] = [[] for _ in range(bins)]
            for i, item in enumerate(items):
                for j in range(min(i, bins - 1) + 1):
                    if x[i][j].x >= 0.5:
                        groups[j].append(item)
                        break
            return [group for group in groups if group]

    raise RuntimeError(f"no feasible packing into {upper_bound} bin(s)")


def _optimal_group_children(node: Node, children: list[Node], chunk_size: int) -> list[list[Node]]:
    """Group a node's children into the fewest archives of at most `chunk_size`.

    Solves a bin-packing MIP over the children of a single node, so files and
    subfolders are only ever packed together with their siblings: items from
    unrelated branches are never mixed. Falls back to `_greedy_group_children`
    when the level is too large for the solver, when greedy already reaches the
    optimal bin count, or when the solver fails or times out.

    Args:
        node (Node): Node whose children are being packed.
        children (list[Node]): Eligible children, in the default packing order.
        chunk_size (int): Target archive size (pre-compression), in bytes.

    Returns:
        list[list[Node]]: One list of children per archive, in creation order.
    """
    greedy = _greedy_group_children(node, children, chunk_size)
    if len(children) <= 1:
        return greedy

    # Files are atomic, so a child larger than `chunk_size` can never share an
    # archive; peel these off before solving, which also keeps every remaining
    # item within capacity so the MIP is always feasible.
    oversized = [child for child in children if (child.data.size or 0) > chunk_size]
    packable = [child for child in children if (child.data.size or 0) <= chunk_size]
    if not packable:
        return greedy

    packable_size = sum(child.data.size or 0 for child in packable)
    lower_bound = max(1, -(-packable_size // chunk_size))

    # Greedy already uses the fewest possible archives, so it cannot be beaten.
    if len(greedy) <= len(oversized) + lower_bound:
        return greedy

    if len(packable) > _MIP_MAX_ITEMS:
        log.warning(
            f"{node.data.path} has {len(packable)} packable children, above the "
            f"{_MIP_MAX_ITEMS}-item limit for exact packing; using greedy packing instead."
        )
        return greedy

    try:
        groups = _solve_bin_packing(
            packable,
            chunk_size,
            lower_bound=lower_bound,
            upper_bound=len(greedy) - len(oversized),
        )
    except Exception as error:  # noqa: BLE001 - any solver failure must fall back to greedy
        log.warning(f"Optimal packing of {node.data.path} failed ({error}); using greedy packing instead.")
        return greedy

    # Oversized items come first, then the solved groups; order inside a group is
    # the input order, so archive numbering stays deterministic.
    return [[child] for child in oversized] + groups


def _validate_ziptree(*, tree: Tree, original_files: set[str]) -> None:
    """Assert that `tree` is a valid chunked tree.

    Invariants every strategy must uphold: every archive is a non-empty node (an
    archive is a directory holding the files it stores) and every file of the
    original tree lives inside exactly one archive, so nothing is dropped or
    duplicated. Custom `ChunkStrategyFn` implementations should call this before
    returning.

    Args:
        tree (Tree): Chunked tree to check.
        original_files (set[str]): Paths of every leaf file before chunking.

    Raises:
        RuntimeError: If a file went missing, an archive is empty, or a file was
            left outside of any archive.
    """
    if diff := original_files - {str(n.data.path) for n in tree.find_all(match=lambda n: n.is_leaf())}:
        raise RuntimeError(f"Detected missing files in ziptree: {diff}")

    def validate(node: Node, _memo: Any) -> SkipBranch | StopTraversal | None:
        if node.data.is_zip:
            if node.is_leaf():
                raise StopTraversal(RuntimeError(f"Found empty zipped node: {node}."))
            raise SkipBranch
        elif node.is_leaf():
            raise StopTraversal(RuntimeError(f"Found non-zipped leaf node: {node}."))
        return None

    if error := tree.visit(validate):
        raise error


def _split_in_place(
    *,
    tree: Tree,
    chunk_size: int,
    min_zip_depth: int,
    group_children: Callable[[Node, list[Node], int], list[list[Node]]],
) -> Tree:
    """Shared driver for strategies that archive a node's own children in place.

    Walks `tree` bottom-up and, for every node too large to keep whole
    (``node.data.size > chunk_size``) or too shallow to leave unzipped
    (``node.depth() <= min_zip_depth``), asks `group_children` which archives to
    create from its eligible children - those not already containing archives -
    and materializes them as sibling zip nodes.

    Strategies that bundle files across subtrees, or place archives somewhere
    other than next to the node being split, should implement `ChunkStrategyFn`
    directly instead of using this driver.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.
        group_children (Callable[[Node, list[Node], int], list[list[Node]]]):
            Per-node packing decision, see `_legacy_group_children`.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    if (root := tree.first_child()) is None:
        # Empty tree
        return tree

    splits: dict[tuple[str | int, int], list[Node]] = {}

    # Cache leaf nodes to avoid repeated find_all traversals
    leaf_nodes = list(tree.find_all(match=lambda n: n.is_leaf()))
    original_files = {str(n.data.path) for n in leaf_nodes}

    # Files are atomic: one larger than the target can never be split across
    # archives, so its archive will exceed `chunk_size`.
    if oversized := [n for n in leaf_nodes if (n.data.size or 0) > chunk_size]:
        largest = sorted(oversized, key=lambda n: n.data.size or 0, reverse=True)
        log.warning(
            f"{len(oversized)} file(s) exceed the {_bytes_to_str(chunk_size)} target "
            "and cannot be split; their archives will be larger than requested. "
            "Largest: " + ", ".join(f"{n.data.path} ({_bytes_to_str(n.data.size or 0)})" for n in largest[:3])
        )

    def find_splits(*, node: Node) -> None:
        # First recurse and propagate the has_zip_descendants label up
        for child in node.children:
            find_splits(node=child)
            node.data.has_zip_descendants = node.data.has_zip_descendants or child.data.has_zip_descendants

        # Early exit: if this node is small enough and deep enough, skip splitting
        if node.data.size <= chunk_size and node.depth() > min_zip_depth:
            return

        # Split node if too big, making sure to exclude children with zip descendants
        children = natsorted(
            filter(lambda n: not n.data.has_zip_descendants, node.children),
            key=lambda n: (n.data.path.is_file(), str(n.data.path)),
        )
        for i, group in enumerate(group_children(node, children, chunk_size)):
            splits[(node.data_id, i)] = group
            node.data.has_zip_descendants = True

    # Find places for zips without modifying tree topology!
    find_splits(node=root)

    # Create new zipnodes for every zip
    for (data_id, i), group in splits.items():
        node = tree.find_first(data_id=data_id)
        assert isinstance(node, Node)

        zipnode_data = PathData(
            path=node.data.path.with_name(f"{node.data.path.stem}_{i}.zip"),
            size=sum(c.data.size or 0 for c in group),
            is_zip=True,
        )
        zipnode = node.up().add(zipnode_data)

        # Move over contents of zip file
        for c in group:
            c.move_to(zipnode)

        # Remove node once we've removed all children
        if not node.children:
            node.remove()

    # Ensure nothing was dropped, duplicated, or left outside of an archive
    _validate_ziptree(tree=tree, original_files=original_files)
    return tree


def _legacy_split(*, tree: Tree, chunk_size: int, min_zip_depth: int) -> Tree:
    """Chunk `tree` with the original bucket packing (`--strategy legacy`).

    Reproduces the behaviour of `upload` before `--strategy` existed; see
    `_legacy_group_children` for the rule and its size guarantee.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    return _split_in_place(
        tree=tree,
        chunk_size=chunk_size,
        min_zip_depth=min_zip_depth,
        group_children=_legacy_group_children,
    )


def _greedy_split(*, tree: Tree, chunk_size: int, min_zip_depth: int) -> Tree:
    """Chunk `tree` greedily, one archive at a time (`--strategy greedy`).

    Every archive is at most `chunk_size`, except one holding a single file larger
    than `chunk_size`; see `_greedy_group_children`.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    return _split_in_place(
        tree=tree,
        chunk_size=chunk_size,
        min_zip_depth=min_zip_depth,
        group_children=_greedy_group_children,
    )


def _optimal_split(*, tree: Tree, chunk_size: int, min_zip_depth: int) -> Tree:
    """Chunk `tree` with exact per-level bin packing (`--strategy optimal`).

    Every archive is at most `chunk_size`, except one holding a single file larger
    than `chunk_size`, and each level uses the fewest archives possible; see
    `_optimal_group_children`.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    return _split_in_place(
        tree=tree,
        chunk_size=chunk_size,
        min_zip_depth=min_zip_depth,
        group_children=_optimal_group_children,
    )


_CHUNK_STRATEGIES: dict[ChunkStrategy, ChunkStrategyFn] = {
    "legacy": _legacy_split,
    "greedy": _greedy_split,
    "optimal": _optimal_split,
}


def split_into_chunks(
    *,
    tree: Tree,
    chunk_size: int = _bytes_from_str("200MB"),
    min_zip_depth: int = 1,
    strategy: ChunkStrategy = "legacy",
) -> Tree:
    """Chunk one partition `tree` into archives using `strategy`.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int, optional): Target archive size (pre-compression), in
            bytes. Defaults to 200MB.
        min_zip_depth (int, optional): Depth (root = 1) at or above which a node
            is archived even when it is smaller than `chunk_size`. Defaults to 1
            (only the root node).
        strategy (ChunkStrategy, optional): Entry of `_CHUNK_STRATEGIES` to use.
            Defaults to "legacy".

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    if strategy not in _CHUNK_STRATEGIES:
        raise ValueError(f"Unknown strategy {strategy!r}; expected one of {', '.join(_CHUNK_STRATEGIES)}.")
    return _CHUNK_STRATEGIES[strategy](tree=tree, chunk_size=chunk_size, min_zip_depth=min_zip_depth)
