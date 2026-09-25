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

    def __call__(
        self,
        *,
        tree: Tree,
        chunk_size: int,
        min_zip_depth: int,
        merge_ratio: float | None = None,
    ) -> Tree:
        """Chunk `tree` into archives of about `chunk_size` bytes.

        Args:
            tree (Tree): Partition subtree, modified in place.
            chunk_size (int): Target archive size (pre-compression), in bytes.
            min_zip_depth (int): Depth (root = 1) at or above which a node is
                archived even when it is smaller than `chunk_size`.
            merge_ratio (float | None, optional): How much a folder holding a file
                larger than `chunk_size` may exceed that file and still be archived
                together with it, as a fraction of `chunk_size`. Defaults to None
                (never merge).

        Returns:
            Tree: `tree`, with archive nodes inserted.
        """
        ...


def _merge_oversized_children(
    children: list[Node],
    chunk_size: int,
    *,
    merge_ratio: float | None,
    fill: Callable[[list[Node]], list[list[Node]]],
) -> list[list[Node]]:
    """Fold the siblings of an oversized file into that file's archive.

    A file larger than `chunk_size` always gets an archive of its own, and its
    small neighbouring files (videos, masks, metadata) are then swept into an
    unrelated packing archive, which makes the big file useless to download on its
    own. When the directory containing the oversized file is no larger than
    `merge_ratio * chunk_size` plus that file, the whole directory is returned as a
    single group instead, so the file and everything it needs travel together.
    Directories whose total is bigger than that (a folder that happens to contain
    one huge file among many others) are left alone and packed by `fill`.

    Every child that ends up inside a merge group has `PathData.related` set, so
    the callers can tell a topmost merged folder from one nested inside another
    (see `_has_related`).

    Args:
        children (list[Node]): Eligible children of one node, in the default
            packing order; children already marked `related` must be skipped, they
            belong to the merge the parent node is performing.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        merge_ratio (float | None): How much a folder may exceed its biggest file
            and still be merged, as a fraction of `chunk_size`; None or 0 disables
            merging.
        fill (Callable[[list[Node]], list[list[Node]]]): Strategy-specific packing
            of the children the merge did not claim (`_greedy_group_children` or
            `_optimal_group_children`; `_legacy_group_children` uses the bucket
            rule inline).

    Returns:
        list[list[Node]]: With a falsy `merge_ratio`, `fill` of every child that is
        not already pulled into a merge (see below), i.e. one archive per child.
        Otherwise one group per oversized child (`child` alone when its folder
        exceeds the ratio) followed by `fill` of the remaining children; children
        marked `PathData.related` are excluded from both, because they are handled
        by the parent node's merge and the caller adds the node itself instead.
    """
    if not merge_ratio or not any((child.data.size or 0) > chunk_size for child in children):
        return fill([child for child in children if not child.data.related])

    oversized = [child for child in children if (child.data.size or 0) > chunk_size]
    # Supporting files are everything but the oversized children themselves: those
    # keep their own archives, so they must not eat into this folder's budget.
    support = [child for child in children if not any(child is big for big in oversized)]
    support_size = sum(child.data.size or 0 for child in support)
    groups: list[list[Node]] = []
    merged: list[Node] = []
    for child in children:
        if child.data.related or any(child is other for other in merged):
            continue
        if not any(child is other for other in oversized):
            continue
        # The supporting files as a whole - not just the overshoot of the biggest
        # file - must stay within `merge_ratio * chunk_size` of it, so a folder
        # holding many files of its own (a split with one huge member) is packed as
        # before instead of collapsing into one oversized archive.
        if support_size > merge_ratio * chunk_size:
            # Even the whole folder is too big to ride along with this file.
            continue
        # `children` are siblings, so everything but the oversized children
        # themselves is a supporting file of this folder. Guard the pathological
        # case of a directory holding several oversized files: those keep their own
        # archives, and a sibling already claimed by an earlier group is not added
        # twice (it would otherwise end up in two archives).
        companions = [
            other
            for other in children
            if other is not child
            and not any(other is big for big in oversized)
            and not any(other is claimed for claimed in merged)
        ] + [child]
        for companion in companions:
            companion.data.related = True
        merged.extend(companions)
        groups.append(companions)

    if groups:
        log.info(
            f"Merging {len(groups)} folder(s) holding oversized files into their "
            "supporting files' archives (see the `merge_ratio` option of `upload`)."
        )
    return groups + fill([child for child in children if not any(child is other for other in merged)])


def _has_related(node: Node) -> bool:
    """True when `node` or any of its descendants holds merged sibling files.

    A node that holds a file merged into an ancestor's archive can no longer be
    archived on its own: doing so would either duplicate that file or strip it from
    its parent, a sibling of its own node. The strategies therefore use this to
    skip such nodes (and thus every node above them) so that only the topmost
    merged folder becomes the archive root.

    Args:
        node (Node): Node whose subtree is inspected.

    Returns:
        bool: True if any node in this subtree has `PathData.related` set.
    """
    return any(n.data.related for n in node.find_all(add_self=True))


def _fill_legacy(children: list[Node], chunk_size: int) -> list[list[Node]]:
    """Group a node's children with the original bucket rule.

    A group is a maximal run of children whose running totals - measured from the
    node's first child - fall inside one ``chunk_size`` bucket, i.e. a split
    happens wherever ``cumulative // chunk_size`` changes.

    Args:
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
    for group in groups:
        size = sum(c.data.size or 0 for c in group)
        if len(group) > 1 and size > chunk_size:
            biggest = max(group, key=lambda c: c.data.size or 0)
            log.warning(
                f"An archive holding {len(group)} children is {_bytes_to_str(size)}, "
                f"above the {_bytes_to_str(chunk_size)} target although no single "
                f"file exceeds it (legacy bucket packing; largest child: "
                f"{biggest.data.path} ({_bytes_to_str(biggest.data.size or 0)}))."
            )
    return groups


def _legacy_group_children(
    node: Node,
    children: list[Node],
    chunk_size: int,
    *,
    merge_ratio: float | None = None,
) -> list[list[Node]]:
    """Group a node's children with the original bucket rule, merging folders.

    See `_fill_legacy` for the packing and `_merge_oversized_children` for the
    merge of a folder into the archive of the oversized file it holds.

    Args:
        node (Node): Node whose children are being packed (unused here; part of
            the common per-node API, see `_split_in_place`).
        children (list[Node]): Eligible children, in the default packing order.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        merge_ratio (float | None, optional): Merge threshold, see
            `_merge_oversized_children`. Defaults to None (no merging).

    Returns:
        list[list[Node]]: One list of children per archive, in creation order.
    """
    if not merge_ratio or not any((child.data.size or 0) > chunk_size for child in children):
        return _fill_legacy([child for child in children if not child.data.related], chunk_size)

    # The merge scan is inlined rather than delegated to `_merge_oversized_children`
    # because this function itself cannot be passed as that function's `fill`
    # callback: it would recurse.
    oversized = [child for child in children if (child.data.size or 0) > chunk_size]
    # Supporting files are everything but the oversized children themselves: those
    # keep their own archives, so they must not eat into this folder's budget.
    support = [child for child in children if not any(child is big for big in oversized)]
    support_size = sum(child.data.size or 0 for child in support)
    merges: list[list[Node]] = []
    merged: list[Node] = []
    for child in children:
        if child.data.related or any(child is other for other in merged):
            continue
        if not any(child is other for other in oversized):
            continue
        # The supporting files as a whole - not just the overshoot of the biggest
        # file - must stay within `merge_ratio * chunk_size` of it, so a folder
        # holding many files of its own (a split with one huge member) is packed as
        # before instead of collapsing into one oversized archive.
        if support_size > merge_ratio * chunk_size:
            continue
        # `children` are siblings, so everything but the oversized children
        # themselves is a supporting file of this folder. Guard the pathological
        # case of a directory holding several oversized files: those keep their own
        # archives, and a sibling already claimed by an earlier group is not added
        # twice (it would otherwise end up in two archives).
        companions = [
            other
            for other in children
            if other is not child
            and not any(other is big for big in oversized)
            and not any(other is claimed for claimed in merged)
        ] + [child]
        for companion in companions:
            companion.data.related = True
        merged.extend(companions)
        merges.append(companions)
    if merges:
        log.info(
            f"Merging {len(merges)} folder(s) holding oversized files into their "
            "supporting files' archives (see the `merge_ratio` option of `upload`)."
        )
    return merges + _fill_legacy(
        [child for child in children if not any(child is other for other in merged)], chunk_size
    )


def _fill_greedy(children: list[Node], chunk_size: int) -> list[list[Node]]:
    """Group children greedily into archives of at most `chunk_size`.

    Children are accumulated in the given order and a group is closed just before
    adding the child that would push it past `chunk_size`.

    Args:
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


def _greedy_group_children(
    node: Node,
    children: list[Node],
    chunk_size: int,
    *,
    merge_ratio: float | None = None,
) -> list[list[Node]]:
    """Group a node's children greedily, merging folders holding oversized files.

    See `_fill_greedy` for the packing and `_merge_oversized_children` for the
    merge of a folder into the archive of the oversized file it holds.

    Args:
        node (Node): Node whose children are being packed (unused here; part of
            the common per-node API, see `_split_in_place`).
        children (list[Node]): Eligible children, in the default packing order.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        merge_ratio (float | None, optional): Merge threshold, see
            `_merge_oversized_children`. Defaults to None (no merging).

    Returns:
        list[list[Node]]: One list of children per archive, in creation order.
    """
    return _merge_oversized_children(
        children,
        chunk_size,
        merge_ratio=merge_ratio,
        fill=lambda remaining: _fill_greedy(remaining, chunk_size),
    )


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


def _fill_optimal(
    children: list[Node],
    chunk_size: int,
    *,
    node: Node,
    merge_ratio: float | None,
) -> list[list[Node]]:
    """Group children into the fewest archives of at most `chunk_size`.

    Solves a bin-packing MIP over the children of a single node, so files and
    subfolders are only ever packed together with their siblings: items from
    unrelated branches are never mixed. Falls back to `_fill_greedy` when the
    level is too large for the solver, when greedy already reaches the optimal bin
    count, or when the solver fails or times out.

    Unlike `_merge_oversized_children` this does not skip `related` children by
    itself; the strategies exclude them before calling it (they belong to a merge
    performed by an ancestor, which marks no `related` on the merged folder's own
    children).

    Args:
        children (list[Node]): Eligible children, in the default packing order.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        node (Node): Node whose children are being packed; used for warnings only.
        merge_ratio (float | None): Merge threshold, see `_merge_oversized_children`.
            A folder merged here counts as oversized, so it is never split up again.

    Returns:
        list[list[Node]]: One list of children per archive, in creation order.
    """
    greedy = _fill_greedy(children, chunk_size)
    if len(children) <= 1:
        return greedy

    # Files are atomic, so a child larger than `chunk_size` can never share an
    # archive; peel these off before solving, which also keeps every remaining
    # item within capacity so the MIP is always feasible. A merged folder counts
    # as oversized too: it already holds a file larger than the target and must
    # not be split up again.
    oversized = [
        child for child in children if (child.data.size or 0) > chunk_size or (merge_ratio and child.data.related)
    ]
    packable = [child for child in children if not any(child is big for big in oversized)]
    if not packable:
        # A level that is nothing but oversized and merged items is exactly the
        # `_merge_oversized_children` output, so greedy packing is accepted as-is.
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


def _optimal_group_children(
    node: Node,
    children: list[Node],
    chunk_size: int,
    *,
    merge_ratio: float | None = None,
) -> list[list[Node]]:
    """Group a node's children optimally, merging folders holding oversized files.

    See `_fill_optimal` for the exact packing and `_merge_oversized_children` for
    the merge of a folder into the archive of the oversized file it holds.

    Args:
        node (Node): Node whose children are being packed.
        children (list[Node]): Eligible children, in the default packing order.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        merge_ratio (float | None, optional): Merge threshold, see
            `_merge_oversized_children`. Defaults to None (no merging).

    Returns:
        list[list[Node]]: One list of children per archive, in creation order.
    """
    return _merge_oversized_children(
        children,
        chunk_size,
        merge_ratio=merge_ratio,
        fill=lambda remaining: _fill_optimal(remaining, chunk_size, node=node, merge_ratio=merge_ratio),
    )


def _assert_binary_merge(*, node: Node, merges: list[list[Node]], chunk_size: int) -> None:
    """Assert that keeping `node` as an archive root cannot split a child group.

    A merge group is materialized as an archive of `node` itself, which only
    reproduces the original layout when the group is `node`'s whole child list:
    the node then holds exactly what the archive will contain. A group holding
    only *part* of the children must never be kept, because the node's remaining
    children, still counted in `node.data.size`, would be silently dropped from
    the archive. `_fill_optimal` returning `greedy` for a level whose packable
    children are reduced to a single item (when `merge_ratio` is set) is the case
    that would break this, so it is asserted rather than assumed.

    Args:
        node (Node): Node about to be kept as an archive root.
        merges (list[list[Node]]): Groups holding `related` children.
        chunk_size (int): Target archive size, for the error message.

    Raises:
        RuntimeError: If a merge holds only part of the node's children.
    """
    for merge in merges:
        if len(merge) != len(node.children):
            raise RuntimeError(
                f"Refusing to archive {node.data.path} in place: it merges only "
                f"{len(merge)} of its {len(node.children)} children, so the merge rule "
                "is inconsistent with `if not packable: return greedy` in `_fill_optimal` "
                f"(chunk_size {_bytes_to_str(chunk_size)})."
            )


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
    group_children: Callable[..., list[list[Node]]],
    merge_ratio: float | None = None,
) -> Tree:
    """Shared driver for strategies that archive a node's own children in place.

    Walks `tree` bottom-up and, for every node too large to keep whole
    (``node.data.size > chunk_size``) or too shallow to leave unzipped
    (``node.depth() <= min_zip_depth``), asks `group_children` which archives to
    create from its eligible children - those not already containing archives -
    and materializes them as sibling zip nodes.

    A group that pulls a node's own children together with the node itself (see
    `_merge_oversized_children`) is turned into an archive *of the node* instead of
    a ``<node>_<i>.zip`` sibling, so the merged files keep their original layout.

    Strategies that bundle files across subtrees, or place archives somewhere
    other than next to the node being split, should implement `ChunkStrategyFn`
    directly instead of using this driver.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.
        group_children (Callable[..., list[list[Node]]]): Per-node packing decision,
            called as
            ``group_children(node, children, chunk_size, merge_ratio=merge_ratio)``;
            see `_legacy_group_children`.
        merge_ratio (float | None, optional): How much a folder may exceed its
            biggest file and still be archived with it, as a fraction of
            `chunk_size`, see `_merge_oversized_children`. Defaults to None.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    if (root := tree.first_child()) is None:
        # Empty tree
        return tree

    splits: dict[tuple[str | int, int], list[Node]] = {}
    # Nodes to keep whole because they became the archive root of a merge of their
    # own children, keyed by data_id.
    kept: dict[str | int, Node] = {}

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
            filter(lambda n: not n.data.has_zip_descendants and n.data_id not in kept, node.children),
            key=lambda n: (n.data.path.is_file(), str(n.data.path)),
        )
        groups = group_children(node, children, chunk_size, merge_ratio=merge_ratio)
        merges = [group for group in groups if any(c.data.related for c in group)]
        # A merge is materialized as an archive of the node itself, which reproduces
        # the original layout only when it holds every child (`_assert_binary_merge`).
        # A partial merge - a folder with several oversized files, where the
        # supporting files can only ride along with one of them - becomes an
        # ordinary `<node>_<i>.zip` sibling holding that merge instead, exactly like
        # the strategies' own packing groups. Moving the merge group's children does
        # not disturb the node's remaining children, they are moved group by group
        # afterwards and the node is removed once it is empty.
        keep = merges and all(len(merge) == len(node.children) for merge in merges)
        if keep:
            _assert_binary_merge(node=node, merges=merges, chunk_size=chunk_size)
            kept[node.data_id] = node

        grouped = groups if keep else [group for group in groups if not any(group is merge for merge in merges)] + merges
        for i, group in enumerate(grouped):
            if keep and any(c.data.related for c in group):
                continue  # materialized as the kept node itself
            splits[(node.data_id, i)] = group
            node.data.has_zip_descendants = True

    # Find places for zips without modifying tree topology!
    find_splits(node=root)

    # Turn merges into archives of the node that holds the merged files
    for kept_node in kept.values():
        kept_node.data.is_zip = True
        # The archive holds the node's children, so the node's size is unchanged.

    # Create new zipnodes for every zip
    for (data_id, i), group in splits.items():
        if data_id in kept and len(group) == 1 and group[0].data.related:
            continue  # already archived as the kept node itself
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


def _legacy_split(
    *,
    tree: Tree,
    chunk_size: int,
    min_zip_depth: int,
    merge_ratio: float | None = None,
) -> Tree:
    """Chunk `tree` with the original bucket packing (`--strategy legacy`).

    Reproduces the behaviour of `upload` before `--strategy` existed; see
    `_legacy_group_children` for the rule and its size guarantee.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.
        merge_ratio (float | None, optional): Merge threshold, see
            `_merge_oversized_children`. Defaults to None.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    return _split_in_place(
        tree=tree,
        chunk_size=chunk_size,
        min_zip_depth=min_zip_depth,
        group_children=_legacy_group_children,
        merge_ratio=merge_ratio,
    )


def _greedy_split(
    *,
    tree: Tree,
    chunk_size: int,
    min_zip_depth: int,
    merge_ratio: float | None = None,
) -> Tree:
    """Chunk `tree` greedily, one archive at a time (`--strategy greedy`).

    Every archive is at most `chunk_size`, except one holding a single file larger
    than `chunk_size`; see `_greedy_group_children`.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.
        merge_ratio (float | None, optional): Merge threshold, see
            `_merge_oversized_children`. Defaults to None.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    return _split_in_place(
        tree=tree,
        chunk_size=chunk_size,
        min_zip_depth=min_zip_depth,
        group_children=_greedy_group_children,
        merge_ratio=merge_ratio,
    )


def _optimal_split(
    *,
    tree: Tree,
    chunk_size: int,
    min_zip_depth: int,
    merge_ratio: float | None = None,
) -> Tree:
    """Chunk `tree` with exact per-level bin packing (`--strategy optimal`).

    Every archive is at most `chunk_size`, except one holding a single file larger
    than `chunk_size`, and each level uses the fewest archives possible; see
    `_optimal_group_children`.

    Args:
        tree (Tree): Partition subtree, modified in place.
        chunk_size (int): Target archive size (pre-compression), in bytes.
        min_zip_depth (int): Depth (root = 1) at or above which a node is
            archived even when it is smaller than `chunk_size`.
        merge_ratio (float | None, optional): Merge threshold, see
            `_merge_oversized_children`. Defaults to None.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    return _split_in_place(
        tree=tree,
        chunk_size=chunk_size,
        min_zip_depth=min_zip_depth,
        group_children=_optimal_group_children,
        merge_ratio=merge_ratio,
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
    merge_ratio: float | None = None,
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
        merge_ratio (float | None, optional): How much a folder holding a file
            larger than `chunk_size` may exceed that file and still be archived
            together with it, as a fraction of `chunk_size`. Defaults to None
            (never merge), which preserves existing archive boundaries.

    Returns:
        Tree: `tree`, with archive nodes inserted.
    """
    if strategy not in _CHUNK_STRATEGIES:
        raise ValueError(f"Unknown strategy {strategy!r}; expected one of {', '.join(_CHUNK_STRATEGIES)}.")
    return _CHUNK_STRATEGIES[strategy](
        tree=tree, chunk_size=chunk_size, min_zip_depth=min_zip_depth, merge_ratio=merge_ratio
    )
