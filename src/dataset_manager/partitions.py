from __future__ import annotations

import re
from fnmatch import translate
from functools import reduce
from pathlib import Path
from typing import cast

from nutree import Tree
from nutree.node import Node

from .tree import populate_filesize


def _compile_patterns(
    patterns: dict[str, str | None],
) -> dict[str, re.Pattern | None]:
    """Pre-compile fnmatch patterns to regex for faster matching.

    Converts glob patterns to regex using fnmatch.translate() and compiles them.
    None patterns remain None (they match the default group).
    """
    compiled: dict[str, re.Pattern | None] = {}
    for name, pattern in patterns.items():
        if pattern is None:
            compiled[name] = None
        else:
            compiled[name] = re.compile(translate(pattern))
    return compiled


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
    # Work on a copy so adding/renaming the default group below does not mutate
    # the caller's dictionary.
    patterns = dict(patterns)

    # Ensure there's a default group
    reverse_patterns = {v: k for k, v in patterns.items()}

    if len(patterns) != len(reverse_patterns):
        raise RuntimeError("Multiple partitions have the same pattern!")

    if None not in reverse_patterns:
        patterns[default_groupname] = None
    else:
        default_groupname = reverse_patterns[None]

    # Pre-compile patterns to regex for faster matching
    compiled_patterns = _compile_patterns(patterns)

    # Single pass: classify each leaf node into its matching partition
    all_leafs = {n.data.path: n for n in tree.find_all(match=lambda n: n.is_leaf())}
    matched_leafs: dict[str, dict[Path, Node]] = {name: {} for name in patterns}

    for path, node in all_leafs.items():
        matched = False
        for pattern_name, pattern_expr in compiled_patterns.items():
            if pattern_name == default_groupname:
                continue
            if pattern_expr is not None and pattern_expr.match(str(path)):
                matched_leafs[pattern_name][path] = node
                matched = True
                break
        if not matched:
            matched_leafs[default_groupname][path] = node

    # Ensure no leaf node overlap and that we didn't miss any nodes
    all_matched_leafs = reduce(set.union, (set(v) for v in matched_leafs.values()))
    assert all_matched_leafs == set(all_leafs)

    # Filter tree into all subtrees, deepcopy all and filter out empty trees
    subtrees: dict[str, Tree] = {}
    for pattern_name, leafs in matched_leafs.items():

        def is_matched(node: Node, leafs: dict[Path, Node] = leafs) -> bool:
            return node.data.path in leafs

        subtrees[pattern_name] = tree.filtered(is_matched)
    subtrees = {
        pattern_name: cast(Tree, populate_filesize(node=st.deepcopy(), refresh=True))
        for pattern_name, st in subtrees.items()
        if st.count
    }
    for pattern_name, st in subtrees.items():
        st.name = pattern_name.title()
    return subtrees
