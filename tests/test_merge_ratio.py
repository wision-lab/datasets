"""Tests for the `merge_ratio` oversized-folder merge in `dataset_manager.chunking`.

This is the only test suite in the repository. It is scoped to the merge mechanic:
which archives the merge produces, that it is off by default, the boundaries where
the rule must not fire, and that `PathData.related` survives serialization.

Trees are built in memory with small byte counts (`CHUNK` = 100 bytes), because
`split_into_chunks` uses `PathData.size` and never touches the filesystem.
"""

from pathlib import Path

import pytest
from nutree import Tree
from nutree.node import Node

from dataset_manager.chunking import ChunkStrategy, split_into_chunks
from dataset_manager.commands.upload import _DEFAULT_MERGE_RATIO, upload
from dataset_manager.tree import PathData

CHUNK = 100
STRATEGIES: tuple[ChunkStrategy, ...] = ("legacy", "greedy", "optimal")

# 0.2 * 100 = 20 bytes of budget for a folder's supporting files, while `cam`'s
# supporting files total 12 bytes (a 110-byte file is what makes it oversized).
RATIO = 0.2


def add(parent: Node, name: str, size: int | None = None) -> Node:
    """Add a child to `parent`; `size=None` makes it a directory."""
    path = parent.data.path / name
    return parent.add(PathData(path=path, is_dir=size is None, size=size))


def child(node: Node | Tree) -> Node:
    """First child of `node` or `tree`, for building fixtures whose shape is known."""
    first = node.first_child()
    assert first is not None
    return first


def build(root: Path, children: dict[str, int | None]) -> Tree:
    """A tree of one root holding `children` (name to size, None for a directory)."""
    tree = Tree("fixture")
    root_node = tree.add(PathData(path=root, is_dir=True, size=0))
    for name, size in children.items():
        add(root_node, name, size)
    root_node.data.size = sum(child.data.size or 0 for child in root_node.children)
    return tree


@pytest.fixture
def oversized_folder(tmp_path: Path) -> Tree:
    """A folder holding one file over the target plus small supporting files."""
    return make_oversized_folder(tmp_path)


def make_oversized_folder(root: Path) -> Tree:
    tree = build(root, {"cam": None, "train": 100})
    root_node = child(tree)
    cam = child(root_node)
    add(cam, "binary.npy", 110)  # over CHUNK, so it cannot share its folder
    add(cam, "video.mov", 11)
    add(cam, "arguments.json", 1)
    cam.data.size = 122
    root_node.data.size = 222
    return tree


@pytest.fixture
def two_oversized_folder(tmp_path: Path) -> Tree:
    """A folder holding two files over the target plus one supporting file."""
    return make_two_oversized_folder(tmp_path)


def make_two_oversized_folder(root: Path) -> Tree:
    tree = build(root, {"cam": None})
    root_node = child(tree)
    cam = child(root_node)
    add(cam, "binary.npy", 110)
    add(cam, "v3.npy", 110)
    add(cam, "video.mov", 10)
    cam.data.size = 230
    root_node.data.size = 230
    return tree


def zip_nodes(tree: Tree) -> list[Node]:
    return list(tree.find_all(match=lambda node: node.data.is_zip))


def zip_entries(tree: Tree) -> list[tuple[str, bool, int]]:
    """(name, is_dir, size) of every archive, sorted; `is_dir` marks a kept folder."""
    return sorted((n.data.path.name, bool(n.data.is_dir), n.data.size or 0) for n in zip_nodes(tree))


def leaf_names(tree: Tree) -> list[str]:
    return sorted(n.data.path.name for n in tree.find_all(match=lambda node: node.is_leaf()))


def archive_of(tree: Tree, name: str) -> Node:
    """The single archive enclosing the leaf called `name`."""
    target = next(n for n in tree.find_all(match=lambda node: node.is_leaf() and node.data.path.name == name))
    for ancestor in target.parent_iterator():
        if ancestor.data.is_zip:
            return ancestor
    raise AssertionError(f"{name} is not inside any archive")


def archive_names(tree: Tree) -> list[str]:
    return [n.data.path.name for n in zip_nodes(tree)]


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_merge_keeps_the_folder_as_archive_root(oversized_folder: Tree, strategy: ChunkStrategy) -> None:
    tree = split_into_chunks(tree=oversized_folder, chunk_size=CHUNK, strategy=strategy, merge_ratio=RATIO)

    assert ("cam", True, 122) in zip_entries(tree)
    assert not any(name.startswith("cam_") for name in archive_names(tree)), "no extra nesting level"
    assert sorted(c.data.path.name for c in archive_of(tree, "binary.npy").children) == [
        "arguments.json",
        "binary.npy",
        "video.mov",
    ]


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_merge_is_disabled_by_default(tmp_path: Path, strategy: ChunkStrategy) -> None:
    merged = split_into_chunks(
        tree=make_oversized_folder(tmp_path), chunk_size=CHUNK, strategy=strategy, merge_ratio=RATIO
    )
    plain = split_into_chunks(tree=make_oversized_folder(tmp_path), chunk_size=CHUNK, strategy=strategy)
    explicit_zero = split_into_chunks(
        tree=make_oversized_folder(tmp_path), chunk_size=CHUNK, strategy=strategy, merge_ratio=0.0
    )

    assert _DEFAULT_MERGE_RATIO == 0.0
    assert zip_entries(plain) == zip_entries(explicit_zero), "omitting the option equals passing 0"
    assert zip_entries(plain) != zip_entries(merged), "0.2 must change this fixture"
    assert ("cam", True, 122) not in zip_entries(plain)
    assert not any(n.data.is_dir for n in zip_nodes(plain)), "folders are only ever archived by a merge"


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_merge_budget_covers_the_supporting_files(tmp_path: Path, strategy: ChunkStrategy) -> None:
    """The budget is the folder's supporting files, not the big file's overshoot."""
    tight = split_into_chunks(tree=make_oversized_folder(tmp_path), chunk_size=CHUNK, strategy=strategy, merge_ratio=0.1)
    loose = split_into_chunks(
        tree=make_oversized_folder(tmp_path), chunk_size=CHUNK, strategy=strategy, merge_ratio=0.12
    )

    assert ("cam", True, 122) not in zip_entries(tight), "10 bytes of budget is too little for 12 bytes"
    assert ("cam", True, 122) in zip_entries(loose), "12 bytes of budget covers 12 bytes"


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_folder_with_many_files_of_its_own_is_not_merged(tmp_path: Path, strategy: ChunkStrategy) -> None:
    tree = build(tmp_path, {"many": None})
    root_node = child(tree)
    many = child(root_node)
    add(many, "big.npy", 110)
    for i in range(4):
        add(many, f"part{i}.npy", 25)
    many.data.size = 210
    root_node.data.size = 210

    split = split_into_chunks(tree=tree, chunk_size=CHUNK, strategy=strategy, merge_ratio=RATIO)

    assert not any(n.data.is_dir for n in zip_nodes(split)), "100 bytes of siblings must not collapse into one archive"


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_two_oversized_files_share_supporting_files_singly(two_oversized_folder: Tree, strategy: ChunkStrategy) -> None:
    """One supporting file rides along with one oversized file, never with both."""
    tree = split_into_chunks(tree=two_oversized_folder, chunk_size=CHUNK, strategy=strategy, merge_ratio=RATIO)

    assert len(zip_nodes(tree)) == 2, "one archive per oversized file"
    contents = sorted(sorted(c.data.path.name for c in z.children) for z in zip_nodes(tree))
    assert contents == [["binary.npy", "video.mov"], ["v3.npy"]] or contents == [
        ["binary.npy"],
        ["v3.npy", "video.mov"],
    ], "the supporting file rides along with exactly one big file"
    assert leaf_names(tree) == ["binary.npy", "v3.npy", "video.mov"]


@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize("ratio", [0.0, RATIO, 5.0])
def test_every_file_lands_in_exactly_one_archive(
    oversized_folder: Tree, two_oversized_folder: Tree, strategy: ChunkStrategy, ratio: float
) -> None:
    """No file is dropped or duplicated, and no archive is left empty."""
    for fixture_tree in (oversized_folder, two_oversized_folder):
        tree = split_into_chunks(tree=fixture_tree, chunk_size=CHUNK, strategy=strategy, merge_ratio=ratio)
        assert leaf_names(tree) == leaf_names(fixture_tree), "a file was lost or duplicated"
        for node in tree.find_all(match=lambda candidate: candidate.is_leaf()):
            assert archive_of(tree, node.data.path.name) is not None
        assert all((n.data.size or 0) > 0 for n in zip_nodes(tree)), "no archive may be empty"


def test_merge_archives_only_the_folder_tree(oversized_folder: Tree) -> None:
    """The kept folder archives its own children, reproducing the original layout."""
    tree = split_into_chunks(tree=oversized_folder, chunk_size=CHUNK, strategy="greedy", merge_ratio=RATIO)
    cam = next(n for n in zip_nodes(tree) if n.data.path.name == "cam")

    assert sorted(c.data.path.name for c in cam.children) == ["arguments.json", "binary.npy", "video.mov"]
    assert cam.data.path == child(child(oversized_folder)).data.path


def test_related_field_round_trips(oversized_folder: Tree, tmp_path: Path) -> None:
    merged = split_into_chunks(tree=oversized_folder, chunk_size=CHUNK, strategy="greedy", merge_ratio=RATIO)
    assert any(n.data.related for n in merged.find_all(match=lambda node: True))

    path = tmp_path / "tree.json"
    merged.save(str(path), mapper=PathData.serialize_mapper, compression=True)
    reloaded = Tree.load(str(path), mapper=PathData.deserialize_mapper, file_meta={})
    assert [(str(n.data.path), n.data.related) for n in reloaded.find_all(match=lambda node: True)] == [
        (str(n.data.path), n.data.related) for n in merged.find_all(match=lambda node: True)
    ]


def test_old_tree_format_still_loads(tmp_path: Path) -> None:
    """A 5-field row (no `related`, no `zip_size`) deserializes with related=False."""
    tree = Tree("old")
    tree.add(PathData(path=tmp_path / "file.npy", is_dir=False, size=7))
    path = str(tmp_path / "old.json")
    tree.save(
        str(path),
        mapper=lambda node, data: {
            **data,
            "data": (
                str(node.data.path),
                node.data.is_dir,
                node.data.is_zip,
                node.data.has_zip_descendants,
                node.data.size,
            ),
        },
        compression=False,
    )

    loaded = Tree.load(path, mapper=PathData.deserialize_mapper, file_meta={})
    assert [(str(n.data.path), n.data.related, n.data.zip_size) for n in loaded.find_all(match=lambda node: True)] == [
        (str(tmp_path / "file.npy"), False, None)
    ]


def test_upload_rejects_a_negative_merge_ratio(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="merge_ratio"):
        upload(tmp_path, merge_ratio=-0.1)
