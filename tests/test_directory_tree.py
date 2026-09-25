"""Tests for the empty-directory prune in `dataset_manager.tree.directory_tree`."""

from pathlib import Path

import pytest

from dataset_manager.commands.upload import upload
from dataset_manager.tree import directory_tree


def leaf_names(tree) -> list[str]:
    """Root-relative names of every leaf, in tree order."""
    return [str(node.data.path.name) for node in tree.find_all(match=lambda n: n.is_leaf())]


def test_empty_directories_are_pruned_before_chunking(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "frame.npy").write_bytes(b"frame")
    (tmp_path / "a" / "empty").mkdir()
    (tmp_path / "b" / "empty" / "deeper").mkdir(parents=True)
    (tmp_path / "c").mkdir()
    (tmp_path / "c" / "only_empty").mkdir()

    tree = directory_tree(tmp_path)

    assert leaf_names(tree) == ["frame.npy"]
    assert not any(node.data.is_dir for node in tree.find_all(match=lambda n: n.is_leaf())), (
        "a directory leaf would be opened as a file and fail the chunk"
    )


def test_a_directory_emptied_by_the_filter_is_pruned(tmp_path: Path) -> None:
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "run.log").write_bytes(b"log")
    (tmp_path / "keep.txt").write_bytes(b"x")

    tree = directory_tree(tmp_path, filter_fn=lambda p: p.suffix != ".log")

    assert leaf_names(tree) == ["keep.txt"]


def test_a_tree_without_files_has_no_file_node(tmp_path: Path) -> None:
    (tmp_path / "only" / "empty").mkdir(parents=True)

    tree = directory_tree(tmp_path)

    assert not any(not node.data.is_dir for node in tree)


def test_upload_rejects_a_directory_without_files(tmp_path: Path) -> None:
    (tmp_path / "only" / "empty").mkdir(parents=True)

    with pytest.raises(ValueError, match="contains no files"):
        upload(tmp_path)
