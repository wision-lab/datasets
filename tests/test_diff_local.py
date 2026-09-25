"""Tests for the local-directory operand of `dataset_manager.commands.diff_s3`.

Scoped to the behavior a local operand adds: the virtual key a local file or archive
member gets, that an extracted copy of an archive-bearing tree compares identical, and
that `_differs` decides same-kind and cross-kind pairings the way the S3 side needs.

Fixtures build tiny archives on disk with `zipfile` using the arcnames `upload` writes
(relative to the archive's own directory), because a real prefix cannot be written here.
"""

import zipfile
from pathlib import Path

import pytest

from dataset_manager.commands.diff_s3 import (
    Fingerprint,
    _collect_local_files,
    _differs,
    _local_key,
    _member_key,
)


def write_local_tree(root: Path) -> None:
    """An archive-bearing tree: two archives plus one loose file, `upload`-style.

    `a/x/part_0.zip` holds `1.jpg`/`2.jpg` (names relative to the archive's directory,
    which is what `upload` stores), so it expands to `a/x/1.jpg` and `a/x/2.jpg`.
    """
    (root / "a" / "x").mkdir(parents=True)
    (root / "b").mkdir(parents=True)
    (root / "b" / "y.txt").write_bytes(b"other")
    with zipfile.ZipFile(root / "a" / "x" / "part_0.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("1.jpg", b"image-one")
        archive.writestr("2.jpg", b"image-two-longer")


def extract_local_tree(root: Path, extracted: Path) -> None:
    """Materialize `root`'s archives into `extracted`, the `-o$(dirname archive)` layout."""
    for archive in sorted(root.rglob("*.zip")):
        with zipfile.ZipFile(archive) as handle:
            for info in handle.infolist():
                target = extracted / archive.parent.relative_to(root) / info.filename
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(handle.read(info))
    for loose in sorted(root.rglob("*")):
        if loose.is_file() and loose.suffix != ".zip":
            target = extracted / loose.relative_to(root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(loose.read_bytes())


def collect(path: Path) -> dict[str, Fingerprint]:
    return _collect_local_files(path=path, workers=2).files


def test_local_key_is_root_relative_with_forward_slashes(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"x")
    assert _local_key(tmp_path / "sub" / "b.txt", tmp_path) == "sub/b.txt"
    assert _local_key(tmp_path / "sub" / ".." / "b.txt", tmp_path) == "b.txt"
    assert _local_key(Path("/etc/hosts"), tmp_path) is None, "a file outside the root has no key"


def test_local_archives_expand_at_their_parent_directory(tmp_path: Path) -> None:
    root = tmp_path / "src"
    write_local_tree(root)
    assert sorted(collect(root)) == ["a/x/1.jpg", "a/x/2.jpg", "b/y.txt"]


def test_extracted_copy_of_an_archive_tree_compares_identical(tmp_path: Path) -> None:
    """The case the local operand exists for: an extracted copy matches member-for-member.

    Both sides produced the same keys through different routes (archive expansion vs the
    filesystem walk), and no path differs.
    """
    root = tmp_path / "src"
    extracted = tmp_path / "extracted"
    write_local_tree(root)
    extract_local_tree(root, extracted)

    source = collect(root)
    target = collect(extracted)
    assert sorted(source) == sorted(target) == ["a/x/1.jpg", "a/x/2.jpg", "b/y.txt"]
    assert not [path for path in source if _differs(source[path], target[path])]


def test_a_change_is_detected_by_size(tmp_path: Path) -> None:
    root = tmp_path / "src"
    extracted = tmp_path / "extracted"
    write_local_tree(root)
    extract_local_tree(root, extracted)
    (extracted / "a" / "x" / "1.jpg").write_bytes(b"image-one-EXTRA")

    source = collect(root)
    target = collect(extracted)
    assert _differs(source["a/x/1.jpg"], target["a/x/1.jpg"]) is True
    assert _differs(source["a/x/2.jpg"], target["a/x/2.jpg"]) is False, "untouched members stay identical"


def test_an_ignored_local_name_never_enters_the_comparison(tmp_path: Path) -> None:
    root = tmp_path / "src"
    write_local_tree(root)
    (root / ".DS_Store").write_bytes(b"junk")
    assert ".DS_Store" not in collect(root), "`upload` never archives it, so it cannot match an object"


def test_an_empty_file_fingerprints_as_zero_bytes(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    (root / "empty.txt").write_bytes(b"")
    fingerprint = collect(root)["empty.txt"]
    assert (fingerprint.size, fingerprint.checksum) == (0, "d41d8cd98f00b204e9800998ecf8427e")


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        # Same kind: the checksums are comparable and decide.
        (Fingerprint("file", 5, "aa"), Fingerprint("file", 5, "aa"), False),
        (Fingerprint("file", 5, "aa"), Fingerprint("file", 5, "bb"), True),
        (Fingerprint("zip", 5, "1234"), Fingerprint("zip", 5, "1234"), False),
        (Fingerprint("zip", 5, "1234"), Fingerprint("zip", 5, "5678"), True),
        # A missing checksum (a tar member, a multipart ETag) compares by size alone.
        (Fingerprint("file", 5, "aa"), Fingerprint("file", 5, None), False),
        (Fingerprint("file", 5, None), Fingerprint("file", 6, "aa"), True),
        # Cross kind (an archive member against an extracted file): checksums are not
        # comparable, so size alone decides - an unchanged extracted copy stays identical.
        (Fingerprint("zip", 5, "4133081544"), Fingerprint("file", 5, "aa"), False),
        (Fingerprint("zip", 5, "4133081544"), Fingerprint("file", 6, "aa"), True),
        (Fingerprint("tar", 5, None), Fingerprint("file", 5, "aa"), False),
    ],
)
def test_differs_decides_by_size_unless_the_kind_and_checksum_are_comparable(
    a: Fingerprint, b: Fingerprint, expected: bool
) -> None:
    assert _differs(a, b) is expected


@pytest.mark.parametrize(
    ("member", "expected"),
    [
        ("1.jpg", "a/x/1.jpg"),
        ("/lead.jpg", "a/x/lead.jpg"),
        ("./dot.jpg", "a/x/dot.jpg"),
        ("sub/../2.jpg", "a/x/sub/2.jpg"),
        ("sub\\win.jpg", "a/x/sub/win.jpg"),
    ],
)
def test_member_keys_are_normalized_the_same_on_both_sides(member: str, expected: str) -> None:
    """Both collectors run a member through `_member_key`, so the keys cannot diverge.

    A leading `/` or a Windows separator would otherwise produce a key only one side
    recognizes, and every such member would be reported as one-sided.
    """
    assert _member_key("a/x", member) == expected


@pytest.mark.parametrize("member", [".", "..", "", "/", "./"])
def test_a_member_that_normalizes_to_nothing_is_skipped(member: str) -> None:
    assert _member_key("a/x", member) is None, "filing it would collide with the archive's own key"


def test_a_leading_slash_member_matches_across_both_collectors(tmp_path: Path) -> None:
    """An archive written with a leading-slash arcname still lines up with its extraction."""
    root = tmp_path / "src"
    (root / "a" / "x").mkdir(parents=True)
    with zipfile.ZipFile(root / "a" / "x" / "part_0.zip", "w") as archive:
        archive.writestr("/1.jpg", b"image-one")
    extracted = tmp_path / "extracted"
    (extracted / "a" / "x").mkdir(parents=True)
    (extracted / "a" / "x" / "1.jpg").write_bytes(b"image-one")

    source = collect(root)
    target = collect(extracted)
    assert sorted(source) == sorted(target) == ["a/x/1.jpg"]


def test_a_tar_is_expanded_with_no_checksum(tmp_path: Path) -> None:
    """A `.tar` member has no CRC32, so it carries a size and no checksum, and still expands."""
    import io
    import tarfile

    root = tmp_path / "src"
    (root / "a").mkdir(parents=True)
    with tarfile.open(root / "a" / "part.tar", "w") as archive:
        for name, data in (("1.jpg", b"image-one"), ("sub/2.jpg", b"image-two-longer")):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))

    files = collect(root)
    assert sorted(files) == ["a/1.jpg", "a/sub/2.jpg"], "members are re-rooted at the archive's directory"
    assert files["a/1.jpg"].kind == "tar"
    assert (files["a/1.jpg"].size, files["a/1.jpg"].checksum) == (9, None)


def test_a_corrupt_archive_falls_back_to_a_loose_file(tmp_path: Path) -> None:
    """A `.zip` that will not open must stay in the diff as a plain file, not vanish.

    Its bytes are still a file on disk, so it is fingerprinted like any other; dropping
    it would hide a real difference from `--fail-on-diff`.
    """
    root = tmp_path / "src"
    root.mkdir()
    (root / "broken.zip").write_bytes(b"not a zip at all")

    files = collect(root)
    assert "broken.zip" in files, "a failed archive must not disappear from the comparison"
    assert files["broken.zip"].kind == "file"


def test_a_compressed_tar_is_reported_and_kept_as_a_loose_file(tmp_path: Path) -> None:
    root = tmp_path / "src"
    root.mkdir()
    (root / "part.tar.gz").write_bytes(b"\x1f\x8b not really gzip")
    files = collect(root)
    assert files["part.tar.gz"].kind == "file"
