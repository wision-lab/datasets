"""Tests for the `dm check-gz` command.

Scoped to the behaviours the command must not regress: the pair join and its
cancellation, the compression-then-verify chain, the deletion guard, generality
over `--name`, and the validation rules. Fixtures are kilobytes in `tmp_path`;
nothing here touches the network.
"""

from __future__ import annotations

import gzip
import os
from pathlib import Path
from threading import Event

import pytest

from dataset_manager.commands.check_gz import (
    MATCH,
    MISMATCH,
    UNREADABLE,
    Compressed,
    FileTask,
    PairState,
    Result,
    _compare_paths,
    _run_pool,
    check_gz,
    compress,
    compressed_path,
    find_pairs,
    hash_file,
    resolve_pair,
)
from dataset_manager.hashing import HashCancelled, md5_stream


def write_pair(directory: Path, data: bytes = b"payload", *, name: str = "binary.npy") -> tuple[Path, Path]:
    """Write `name` and an identical `name.gz` under `directory`; return both paths."""
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / name
    source.write_bytes(data)
    gz = compressed_path(source)
    with gzip.open(gz, "wb") as handle:
        handle.write(data)
    return source, gz


# --- hashing and comparison ---------------------------------------------------


def test_md5_stream_matches_hashlib_for_whole_file(tmp_path: Path) -> None:
    import hashlib

    data = os.urandom(300_000)
    path = tmp_path / "blob.bin"
    path.write_bytes(data)

    with path.open("rb", buffering=0) as handle:
        digest, size = md5_stream(handle, block_size=64 * 1024)

    assert (digest, size) == (hashlib.md5(data).hexdigest(), len(data))


def test_md5_stream_hashes_an_empty_stream(tmp_path: Path) -> None:
    import hashlib

    path = tmp_path / "empty.bin"
    path.write_bytes(b"")

    with path.open("rb", buffering=0) as handle:
        assert md5_stream(handle) == (hashlib.md5(b"").hexdigest(), 0)


def test_md5_stream_cancel_raises_without_a_digest(tmp_path: Path) -> None:
    # The original script could leave a truncated digest behind when it stopped
    # early; the shared helper must refuse to produce one.
    path = tmp_path / "big.bin"
    path.write_bytes(os.urandom(4096))

    with path.open("rb", buffering=0) as handle, pytest.raises(HashCancelled):
        md5_stream(handle, block_size=512, cancelled=lambda: True)


def test_compare_reports_match_and_mismatch(tmp_path: Path) -> None:
    source, gz = write_pair(tmp_path / "good", b"same")
    assert _compare_paths(source, gz).status is MATCH

    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "binary.npy").write_bytes(b"aaaa")
    with gzip.open(str(bad / "binary.npy.gz"), "wb") as handle:
        handle.write(b"bbbb")
    assert _compare_paths(bad / "binary.npy", bad / "binary.npy.gz").status is MISMATCH


def test_resolve_pair_names_both_sizes_on_a_truncated_member(tmp_path: Path) -> None:
    data = os.urandom(50_000)
    source = tmp_path / "binary.npy"
    source.write_bytes(data)
    raw = gzip.compress(data)
    gz = compressed_path(source)
    gz.write_bytes(raw[: len(raw) // 2])

    result = resolve_pair(
        PairState(hash_file(FileTask(source, "source", 0, Event())), hash_file(FileTask(gz, "gz", 0, Event())))
    )

    assert result.status is UNREADABLE
    assert "EOFError" in result.detail


# --- generality over --name ---------------------------------------------------


def test_compressed_path_appends_the_suffix(tmp_path: Path) -> None:
    assert compressed_path(Path("a/b/x")) == Path("a/b/x.gz")
    assert compressed_path(Path("a/b/x.npy")) == Path("a/b/x.npy.gz")


def test_find_pairs_accepts_any_file_name(tmp_path: Path) -> None:
    with_pair = tmp_path / "with"
    without = tmp_path / "without"
    write_pair(with_pair, b"data", name="clip.mp4")
    without.mkdir()
    (without / "clip.mp4").write_bytes(b"data")

    entries = {entry.source.parent.name: entry for entry in find_pairs(tmp_path, "clip.mp4")}

    assert entries["with"].gz == compressed_path(with_pair / "clip.mp4")
    assert entries["without"].gz is None


def test_find_pairs_never_mentions_another_name(tmp_path: Path) -> None:
    write_pair(tmp_path / "a", b"data", name="clip.mp4")
    write_pair(tmp_path / "b", b"data", name="binary.npy")

    assert [entry.source.name for entry in find_pairs(tmp_path, "clip.mp4")] == ["clip.mp4"]


def test_check_gz_scans_only_the_named_file(tmp_path: Path) -> None:
    write_pair(tmp_path / "a", b"data", name="clip.mp4")
    write_pair(tmp_path / "b", b"data", name="binary.npy")

    # Exits cleanly and only ever considered the mp4 pair.
    assert check_gz(tmp_path, name="clip.mp4", quiet=True) is None


# --- compression --------------------------------------------------------------


def test_compress_writes_beside_the_source_and_preserves_mtime(tmp_path: Path) -> None:
    source = tmp_path / "x"
    source.write_bytes(b"hello world" * 100)
    os.utime(source, (1_000_000, 1_000_000))

    result = compress(source)

    assert result.ok
    assert result.gz == compressed_path(source)
    assert result.gz.stat().st_mtime == pytest.approx(1_000_000, abs=2)
    with gzip.open(result.gz, "rb") as handle:
        assert handle.read() == source.read_bytes()


def test_compress_skips_an_existing_target_unless_forced(tmp_path: Path) -> None:
    source = tmp_path / "x"
    source.write_bytes(b"payload")
    existing = compressed_path(source)
    existing.write_bytes(b"stale")

    skipped = compress(source)
    assert not skipped.ok
    assert "already exists" in skipped.error
    assert existing.read_bytes() == b"stale"

    forced = compress(source, force=True)
    assert forced.ok
    with gzip.open(existing, "rb") as handle:
        assert handle.read() == b"payload"


# --- the pool: pairing, cancellation, deletion --------------------------------


def make_pair(tmp_path: Path, name: str, data: bytes, *, gz: bytes | None = None) -> Path:
    """Write `name` plus its `.gz` sibling (`gz` bytes, or `data` when None)."""
    source = tmp_path / name / "binary.npy"
    source.parent.mkdir(parents=True)
    source.write_bytes(data)
    with gzip.open(compressed_path(source), "wb") as handle:
        handle.write(data if gz is None else gz)
    return source


def run(tmp_path: Path, **kwargs) -> int:
    """Run the pool over every `binary.npy` under `tmp_path`, with test defaults."""
    scanned = list(find_pairs(tmp_path, kwargs.pop("name", "binary.npy")))
    assert scanned, "fixture produced no entries"
    defaults = {
        "workers": 1,
        "level": 9,
        "force": False,
        "keep": True,
        "dry_run": False,
        "check": True,
        "compress": False,
        "quiet": True,
    }
    defaults.update(kwargs)
    return _run_pool(scanned, **defaults)  # type: ignore[arg-type]


def test_pool_pairs_and_unlinks_only_the_match(tmp_path: Path) -> None:
    good = make_pair(tmp_path, "good", b"identical")
    make_pair(tmp_path, "bad", b"original", gz=b"different")

    rc = run(tmp_path, keep=False)

    assert rc == 1
    assert not good.exists(), "a proven match must be deleted with --no-keep"
    assert (tmp_path / "bad" / "binary.npy").exists(), "a mismatch must never be deleted"


def test_pool_keeps_sources_by_default(tmp_path: Path) -> None:
    source = make_pair(tmp_path, "good", b"identical")

    assert run(tmp_path) == 0
    assert source.exists()


def test_hash_file_stops_when_the_pair_was_cancelled(tmp_path: Path) -> None:
    # The early-stop contract: a task whose pair's event is already set must not
    # read the file at all, and must not leave a truncated digest behind.
    data = os.urandom(4 * 1024 * 1024)
    source = tmp_path / "binary.npy"
    source.write_bytes(data)
    cancel = Event()
    cancel.set()
    task = FileTask(source, "source", 0, cancel)

    result = hash_file(task)

    assert result.error == "stopped early: the other file of the pair failed"
    assert result.digest == ""
    assert result.size == 0


def test_resolve_pair_reports_cancelled_when_the_peer_stopped_early(tmp_path: Path) -> None:
    source = tmp_path / "binary.npy"
    source.write_bytes(b"payload")
    source_task = FileTask(source, "source", 0, Event(), error="stopped early: the other file of the pair failed")
    gz_task = FileTask(compressed_path(source), "gz", 0, Event(), error="EOFError: broken")

    assert resolve_pair(PairState(source_task, gz_task)).status is UNREADABLE


def test_pool_early_stops_the_sibling_of_a_failed_hash(tmp_path: Path) -> None:
    # A large source cannot finish in one block, so it is still reading when the
    # truncated `.gz` fails and sets the shared cancel event.
    data = os.urandom(4 * 1024 * 1024)
    source = tmp_path / "pair" / "binary.npy"
    source.parent.mkdir(parents=True)
    source.write_bytes(data)
    raw = gzip.compress(data)
    compressed_path(source).write_bytes(raw[: len(raw) // 2])

    assert run(tmp_path, workers=2) == 1


def test_pool_compresses_then_verifies_a_siblingless_file(tmp_path: Path) -> None:
    siblingless = tmp_path / "alone" / "binary.npy"
    siblingless.parent.mkdir(parents=True)
    siblingless.write_bytes(os.urandom(20_000))

    rc = run(tmp_path, compress=True, keep=False)

    assert rc == 0
    assert not siblingless.exists()
    assert compressed_path(siblingless).is_file()


def test_pool_reports_a_corrupt_compression(tmp_path: Path) -> None:
    siblingless = tmp_path / "alone" / "binary.npy"
    siblingless.parent.mkdir(parents=True)
    siblingless.write_bytes(os.urandom(20_000))

    from dataset_manager.commands import check_gz as module

    real = module.compress

    def corrupt(source: Path, **kwargs) -> Compressed:
        result = real(source, **kwargs)
        # Overwrite with different bytes before the verification hash reads it.
        with gzip.open(result.gz, "wb") as handle:
            handle.write(b"corrupt")
        return result

    module.compress = corrupt  # type: ignore[assignment]
    try:
        rc = run(tmp_path, compress=True)
    finally:
        module.compress = real  # type: ignore[assignment]

    assert rc == 1, "a verification mismatch must fail the run"


def test_unlink_verified_refuses_when_the_sibling_changed(tmp_path: Path) -> None:
    from dataset_manager.commands.check_gz import _unlink_verified

    source, gz = write_pair(tmp_path / "x", b"original")
    with gzip.open(gz, "wb") as handle:
        handle.write(b"tampered")

    _unlink_verified(Result(source=source, gz=gz, status=MATCH), quiet=True)

    assert source.exists()


def test_unlink_verified_removes_a_still_matching_source(tmp_path: Path) -> None:
    from dataset_manager.commands.check_gz import _unlink_verified

    source, gz = write_pair(tmp_path / "x", b"original")

    _unlink_verified(Result(source=source, gz=gz, status=MATCH), quiet=True)

    assert not source.exists()
    assert gz.exists()


# --- validation ---------------------------------------------------------------


def test_validation_rejects_unsafe_flag_combinations(tmp_path: Path) -> None:
    write_pair(tmp_path, b"data")

    with pytest.raises(ValueError, match="cannot be disabled together"):
        check_gz(tmp_path, keep=False, check=False)
    with pytest.raises(ValueError, match="nothing else to do"):
        check_gz(tmp_path, check=False)
    with pytest.raises(ValueError, match="at least 1"):
        check_gz(tmp_path, workers=0)
    with pytest.raises(ValueError, match="at least 1"):
        check_gz(tmp_path, workers=-3)
    with pytest.raises(ValueError, match="must not end in"):
        check_gz(tmp_path, name="x.gz")
    with pytest.raises(ValueError, match="Not a directory"):
        check_gz(tmp_path / "nope")
