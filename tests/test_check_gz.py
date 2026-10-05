"""Tests for the `dm check-gz` command.

Scoped to the behaviours the command must not regress: the pair join and its
cancellation, the compression-then-verify chain, the deletion guard, generality
over `--name`, the validation rules, and the progress display (per-row sizes in
the description, the overall completion count, and immediate deletion on a
match). Fixtures are kilobytes in `tmp_path`; nothing here touches the network.
"""

from __future__ import annotations

import gzip
import os
import time
from collections.abc import Buffer
from pathlib import Path
from threading import Event
from typing import IO, Self, cast

import pytest

from dataset_manager.commands.check_gz import (
    MATCH,
    MISMATCH,
    UNREADABLE,
    Compressed,
    FileTask,
    PairState,
    _run_pool,
    check_gz,
    compress,
    compressed_path,
    find_pairs,
    hash_file,
    resolve_pair,
)
from dataset_manager.hashing import HashCancelled, md5_stream
from dataset_manager.sizes import _bytes_to_str


def write_pair(directory: Path, data: bytes = b"payload", *, name: str = "binary.npy") -> tuple[Path, Path]:
    """Write `name` and an identical `name.gz` under `directory`; return both paths."""
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / name
    source.write_bytes(data)
    gz = compressed_path(source)
    with gzip.open(gz, "wb") as handle:
        handle.write(data)
    return source, gz


def _hashed(source: Path, gz: Path) -> PairState:
    """A `PairState` whose two files are already hashed, as the pool would leave it."""
    state = PairState(
        source=FileTask(source, "source", 0, Event()),
        gz=FileTask(gz, "gz", 0, Event()),
    )
    hash_file(state.source)
    hash_file(state.gz)
    return state


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


def test_md5_stream_reports_decompressed_bytes_via_on_bytes(tmp_path: Path) -> None:
    # `on_bytes` is the *hashed* byte count: for a gz member that is decompressed.
    data = b"abcdefgh" * 5000
    path = tmp_path / "x.gz"
    with gzip.open(path, "wb") as handle:
        handle.write(data)

    with gzip.GzipFile(fileobj=path.open("rb")) as handle:
        chunks: list[int] = []
        _digest, size = md5_stream(cast("IO[bytes]", handle), block_size=64 * 1024, on_bytes=chunks.append)

    assert size == len(data)
    assert sum(chunks) == len(data)


def test_md5_stream_reports_underlying_offset_via_on_progress(tmp_path: Path) -> None:
    # The gz row must count bytes consumed from the file on disk, not decompressed
    # bytes, or its bar cannot close against the compressed file's size.
    data = b"abcdefgh" * 5000
    path = tmp_path / "x.gz"
    with gzip.open(path, "wb") as handle:
        handle.write(data)
    stored = path.stat().st_size
    assert stored < len(data), "fixture must actually compress"

    raw = path.open("rb")
    positions: list[int] = []
    with gzip.GzipFile(fileobj=raw) as handle:
        _digest, size = md5_stream(
            cast("IO[bytes]", handle), block_size=64 * 1024, on_progress=positions.append, source=raw
        )

    assert size == len(data)
    assert positions, "on_progress never fired"
    assert positions == sorted(positions), "progress must be monotonic"
    assert max(positions) <= stored, "must never report more than the file holds"
    assert max(positions) > 0


def test_md5_stream_on_progress_defaults_to_the_stream_itself(tmp_path: Path) -> None:
    # Without `source`, the object being hashed is measured directly, which is
    # what plain files rely on.
    path = tmp_path / "plain.bin"
    path.write_bytes(os.urandom(50_000))

    positions: list[int] = []
    with path.open("rb", buffering=0) as handle:
        md5_stream(handle, block_size=4096, on_progress=positions.append)

    assert positions[-1] == 50_000


def test_md5_stream_position_falls_back_to_zero(tmp_path: Path) -> None:
    # A stream that cannot report a position must not break the hash: `tell` may
    # raise on a non-seekable object, and progress must degrade, not fail.
    data = os.urandom(20_000)
    source = tmp_path / "seekless.bin"
    source.write_bytes(data)

    class NoTell:
        def __init__(self, path: Path) -> None:
            self._handle = path.open("rb", buffering=0)

        def readinto(self, buffer: Buffer) -> int:
            return self._handle.readinto(buffer)

        def tell(self) -> int:
            raise OSError("not seekable")

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *exc: object) -> None:
            self._handle.close()

    import hashlib

    seen: list[int] = []
    with NoTell(source) as handle:
        digest, size = md5_stream(handle, block_size=4096, on_progress=seen.append)  # type: ignore[arg-type]

    assert digest == hashlib.md5(data).hexdigest()
    assert size == len(data)
    assert seen and set(seen) == {0}


def test_resolve_pair_reports_match_and_mismatch(tmp_path: Path) -> None:
    """`resolve_pair` on two hashed tasks is the comparison, with no path helper in between."""
    source, gz = write_pair(tmp_path / "good", b"same")
    assert resolve_pair(_hashed(source, gz)).status is MATCH

    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "binary.npy").write_bytes(b"aaaa")
    with gzip.open(str(bad / "binary.npy.gz"), "wb") as handle:
        handle.write(b"bbbb")
    assert resolve_pair(_hashed(bad / "binary.npy", bad / "binary.npy.gz")).status is MISMATCH


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
        "root": tmp_path,
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


def test_hash_file_reports_bytes_read_from_disk_for_a_gz(tmp_path: Path) -> None:
    # The gz bar counts compressed bytes consumed; without this the row's total
    # (the stored size) could never be reached.
    data = b"abcdefgh" * 25_000
    source = tmp_path / "binary.npy"
    source.write_bytes(data)
    with gzip.open(compressed_path(source), "wb", compresslevel=9) as handle:
        handle.write(data)
    stored = compressed_path(source).stat().st_size
    assert stored < len(data)

    seen: list[int] = []
    task = hash_file(FileTask(compressed_path(source), "gz", 0, Event()), on_progress=seen.append)

    assert task.digest and task.size == len(data)
    assert max(seen) == stored, "progress must reach exactly the stored size"


def test_hash_file_reports_bytes_read_for_a_plain_file(tmp_path: Path) -> None:
    data = os.urandom(100_000)
    source = tmp_path / "binary.npy"
    source.write_bytes(data)

    seen: list[int] = []
    task = hash_file(FileTask(source, "source", 0, Event()), on_progress=seen.append)

    assert task.size == len(data)
    assert max(seen) == len(data)


def test_pool_rows_close_at_their_file_size(tmp_path: Path) -> None:
    # The regression this guards: rows used to be created with total=0 and only
    # advanced, so every bar sat at 0%. Each row must reach its own total, and its
    # description must carry the size it has handled against the file's size.
    data = b"abcdefgh" * 30_000
    source = make_pair(tmp_path, "z", data)
    stored = compressed_path(source).stat().st_size
    assert stored != len(data)

    from dataset_manager.progress import UploadProgress

    rows: dict[object, tuple[str, float, float]] = {}
    original = UploadProgress.update

    def spy(self, task_id, **kwargs):
        original(self, task_id, **kwargs)
        if task_id != self.overall_taskid:
            with self._lock:
                task = self._tasks[task_id]
                rows[task_id] = (task.description, task.total, task.completed)

    UploadProgress.update = spy  # type: ignore[method-assign]
    try:
        assert run(tmp_path) == 0
    finally:
        UploadProgress.update = original  # type: ignore[method-assign]

    assert rows, "no progress rows were observed"
    # Descriptions change as bytes flow, so key the last state by the row's name.
    final = {description.split(" (")[0]: (description, total, done) for description, total, done in rows.values()}
    source_row = final["Hashing z/binary.npy"]
    gz_row = final["Hashing z/binary.npy.gz"]
    assert source_row[1] == len(data)
    assert gz_row[1] == stored, "the gz row must total the stored size"
    assert _bytes_to_str(len(data)) in source_row[0], source_row[0]
    assert _bytes_to_str(stored) in gz_row[0], gz_row[0]
    for description, total, completed in final.values():
        assert total > 0, f"{description} had a zero total, so its bar cannot move"
        assert completed == total, f"{description} ended at {completed}/{total}"


def test_overall_row_counts_completed_pairs(tmp_path: Path) -> None:
    # The reported symptom: the overall bar showed a percentage, and it was the
    # last row to finish rather than a count of pairs. It must now read
    # `completed/total` and reach the total on the final pair.
    make_pair(tmp_path, "a", b"identical")
    make_pair(tmp_path, "b", b"identical")

    from dataset_manager.progress import UploadProgress

    descriptions: list[str] = []
    original = UploadProgress.update

    def spy(self, task_id, **kwargs):
        original(self, task_id, **kwargs)
        if task_id == self.overall_taskid:
            with self._lock:
                descriptions.append(self._tasks[task_id].description)

    UploadProgress.update = spy  # type: ignore[method-assign]
    try:
        assert run(tmp_path, workers=2) == 0
    finally:
        UploadProgress.update = original  # type: ignore[method-assign]

    assert descriptions[0].endswith("1/2 pair(s) completed"), descriptions
    assert descriptions[-1].endswith("2/2 pair(s) completed"), descriptions
    assert not any("%" in text for text in descriptions), "the overall row must not show a percentage"


def test_overall_bar_fill_tracks_pairs_not_rows(tmp_path: Path) -> None:
    # A pair costs two hashing rows, so a bar derived from row completions reaches
    # 100% after roughly half the pairs and then sits there. The fill must agree
    # with the count in its own description.
    make_pair(tmp_path, "a", b"identical")
    make_pair(tmp_path, "b", b"identical")

    from dataset_manager.progress import UploadProgress

    seen: list[tuple[float, float, str]] = []
    original = UploadProgress.update

    def spy(self, task_id, **kwargs):
        original(self, task_id, **kwargs)
        if task_id == self.overall_taskid:
            with self._lock:
                task = self._tasks[task_id]
                seen.append((task.completed, task.total, task.description))

    UploadProgress.update = spy  # type: ignore[method-assign]
    try:
        assert run(tmp_path, workers=2) == 0
    finally:
        UploadProgress.update = original  # type: ignore[method-assign]

    assert seen, "the overall row never advanced"
    for completed, total, description in seen:
        assert total == 2, f"overall total drifted to {total}"
        count = int(description.split(":")[1].split("/")[0])
        assert completed == count, f"bar at {completed}/2 but description says {description}"
    assert seen[-1][0] == 2


def test_pool_counts_unchecked_compressions(tmp_path: Path) -> None:
    # `--compress --no-check` has no comparison to report, so the entry is counted
    # when the write lands. Dropping that `advance` would leave the bar at 0/N.
    for name in ("a", "b"):
        source = tmp_path / name / "binary.npy"
        source.parent.mkdir(parents=True)
        source.write_bytes(os.urandom(10_000))

    from dataset_manager.progress import UploadProgress

    seen: list[str] = []
    original = UploadProgress.update

    def spy(self, task_id, **kwargs):
        original(self, task_id, **kwargs)
        if task_id == self.overall_taskid:
            with self._lock:
                seen.append(self._tasks[task_id].description)

    UploadProgress.update = spy  # type: ignore[method-assign]
    try:
        assert run(tmp_path, compress=True, check=False) == 0
    finally:
        UploadProgress.update = original  # type: ignore[method-assign]

    assert seen[0].endswith("1/2 pair(s) completed"), seen
    assert seen[-1].endswith("2/2 pair(s) completed"), seen


def test_pool_rows_hide_once_finished(tmp_path: Path) -> None:
    # The reported symptom: per-file bars reached 100% and stayed on screen. Each
    # row must be retired when its file lands, leaving only the overall row.
    make_pair(tmp_path, "a", b"identical")
    make_pair(tmp_path, "b", b"identical")

    from dataset_manager.progress import UploadProgress

    final: dict[object, bool] = {}
    original = UploadProgress.update

    def spy(self, task_id, **kwargs):
        original(self, task_id, **kwargs)
        with self._lock:
            final[task_id] = self._tasks[task_id].visible

    UploadProgress.update = spy  # type: ignore[method-assign]
    try:
        assert run(tmp_path, workers=2) == 0
    finally:
        UploadProgress.update = original  # type: ignore[method-assign]

    assert sum(final.values()) == 1, "only the overall row may remain visible"


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


def test_pool_counts_a_failed_compression(tmp_path: Path) -> None:
    # A compression that fails (e.g. "already exists (use --force)") still finishes
    # the entry: the run must not end reading N-1/N as though it were still busy.
    siblingless = tmp_path / "alone" / "binary.npy"
    siblingless.parent.mkdir(parents=True)
    siblingless.write_bytes(os.urandom(10_000))

    from dataset_manager.commands import check_gz as module
    from dataset_manager.progress import UploadProgress

    seen: list[str] = []
    original_update = UploadProgress.update

    def spy(self, task_id, **kwargs):
        original_update(self, task_id, **kwargs)
        if task_id == self.overall_taskid:
            with self._lock:
                seen.append(self._tasks[task_id].description)

    real = module._compress_one
    module._compress_one = lambda *args, **kwargs: False  # type: ignore[assignment]
    UploadProgress.update = spy  # type: ignore[method-assign]
    try:
        assert run(tmp_path, compress=True) == 1
    finally:
        module._compress_one = real  # type: ignore[assignment]
        UploadProgress.update = original_update  # type: ignore[method-assign]

    assert seen and seen[-1].endswith("1/1 pair(s) completed"), seen


def test_pool_deletes_a_match_before_the_run_finishes(tmp_path: Path) -> None:
    # Deletion follows the check that proved the pair identical, not the end of
    # the run. The slow pair's hashing is delayed so the fast pair is provably
    # resolved first: when its deletion lands, the slow source must still be on
    # disk. A run that batched deletions until the end would show it already gone.
    slow = tmp_path / "slow" / "binary.npy"
    slow.parent.mkdir(parents=True)
    data = os.urandom(4 * 1024 * 1024)
    slow.write_bytes(data)
    with gzip.open(compressed_path(slow), "wb") as handle:
        handle.write(data)
    make_pair(tmp_path, "fast", b"identical")

    from dataset_manager.commands import check_gz as module

    real_hash = module.hash_file

    def delayed(task, **kwargs):
        if task.path == slow:
            time.sleep(1.0)
        return real_hash(task, **kwargs)

    observed: list[tuple[bool, bool]] = []
    real = module._unlink_verified

    def spy(state, quiet: bool) -> None:
        real(state, quiet)
        # Sampled after this pair's deletion: (this source still here?, slow here?).
        observed.append((state.source.path.exists(), slow.exists()))

    module.hash_file = delayed  # type: ignore[assignment]
    module._unlink_verified = spy  # type: ignore[assignment]
    try:
        assert run(tmp_path, workers=2, keep=False) == 0
    finally:
        module.hash_file = real_hash  # type: ignore[assignment]
        module._unlink_verified = real  # type: ignore[assignment]

    assert observed, "no deletion observed"
    assert observed[0] == (False, True), "the match must be deleted while the run is still going"
    assert all(not still_here for still_here, _ in observed), "every match must be deleted"
    assert not slow.exists()


def test_unlink_verified_refuses_when_the_sibling_changed(tmp_path: Path) -> None:
    from dataset_manager.commands.check_gz import _unlink_verified

    source, gz = write_pair(tmp_path / "x", b"original")
    state = _hashed(source, gz)
    # Rewrite the sibling long after its digest was taken: the metadata fingerprint
    # no longer matches, which is the signal the guard reads.
    with gzip.open(gz, "wb") as handle:
        handle.write(b"tampered")

    _unlink_verified(state, quiet=True)

    assert source.exists()


def test_unlink_verified_refuses_when_the_source_changed(tmp_path: Path) -> None:
    from dataset_manager.commands.check_gz import _unlink_verified

    source, gz = write_pair(tmp_path / "x", b"original")
    state = _hashed(source, gz)
    source.write_bytes(b"changed after the hash")

    _unlink_verified(state, quiet=True)

    assert source.exists()


def test_unlink_verified_removes_a_still_matching_source(tmp_path: Path) -> None:
    from dataset_manager.commands.check_gz import _unlink_verified

    source, gz = write_pair(tmp_path / "x", b"original")

    _unlink_verified(_hashed(source, gz), quiet=True)

    assert not source.exists()
    assert gz.exists()


def test_unlink_verified_refuses_when_the_sibling_is_gone(tmp_path: Path) -> None:
    from dataset_manager.commands.check_gz import _unlink_verified

    source, gz = write_pair(tmp_path / "x", b"original")
    state = _hashed(source, gz)
    gz.unlink()

    _unlink_verified(state, quiet=True)

    assert source.exists()


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
