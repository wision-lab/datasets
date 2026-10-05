"""Verify that each dataset file has a byte-identical `.gz` sibling.

One worker thread hashes one file, so `--workers N` starts exactly `N` threads and
never `2N`. Each in-flight file gets its own progress row; a pair is compared (and,
with `--no-keep`, its source unlinked) the moment both of its hashes land, and a
failed hash cancels its sibling instead of letting it finish.
"""

from __future__ import annotations

import gzip
import os
from collections import defaultdict
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event
from typing import IO, Literal, cast

from rich.console import Console
from rich.status import Status as Spinner

from ..app import app
from ..hashing import HashCancelled, md5_stream
from ..log import log
from ..progress import UpdateFn, UploadProgress
from ..sizes import _bytes_to_str

GZIP_SUFFIX = ".gz"

# Read size for compression input only. Hashing does not use it: `md5_stream` reads
# into a fixed block, and `GzipFile` re-blocks raw reads regardless.
_COMPRESS_CHUNK = 1 << 20


def compressed_path(source: Path) -> Path:
    """The compressed sibling the command looks for and writes.

    The only definition of the `<source>.gz` convention: `--name` selects the
    source file name, and every caller derives the counterpart through this
    function rather than by concatenating the suffix.
    """
    return source.with_name(source.name + GZIP_SUFFIX)


Status = Literal["MATCH", "MISMATCH", "UNREADABLE", "CANCELLED"]
MATCH: Status = "MATCH"
MISMATCH: Status = "MISMATCH"
UNREADABLE: Status = "UNREADABLE"
CANCELLED: Status = "CANCELLED"

Role = Literal["source", "gz"]


@dataclass(slots=True)
class FileTask:
    """One file to hash: the unit of work and of progress-bar display.

    A pair contributes two of these (source, compressed); a file with no sibling
    contributes one, plus a later compression job. `pair` links the two tasks of
    a pair so the join can find each other, and `cancel` is the shared abort flag
    both of them poll. `expected` is the raw on-disk size, measured at construction
    so the bar has a total before hashing starts; `size` is what hashing actually
    read, which the comparison uses and which differs from `expected` for a `gz`.
    """

    path: Path
    role: Role
    pair: int
    cancel: Event
    expected: int = 0
    digest: str = ""
    size: int = 0
    error: str = ""
    task_id: object | None = None  # rich TaskID, set once the row exists


@dataclass(slots=True)
class PairState:
    """Join state for one pair: both files' tasks and whether it is resolved."""

    source: FileTask
    gz: FileTask
    done: bool = False
    counted: bool = False


@dataclass(slots=True)
class Outcome:
    """Running totals for one `_run_pool` call, contributed to by its helpers.

    A mutable object rather than bare locals the join and reporting helpers would
    have to rebind through `nonlocal`: this is the single place the run's verdict
    accumulates, and `bad` is what the exit code is derived from.
    """

    counts: dict[Status, int] = field(default_factory=lambda: defaultdict(int))
    bad: bool = False
    settled: int = 0


@dataclass(frozen=True, slots=True)
class Entry:
    """One file found by the walk. `gz` is None when there is no compressed sibling."""

    source: Path
    size: int
    gz: Path | None = None


@dataclass(frozen=True, slots=True)
class Result:
    """Outcome for one resolved pair, or for one compression."""

    source: Path
    gz: Path
    status: Status
    detail: str = ""
    source_bytes: int = 0
    gz_bytes: int = 0
    source_md5: str = ""
    gz_md5: str = ""


@dataclass(frozen=True, slots=True)
class Compressed:
    """Outcome of one `compress` call."""

    source: Path
    gz: Path
    src_bytes: int
    gz_bytes: int
    error: str = ""

    @property
    def ok(self) -> bool:
        return not self.error


def _open(path: Path) -> tuple[IO[bytes], IO[bytes] | None]:
    """Open a plain file or a gzip stream; also return the underlying raw file.

    `GzipFile` re-blocks reads internally and wraps the raw file in its own
    `BufferedReader`, so neither the buffer size passed here nor the size of a
    read on the decompressed side changes how many syscalls reach the filesystem:
    measured at ~101k raw reads for an 11 GiB member at chunk sizes from 64 KiB to
    64 MiB. There is no read-size tuning knob here worth having.

    The second element is the raw file under a `GzipFile`, whose offset measures
    the compressed bytes consumed, so a `gz` row can report "bytes read / bytes in
    the file" against the stored size. It is None for a plain file, where the
    stream's own offset already is that number.
    """
    if path.suffix == GZIP_SUFFIX:
        raw = path.open("rb")
        return cast("IO[bytes]", gzip.GzipFile(fileobj=raw)), raw
    return path.open("rb", buffering=0), None


def hash_file(
    task: FileTask,
    *,
    on_progress: Callable[[int], None] | None = None,
) -> FileTask:
    """Hash one file, honouring its pair's cancel flag. Runs in a worker thread.

    Returns the task with `digest`/`size` filled, or `error` set. `HashCancelled`
    is recorded as an error with a fixed message so the pair join can tell "the
    peer failed" apart from "this file is wrong".

    `on_progress` receives the bytes read from the file on disk (compressed bytes
    for a `.gz`), which is the only reading that closes against the file's size.
    """
    fileobj: IO[bytes] | None = None
    raw: IO[bytes] | None = None
    try:
        fileobj, raw = _open(task.path)
        with fileobj:
            task.digest, task.size = md5_stream(
                fileobj,
                cancelled=task.cancel.is_set,
                on_progress=on_progress,
                source=raw,
            )
    except HashCancelled:
        task.error = "stopped early: the other file of the pair failed"
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        task.error = f"{type(exc).__name__}: {exc}"
    finally:
        # A `GzipFile` does not close the file it was handed, so close it here.
        if raw is not None and not raw.closed:
            raw.close()
    return task


def resolve_pair(state: PairState) -> Result:
    """Turn two finished file tasks into one result.

    A pair is only compared once both hashes are in. If either failed, the other
    is reported as CANCELLED rather than compared: there is nothing to compare a
    missing digest against, and saying so is clearer than reporting the surviving
    file as fine.
    """
    source, gz = state.source, state.gz
    if source.error or gz.error:
        who = source if source.error else gz
        other = gz if source.error else source
        status = CANCELLED if who.error.startswith("stopped early") else UNREADABLE
        if other.error and other is not who:
            status = UNREADABLE
        return Result(source.path, gz.path, status, detail=f"{who.path.name}: {who.error}")
    if source.digest != gz.digest or source.size != gz.size:
        why = (
            f"raw sizes {source.size} vs {gz.size}"
            if source.digest == gz.digest
            else f"source={source.digest} gz={gz.digest} (raw sizes {source.size} vs {gz.size})"
        )
        return Result(
            source.path,
            gz.path,
            MISMATCH,
            detail=why,
            source_bytes=source.size,
            gz_bytes=gz.size,
            source_md5=source.digest,
            gz_md5=gz.digest,
        )
    return Result(
        source.path,
        gz.path,
        MATCH,
        source_bytes=source.size,
        gz_bytes=gz.size,
        source_md5=source.digest,
        gz_md5=gz.digest,
    )


def compress(
    source: Path, level: int = 9, force: bool = False, on_progress: Callable[[int], None] | None = None
) -> Compressed:
    """Write `<source>.gz` the way `gzip -<level> -k` would: original kept, FNAME
    recorded, source mtime preserved.

    Not byte-identical to GNU gzip: CPython's `zlib` here links `zlib-ng`, whose
    deflate stream differs from the one `gzip` 1.14 emits at the same level (same
    CRC32, same ISIZE, same decompressed bytes, different compressed bytes). Any
    tool that re-reads the result sees identical content.

    Written to a temporary sibling and renamed, so an interrupted run never leaves
    a partial `.gz` that a later pass would treat as authoritative.

    `on_progress` receives the source bytes written so far after each block, so a
    bar can show the run against the file's size without tracking the delta itself.
    """
    gz = compressed_path(source)
    if gz.exists() and not force:
        return Compressed(source, gz, 0, gz.stat().st_size, error="already exists (use --force)")
    tmp = gz.with_name(gz.name + ".tmp")
    try:
        src = source.stat()
        written = 0
        with (
            source.open("rb") as fin,
            tmp.open("wb") as fout,
            gzip.GzipFile(
                filename=source.name,
                mode="wb",
                compresslevel=level,
                fileobj=fout,
                mtime=int(src.st_mtime),
            ) as gzfile,
        ):
            while chunk := fin.read(_COMPRESS_CHUNK):
                gzfile.write(chunk)
                written += len(chunk)
                if on_progress is not None:
                    on_progress(written)
        tmp.replace(gz)
        # Match gzip -k, which leaves the .gz carrying the source file's timestamp.
        os.utime(gz, (src.st_atime, src.st_mtime))
    except OSError as exc:
        tmp.unlink(missing_ok=True)
        return Compressed(source, gz, 0, 0, error=f"{type(exc).__name__}: {exc}")
    return Compressed(source, gz, written, gz.stat().st_size)


def _compare_paths(source: Path, gz: Path) -> Result:
    """Hash both files in the calling thread and compare. No new threads."""
    src = FileTask(source, "source", 0, Event())
    gzp = FileTask(gz, "gz", 0, Event())
    return resolve_pair(PairState(hash_file(src), hash_file(gzp)))


def _unlink_verified(result: Result, quiet: bool) -> None:
    """Remove a source file whose compressed sibling was just proven identical.

    Called only with a MATCH from the comparison that just ran, so the premise is
    established, not assumed. Deletion is the one unrecoverable thing this command
    can do, so the sibling is re-compared immediately before `unlink`: a concurrent
    writer or a partially written sibling would otherwise let a stale match
    destroy the last copy of the payload.
    """
    source, gz = result.source, result.gz
    if not gz.is_file():
        log.warning(f"Refusing to delete {source}: {gz.name} is gone")
        return
    if _compare_paths(source, gz).status is not MATCH:
        log.warning(f"Refusing to delete {source}: {gz.name} changed since it was compared")
        return
    try:
        size = source.stat().st_size
        source.unlink()
    except OSError as exc:
        log.warning(f"Could not delete {source}: {exc}")
        return
    if not quiet:
        log.info(f"Deleted {source} ({_bytes_to_str(size)})")


def _safe_size(path: Path) -> int:
    """Size in bytes, or 0 if it cannot be read.

    Zero rather than an exception: a file that cannot be stat'd fails loudly in the
    hash that follows, and a scan that aborts on one bad entry would hide every
    other file in the tree.
    """
    try:
        return path.stat().st_size
    except OSError:
        return 0


def find_pairs(
    root: Path,
    name: str,
    follow_symlinks: bool = False,
    on_dir: Callable[[int, int, int], None] | None = None,
) -> Iterator[Entry]:
    """Yield each `<name>` under `root`, with its uncompressed size and sibling.

    `gz` is the `compressed_path` when the compressed sibling exists and None when
    it does not (the compression candidate).

    Sizes are collected here rather than in a separate pass: the scan already stats
    each entry to test for the compressed sibling, so measuring size costs nothing
    extra, whereas a dedicated pre-pass would add one round-trip per file on a
    network mount. The per-file progress bars need the byte totals before the work
    starts.

    Order is depth-first and deterministic. `on_dir` receives (directories visited,
    pairs found, files without a sibling) so a caller can show the walk progressing;
    on a large tree the walk can outlast every comparison.
    """
    pairs = orphans = 0
    for dirs, (dirpath, dirnames, filenames) in enumerate(os.walk(root, followlinks=follow_symlinks), start=1):
        dirnames.sort()
        item: Entry | None = None
        if name in filenames:
            source = Path(dirpath, name)
            gz = compressed_path(source)
            # One stat for the size we need anyway, plus a cheap name test for the
            # sibling; `os.walk` already told us the directory listing.
            size = _safe_size(source)
            if gz.name in filenames or (follow_symlinks and gz.is_file()):
                pairs += 1
                item = Entry(source, size, gz)
            else:
                orphans += 1
                item = Entry(source, size)
        if on_dir is not None:
            on_dir(dirs, pairs, orphans)
        if item is not None:
            yield item


def _overall_description(done: int, total: int) -> str:
    """The overall bar's text: pairs settled out of pairs found, never a percentage.

    A count is what tells the user whether the run is close to finishing; with
    hours-long files a percentage of bytes looks stuck at 0% regardless.
    """
    return f"[green]Overall progress: {done}/{total} pair(s) completed"


def _sized(label: str, done: int, total: int) -> str:
    """A row's description with the bytes handled so far against the file's size.

    The size belongs next to the name in the description: it is what tells a user
    how far a multi-GB hash has actually got, in units that mean something when
    the files in the tree differ by orders of magnitude.
    """
    return f"{label} ({_bytes_to_str(done)}/{_bytes_to_str(total)})"


def _describe(result: Result) -> tuple[str, str]:
    """Line to print for a finished result: (rich style, text)."""
    if result.status is MISMATCH:
        return "red", f"{result.status}: {result.source}\n    {result.detail}"
    if result.status is UNREADABLE:
        return "yellow", f"{result.status}: {result.source}\n    {result.detail}"
    if result.status is CANCELLED:
        return "yellow", f"{result.status}: {result.source}\n    {result.detail}"
    return (
        "green",
        (
            f"{result.status}: {result.source} "
            f"({_bytes_to_str(result.source_bytes)} raw, {_bytes_to_str(result.gz_bytes)} gz)"
        ),
    )


def _run_pool(
    entries: list[Entry],
    *,
    root: Path,
    workers: int,
    level: int,
    force: bool,
    keep: bool,
    dry_run: bool,
    check: bool,
    compress: bool,
    quiet: bool,
) -> int:  # exit code contribution: 1 on any bad outcome, else 0
    """Submit every file-hash task, join pairs as they complete, and act on each.

    `root` is only used to shorten progress descriptions to the path relative to the
    scanned directory, which is what makes a row readable without being unique.

    One `FileTask` per file (`--compress` adds a compression task plus a verifying
    hash task for files with no sibling). The pool is exactly `workers` threads;
    nothing here starts a thread of its own. `wait(..., return_when=FIRST_COMPLETED)`
    is the join: when both tasks of a pair have returned, `resolve_pair` decides, the
    pair is compared, and a `--no-keep` MATCH is unlinked right here rather than after
    the run. A failed task sets its pair's `cancel` event, which the sibling polls
    between blocks, so a broken `.gz` stops its partner's hashing instead of letting
    it finish.
    """
    console = Console(stderr=True)

    def short(path: Path) -> str:
        """Path relative to `root`: unique within the run, without the prefix noise."""
        return str(path.relative_to(root))

    pairs: list[PairState] = []
    tasks: list[FileTask] = []
    # Scanned pairs are the first `scanned` entries; the compression/verify chain
    # appends to `pairs` afterwards. `spare` marks a pair whose compressed sibling
    # did not exist at scan time (the compression candidate).
    spare: set[int] = set()
    for entry in entries:
        index = len(pairs)
        cancel = Event()
        if entry.gz is not None:
            state = PairState(
                source=FileTask(entry.source, "source", index, cancel, expected=entry.size),
                gz=FileTask(entry.gz, "gz", index, cancel, expected=_safe_size(entry.gz)),
            )
            tasks.extend((state.source, state.gz))
        else:
            state = PairState(
                source=FileTask(entry.source, "source", index, cancel, expected=entry.size),
                gz=FileTask(compressed_path(entry.source), "gz", index, cancel),
            )
            spare.add(index)
            tasks.append(state.source)
        pairs.append(state)
    scanned = len(pairs)

    # One progress display for the whole pool, so the per-file rows and the
    # overall row are created by the same object. The overall row is sized by the
    # scan and driven by `advance` alone: a pair costs two hashing rows, so a
    # row-derived bar would reach 100% after half the pairs. Its percentage column
    # is dropped because its description carries the exact count.
    total = len(entries)
    progress = UploadProgress(
        description=_overall_description(0, total),
        total=total,
        derive_overall=False,
        show_percent=False,
    )

    outcome = Outcome()

    def advance(pair: PairState) -> None:
        """Count one entry as processed, once, advancing the overall row.

        Separate from reporting because the two do not coincide for
        `--compress --no-check`: that entry is finished when its write lands,
        with no comparison to report.
        """
        if pair.counted:
            return
        pair.counted = True
        outcome.settled += 1
        progress.update(
            progress.overall_task,
            completed=outcome.settled,
            total=total,
            description=_overall_description(outcome.settled, total),
        )

    def emit(pair: PairState, result: Result) -> None:
        outcome.counts[result.status] += 1
        if result.status is not MATCH:
            outcome.bad = True
        style, line = _describe(result)
        if result.status is not MATCH or not quiet:
            console.print(line, style=style, highlight=False)
        if result.status is MATCH and not keep:
            if dry_run:
                log.info(f"[dry run] would delete {result.source}")
            else:
                _unlink_verified(result, quiet)
        advance(pair)

    def settle(pair: PairState) -> None:
        """Resolve a pair whose two hashes are both terminal, once."""
        if pair.done:
            return
        pair.done = True
        emit(pair, resolve_pair(pair))

    def terminal(task: FileTask) -> bool:
        return bool(task.digest) or bool(task.error)

    with progress:
        rows: dict[int, UpdateFn] = {}

        def submit(task: FileTask) -> Future[FileTask]:
            # The total is the raw on-disk size measured at scan time, so the bar
            # advances from the first block; `on_progress` reports the offset into
            # the file on disk, which is why a `.gz` row totals its stored size.
            def report(position: int) -> None:
                tick(completed=position, description=_sized(f"Hashing {short(task.path)}", position, task.expected))

            tick = progress.add_task(_sized(f"Hashing {short(task.path)}", 0, task.expected), total=task.expected)
            rows[id(task)] = tick
            task.task_id = tick
            return pool.submit(hash_file, task, on_progress=report)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            pending: dict[Future[FileTask], FileTask] = {submit(task): task for task in tasks}

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    task = pending.pop(future)
                    future.result()  # hash_file records failures on the task; nothing raises
                    tick = rows.pop(id(task))
                    # Retire the row as-is: progress already reached the file's
                    # size via `on_progress`, so nothing needs to be invented here.
                    tick(visible=False)
                    state = pairs[task.pair]

                    if task.error and not task.error.startswith("stopped early"):
                        # Cancel the sibling: `future.cancel()` drops it if it has
                        # not started, and the shared event stops it within one
                        # block if it has.
                        task.cancel.set()
                        for other_future, other_task in pending.items():
                            if other_task is not task and other_task.pair == task.pair:
                                other_future.cancel()

                    if state.source is not task and state.gz is not task:
                        # A file whose compression already failed: reported then.
                        continue

                    if task.pair in spare and task.role == "source":
                        # The scanned hash of a file with no sibling just landed.
                        if task.error:
                            continue
                        if compress:
                            if not _compress_one(
                                console,
                                progress,
                                task,
                                label=short(task.path),
                                level=level,
                                force=force,
                                quiet=quiet,
                            ):
                                # A failed compression is still a finished entry:
                                # count it, or the run ends reading N-1/N as if it
                                # were still working.
                                outcome.bad = True
                                advance(state)
                                continue
                            if not check:
                                # Nothing to compare, so the entry is finished the
                                # moment its write lands: count it, or the bar
                                # would never leave zero in this mode.
                                advance(state)
                                continue
                            # Verify the write: hash the new `.gz` against the
                            # source digest already in hand, exactly like a
                            # scanned pair. The pair settles on that join, so an
                            # entry is counted only once its bytes are proven.
                            gz_task = FileTask(
                                compressed_path(task.path),
                                "gz",
                                task.pair,
                                Event(),
                                expected=_safe_size(compressed_path(task.path)),
                            )
                            state.gz = gz_task
                            pending[submit(gz_task)] = gz_task
                        continue

                    if terminal(state.source) and terminal(state.gz):
                        settle(state)

    if outcome.counts:
        log.info(
            f"{scanned} pair(s) checked with {workers} worker(s): "
            f"{outcome.counts[MATCH]} match, {outcome.counts[MISMATCH]} mismatch, "
            f"{outcome.counts[UNREADABLE]} unreadable, {outcome.counts[CANCELLED]} cancelled"
        )
    return 1 if outcome.bad else 0


def _compress_one(
    console: Console,
    progress: UploadProgress,
    task: FileTask,
    *,
    label: str,
    level: int,
    force: bool,
    quiet: bool,
) -> bool:
    """Compress one sibling-less source, showing its own bar. True when it worked.

    `label` is the source path relative to the scan root, used for the bar so it
    matches the `Hashing` rows. The row itself is display only: the overall bar is
    driven by `advance`, which counts the entry once the write succeeds (or fails)
    rather than when this row is retired.
    """
    # The row starts empty: `on_progress` reports the source bytes as they are
    # read, so the bar and its size text track the actual stream rather than
    # claiming the file is done before the first block is compressed.
    tick = progress.add_task(_sized(f"Compressing {label}", 0, task.expected), total=task.expected)

    def report(written: int) -> None:
        tick(completed=written, description=_sized(f"Compressing {label}", written, task.expected))

    try:
        result = compress(task.path, level=level, force=force, on_progress=report)
    finally:
        tick(visible=False)
    if not result.ok:
        if not quiet:
            console.print(f"skipped {result.source}: {result.error}", style="yellow", highlight=False)
        return False
    if not quiet:
        ratio = f" ({result.gz_bytes / result.src_bytes:.0%})" if result.src_bytes else ""
        console.print(
            f"compressed {result.source.name} -> {result.gz.name} {_bytes_to_str(result.gz_bytes)}{ratio}",
            style="cyan",
            highlight=False,
        )
    return True


@app.command
def check_gz(
    directory: Path,
    /,
    name: str = "binary.npy",
    workers: int = 1,
    quiet: bool = False,
    follow_symlinks: bool = False,
    compress: bool = False,
    level: int = 9,
    force: bool = False,
    keep: bool = True,
    dry_run: bool = False,
    check: bool = True,
) -> None:
    """Verify that each `<name>` has a `<name>.gz` sibling with identical contents.

    Every file found is hashed as its own task, one worker thread per task, so
    `--workers N` starts exactly `N` threads. A pair contributes two tasks and is
    compared as soon as both hashes land; if either hash fails, the other stops
    early rather than hashing to completion. Files with no compressed sibling are
    compressed with `--compress` and then hashed again to prove the write.

    Args:
        directory (Path): Root directory to scan.
        name (str, optional): File name to look for. Any name works; the
            compressed sibling is `<name>.gz`. Default `binary.npy`.
        workers (int, optional): Number of worker threads, one file task each.
            Exactly this many threads are started. Default 1.
        quiet (bool, optional): Print only problems, not every match.
        follow_symlinks (bool, optional): Follow symlinked directories during the scan.
        compress (bool, optional): gzip a `name` file that has no `.gz` sibling,
            like `gzip -9 -k` (original kept, source mtime preserved).
        level (int, optional): gzip level for `--compress`. Default 9.
        force (bool, optional): With `--compress`, overwrite an existing `.gz`
            instead of skipping it.
        keep (bool, optional): Keep the source file after a verified match
            (default); `--no-keep` deletes it, and only ever when the comparison
            proved the `.gz` holds the same bytes and a re-check just before the
            unlink agreed.
        dry_run (bool, optional): With `--no-keep`, report what would be deleted
            without deleting it.
        check (bool, optional): Compare each file against its compressed sibling
            (default); `--no-check` skips verification, which forces `--keep` since
            an unverified pair is never provably identical.
    """
    if not check and not keep:
        raise ValueError("`check` cannot be disabled together with `keep`: deletion requires a verified match")
    if not check and not compress:
        raise ValueError("`check` cannot be disabled unless `compress` is enabled: there is nothing else to do")
    if workers < 1:
        raise ValueError("Argument `workers` must be at least 1.")
    if name.endswith(GZIP_SUFFIX):
        raise ValueError(f"`name` must not end in {GZIP_SUFFIX!r}: the compressed sibling is formed by appending it")
    if not directory.is_dir():
        raise ValueError(f"Not a directory: {directory}")

    with Spinner(f"scanning {directory} for {name}"):
        entries = list(find_pairs(directory, name, follow_symlinks))

    if not entries:
        log.info(f"no {name} files under {directory}")
        return

    pairs = sum(1 for entry in entries if entry.gz is not None)
    orphans = len(entries) - pairs
    if orphans and not compress:
        log.info(f"{orphans} {name} file(s) have no .gz; pass --compress to create them.")

    rc = _run_pool(
        entries,
        root=directory,
        workers=workers,
        level=level,
        force=force,
        keep=keep,
        dry_run=dry_run,
        check=check,
        compress=compress,
        quiet=quiet,
    )
    if rc:
        raise SystemExit(1)
