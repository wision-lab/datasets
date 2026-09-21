from __future__ import annotations

import contextlib
import json
import os
import shutil
import sys
import tempfile
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from typing import Annotated, Any, cast

import boto3
import questionary
import tyro
from nutree import SkipBranch, Tree
from nutree.node import Node
from rich.status import Status

from ..app import app
from ..archives import write_zip_stream
from ..chunking import ChunkStrategy, split_into_chunks
from ..log import log
from ..metadata import METADATA_KEY, collect_metadata
from ..partitions import partition_tree_by_fnmatches
from ..progress import UpdateFn, UploadProgress
from ..s3 import (
    _S3_RETRY_CONFIG,
    S3Connection,
    check_exists,
    upload_file,
)
from ..sizes import MemSize, _bytes_from_str, _bytes_to_str
from ..tree import (
    PathData,
    directory_tree,
    is_match,
    is_not_dunder,
    is_not_hidden,
)

# At most one of `--tmp-dir` / `--output-dir` may be set: the former is scratch
# space that is cleaned up after each archive is built, while the latter is a
# persistent local copy of the archives. See the `upload` command.
_ARCHIVE_LOCATION_GROUP = tyro.conf.create_mutex_group(required=False, title="archive location")


def _default_upload_workers(*, chunk_size: int, output_dir: Path | None, tmp_dir: Path | None) -> int:
    """Pick a default number of upload workers when none is given.

    With `output_dir` the archives are retained, so free disk space does not
    bound the total footprint and we simply use every core. With a scratch dir
    (`tmp_dir` or the OS temp dir) each archive is deleted right after upload, so
    at most `workers` archives coexist on disk; spending only half of
    the free space on them keeps the worker count within the available space.
    """
    cpu_count = os.cpu_count() or 1

    if output_dir is not None:
        log.warning(
            "`workers` was not specified and `--output-dir` retains "
            f"archives, so free space is not a usable bound: using {cpu_count} "
            "worker(s) (one per core). Pass `--workers` to override."
        )
        return cpu_count

    scratch = tmp_dir if tmp_dir is not None else Path(tempfile.gettempdir())
    # The scratch dir may not exist yet; probe the nearest existing ancestor.
    probe = scratch.resolve()
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent

    free = shutil.disk_usage(probe).free
    budget = free // 2
    by_space = max(1, budget // chunk_size)
    workers = min(by_space, cpu_count)

    log.warning(
        f"`workers` was not specified: using {workers} worker(s) "
        f"({by_space} chunk(s) of {_bytes_to_str(chunk_size)} fit in half of the "
        f"{_bytes_to_str(free)} free at {probe}, capped by {cpu_count} core(s)). "
        "Pass `--workers` to override."
    )
    if budget < chunk_size:
        log.warning(
            f"Scratch space at {probe} may be too small for a single "
            f"{_bytes_to_str(chunk_size)} archive; freeing space is recommended."
        )
    return workers


def _confirm_zip_partition(subtrees: dict[str, Tree]) -> None:
    """Prompt for confirmation of the partition, allowing inspection first.

    The `Inspect` option prints every subtree in full, including the contents of
    each archive that the slim preview above omits, then re-prompts so the user
    can study the partition before committing. Aborting or interrupting the
    prompt exits the process with status 1.
    """
    while True:
        choice = questionary.select(
            "Confirm zip partition?",
            choices=["Confirm", "Inspect tree", "Abort"],
            default="Confirm",
        ).ask()
        if choice == "Confirm":
            return
        if choice is None or choice == "Abort":
            sys.exit(1)
        for tree in subtrees.values():
            tree.print(repr="{node.data}")
            print()


@app.command
def upload(
    path: Path,
    /,
    s3: S3Connection = S3Connection(),  # noqa: B008
    chunk_size: MemSize = _bytes_from_str("10GB"),  # noqa: B008
    strategy: ChunkStrategy = "legacy",
    exclude: list[str] = [],  # noqa: B006
    tmp_dir: Annotated[Path | None, _ARCHIVE_LOCATION_GROUP] = None,
    output_dir: Annotated[Path | None, _ARCHIVE_LOCATION_GROUP] = None,
    trees_dir: Path | None = None,
    partitions: Path | None = None,
    min_zip_depth: int = 1,
    follow_symlinks: bool = False,
    overwrite: bool = False,
    workers: int | None = None,
) -> None:
    """Auto-partition dataset into archives and upload them to an S3 bucket.

    Args:
        path (Path): Directory to upload.
        chunk_size (MemSize, optional): Target size of archives (pre-compression).
            With `--strategy greedy` every archive is at most this size, except
            one holding a single file larger than it (files are atomic and cannot
            be split). With `--strategy legacy` archives are packed into
            cumulative-size buckets and can reach almost twice this size. Either
            way archives are LZMA compressed, so their on-disk size is normally
            smaller than the target.
        strategy (ChunkStrategy, optional): How a node's children are grouped into
            archives. `legacy` is the original bucket packing; `greedy` closes an
            archive as soon as adding another child would exceed `chunk_size`.
            Both archive exactly the same files, but with different boundaries, so
            switching strategy changes the S3 object keys: an existing bucket has
            to be re-uploaded (with `--overwrite`) to switch.
        exclude (list[str], optional): Space separated list of path exclusion
            patterns. Warning something like "logs/" will match any path that
            contains logs. Internally uses `Path.match`.
        tmp_dir (Path, optional): Location of scratch dir used to build
            archives. Useful if the `chunk_size` is more than a few GBs. Defaults
            to the OS default tmp directory. Its contents are deleted once each
            archive has been built. Mutually exclusive with `output_dir`.
        output_dir (Path, optional): Directory into which archives are written,
            mirroring the S3 object-key layout. Unlike `tmp_dir`, its contents
            are never deleted, so it can be used to produce a local copy of the
            dataset (e.g. when no S3 bucket/prefix is set). Mutually exclusive
            with `tmp_dir`.
        trees_dir (Path, optional): Location in which to save trees. Defaults to `path`.
        partitions (Path, optional): Path of json file containing partition names,
            their matching patterns and optionally a per-partition `min_zip_depth`.
        min_zip_depth (int, optional): Allow for zipping a node if it is
            shallower than this depth, were the root is at a depth of 1,
            even if it's less than `chunk_size`. Default 1 (only root node)
        follow_symlinks (bool, optional): If true, symlinks will be followed.
            Careful, this can lead to infinite recursions!
        overwrite (bool, optional): If true, objects in the S3 bucket will be
            overwritten by new ones that share the same key, otherwise the
            conflicting uploads are skipped.
        workers (int, optional): Maximum number of archives to zip and
            upload concurrently. Archives are independent, so raising this
            overlaps compression (CPU-bound) with uploads (I/O-bound). When left
            unset it is derived from the free space of the scratch/output dir and
            the number of cores, and a warning reports the chosen value.
    """
    if min_zip_depth <= 0:
        raise ValueError("Argument `min_zip_depth` must be at least 1.")

    # Also enforced by the `_ARCHIVE_LOCATION_GROUP` tyro marker; kept here so
    # programmatic callers get the same guarantee.
    if output_dir is not None and tmp_dir is not None:
        raise ValueError("Arguments `output_dir` and `tmp_dir` are mutually exclusive.")

    if workers is not None and workers < 1:
        raise ValueError("Argument `workers` must be at least 1.")

    # Create filesystem tree and split it into zip-sized chunks
    def path_filter(p):
        keep = is_not_hidden(p) and is_not_dunder(p) and not is_match(p, patterns=exclude)
        if not keep:
            log.debug(f"Excluding {p} ({_bytes_to_str(p.stat().st_size)})")
        return keep

    def on_error(e: OSError) -> None:
        # Surface walk errors (e.g. permission denied) instead of silently
        # dropping a subtree and uploading an incomplete archive.
        log.warning(f"Failed to walk part of {path}: {e}")

    with Status("Building Tree...", spinner="bouncingBall"):
        file_tree = directory_tree(
            path,
            on_error=on_error,
            filter_fn=path_filter,
            follow_symlinks=follow_symlinks,
        )

    with Status("Partitioning Tree...", spinner="bouncingBall"):
        if partitions:
            with open(partitions, "r") as f:
                partitions_dict = json.load(f)
        else:
            partitions_dict = {"": {"pattern": None}}

        patterns = {k: v.get("pattern") for k, v in partitions_dict.items()}
        subtrees = partition_tree_by_fnmatches(tree=file_tree, patterns=patterns)

    with Status(
        f"Splitting into {_bytes_to_str(chunk_size)} chunks ({strategy} strategy)...",
        spinner="bouncingBall",
    ):
        subtrees = {
            k: split_into_chunks(
                tree=st,
                chunk_size=chunk_size,
                min_zip_depth=partitions_dict[k].get("min_zip_depth", min_zip_depth),
                strategy=strategy,
            )
            for k, st in subtrees.items()
        }

    for prefix, tree in subtrees.items():
        slim_tree = tree.filtered(lambda n: SkipBranch(and_self=False) if n.data.is_zip else True)
        slim_tree.name = (prefix or "Zip Tree").title()
        slim_tree.print(repr="{node.data}")
        print()

    _confirm_zip_partition(subtrees)

    # Confirm all s3 settings, ensure we don't accidentally upload anything
    uploading = s3.bucket is not None and s3.prefix is not None
    if uploading:
        if not questionary.confirm(
            "Not running in local mode, this will upload artifacts to S3. Confirm?",
            default=False,
        ).ask():
            sys.exit(1)

        public = questionary.confirm("Make uploaded artifacts public?", default=False).ask()
        s3_client = boto3.client("s3", config=_S3_RETRY_CONFIG)
        exists: Callable = partial(check_exists, s3_client=s3_client, conn=s3)
        upload: Callable = partial(upload_file, s3_client=s3_client, conn=s3, public=public)
    else:
        # Do not check existence if not uploading
        def _log_upload(src, dst):
            log.info(f"Would have uploaded {src} to {dst}.")

        def _not_exists(*args, **kwargs):
            return 0

        upload = _log_upload
        exists = _not_exists

    if output_dir is not None:
        # Write archives straight into output_dir, mirroring the object-key
        # layout. Its contents are never deleted, which makes this the reliable
        # way to produce a local archive.
        output_dir.mkdir(exist_ok=True, parents=True)
        context = partial(contextlib.nullcontext, enter_result=output_dir.resolve())
    else:
        # We're not persisting anything, so we can safely re-enter into a new
        # temporary directory every time which helps keep it a manageable size.
        if tmp_dir is not None:
            tmp_dir.mkdir(exist_ok=True, parents=True)
        context = partial(
            cast(Callable, tempfile.TemporaryDirectory),
            dir=tmp_dir.resolve() if tmp_dir is not None else None,
            delete=True,
        )

    # Worker count defaults to one derived from scratch space and core count.
    if workers is None:
        workers = _default_upload_workers(chunk_size=chunk_size, output_dir=output_dir, tmp_dir=tmp_dir)

    # Traverse each partition once and tag every top-level zip node. Descendants
    # of a zip node are skipped (a zip node subsumes its whole subtree), so the
    # tagged archives are mutually independent. Collecting them up-front, rather
    # than zipping during the walk, is what allows the worker pool below to build
    # and upload several archives concurrently.
    def collect_zipnodes(
        node: Node,
        _memo: Any,
        *,
        prefix: str | None,
        jobs: list[tuple[str | None, Node]],
    ) -> SkipBranch | None:
        if node.data.is_zip:
            jobs.append((prefix, node))
            raise SkipBranch
        return None

    zip_jobs: list[tuple[str | None, Node]] = []
    for prefix, tree in subtrees.items():
        tree.visit(partial(collect_zipnodes, prefix=prefix, jobs=zip_jobs))

    # Zips up every descendant of a zip node and uploads the result. Runs in a
    # worker thread, so it only touches node-local state (`node.data.zip_size`)
    # and the thread-safe `tick` callback; no two workers share an archive.
    def zip_and_upload(
        prefix: str | None,
        node: Node,
        tick: UpdateFn,
        *,
        context: Callable,
    ) -> None:
        object_key = Path(prefix or "") / node.data.path.relative_to(path.parent.resolve())
        try:
            tick(description=f"Compressing {node.data.path.name}")

            if not overwrite and (zip_size := exists(key=object_key)):
                log.info(f"Skipping {object_key} as objects with the same key exists in bucket.")
                node.data.zip_size = zip_size
                return

            with context() as tmpdir:
                zip_path = Path(tmpdir) / object_key
                zip_path.parent.mkdir(exist_ok=True, parents=True)

                try:
                    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_LZMA) as archive:
                        if s3.bucket is not None or s3.prefix is not None or output_dir is not None:
                            zip_root = node.data.path.parent.resolve()
                            for n in node.find_all(match=lambda n: n.is_leaf(), add_self=True):
                                # Byte-smooth progress, even within a single large file.
                                write_zip_stream(
                                    archive,
                                    n.data.path,
                                    arcname=n.data.path.relative_to(zip_root),
                                    on_bytes=lambda written: tick(advance=written),
                                )
                    node.data.zip_size = zip_path.stat().st_size
                except BaseException:
                    # Never leave a truncated archive behind: with `--output-dir`
                    # it would outlive the run and look like a complete chunk.
                    zip_path.unlink(missing_ok=True)
                    raise

                if uploading:
                    # Reset the bar for the upload phase: its total switches from
                    # the pre-compression size to the archive's on-disk size.
                    tick(
                        description=f"Uploading {node.data.path.name}",
                        total=node.data.zip_size,
                        completed=0,
                    )
                    upload(
                        zip_path,
                        object_key,
                        callback=lambda transferred: tick(advance=transferred),
                    )
        except OSError as error:
            # Name the archive so a worker failure is actionable straight from the
            # log, without having to dig the key out of the tree.
            error.add_note(f"while building archive {object_key}")
            raise
        finally:
            # Hide the chunk bar and fold it into the overall progress bar.
            tick(visible=False)

    if zip_jobs:
        with UploadProgress() as progress:
            ticks = [
                progress.add_task(f"Compressing {node.data.path.name}", total=node.data.size or 0)
                for _, node in zip_jobs
            ]
            failures: list[BaseException] = []
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(zip_and_upload, prefix, node, tick, context=context)
                    for (prefix, node), tick in zip(zip_jobs, ticks)
                ]
                for future in as_completed(futures):
                    if (error := future.exception()) is not None:
                        # Surface the failure immediately: a single chunk can take
                        # minutes to hours (multi-GB LZMA archive, slow upload), so
                        # waiting for the remaining chunks to finish would hide it.
                        # The rest keep running, so no completed work is lost.
                        log.error(f"Failed to archive/upload a chunk: {error}", exc_info=error)
                        failures.append(error)
            if failures:
                # Non-zero exit; also skips the compression-ratio summary below,
                # which would divide by zero if every chunk of a partition failed.
                raise failures[0]

    # Save all subtrees for future inspection, tagging each with the
    # provenance of the run that produced it (commit, command, timestamp).
    ((trees_dir or path) / "trees").mkdir(exist_ok=True, parents=True)
    meta = {METADATA_KEY: collect_metadata()}
    for k, st in subtrees.items():
        st.save(
            (trees_dir or path) / "trees" / f"{k or 'tree'}.json",
            mapper=PathData.serialize_mapper,
            compression=True,
            meta=meta,
        )
        size = sum(n.data.size for n in st.children)
        compressed_size = sum(n.data.zip_size or 0 for n in st)
        log.info(
            f"Total file: {_bytes_to_str(size)}, "
            f"Compressed size: {_bytes_to_str(compressed_size)}, "
            f"Compression ratio: ({size / compressed_size:.1f}x)"
        )
