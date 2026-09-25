# Development

Internal tooling to chunk, archive, and upload research datasets to S3.

## Setup

Requires Python >= 3.14 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync            # create .venv and install runtime + dev deps
uv run dm --help   # run the CLI (alias: dataset-manager / python -m dataset_manager)
```

For uploads, credentials must exist in `~/.aws/credentials`, and `AWS_ENDPOINT_URL`
should point at `https://web.s3.wisc.edu/` (public) or `https://campus.s3.wisc.edu/`
(private). `diff-s3` reads are unsigned by default and fall back to the public
`https://web.s3.wisc.edu/` endpoint when `--endpoint-url`/`AWS_ENDPOINT_URL` are
unset; pass `--sign` (and a private endpoint) to read private buckets.

## Commands

| Command | Purpose |
| --- | --- |
| `show-tree PATH [--full] [--s3-prefix P] [--html] [--meta]` | Print a saved tree JSON, optionally as links to public S3 objects. |
| `upload PATH [--s3.bucket B --s3.prefix P] [--strategy legacy\|greedy\|optimal] [--merge-ratio R] [--chunk-size N] [--partitions FILE] [--output-dir DIR] [--workers N]` | Walk a directory, partition it, split into archives, and upload (or write locally). |
| `diff-s3 SOURCE TARGET [--bucket B] [--sign] [--workers N] [--summary-only] [--endpoint-url URL]` | Diff the extracted file trees of two S3 prefixes. `.zip` and uncompressed `.tar` archives are expanded by reading their member lists (no download); nested archives and compressed tars are reported but not expanded. Either operand may instead be an existing local directory: its archives are expanded in place from the filesystem, and loose files are compared by size + MD5. |


## Chunking

`upload` splits each partition into archives of about `--chunk-size` with one of
three strategies (`--strategy`), all in `src/dataset_manager/chunking.py`:

- `legacy` — original bucket packing; an archive can reach almost twice the target.
- `greedy` — closes an archive before it would exceed the target.
- `optimal` — per-level bin-packing MIP (falls back to `greedy` for very large
  levels or on solver failure).

Files are atomic, so a file larger than the target always gets an archive of its
own, which would otherwise strand its small neighbouring files (videos, masks,
metadata) in an unrelated archive. `--merge-ratio R` (default `0`, off) folds such
a folder into the oversized file's archive when its *supporting* files — every
child except the oversized ones — total at most `R * chunk_size`; the folder itself
then becomes the archive root, so its contents keep the original layout. Folders
holding several oversized files keep one archive per oversized file, the
supporting files riding along with exactly one of them. Nonzero `R` changes archive
boundaries and therefore S3 object keys.

## Tree Metadata

Each tree saved by `upload` embeds provenance in its nutree file header (via
`save(..., meta=...)`): creation time, git commit (and whether the work tree was
dirty), and the command that produced it. `show-tree --meta` prints these fields;
trees saved before this feature simply report no metadata. Collection and
formatting live in `src/dataset_manager/metadata.py`.

## Dev Tools

```bash
uv run pytest tests          # test suite (tests/; fixtures are in-memory, a few kB)
uv run ruff format src tests # format (line-length 121)
uv run ruff check src tests  # lint
uv run mypy                  # type-check `src` and `tests` (config in pyproject.toml)
```

`boto3` is pinned to `1.35.31`; `main()` warns if another version is installed.
Keep test fixtures free of large files by setting `PathData.size` directly instead
of writing bytes to disk (see `tests/test_merge_ratio.py`); that also keeps the
suite runnable in a few hundred milliseconds.