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
| `upload PATH [--s3.bucket B --s3.prefix P] [--strategy legacy\|greedy\|optimal] [--chunk-size N] [--partitions FILE] [--output-dir DIR] [--workers N]` | Walk a directory, partition it, split into archives, and upload (or write locally). |
| `diff-s3 SOURCE TARGET [--bucket B] [--sign] [--workers N] [--summary-only] [--endpoint-url URL]` | Diff the extracted file trees of two S3 prefixes. `.zip` and uncompressed `.tar` archives are expanded by reading their member lists (no download); nested archives and compressed tars are reported but not expanded. |


## Tree Metadata

Each tree saved by `upload` embeds provenance in its nutree file header (via
`save(..., meta=...)`): creation time, git commit (and whether the work tree was
dirty), and the command that produced it. `show-tree --meta` prints these fields;
trees saved before this feature simply report no metadata. Collection and
formatting live in `src/dataset_manager/metadata.py`.

## Dev Tools

```bash
uv run ruff format src      # format (line-length 121)
uv run ruff check src       # lint
uv run mypy                 # type-check (config in pyproject.toml)
```

`boto3` is pinned to `1.35.31`; `main()` warns if another version is installed.