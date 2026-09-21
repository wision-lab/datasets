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
(private).

## Commands

| Command | Purpose |
| --- | --- |
| `show-tree PATH [--full] [--s3-prefix P] [--html]` | Print a saved tree JSON, optionally as links to public S3 objects. |
| `upload PATH [--s3.bucket B --s3.prefix P] [--strategy legacy\|greedy] [--chunk-size N] [--partitions FILE] [--output-dir DIR] [--workers N]` | Walk a directory, partition it, split into archives, and upload (or write locally). |


## Dev Tools

```bash
uv run ruff format src      # format (line-length 121)
uv run ruff check src       # lint
uv run mypy                 # type-check (config in pyproject.toml)
```

`boto3` is pinned to `1.35.31`; `main()` warns if another version is installed.