from __future__ import annotations

import datetime
import shlex
import subprocess
import sys
from typing import Any

# Header key under which our provenance fields are stored in a tree file, so
# they cannot collide with nutree's own `$`-prefixed header entries.
METADATA_KEY = "dataset_manager"

# The provenance fields, as ordered (key, label) pairs. Also drives the
# `edit-tree` prompts, so a new field only has to be declared here.
METADATA_FIELDS: tuple[tuple[str, str], ...] = (
    ("created_utc", "Created (UTC)"),
    ("command", "Command"),
    ("git_commit", "Git commit"),
    ("git_dirty", "Dirty work tree"),
)

# Fields in `METADATA_FIELDS` that hold a boolean rather than text.
BOOLEAN_METADATA_FIELDS = frozenset({"git_dirty"})


def _git(*args: str) -> str | None:
    """Run a git command, returning stripped stdout or None if unavailable."""
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _command_line() -> str:
    """Render the invocation as `dataset-manager <args>`.

    `sys.argv` omits the interpreter and mangles the program slot for `-m`/`-c`
    invocations, so start from `sys.orig_argv` (the OS-level vector, i.e. the
    interpreter plus script or `-m module` and the remaining arguments), drop the
    interpreter and program slot, then substitute the canonical tool name.
    """
    argv = list(getattr(sys, "orig_argv", None) or [sys.executable, *sys.argv])
    rest = argv[1:]
    if rest[:1] == ["-m"]:
        rest = rest[2:]  # drop `-m <module>`
    else:
        rest = rest[1:]  # drop the entry-point script path
    return shlex.join(["dataset-manager", *rest])


def collect_metadata() -> dict[str, Any]:
    """Snapshot provenance for a tree: creation time, git state and command."""
    metadata: dict[str, Any] = {
        "created_utc": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "command": _command_line(),
    }
    if commit := _git("rev-parse", "HEAD"):
        metadata["git_commit"] = commit
        # `git status --porcelain` is empty on a clean work tree.
        metadata["git_dirty"] = bool(_git("status", "--porcelain"))
    return metadata


def format_metadata(meta: dict[str, Any]) -> str:
    """Render metadata as plain labelled lines for display."""
    lines: list[str] = []
    for key, label in METADATA_FIELDS:
        if key not in meta:
            continue
        value = meta[key]
        if isinstance(value, bool):
            value = "yes" if value else "no"
        lines.append(f"{label}: {value}")
    return "\n".join(lines)
