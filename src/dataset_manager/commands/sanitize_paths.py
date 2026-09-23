from __future__ import annotations

from pathlib import Path

from ..app import app
from ..log import log
from ..paths import sanitize_tree


@app.command
def sanitize_paths(path: Path, /) -> None:
    """Rename files under PATH to Windows/SMB-safe names.

    Characters that are illegal on Windows and on SMB/Windows-backed shares (`:`,
    `?`, `*`, `"`, `<`, `>`, `|`, `\\`) are replaced with `_`, e.g.
    `mean_frames/0:512.jpg` becomes `mean_frames/0_512.jpg`. `dm diff-s3
    --normalize` applies the same conversion, so an old prefix can be compared
    against its repackaged copy.

    Args:
        path (Path): Directory to sanitize in place.
    """
    if not path.is_dir():
        raise NotADirectoryError(f"{path} is not a directory")
    renamed = sanitize_tree(path)
    log.info(f"Renamed {renamed} path(s) under {path} to Windows/SMB-safe names.")
