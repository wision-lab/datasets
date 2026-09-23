from __future__ import annotations

import os
from pathlib import Path

# Characters that are illegal on Windows and on SMB/Windows-backed shares. They
# are replaced with `_` rather than percent-encoded so the result stays human
# readable, e.g. `mean_frames/0:512.jpg` becomes `mean_frames/0_512.jpg`. `/` is
# deliberately absent: it is the path separator and must be preserved.
_UNSAFE_CHARACTERS = frozenset({":", "?", "*", '"', "<", ">", "|", "\\"})
_REPLACEMENT = "_"


def sanitize_name(name: str) -> str:
    """Replace characters that are illegal on Windows/SMB shares with `_`.

    Used both to rename files before upload (`dm sanitize-paths`) and to compare
    an old prefix against its converted copy (`dm diff-s3 --normalize`).
    """
    return "".join(_REPLACEMENT if char in _UNSAFE_CHARACTERS else char for char in name)


def sanitize_tree(root: str | os.PathLike) -> int:
    """Rename every file and directory under `root` to a Windows/SMB-safe name.

    Traversal is bottom-up, so a directory is renamed only after its contents.
    Returns the number of entries renamed, and raises `FileExistsError` rather
    than overwriting when a sanitized name collides with an existing entry.
    """
    root = Path(root)
    renamed = 0
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        for name in (*filenames, *dirnames):
            sanitized = sanitize_name(name)
            if sanitized == name:
                continue
            src = Path(dirpath) / name
            dst = Path(dirpath) / sanitized
            if dst.exists():
                raise FileExistsError(f"Cannot rename {src} to {dst}: target already exists")
            src.rename(dst)
            renamed += 1
    return renamed
