from __future__ import annotations

import sys

from rich import get_console
from rich.pager import SystemPager


def show(text: str) -> bool:
    """Display `text`, paging it when stdout is an interactive terminal.

    Paging is skipped whenever stdout is not a TTY (piped, redirected, or
    captured), so callers that consume the output programmatically — regenerating
    the README tree sections, `show-tree ... > file` — get exactly the bytes they
    would have gotten from `print`. Non-TTY output is written directly rather than
    through `rich`, which would wrap and stylize it.

    Returns:
        True if the text was handed to the pager, False if it was printed as-is.
    """
    if not sys.stdout.isatty():
        print(text)
        return False
    # `Console.pager` is a context manager and `SystemPager` delegates to
    # `pydoc.pager`, which picks `$PAGER`/`$MANPAGER` first and falls back to
    # `less`, `more` or plain output if none of them can be run.
    with get_console().pager(SystemPager()):
        get_console().print(text, markup=False, highlight=False, soft_wrap=True)
    return True
