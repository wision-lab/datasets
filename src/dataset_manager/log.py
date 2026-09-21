from __future__ import annotations

import logging
import os

import tyro
from rich.logging import RichHandler
from rich.traceback import install

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO").upper(),
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logging.getLogger("PIL").setLevel(logging.WARNING)
log = logging.getLogger("rich")
install(suppress=[tyro])
