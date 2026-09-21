from __future__ import annotations

import boto3

from . import commands  # noqa: F401  (importing registers the subcommands)
from .app import app
from .log import log


def main() -> None:
    if boto3.__version__ != "1.35.31":
        log.warning("Please use boto3==1.35.31 as other versions might fail to upload files!!")
    app.cli()
