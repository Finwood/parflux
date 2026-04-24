import locale
import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.logging import RichHandler

from .session import Session

app = typer.Typer(pretty_exceptions_show_locals=False, add_completion=False, no_args_is_help=True)
console = Console()
log = logging.getLogger(__name__)

locale.setlocale(locale.LC_ALL, "")


@app.command()
def main(
    query: Annotated[
        list[str],
        typer.Argument(help="<bucket> or <bucket>/<measurement>, can be specified multiple times", show_default=False),
    ],
    dest: Annotated[
        Optional[Path],
        typer.Option("--dest", "-d", help="target base directory, defaults to current directory", show_default=False),
    ] = None,
    filter: Annotated[list[str], typer.Option("--filter", "-f", help="additional flux filters")] = [],
    start: Optional[datetime] = None,
    stop: Optional[datetime] = None,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
    reload_env: Annotated[bool, typer.Option("--reload-env", "-r")] = False,
):
    """Download Bucket or Single Measurement from InfluxDB.

    To download an entire bucket, specify just the bucket name. To download only specific measurements, specify the full
    measurement path like <bucket>/<measurement_name>. Multiple measurement and bucket queries can be selected by simply
    providing multiple query arguments.

    Every measurement will be saved in a separate parquet file, <dest>/<bucket>/<measurement>.parquet.

    The data may be filtered further by specifying one or multiple flux filter queries.

    Attention: No input is sanitized to protect against flux injection. Don't break the query!
    """
    if reload_env:
        from dotenv import load_dotenv

        load_dotenv()

    logging.basicConfig(
        level={
            0: logging.WARNING,
            1: logging.INFO,
            2: logging.DEBUG,
        }.get(verbose, logging.NOTSET),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler()],
    )

    session = Session(start, stop)
    session.download(query, filter, dest)
