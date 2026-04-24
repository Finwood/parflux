import locale
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.logging import RichHandler

from .core import DEFAULT_BATCH_SIZE, download

app = typer.Typer(pretty_exceptions_show_locals=False, add_completion=False, no_args_is_help=True)
console = Console()
log = logging.getLogger(__name__)

locale.setlocale(locale.LC_ALL, "")
DEFAULT_DURATION = timedelta(days=1)
DEFAULT_BATCH_SIZE_HOURS = int(DEFAULT_BATCH_SIZE.total_seconds() // 3600)


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
    batch_size: Annotated[
        int,
        typer.Option("--batch-size", help="query batch size in hours", min=1),
    ] = DEFAULT_BATCH_SIZE_HOURS,
    start: Annotated[Optional[datetime], typer.Option("--start", help="start timestamp (inclusive)")] = None,
    stop: Annotated[Optional[datetime], typer.Option("--stop", help="stop timestamp (exclusive)")] = None,
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

    if dest is None:
        dest = Path(".")

    if stop is None:
        stop = datetime.now().replace(microsecond=0).astimezone()
    else:
        stop = stop.astimezone()

    if start is None:
        start = stop - DEFAULT_DURATION
    else:
        start = start.astimezone()

    download(
        queries=query,
        start=start,
        stop=stop,
        basedir=dest,
        filters=filter,
        batch_size=timedelta(hours=batch_size),
    )
