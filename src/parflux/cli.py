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
DATETIME_FORMATS = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"]


def _now() -> datetime:
    return datetime.now()


@app.command()
def main(
    query: Annotated[
        list[str],
        typer.Argument(
            help="One or more selectors in the form <bucket> or <bucket>/<measurement>.",
            show_default=False,
        ),
    ],
    start: Annotated[
        Optional[datetime],
        typer.Option(
            "--start",
            "-s",
            help="Start timestamp (inclusive), e.g. [bold green]2026-04-24T14:45:00+02:00[/bold green] or [bold green]2025-01-01[/bold green]. "
            "If no timezone is specified, the local timezone is assumed. "
            r"[dim]\[default: END - 1 day][/dim]",
            metavar="START",
            formats=DATETIME_FORMATS,
        ),
    ] = None,
    end: Annotated[
        Optional[datetime],
        typer.Option(
            "--end",
            "-e",
            help="End timestamp (exclusive). If no timezone is specified, the local timezone is assumed. "
            r"[dim]\[default: now][/dim]",
            metavar="END",
            formats=DATETIME_FORMATS,
        ),
    ] = None,
    dest: Annotated[
        Path,
        typer.Option(
            "--dest",
            "-d",
            help="Destination base directory where parquet files should be saved. "
            r"[dim]\[default: current directory][/dim]",
            show_default=False,
        ),
    ] = Path("."),
    filter: Annotated[
        list[str],
        typer.Option(
            "--filter",
            "-f",
            help="Additional flux filters to apply to the query. The current record is available as [bold green]r[/bold green]. "
            "Can be specified multiple times.\b\n\n"
            "Example: [bold green]r.host == 'h1'[/bold green] or [bold green]r.env =~ /prod/[/bold green]",
        ),
    ] = [],
    verbose: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Increase verbosity. Can be specified multiple times.",
            metavar="",
            show_default=False,
        ),
    ] = 0,
    reload_env: Annotated[
        bool, typer.Option("--reload-env", "-r", help="Reload environment variables from .env file.")
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            min=1,
            metavar="HOURS",
            help="Query batch size in hours.",
        ),
    ] = DEFAULT_BATCH_SIZE_HOURS,
):
    """Export InfluxDB v2 data to parquet files.

    [not dim]Provide one or more selectors as <bucket> or <bucket>/<measurement>.
    Results are written to <dest>/<bucket>/<measurement>.parquet and can be constrained by time range and optional Flux filters.

    [bold red]Warning:[/bold red] Query input is used as-is and is not sanitized against Flux injection. Only run trusted queries.
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

    if end is None:
        end = _now().replace(microsecond=0).astimezone()
    else:
        end = end.astimezone()

    if start is None:
        start = end - DEFAULT_DURATION
    else:
        start = start.astimezone()

    download(
        queries=query,
        start=start,
        end=end,
        basedir=dest,
        filters=filter,
        batch_size=timedelta(hours=batch_size),
    )
