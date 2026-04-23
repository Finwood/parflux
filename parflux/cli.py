import locale
import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .session import Session

app = typer.Typer(pretty_exceptions_show_locals=False)
SESSION_KEY = f"{__name__}.session"
console = Console()
log = logging.getLogger(__name__)

locale.setlocale(locale.LC_ALL, "")


@app.command()
def get(
    ctx: typer.Context,
    query: Annotated[
        list[str],
        typer.Argument(help="<bucket> or <bucket>/<measurement>, can be specified multiple times", show_default=False),
    ],
    dest: Annotated[
        Optional[Path],
        typer.Option("--dest", "-d", help="target base directory, defaults to current directory", show_default=False),
    ] = None,
    filter: Annotated[list[str], typer.Option("--filter", "-f", help="additional flux filters")] = [],
):
    """Download Bucket or Single Measurement from InfluxDB.

    To download an entire bucket, specify just the bucket name. To download only specific measurements, specify the full
    measurement path like <bucket>/<measurement_name>. Multiple measurement and bucket queries can be selected by simply
    providing multiple query arguments.

    Every measurement will be saved in a separate parquet file, <dest>/<bucket>/<measurement>.parquet.

    The data may be filtered further by specifying one or multiple flux filter queries.

    Attention: No input is sanitized to protect against flux injection. Don't break the query!
    """
    session: Session = ctx.meta[SESSION_KEY]
    session.download(query, filter, dest)


@app.command("list")
def list_(
    ctx: typer.Context,
    path: Annotated[Optional[str], typer.Argument()] = None,
):
    session: Session = ctx.meta[SESSION_KEY]
    if path is None:
        table = Table("ID", "Name", "Description", "Retention")
        for b in session.list_buckets():
            retention = str(b.retention) if b.retention else "forever"
            table.add_row(b.id, b.name, b.description, retention)

        console.print(table)
    else:
        table = Table()
        table.add_column("Bucket")
        table.add_column("Measurement")
        match path.split("/"):
            case [bucket]:
                table.title = (
                    f"Measurements\n{session.start.replace(tzinfo=None)} - {session.stop.replace(tzinfo=None)}"
                )
                measurements = session.list_measurements(bucket)
                for measurement in measurements:
                    table.add_row(bucket, measurement)
            case [bucket, measurement]:
                table.title = (
                    f"Record Count\n{session.start.replace(tzinfo=None)} - {session.stop.replace(tzinfo=None)}"
                )
                table.add_column("Field")
                table.add_column("Count", justify="right")
                for field, count in session.count_samples_in_measurement(bucket, measurement).items():
                    table.add_row(bucket, measurement, field, f"{count:n}")
            case _:
                raise typer.BadParameter(f"path should be '<bucket>' or '<bucket>/<measurement>', got '{path}'")
        console.print(table)


@app.callback()
def main(
    ctx: typer.Context,
    start: Optional[datetime] = None,
    stop: Optional[datetime] = None,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
    reload_env: Annotated[bool, typer.Option("--reload-env", "-r")] = False,
):
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

    ctx.meta[SESSION_KEY] = Session(start, stop)
