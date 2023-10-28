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
    dest: Annotated[Optional[Path], typer.Option(help="directory or file")] = None,
    filter: Annotated[list[str], typer.Option("--filter", "-f", help="additional flux filters")] = [],
):
    """Download Bucket or Single Measurement from InfluxDB.

    To download an entire bucket, specify just the bucket name. To download only specific measurements, specify the full
    measurement path like <bucket>/<measurement_name>. Multiple measurement and bucket queries can be selected by simply
    providing multiple query arguments.
    """
    session: Session = ctx.meta[SESSION_KEY]

    for q in query:
        match q.split("/"):
            case [bucket]:
                log.info(f"downloading entire bucket '{bucket}'")
                session.download_bucket(bucket, filter, dest=dest)
            case [bucket, measurement]:
                log.info(f"downloading measurement '{measurement}' from bucket '{bucket}'")
                session.download_measurement(bucket, measurement, filter, dest=dest)
            case _:
                log.warning(f"invalid query, skipping: '{q}'")


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
        match path.split("/"):
            case [bucket]:
                measurements = session.list_measurements(bucket)
                console.print(measurements)
            case [bucket, measurement]:
                table = Table(
                    title=f"Record Count\n{session.start.replace(tzinfo=None)} - {session.stop.replace(tzinfo=None)}"
                )
                table.add_column("Bucket")
                table.add_column("Measurement")
                table.add_column("Field")
                table.add_column("Count", justify="right")
                for field, count in session.count_records_in_measurement(bucket, measurement).items():
                    table.add_row(bucket, measurement, field, f"{count:n}")
                console.print(table)
            case _:
                raise typer.BadParameter(f"path should be '<bucket>' or '<bucket>/<measurement>', got '{path}'")


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
