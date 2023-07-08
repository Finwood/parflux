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


def path_callback(value: str):
    if not 1 <= len(value.split("/")) <= 2:
        raise typer.BadParameter("Path should be 'bucket' or 'bucket/measurement'")
    return value


def empty_path_callback(value: str | None):
    if value is None:
        return
    return path_callback(value)


@app.command()
def get(
    ctx: typer.Context,
    path: Annotated[str, typer.Argument(callback=path_callback)],
    file: Annotated[
        Path,
        typer.Argument(exists=False, file_okay=True, dir_okay=False, writable=True),
    ],
):
    """Download Bucket or Single Measurement"""
    console.print(f"about to load {path}...")
    session: Session = ctx.meta[SESSION_KEY]

    match path.split("/"):
        case [bucket]:
            print("Bucket download not implemented yet")
        case [bucket, measurement]:
            session.download_measurement(bucket, measurement, file)


@app.command()
def list(
    ctx: typer.Context,
    path: Annotated[Optional[str], typer.Argument(callback=empty_path_callback)] = None,
):
    session: Session = ctx.meta[SESSION_KEY]
    if path is None:
        table = Table("ID", "Name", "Description", "Retention")
        for b in session.list_buckets():
            retention = str(b.retention) if b.retention else "forever"
            table.add_row(b.id, b.name, b.description, retention)

        console.print(table)
    else:
        path_components = path.split("/")
        if len(path_components) == 1:
            measurements = session.list_measurements(path)
            console.print(measurements)
        elif len(path_components) == 2:
            bucket, measurement = path_components
            console.print(session.count_records_in_measurement(bucket, measurement))
        else:
            raise RuntimeError("Should have been caught by typer")


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
