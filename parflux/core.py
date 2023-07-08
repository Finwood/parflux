import csv
import logging
import shutil
import subprocess
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Generator

import duckdb
import influxdb_client
from influxdb_client import InfluxDBClient

from .types import Bucket

if TYPE_CHECKING:
    from collections.abc import Sequence

    from urllib3 import HTTPResponse


log = logging.getLogger(__name__)

BATCH_SIZE = timedelta(days=1)
DIALECT = influxdb_client.Dialect(
    header=True,
    delimiter=",",
    comment_prefix="#",
    annotations=["datatype"],
    date_time_format="RFC3339",
)

# https://docs.influxdata.com/influxdb/v2.7/reference/syntax/annotated-csv/
INFLUX_TYPE_MAP = {
    "boolean": "BOOLEAN",
    "unsignedLong": "UBIGINT",
    "long": "BIGINT",
    "double": "DOUBLE",
    "string": "VARCHAR",
    "base64Binary": "VARCHAR",  # further conversion needed
    "dateTime": "TIMESTAMPTZ",
    "dateTime:number": "UBIGINT",  # further conversion needed
    "dateTime:RFC3339": "TIMESTAMPTZ",
    "dateTime:RFC3339Nano": "TIMESTAMPTZ",
    "duration": "UBIGINT",
}


def get_influx_csv_schema(file: Path) -> dict[str, tuple[str, str]]:
    with file.open() as csv_file:
        reader = csv.reader(csv_file)
        datatypes = next(reader)
        column_names = next(reader)
    assert datatypes[0] == "#datatype"
    assert column_names[0] == ""
    return {name: (dtype, INFLUX_TYPE_MAP[dtype]) for name, dtype in zip(column_names[1:], datatypes[1:])}


def iter_batches(start: datetime, stop: datetime) -> Generator[tuple[datetime, datetime], None, None]:
    batch_start = start
    while batch_start < stop:
        batch_stop = min(batch_start + BATCH_SIZE, stop)
        yield batch_start, batch_stop
        batch_start = batch_stop


def download_measurement(
    db: InfluxDBClient,
    bucket: Bucket | str,
    measurement: str,
    dest: Path,
    start: datetime,
    stop: datetime,
) -> None:
    if isinstance(bucket, Bucket):
        bucket = bucket.name
    start = start.astimezone()
    stop = stop.astimezone()

    log.info(f"downloading {bucket}/{measurement} in range [{start}, {stop})...")

    with TemporaryDirectory(prefix="pfx-get-") as tempdir_name:
        tmp = Path(tempdir_name)
        assert tmp.exists() and tmp.is_dir() and not any(tmp.glob("*"))
        for i, (bstart, bstop) in enumerate(iter_batches(start, stop)):
            file = tmp / f"{dest.stem}-{i:04d}.parquet"

            query_str = textwrap.dedent(
                f"""\
                from (bucket: "{bucket}")
                    |> range(start: {bstart.isoformat()}, stop: {bstop.isoformat()})
                    |> filter(fn: (r) => r._measurement == "{measurement}")
                    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                    |> drop(columns: ["_start", "_stop"])"""
            )

            query(db, query_str, file)

        parquet_file_glob = f"{tmp}/{dest.stem}-*.parquet"
        log.debug(f"merging '{parquet_file_glob}' into one...")

        with duckdb.connect() as con:
            # TODO: S3
            dest.parent.mkdir(exist_ok=True, parents=True)
            query_str = f"copy (select * from read_parquet('{parquet_file_glob}', union_by_name=True)) to '{dest}'"
            log.debug(query_str)
            con.sql(query_str)

        log.info(
            f'Measurement "{bucket}/{measurement}" downloaded to "{dest}" ({dest.stat().st_size/1024**2:.0f} MiB).'
        )


def query(db: InfluxDBClient, query: str, dest_file: Path):
    with TemporaryDirectory(prefix="pfx-query-") as tempdir_name:
        base = Path(tempdir_name)
        assert base.exists() and base.is_dir() and not any(base.glob("*"))
        raw_file = base / f"{dest_file.stem}.txt"
        log.debug(query)

        response: "HTTPResponse" = db.query_api().query_raw(query, dialect=DIALECT)

        with response, raw_file.open("wb") as fobj:
            shutil.copyfileobj(response, fobj)

        if raw_file.exists() and raw_file.stat().st_size > 2:
            log.debug(f"query result stored in {raw_file} ({raw_file.stat().st_size/1024**2:.0f} MiB)")

            # TODO: think about a named FIFO and decoupled (background) csplit to save disk/buffer space
            list_of_csvs = _split_raw_influxdb_response(raw_file, keep=False)
            _parse_influxdb_csv_files(list_of_csvs, dest_file)
        else:
            log.info(f"Query did not return any result")


def _split_raw_influxdb_response(file: Path, keep: bool = False) -> list[Path]:
    assert isinstance(file, Path) and file.is_file()
    basedir = file.parent
    PATTERN = f"{file.stem}-*.csv"
    assert not any(basedir.glob(PATTERN))
    csplit = shutil.which("csplit")
    assert csplit
    subprocess.run(
        [
            csplit,
            f"--prefix={file.stem}-",
            "--suffix-format=%04d.csv",
            "--suppress-matched",
            "--elide-empty-files",
            file.name,
            "/^\r$/",
            "{*}",
        ],
        cwd=basedir,
        capture_output=True,
        check=True,
    )
    if not keep:
        file.unlink()
    csv_files = sorted(list(basedir.glob(PATTERN)))
    if not csv_files:
        log.debug(f"{file} did not contain any data")
    elif len(csv_files) == 1:
        log.debug(f"{file} only contained a single CSV")
    else:
        log.debug(f"{file} split into {len(csv_files)} CSV files")
    return csv_files


def _parse_influxdb_csv_files(csv_source: "Sequence[Path]", parquet_dest: Path):
    with duckdb.connect() as con:
        for file in csv_source:
            dtypes = get_influx_csv_schema(file)
            duckdb_types = {k: v for k, (_, v) in dtypes.items()}
            columns = dtypes.keys()
            binary_columns = {key for key, (value, _) in dtypes.items() if value == "base64Binary"}
            if binary_columns:
                log.error(
                    f"columns {binary_columns} in {file} have 'base64Binary' type, "
                    f"which is not supported at the moment. Skipping."
                )
                continue
            numeric_timestamp_columns = {key for key, (value, _) in dtypes.items() if value == "dateTime:number"}
            if numeric_timestamp_columns:
                log.error(
                    f"columns {numeric_timestamp_columns} in {file} have 'dateTime:number' type, "
                    f"which is not supported at the moment. Skipping."
                )
                continue

            (
                con.read_csv(file, header=True, skiprows=1, dtype=duckdb_types)
                .project(", ".join(f'"{col}"' for col in columns if col not in ["result"]))
                .create(table_name=file.stem)
            )

        table_names = {f.stem for f in csv_source}
        union_name = parquet_dest.stem

        query_str = f'create view "{union_name}" as ' + " union by name ".join(
            f'(select * from "{tn}")' for tn in sorted(table_names)
        )
        log.debug(query_str)
        con.sql(query_str)

        # TODO: S3
        parquet_dest.parent.mkdir(exist_ok=True, parents=True)
        query_str = f"copy (select * from \"{union_name}\" order by _time) to '{parquet_dest}'"
        log.debug(query_str)
        con.sql(query_str)

    log.debug(
        f"{len(csv_source)} CSV files combined into {parquet_dest} ({parquet_dest.stat().st_size/1024**2:.0f} MiB)"
    )
