import csv
import getpass
import logging
import shutil
import subprocess
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Generator, Literal, Optional

import duckdb
import influxdb_client
from influxdb_client import InfluxDBClient, QueryApi
from slugify import slugify

if TYPE_CHECKING:
    from collections.abc import Sequence

    from urllib3 import HTTPResponse


log = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = timedelta(days=1)
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


def relation_name(name: str) -> str:
    return slugify(name)


def get_influx_csv_schema(file: Path) -> dict[str, tuple[str, str]]:
    with file.open() as csv_file:
        reader = csv.reader(csv_file)
        datatypes = next(reader)
        column_names = next(reader)
    assert datatypes[0] == "#datatype"
    assert column_names[0] == ""
    return {name: (dtype, INFLUX_TYPE_MAP[dtype]) for name, dtype in zip(column_names[1:], datatypes[1:])}


def iter_batches(
    start: datetime, stop: datetime, batch_size: timedelta = DEFAULT_BATCH_SIZE
) -> Generator[tuple[datetime, datetime], None, None]:
    batch_start = start
    while batch_start < stop:
        batch_stop = min(batch_start + batch_size, stop)
        yield batch_start, batch_stop
        batch_start = batch_stop


def list_measurements(db: InfluxDBClient, bucket: str) -> list[str]:
    """List measurements in bucket, optionally limited to time range."""
    api: QueryApi = db.query_api()

    return _get_list_of_measurements_from_influxdb_schema(api, bucket)


def download(
    queries: list[str],
    *,
    start: datetime,
    stop: datetime,
    basedir: Path,
    filters: list[str],
    batch_size: timedelta = DEFAULT_BATCH_SIZE,
    overwrite: bool = False,
) -> None:
    db = InfluxDBClient.from_env_properties()
    if not db.ping():
        raise ConnectionError("InfluxDB seems unreachable, please check environment variables.")

    start = start.astimezone()
    stop = stop.astimezone()

    with TemporaryDirectory(prefix=f"parflux-{getpass.getuser()}-", dir="/var/tmp") as tempdir_name:
        tmp = Path(tempdir_name).resolve()
        log.debug(f"session temporary directory: {tmp}")

        for query in queries:
            match query.split("/"):
                case [bucket]:
                    measurements = list_measurements(db, bucket)
                    log.info(f"loading {len(measurements)} measurements from bucket '{bucket}'")
                    for measurement in measurements:
                        log.info(f"loading '{measurement}' from bucket '{bucket}'")
                        try:
                            download_measurement(
                                db,
                                bucket,
                                measurement,
                                basedir,
                                start,
                                stop,
                                filters,
                                tmp,
                                batch_size=batch_size,
                                overwrite=overwrite,
                            )
                        except Exception:
                            log.exception(f"loading '{bucket}/{measurement}' in range [{start}, {stop}) failed:\n")
                            continue
                case [bucket, measurement]:
                    log.info(f"loading '{measurement}' from bucket '{bucket}'")
                    download_measurement(
                        db,
                        bucket,
                        measurement,
                        basedir,
                        start,
                        stop,
                        filters,
                        tmp,
                        batch_size=batch_size,
                        overwrite=overwrite,
                    )
                case _:
                    log.warning(f"invalid query, skipping: '{query}'")


def download_measurement(
    db: InfluxDBClient,
    bucket: str,
    measurement: str,
    basedir: Path,
    start: datetime,
    stop: datetime,
    filters: list[str] = [],
    cache_dir: Optional[Path] = None,
    batch_size: timedelta = DEFAULT_BATCH_SIZE,
    overwrite: bool = False,
) -> Path | None:
    start = start.astimezone()
    stop = stop.astimezone()

    destfile = basedir / bucket / f"{measurement}.parquet"
    if destfile.exists() and not overwrite:
        log.error(f'Skipping "{bucket}/{measurement}": file "{destfile}" already exists.')
        return

    log.debug(f"downloading {bucket}/{measurement} in range [{start}, {stop})...")

    with TemporaryDirectory(prefix="pfx-get-", dir=cache_dir) as tempdir_name:
        tmp = Path(tempdir_name)
        assert tmp.exists() and tmp.is_dir() and not any(tmp.glob("*"))
        for i, (bstart, bstop) in enumerate(iter_batches(start, stop, batch_size=batch_size)):
            file = tmp / f"{destfile.stem}-{i:04d}.parquet"

            combined_filters = [f'r._measurement == "{measurement}"'] + filters
            filter_string = " and ".join(combined_filters)

            query_str = textwrap.dedent(
                f"""\
                from (bucket: "{bucket}")
                    |> range(start: {bstart.isoformat()}, stop: {bstop.isoformat()})
                    |> filter(fn: (r) => {filter_string})
                    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                    |> drop(columns: ["_start", "_stop"])"""
            )

            query(db, query_str, file, cache_dir)

        pattern = f"{destfile.stem}-*.parquet"
        files = list(tmp.glob(pattern))

        if not files:
            log.info(f'Measurement "{bucket}/{measurement}" did not contain any samples.')
            return

        destfile.parent.mkdir(exist_ok=True, parents=True)

        if len(files) == 1:
            log.debug("query only returned one chunk, no merge required")
            shutil.move(files[0], destfile)
        else:
            log.debug(f"merging {len(files)} parquet files into one...")

            try:
                with duckdb.connect(str(tmp / "duck.db")) as con:
                    query_str = (
                        f"copy (select * from read_parquet('{tmp}/{pattern}', union_by_name=True)) to '{destfile}'"
                    )
                    log.debug(query_str)
                    con.sql(query_str)
            except duckdb.OutOfMemoryException:  # pragma: no cover
                log.warning(f"merging {len(files)} parquet files failed with OOM, moving all files instead")
                for src_file in files:
                    shutil.move(src_file, destfile.parent)
                destfile.unlink()

    if destfile.exists():
        dsize_MiB = destfile.stat().st_size / 1024**2
        log.info(f'Measurement "{bucket}/{measurement}" downloaded to "{destfile}" ({dsize_MiB:.0f} MiB).')

        return destfile
    else:  # pragma: no cover, this is the OOM fallback case
        return destfile.parent


def query(
    db: InfluxDBClient,
    query: str,
    dest_file: Path,
    cache_dir: Optional[Path] = None,
):
    with TemporaryDirectory(prefix="pfx-query-", dir=cache_dir) as tempdir_name:
        base = Path(tempdir_name)
        assert base.exists() and base.is_dir() and not any(base.glob("*"))
        raw_file = base / f"{dest_file.stem}.txt"
        log.debug(query)

        response: "HTTPResponse" = db.query_api().query_raw(query, dialect=DIALECT)

        with response, raw_file.open("wb") as fobj:
            shutil.copyfileobj(response, fobj)

        if raw_file.exists() and raw_file.stat().st_size > 2:
            rsize_MiB = raw_file.stat().st_size / 1024**2
            log.debug(f"raw query result stored in {raw_file} ({rsize_MiB:.0f} MiB)")

            with duckdb.connect(str(base / "duck.db")) as con:
                table_name = load_raw_query(con, raw_file)

                dest_file.parent.mkdir(exist_ok=True, parents=True)
                query_str = f"copy (select * from \"{table_name}\" order by _time) to '{dest_file}'"
                log.debug(query_str)
                con.sql(query_str)

            psize_MiB = dest_file.stat().st_size / 1024**2
            log.debug(f"query result stored in {dest_file} ({psize_MiB:.0f} MiB)")

        else:
            log.debug("Query did not return any result")


def load_raw_query(
    con: duckdb.DuckDBPyConnection,
    raw_file: Path,
    table_name: Optional[str] = None,
    keep: bool = False,
) -> str | None:
    if table_name is None:
        table_name = relation_name(raw_file.stem)

    list_of_csvs = _split_raw_influxdb_response(raw_file, keep)

    csv_tables = [relation_name(f.stem) for f in list_of_csvs]
    for file, tn in zip(list_of_csvs, csv_tables):
        load_annotated_csv(con, file, tn)

    return union_tables(con, csv_tables, table_name)


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


def load_annotated_csv(
    con: duckdb.DuckDBPyConnection,
    csv_file: Path,
    table_name: Optional[str] = None,
    keep: bool = False,
) -> str:
    if not table_name:
        table_name = relation_name(csv_file.stem)
    dtypes = get_influx_csv_schema(csv_file)
    duckdb_types = {k: v for k, (_, v) in dtypes.items()}
    columns = dtypes.keys()

    unsupported_types = {"base64Binary", "dateTime:number"}
    error_columns = {key: value for key, (value, _) in dtypes.items() if value in unsupported_types}
    if error_columns:
        _cols = set(error_columns.keys())
        _types = set(error_columns.values())
        log.error(f"columns {_cols} in {csv_file} have types {_types}, which are not supported at the moment.")
        raise TypeError(f"CSV types {_types!r} not supported for columns {', '.join(_cols)}")

    table = con.read_csv(csv_file, header=True, skiprows=1, dtype=duckdb_types).project(
        ", ".join(f'"{col}"' for col in columns if col not in ["result", "table"])
    )
    table.create(table_name)
    csize_MiB = csv_file.stat().st_size / 1024**2
    log.debug(f'table "{table_name}" created from {csv_file} ({csize_MiB:5.1f} MiB)')
    if not keep:
        csv_file.unlink()

    return table_name


def union_tables(
    con: duckdb.DuckDBPyConnection,
    tables: "Sequence[str]",
    target_table_name: str,
    kind: Literal["table", "view"] = "view",
    keep: bool = False,
) -> str:
    if kind.lower() not in {"table", "view"}:
        raise ValueError(f"only table or view allowed, got {kind}")
    query_str = f'create {kind} "{target_table_name}" as ' + " union by name ".join(
        f'(select * from "{tn}")' for tn in tables
    )
    log.debug(query_str)
    con.sql(query_str)

    if kind == "table" and not keep:
        for table in tables:
            con.sql(f'drop table "{table}"')

    return target_table_name


def _get_list_of_measurements_from_influxdb_schema(api: QueryApi, bucket: str) -> list[str]:
    query_str = textwrap.dedent(
        f"""\
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{bucket}")"""
    )
    log.debug(query_str)
    response = api.query(query_str)
    measurements: list[str] = []
    for row in response.to_values(["_value"]):
        if len(row) == 1 and isinstance(row[0], str):
            measurements.append(row[0])
    return measurements
