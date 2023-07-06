import itertools
import shutil
import subprocess
import textwrap
from collections.abc import Generator
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
import influxdb_client
import pandas as pd
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.flux_table import TableList
from urllib3 import HTTPResponse

from .types import Bucket

DIALECT = influxdb_client.Dialect(
    header=True,
    delimiter=",",
    comment_prefix="#",
    annotations=[],
    date_time_format="RFC3339",
)


class Session:
    _default_duration: timedelta = timedelta(days=1)

    def __init__(
        self, start: Optional[datetime] = None, stop: Optional[datetime] = None
    ):
        self.db = InfluxDBClient.from_env_properties()

        if start is None and stop is None:
            stop = datetime.now().astimezone()
            start = stop - self._default_duration
        assert not (start is None and stop is None)
        if start is None:
            start = stop - self._default_duration
        if stop is None:
            stop = start + self._default_duration
        assert isinstance(start, datetime) and isinstance(stop, datetime)

        self.start = start
        self.stop = stop

    @property
    def start(self) -> datetime:
        return self._start

    @start.setter
    def start(self, value: datetime) -> None:
        if not isinstance(value, datetime):
            raise ValueError()
        self._start = value.astimezone()

    @property
    def stop(self) -> datetime:
        return self._stop

    @stop.setter
    def stop(self, value: datetime) -> None:
        if not isinstance(value, datetime):
            raise ValueError()
        self._stop = value.astimezone()

    def list_buckets(self) -> list[Bucket]:
        api: influxdb_client.BucketsApi = self.db.buckets_api()
        response: influxdb_client.Buckets = api.find_buckets()
        buckets: list[influxdb_client.Bucket] = response.buckets

        return [Bucket.from_openapi_model(bucket) for bucket in buckets]

    def list_measurements(self, bucket: Bucket | str) -> list[str]:
        if isinstance(bucket, Bucket):
            bucket = bucket.name
        api: QueryApi = self.db.query_api()
        query_str = textwrap.dedent(
            f"""\
            import "influxdata/influxdb/schema"

            schema.measurements(
                bucket: "{bucket}",
                start: {self.start.isoformat(timespec="seconds")},
                stop: {self.stop.isoformat(timespec="seconds")}
            )
            """
        )
        response = api.query(query_str)

        return list(itertools.chain(*response.to_values(["_value"])))

    def count_records_in_measurement(
        self, bucket: Bucket | str, measurement: str
    ) -> dict[str, int]:
        if isinstance(bucket, Bucket):
            bucket = bucket.name
        api: QueryApi = self.db.query_api()
        query_str = textwrap.dedent(
            f"""\
            from (bucket: "{bucket}")
                |> range(
                    start: {self.start.isoformat(timespec="seconds")},
                    stop: {self.stop.isoformat(timespec="seconds")}
                )
                |> filter(fn: (r) => r._measurement == "{measurement}")
                |> keep(columns: ["_field", "_value"])
                |> count()            
            """
        )
        response: TableList = api.query(query_str)
        assert isinstance(response, TableList)
        return {
            field: count for field, count in response.to_values(["_field", "_value"])
        }

    def download_measurement(
        self, bucket: Bucket | str, measurement: str, dest_file: Path
    ) -> None:
        if isinstance(bucket, Bucket):
            bucket = bucket.name
        api: QueryApi = self.db.query_api()
        query_str = textwrap.dedent(
            f"""\
            from (bucket: "{bucket}")
                |> range(
                    start: {self.start.isoformat(timespec="seconds")},
                    stop: {self.stop.isoformat(timespec="seconds")}
                )
                |> filter(fn: (r) => r._measurement == "{measurement}")
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> drop(columns: ["_start", "_stop"])
            """
        )

        with TemporaryDirectory(prefix="parflux-") as tempdir_name:
            base = Path(tempdir_name)
            assert base.exists() and base.is_dir() and not any(base.glob("*"))
            raw_file = base / "raw.txt"

            response: HTTPResponse = api.query_raw(query_str, dialect=DIALECT)

            with response, raw_file.open("wb") as fobj:
                shutil.copyfileobj(response, fobj)

            csplit = subprocess.run(
                [
                    "csplit",
                    "--prefix=",
                    "--suffix-format=%04d.csv",
                    "--suppress-matched",
                    "--elide-empty-files",
                    raw_file.name,
                    "/^\r$/",
                    "{*}",
                ],
                cwd=base,
                capture_output=True,
                check=True,
            )
            raw_file.unlink()
            dest_file.parent.mkdir(exist_ok=True, parents=True)

            with duckdb.connect() as con:
                con.sql(
                    textwrap.dedent(
                        f"""\
                        create table data as
                        select *
                        from read_csv_auto(
                            '{base}/*.csv',
                            union_by_name=True,
                            types={{"_time": "TIMESTAMPTZ"}}
                        )"""
                    )
                )

                for column_name in "column00", "result", "table", "_start", "_stop":
                    con.sql(f'alter table data drop if exists "{column_name}"')

                con.sql(f"copy (select * from data) to '{dest_file}'")
