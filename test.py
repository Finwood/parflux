import dataclasses
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
from urllib3 import HTTPResponse

try:
    from dotenv import load_dotenv
except ImportError:
    pass
else:
    load_dotenv()

Berlin = ZoneInfo("Europe/Berlin")

STOP = datetime.now().astimezone()
START = STOP - timedelta(days=3)

# START = datetime.fromisoformat("2023-06-30T00:00:00+02:00")
# STOP = datetime.fromisoformat("2023-07-01T00:00:00+02:00")


@dataclasses.dataclass
class Bucket:
    id: str
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    retention: Optional[timedelta]

    @classmethod
    def from_openapi_model(cls, model: influxdb_client.Bucket):
        try:
            retention = (
                timedelta(seconds=model.retention_rules[0].every_seconds) or None
            )
        except IndexError:
            retention = None
        return cls(
            id=model.id,
            name=model.name,
            description=model.description,
            created_at=model.created_at,
            updated_at=model.updated_at,
            retention=retention,
        )


def list_buckets(db: InfluxDBClient) -> list[Bucket]:
    api: influxdb_client.BucketsApi = db.buckets_api()
    response: influxdb_client.Buckets = api.find_buckets()
    buckets: list[influxdb_client.Bucket] = response.buckets

    return [Bucket.from_openapi_model(bucket) for bucket in buckets]


def list_measurements(db: InfluxDBClient, bucket: Bucket | str) -> list[str]:
    if isinstance(bucket, Bucket):
        bucket = bucket.name
    api: QueryApi = db.query_api()
    query_str = textwrap.dedent(
        f"""\
        import "influxdata/influxdb/schema"

        schema.measurements(
            bucket: "{bucket}",
            start: {START.isoformat(timespec="seconds")},
            stop: {STOP.isoformat(timespec="seconds")}
        )
        """
    )
    response = api.query(query_str)

    return list(itertools.chain(*response.to_values(["_value"])))


def count_records_in_measurement(
    db: InfluxDBClient, bucket: Bucket | str, measurement: str
) -> dict[str, int]:
    if isinstance(bucket, Bucket):
        bucket = bucket.name
    api: QueryApi = db.query_api()
    query_str = textwrap.dedent(
        f"""\
        from (bucket: "{bucket}")
            |> range(
                start: {START.isoformat(timespec="seconds")},
                stop: {STOP.isoformat(timespec="seconds")}
            )
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> keep(columns: ["_field", "_value"])
            |> count()            
        """
    )
    response = api.query(query_str)
    return {field: count for field, count in response.to_values(["_field", "_value"])}


def test_df_stream(db: InfluxDBClient, bucket: Bucket | str, measurement: str) -> None:
    if isinstance(bucket, Bucket):
        bucket = bucket.name
    api: QueryApi = db.query_api()
    query_str = textwrap.dedent(
        f"""\
        from (bucket: "{bucket}")
            |> range(
                start: {START.isoformat(timespec="seconds")},
                stop: {STOP.isoformat(timespec="seconds")}
            )
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """
    )
    response: Generator[pd.DataFrame] = api.query_data_frame_stream(query_str)

    for df in response:
        assert isinstance(df, pd.DataFrame)
        if len(df) == 0:
            continue
        df.drop(columns=["result", "table", "_start", "_stop"], inplace=True)
        parquet_file = Path(
            f"{START.isoformat(timespec='seconds')}/{measurement}/{uuid4()}.parquet"
        )
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        # print(df)
        df.to_parquet(parquet_file)

    # 24:50


def test_raw(db: InfluxDBClient, bucket: Bucket | str, measurement: str) -> None:
    if isinstance(bucket, Bucket):
        bucket = bucket.name
    api: QueryApi = db.query_api()
    query_str = textwrap.dedent(
        f"""\
        from (bucket: "{bucket}")
            |> range(
                start: {START.isoformat(timespec="seconds")},
                stop: {STOP.isoformat(timespec="seconds")}
            )
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """
    )
    _dialect = influxdb_client.Dialect(
        header=True,
        delimiter=",",
        comment_prefix="#",
        annotations=[],  # ["datatype", "group", "default"]
        date_time_format="RFC3339",
    )

    with TemporaryDirectory() as tempdir_name:
        base = Path(tempdir_name)
        assert base.exists() and base.is_dir()
        raw_file = base / "raw.txt"

        response: HTTPResponse = api.query_raw(query_str, dialect=_dialect)

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
        dest_file = Path(
            f"data/{bucket}/{START.isoformat(timespec='seconds')}/{measurement}.parquet"
        )
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


if __name__ == "__main__":
    client = InfluxDBClient.from_env_properties()

    # bucket = "pyric"
    for bucket in sorted(list_buckets(client), key=lambda b: b.updated_at):
        # measurements = list_measurements(client, bucket.name)
        # print(f"{bucket.name}:")

        # measurement = "reg.udral.physics.electricity.SourceTs.0.1"
        for measurement in list_measurements(client, bucket):
            record_count = count_records_in_measurement(client, bucket, measurement)
            total = sum(record_count.values())
            if total:
                print(f"{measurement}: {total}")
                test_raw(client, bucket, measurement)
