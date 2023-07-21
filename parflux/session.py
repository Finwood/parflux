import getpass
import itertools
import logging
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

import influxdb_client
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.flux_table import TableList

from .core import download_measurement
from .types import Bucket

log = logging.getLogger(__name__)


class Session:
    _default_duration: timedelta = timedelta(days=1)

    def __init__(self, start: Optional[datetime] = None, stop: Optional[datetime] = None):
        self.db: InfluxDBClient = InfluxDBClient.from_env_properties()
        if not self.db.ping():
            raise ConnectionError("InfluxDB seems unreachable, please check environment variables.")

        if stop is None:
            stop = datetime.now().replace(microsecond=0).astimezone()
        if start is None:
            start = stop - self._default_duration
        assert isinstance(start, datetime) and isinstance(stop, datetime)

        self.start = start
        self.stop = stop

        self._temporary_directory = TemporaryDirectory(prefix=f"parflux-{getpass.getuser()}-", dir="/var/tmp")
        self.tmp = Path(self._temporary_directory.name).resolve()
        log.debug(f"session temporary directory: {self.tmp}")

        # TODO: session base directory

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

    def count_records_in_measurement(self, bucket: Bucket | str, measurement: str) -> dict[str, int]:
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
        return {field: count for field, count in response.to_values(["_field", "_value"])}

    def download_measurement(
        self,
        bucket: Bucket | str,
        measurement: str,
        dest: Optional[Path] = None,
    ) -> None:
        default_filename = f"{measurement}.parquet"
        if dest is None:
            dest = Path(default_filename)
        if dest.is_dir():
            dest = dest / default_filename
        return download_measurement(self.db, bucket, measurement, dest, self.start, self.stop, self.tmp)

    def download_bucket(self, bucket: Bucket | str, dest: Optional[Path] = None) -> None:
        if isinstance(bucket, Bucket):
            bucket = bucket.name
        if dest is None:
            dest = Path(bucket)
        if not dest.is_dir():
            dest.mkdir(parents=True)
        for measurement in self.list_measurements(bucket):
            self.download_measurement(bucket, measurement, dest / f"{measurement}.parquet")
