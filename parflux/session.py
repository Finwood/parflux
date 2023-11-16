import getpass
import logging
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

import influxdb_client
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.flux_table import TableList

from .core import download_measurement, list_measurements
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
        return list_measurements(self.db, bucket, self.start, self.stop)

    def count_samples_in_measurement(self, bucket: Bucket | str, measurement: str) -> dict[str, int]:
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

    def download(self, queries: list[str], filters: list[str] = [], basedir: Optional[Path] = None) -> None:
        if basedir is None:
            basedir = Path(".")  # current directory

        for query in queries:
            match query.split("/"):
                case [bucket]:
                    measurements = self.list_measurements(bucket)
                    log.info(f"loading {len(measurements)} measurements from bucket '{bucket}'")
                    for m in measurements:
                        log.info(f"loading '{m}' from bucket '{bucket}'")
                        download_measurement(self.db, bucket, m, basedir, self.start, self.stop, filters, self.tmp)
                case [bucket, measurement]:
                    log.info(f"loading '{measurement}' from bucket '{bucket}'")
                    download_measurement(
                        self.db, bucket, measurement, basedir, self.start, self.stop, filters, self.tmp
                    )
                case _:
                    log.warning(f"invalid query, skipping: '{query}'")
