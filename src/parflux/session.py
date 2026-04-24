import getpass
import logging
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from influxdb_client import InfluxDBClient

from .core import download_measurement, list_measurements

log = logging.getLogger(__name__)


class Session:
    _default_duration: timedelta = timedelta(days=1)

    def __init__(
        self,
        start: Optional[datetime] = None,
        stop: Optional[datetime] = None,
        *,
        db: Optional[InfluxDBClient] = None,
    ):
        if db is None:
            db = InfluxDBClient.from_env_properties()
        if not db.ping():
            raise ConnectionError("InfluxDB seems unreachable, please check environment variables.")
        self.db: InfluxDBClient = db

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

    def list_measurements(self, bucket: str) -> list[str]:
        return list_measurements(self.db, bucket)

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
                        try:
                            download_measurement(self.db, bucket, m, basedir, self.start, self.stop, filters, self.tmp)
                        except Exception as _e:
                            log.exception(f"loading '{bucket}/{m}' in range [{self.start}, {self.stop}) failed:\n")
                            continue
                case [bucket, measurement]:
                    log.info(f"loading '{measurement}' from bucket '{bucket}'")
                    download_measurement(
                        self.db, bucket, measurement, basedir, self.start, self.stop, filters, self.tmp
                    )
                case _:
                    log.warning(f"invalid query, skipping: '{query}'")
