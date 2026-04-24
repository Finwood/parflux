"""Unit tests for parflux.core.download orchestration with mocked dependencies."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from influxdb_client import InfluxDBClient

from parflux.core import download

UTC = timezone.utc


@pytest.fixture
def mock_db():
    db = MagicMock(spec=InfluxDBClient)
    db.ping.return_value = True
    return db


@pytest.fixture
def time_range():
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


class TestDownloadInit:
    def test_uses_env_client(self, mock_db, mocker, time_range, tmp_path):
        factory = mocker.patch("parflux.core.InfluxDBClient.from_env_properties", return_value=mock_db)
        mocker.patch("parflux.core.download_measurement")
        start, end = time_range

        download(["bucket/cpu"], start=start, end=end, basedir=tmp_path, filters=[])

        factory.assert_called_once()

    def test_raises_connection_error_when_ping_fails(self, mocker, time_range, tmp_path):
        db = MagicMock(spec=InfluxDBClient)
        db.ping.return_value = False
        mocker.patch("parflux.core.InfluxDBClient.from_env_properties", return_value=db)
        start, end = time_range

        with pytest.raises(ConnectionError, match="InfluxDB seems unreachable"):
            download(["bucket/cpu"], start=start, end=end, basedir=tmp_path, filters=[])


class TestDownloadOrchestration:
    def test_bucket_only_query_lists_and_downloads_each_measurement(self, mock_db, mocker, time_range, tmp_path):
        mocker.patch("parflux.core.InfluxDBClient.from_env_properties", return_value=mock_db)
        list_fn = mocker.patch("parflux.core.list_measurements", return_value=["cpu", "mem"])
        dload = mocker.patch("parflux.core.download_measurement")
        start, end = time_range

        download(["mybucket"], start=start, end=end, basedir=tmp_path, filters=['r.host == "h1"'])

        list_fn.assert_called_once_with(mock_db, "mybucket")
        assert dload.call_count == 2
        buckets = {call.args[1] for call in dload.call_args_list}
        measurements = {call.args[2] for call in dload.call_args_list}
        assert buckets == {"mybucket"}
        assert measurements == {"cpu", "mem"}

    def test_bucket_and_measurement_query_downloads_single_measurement(self, mock_db, mocker, time_range, tmp_path):
        mocker.patch("parflux.core.InfluxDBClient.from_env_properties", return_value=mock_db)
        list_fn = mocker.patch("parflux.core.list_measurements")
        dload = mocker.patch("parflux.core.download_measurement")
        start, end = time_range

        download(["mybucket/cpu"], start=start, end=end, basedir=tmp_path, filters=[])

        list_fn.assert_not_called()
        dload.assert_called_once()
        _, bucket, measurement, *_ = dload.call_args.args
        assert bucket == "mybucket"
        assert measurement == "cpu"

    def test_invalid_query_is_skipped_with_warning(self, mock_db, mocker, time_range, tmp_path, caplog):
        mocker.patch("parflux.core.InfluxDBClient.from_env_properties", return_value=mock_db)
        dload = mocker.patch("parflux.core.download_measurement")
        mocker.patch("parflux.core.list_measurements")
        start, end = time_range

        download(["too/many/slashes"], start=start, end=end, basedir=tmp_path, filters=[])

        dload.assert_not_called()
        assert any("invalid query" in rec.message for rec in caplog.records)

    def test_download_continues_after_single_measurement_failure(self, mock_db, mocker, time_range, tmp_path):
        mocker.patch("parflux.core.InfluxDBClient.from_env_properties", return_value=mock_db)
        mocker.patch("parflux.core.list_measurements", return_value=["cpu", "mem"])
        dload = mocker.patch(
            "parflux.core.download_measurement",
            side_effect=[RuntimeError("boom"), None],
        )
        start, end = time_range

        download(["mybucket"], start=start, end=end, basedir=tmp_path, filters=[])

        assert dload.call_count == 2

    def test_forwards_explicit_args_to_download_measurement(self, mock_db, mocker, time_range, tmp_path):
        mocker.patch("parflux.core.InfluxDBClient.from_env_properties", return_value=mock_db)
        dload = mocker.patch("parflux.core.download_measurement")
        start, end = time_range

        download(["bucket/cpu"], start=start, end=end, basedir=tmp_path, filters=['r.site == "lab"'])

        call = dload.call_args
        assert call.args[3] == tmp_path
        assert call.args[4] == start
        assert call.args[5] == end
        assert call.args[6] == ['r.site == "lab"']
        assert call.args[7].is_absolute()
        assert call.args[7].parent == Path("/var/tmp")
        assert call.kwargs["batch_size"] == timedelta(days=1)
