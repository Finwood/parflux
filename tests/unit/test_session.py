"""Unit tests for parflux.session.Session with a mocked InfluxDBClient."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from influxdb_client import InfluxDBClient

from parflux.session import Session

UTC = timezone.utc


@pytest.fixture
def mock_db():
    db = MagicMock(spec=InfluxDBClient)
    db.ping.return_value = True
    return db


class TestSessionInit:
    def test_ping_is_called_even_with_injected_client(self, mock_db):
        Session(db=mock_db)
        mock_db.ping.assert_called_once()

    def test_raises_connection_error_when_ping_fails(self, mock_db):
        mock_db.ping.return_value = False
        with pytest.raises(ConnectionError, match="InfluxDB seems unreachable"):
            Session(db=mock_db)

    def test_uses_env_client_when_none_provided(self, mocker):
        fake_client = MagicMock(spec=InfluxDBClient)
        fake_client.ping.return_value = True
        factory = mocker.patch(
            "parflux.session.InfluxDBClient.from_env_properties",
            return_value=fake_client,
        )

        session = Session()

        factory.assert_called_once()
        assert session.db is fake_client

    def test_default_stop_is_now_and_start_is_one_day_earlier(self, mock_db):
        session = Session(db=mock_db)
        assert isinstance(session.start, datetime)
        assert isinstance(session.stop, datetime)
        assert session.stop - session.start == timedelta(days=1)

    def test_accepts_explicit_start_and_stop(self, mock_db):
        start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        stop = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)

        session = Session(start, stop, db=mock_db)

        assert session.start == start
        assert session.stop == stop

    def test_creates_temporary_directory(self, mock_db):
        session = Session(db=mock_db)
        assert session.tmp.exists()
        assert session.tmp.is_dir()


class TestSessionTimeSetters:
    def test_rejects_non_datetime_start(self, mock_db):
        session = Session(db=mock_db)
        with pytest.raises(ValueError):
            session.start = "2024-01-01"

    def test_rejects_non_datetime_stop(self, mock_db):
        session = Session(db=mock_db)
        with pytest.raises(ValueError):
            session.stop = 12345


class TestSessionDelegation:
    def test_list_measurements_delegates_to_core(self, mock_db, mocker):
        core_fn = mocker.patch("parflux.session.list_measurements", return_value=["cpu", "mem"])
        session = Session(db=mock_db)

        result = session.list_measurements("mybucket")

        assert result == ["cpu", "mem"]
        core_fn.assert_called_once_with(mock_db, "mybucket")


class TestSessionDownload:
    def test_bucket_only_query_lists_and_downloads_each_measurement(self, mock_db, mocker, tmp_path):
        mocker.patch("parflux.session.list_measurements", return_value=["cpu", "mem"])
        download = mocker.patch("parflux.session.download_measurement")

        session = Session(db=mock_db)
        session.download(["mybucket"], basedir=tmp_path)

        assert download.call_count == 2
        buckets = {call.args[1] for call in download.call_args_list}
        measurements = {call.args[2] for call in download.call_args_list}
        assert buckets == {"mybucket"}
        assert measurements == {"cpu", "mem"}

    def test_bucket_and_measurement_query_downloads_single_measurement(self, mock_db, mocker, tmp_path):
        list_fn = mocker.patch("parflux.session.list_measurements")
        download = mocker.patch("parflux.session.download_measurement")

        session = Session(db=mock_db)
        session.download(["mybucket/cpu"], basedir=tmp_path)

        list_fn.assert_not_called()
        download.assert_called_once()
        _, bucket, measurement, *_ = download.call_args.args
        assert bucket == "mybucket"
        assert measurement == "cpu"

    def test_invalid_query_is_skipped_with_warning(self, mock_db, mocker, tmp_path, caplog):
        download = mocker.patch("parflux.session.download_measurement")
        mocker.patch("parflux.session.list_measurements")

        session = Session(db=mock_db)
        session.download(["too/many/slashes"], basedir=tmp_path)

        download.assert_not_called()
        assert any("invalid query" in rec.message for rec in caplog.records)

    def test_download_continues_after_single_measurement_failure(self, mock_db, mocker, tmp_path):
        mocker.patch("parflux.session.list_measurements", return_value=["cpu", "mem"])
        download = mocker.patch(
            "parflux.session.download_measurement",
            side_effect=[RuntimeError("boom"), None],
        )

        session = Session(db=mock_db)
        session.download(["mybucket"], basedir=tmp_path)

        assert download.call_count == 2

    def test_defaults_basedir_to_cwd_when_none(self, mock_db, mocker):
        mocker.patch("parflux.session.list_measurements")
        download = mocker.patch("parflux.session.download_measurement")

        session = Session(db=mock_db)
        session.download(["mybucket/cpu"])

        passed_basedir = download.call_args.args[3]
        assert passed_basedir == Path(".")
