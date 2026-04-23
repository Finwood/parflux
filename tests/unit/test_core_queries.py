"""Unit tests for parflux.core.list_measurements and parflux.core.query."""

import io
import shutil
from unittest.mock import MagicMock

import duckdb
import pytest
from influxdb_client import InfluxDBClient

from parflux.core import list_measurements, query


@pytest.fixture
def mock_db():
    return MagicMock(spec=InfluxDBClient)


class TestListMeasurements:
    def test_returns_parsed_measurement_names(self, mock_db):
        api = mock_db.query_api.return_value
        api.query.return_value.to_values.return_value = [["cpu"], ["mem"], ["disk"]]

        assert list_measurements(mock_db, "my_bucket") == ["cpu", "mem", "disk"]

    def test_skips_non_string_rows(self, mock_db):
        api = mock_db.query_api.return_value
        api.query.return_value.to_values.return_value = [["cpu"], [None], [], [42], ["mem"]]

        assert list_measurements(mock_db, "my_bucket") == ["cpu", "mem"]

    def test_passes_bucket_name_into_query(self, mock_db):
        api = mock_db.query_api.return_value
        api.query.return_value.to_values.return_value = []

        list_measurements(mock_db, "example_bucket")

        flux_query = api.query.call_args.args[0]
        assert 'bucket: "example_bucket"' in flux_query


class HTTPResponseStub(io.BytesIO):
    """BytesIO that also supports the context-manager interface query() expects."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


@pytest.mark.skipif(shutil.which("csplit") is None, reason="requires GNU csplit")
class TestQuery:
    def test_writes_parquet_when_response_has_data(self, mock_db, tmp_path):
        from tests.conftest import MULTI_TABLE_RAW

        mock_db.query_api.return_value.query_raw.return_value = HTTPResponseStub(MULTI_TABLE_RAW.encode())

        dest = tmp_path / "out" / "result.parquet"
        query(mock_db, 'from(bucket: "x")', dest, cache_dir=tmp_path)

        assert dest.exists()
        with duckdb.connect(":memory:") as con:
            row_count = con.sql(f"select count(*) from read_parquet('{dest}')").fetchone()[0]
            assert row_count == 3

    def test_no_parquet_written_for_empty_response(self, mock_db, tmp_path):
        mock_db.query_api.return_value.query_raw.return_value = HTTPResponseStub(b"")

        dest = tmp_path / "out" / "empty.parquet"
        query(mock_db, 'from(bucket: "x")', dest, cache_dir=tmp_path)

        assert not dest.exists()
