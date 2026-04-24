"""Unit tests for parflux.core.download_measurement.

Every test patches ``parflux.core.query`` so no real InfluxDB call ever happens;
the patched callable fabricates parquet files on disk to drive each branch.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest
from influxdb_client import InfluxDBClient

from parflux.core import DEFAULT_BATCH_SIZE, download_measurement

UTC = timezone.utc


def _write_parquet(path: Path, value: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as con:
        con.sql(f"copy (select {value} as _value, now() as _time) to '{path}'")


def _make_query_side_effect(rows_per_batch: int):
    """Return a side_effect that writes a parquet file per call."""

    counter = {"n": 0}

    def side_effect(db, query_str, dest_file, cache_dir=None):
        _write_parquet(Path(dest_file), rows_per_batch)
        counter["n"] += 1

    side_effect.counter = counter
    return side_effect


@pytest.fixture
def mock_db():
    return MagicMock(spec=InfluxDBClient)


class TestDownloadMeasurement:
    def test_skips_when_destfile_exists_and_overwrite_false(self, mock_db, tmp_path, mocker):
        destdir = tmp_path / "mybucket"
        destdir.mkdir(parents=True)
        existing = destdir / "cpu.parquet"
        existing.write_bytes(b"placeholder")

        q = mocker.patch("parflux.core.query")

        result = download_measurement(
            mock_db,
            "mybucket",
            "cpu",
            tmp_path,
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 6, tzinfo=UTC),
        )

        assert result is None
        assert existing.read_bytes() == b"placeholder"
        q.assert_not_called()

    def test_single_batch_single_file_path(self, mock_db, tmp_path, mocker):
        mocker.patch("parflux.core.query", side_effect=_make_query_side_effect(1))

        result = download_measurement(
            mock_db,
            "mybucket",
            "cpu",
            tmp_path,
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 6, tzinfo=UTC),
        )

        expected = tmp_path / "mybucket" / "cpu.parquet"
        assert result == expected
        assert expected.exists()
        with duckdb.connect(":memory:") as con:
            count = con.sql(f"select count(*) from read_parquet('{expected}')").fetchone()[0]
            assert count == 1

    def test_multi_batch_merges_parquet_files(self, mock_db, tmp_path, mocker):
        side_effect = _make_query_side_effect(1)
        mocker.patch("parflux.core.query", side_effect=side_effect)

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = start + 3 * DEFAULT_BATCH_SIZE

        result = download_measurement(mock_db, "mybucket", "cpu", tmp_path, start, end)

        expected = tmp_path / "mybucket" / "cpu.parquet"
        assert result == expected
        assert expected.exists()
        assert side_effect.counter["n"] == 3
        with duckdb.connect(":memory:") as con:
            count = con.sql(f"select count(*) from read_parquet('{expected}')").fetchone()[0]
            assert count == 3

    def test_returns_none_when_query_produces_no_files(self, mock_db, tmp_path, mocker):
        mocker.patch("parflux.core.query")  # no-op: creates no parquet files

        result = download_measurement(
            mock_db,
            "mybucket",
            "cpu",
            tmp_path,
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 6, tzinfo=UTC),
        )

        assert result is None
        assert not (tmp_path / "mybucket" / "cpu.parquet").exists()

    def test_overwrite_true_replaces_existing_file(self, mock_db, tmp_path, mocker):
        destdir = tmp_path / "mybucket"
        destdir.mkdir(parents=True)
        existing = destdir / "cpu.parquet"
        existing.write_bytes(b"placeholder")

        mocker.patch("parflux.core.query", side_effect=_make_query_side_effect(1))

        result = download_measurement(
            mock_db,
            "mybucket",
            "cpu",
            tmp_path,
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 6, tzinfo=UTC),
            overwrite=True,
        )

        assert result == existing
        assert existing.read_bytes() != b"placeholder"

    def test_passes_filters_into_query_string(self, mock_db, tmp_path, mocker):
        captured = []

        def capture(db, query_str, dest_file, cache_dir=None):
            captured.append(query_str)
            _write_parquet(Path(dest_file), 1)

        mocker.patch("parflux.core.query", side_effect=capture)

        download_measurement(
            mock_db,
            "mybucket",
            "cpu",
            tmp_path,
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, tzinfo=UTC) + timedelta(hours=1),
            filters=['r.host == "h1"'],
        )

        assert captured, "query should have been called"
        assert 'r._measurement == "cpu"' in captured[0]
        assert 'r.host == "h1"' in captured[0]
