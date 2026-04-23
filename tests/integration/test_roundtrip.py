"""End-to-end smoke test against a live InfluxDB instance."""

import duckdb
import pytest

from parflux.session import Session

pytestmark = pytest.mark.integration


def test_download_bucket_roundtrip(influx_client, seeded_bucket, tmp_path):
    bucket_name, start, stop, expected_rows = seeded_bucket

    session = Session(start, stop, db=influx_client)
    session.download([bucket_name], basedir=tmp_path)

    parquet_file = tmp_path / bucket_name / "cpu.parquet"
    assert parquet_file.exists(), f"expected {parquet_file} to be created"

    with duckdb.connect(":memory:") as con:
        count = con.sql(f"select count(*) from read_parquet('{parquet_file}')").fetchone()[0]
        assert count == expected_rows

        columns = [row[0] for row in con.sql(f"describe select * from read_parquet('{parquet_file}')").fetchall()]
        assert "_time" in columns
        assert "usage" in columns
