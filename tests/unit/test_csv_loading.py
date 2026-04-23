"""Unit tests for the DuckDB-backed CSV loading helpers in parflux.core."""

import shutil

import duckdb
import pytest

from parflux.core import (
    _split_raw_influxdb_response,
    load_annotated_csv,
    load_raw_query,
    union_tables,
)


@pytest.fixture
def con():
    with duckdb.connect(":memory:") as c:
        yield c


class TestLoadAnnotatedCsv:
    def test_creates_table_with_expected_columns_and_rows(self, con, simple_annotated_csv):
        table_name = load_annotated_csv(con, simple_annotated_csv, keep=True)
        assert table_name == "simple"

        columns = [row[0] for row in con.sql(f'describe "{table_name}"').fetchall()]
        assert "result" not in columns
        assert "table" not in columns
        for expected in ("_time", "_measurement", "_field", "_value"):
            assert expected in columns

        row_count = con.sql(f'select count(*) from "{table_name}"').fetchone()[0]
        assert row_count == 3

    def test_removes_source_file_when_keep_false(self, con, simple_annotated_csv):
        load_annotated_csv(con, simple_annotated_csv, keep=False)
        assert not simple_annotated_csv.exists()

    def test_keep_retains_source_file(self, con, simple_annotated_csv):
        load_annotated_csv(con, simple_annotated_csv, keep=True)
        assert simple_annotated_csv.exists()

    def test_custom_table_name_is_used(self, con, simple_annotated_csv):
        table_name = load_annotated_csv(con, simple_annotated_csv, table_name="cpu_usage", keep=True)
        assert table_name == "cpu_usage"
        row_count = con.sql('select count(*) from "cpu_usage"').fetchone()[0]
        assert row_count == 3

    def test_rejects_unsupported_type(self, con, unsupported_annotated_csv):
        with pytest.raises(TypeError, match="not supported"):
            load_annotated_csv(con, unsupported_annotated_csv)


class TestUnionTables:
    def _make_table(self, con, name, value):
        con.sql(f'create table "{name}" as select {value} as v')

    def test_view_combines_rows(self, con):
        self._make_table(con, "a", 1)
        self._make_table(con, "b", 2)

        union_tables(con, ["a", "b"], "combined", kind="view")

        values = sorted(row[0] for row in con.sql('select v from "combined"').fetchall())
        assert values == [1, 2]
        table_count = con.sql("select count(*) from duckdb_tables where table_name in ('a', 'b')").fetchone()[0]
        assert table_count == 2

    def test_table_kind_drops_sources_by_default(self, con):
        self._make_table(con, "x", 1)
        self._make_table(con, "y", 2)

        union_tables(con, ["x", "y"], "merged", kind="table")

        table_names = {row[0] for row in con.sql("select table_name from duckdb_tables").fetchall()}
        assert "merged" in table_names
        assert "x" not in table_names
        assert "y" not in table_names

    def test_table_kind_keeps_sources_when_requested(self, con):
        self._make_table(con, "p", 1)
        self._make_table(con, "q", 2)

        union_tables(con, ["p", "q"], "kept", kind="table", keep=True)

        table_names = {row[0] for row in con.sql("select table_name from duckdb_tables").fetchall()}
        assert {"p", "q", "kept"} <= table_names

    def test_rejects_invalid_kind(self, con):
        with pytest.raises(ValueError, match="only table or view"):
            union_tables(con, ["a"], "x", kind="materialized")


@pytest.mark.skipif(shutil.which("csplit") is None, reason="requires GNU csplit")
class TestSplitRawInfluxdbResponse:
    def test_splits_multi_table_raw_file(self, multi_table_raw):
        files = _split_raw_influxdb_response(multi_table_raw, keep=True)
        assert len(files) == 2
        assert all(f.suffix == ".csv" for f in files)
        for f in files:
            assert f.read_text().startswith("#datatype")

    def test_removes_source_when_keep_false(self, multi_table_raw):
        _split_raw_influxdb_response(multi_table_raw, keep=False)
        assert not multi_table_raw.exists()

    def test_keeps_source_when_keep_true(self, multi_table_raw):
        _split_raw_influxdb_response(multi_table_raw, keep=True)
        assert multi_table_raw.exists()

    def test_returns_empty_for_file_without_content(self, tmp_path):
        empty = tmp_path / "nothing.txt"
        empty.write_text("")
        assert _split_raw_influxdb_response(empty, keep=True) == []


@pytest.mark.skipif(shutil.which("csplit") is None, reason="requires GNU csplit")
class TestLoadRawQuery:
    def test_end_to_end_creates_combined_view(self, con, multi_table_raw):
        table_name = load_raw_query(con, multi_table_raw)
        assert table_name == "multi-table"

        row_count = con.sql(f'select count(*) from "{table_name}"').fetchone()[0]
        assert row_count == 3

    def test_custom_table_name(self, con, multi_table_raw):
        table_name = load_raw_query(con, multi_table_raw, table_name="my_data")
        assert table_name == "my_data"
        row_count = con.sql('select count(*) from "my_data"').fetchone()[0]
        assert row_count == 3
