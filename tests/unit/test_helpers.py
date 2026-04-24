"""Unit tests for pure helper functions in parflux.core."""

from datetime import datetime, timedelta, timezone

import pytest

from parflux.core import DEFAULT_BATCH_SIZE, get_influx_csv_schema, iter_batches, relation_name


class TestRelationName:
    def test_lowercases_and_slugs(self):
        assert relation_name("My Bucket") == "my-bucket"

    def test_strips_non_alphanumeric(self):
        assert relation_name("foo/bar_baz.42") == "foo-bar-baz-42"

    def test_idempotent(self):
        once = relation_name("Some Measurement")
        assert relation_name(once) == once


UTC = timezone.utc


class TestIterBatches:
    def test_single_batch_when_range_shorter_than_batch_size(self):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = start + timedelta(hours=6)
        batches = list(iter_batches(start, end))
        assert batches == [(start, end)]

    def test_exact_multiple_of_batch_size(self):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = start + 3 * DEFAULT_BATCH_SIZE
        batches = list(iter_batches(start, end))
        assert len(batches) == 3
        assert batches[0] == (start, start + DEFAULT_BATCH_SIZE)
        assert batches[-1] == (start + 2 * DEFAULT_BATCH_SIZE, end)

    def test_trailing_partial_batch(self):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = start + DEFAULT_BATCH_SIZE + timedelta(hours=3)
        batches = list(iter_batches(start, end))
        assert len(batches) == 2
        assert batches[0] == (start, start + DEFAULT_BATCH_SIZE)
        assert batches[1] == (start + DEFAULT_BATCH_SIZE, end)

    def test_empty_when_start_equals_end(self):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        assert list(iter_batches(start, start)) == []

    def test_empty_when_end_before_start(self):
        start = datetime(2024, 1, 2, tzinfo=UTC)
        end = datetime(2024, 1, 1, tzinfo=UTC)
        assert list(iter_batches(start, end)) == []

    def test_batches_cover_full_range_without_overlap(self):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = start + timedelta(days=2, hours=5)
        batches = list(iter_batches(start, end))
        assert batches[0][0] == start
        assert batches[-1][1] == end
        for (_, prev_end), (next_start, _) in zip(batches, batches[1:]):
            assert prev_end == next_start


class TestGetInfluxCsvSchema:
    def test_maps_datatypes_to_duckdb_types(self, simple_annotated_csv):
        schema = get_influx_csv_schema(simple_annotated_csv)
        assert schema == {
            "result": ("string", "VARCHAR"),
            "table": ("long", "BIGINT"),
            "_time": ("dateTime:RFC3339", "TIMESTAMPTZ"),
            "_measurement": ("string", "VARCHAR"),
            "_field": ("string", "VARCHAR"),
            "_value": ("double", "DOUBLE"),
        }

    def test_rejects_file_without_datatype_header(self, tmp_path):
        bad = tmp_path / "bad.csv"
        bad.write_text("result,table,_time\n,result,table,_time\n")
        with pytest.raises(AssertionError):
            get_influx_csv_schema(bad)

    def test_rejects_unknown_datatype(self, tmp_path):
        bad = tmp_path / "unknown_dtype.csv"
        bad.write_text("#datatype,mystery\n,col\n")
        with pytest.raises(KeyError):
            get_influx_csv_schema(bad)
