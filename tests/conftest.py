"""Shared pytest configuration.

Integration tests (marked ``integration``) hit a live InfluxDB instance and are
skipped by default; set ``PARFLUX_RUN_INTEGRATION=1`` to opt in.
"""

import os
import textwrap

import pytest

INTEGRATION_ENV_VAR = "PARFLUX_RUN_INTEGRATION"


def pytest_collection_modifyitems(config, items):
    if os.environ.get(INTEGRATION_ENV_VAR) == "1":
        return
    skip = pytest.mark.skip(reason=f"set {INTEGRATION_ENV_VAR}=1 to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


SIMPLE_ANNOTATED_CSV = textwrap.dedent(
    """\
    #datatype,string,long,dateTime:RFC3339,string,string,double
    ,result,table,_time,_measurement,_field,_value
    ,_result,0,2024-01-01T00:00:00Z,cpu,usage,0.5
    ,_result,0,2024-01-01T00:00:01Z,cpu,usage,0.6
    ,_result,0,2024-01-01T00:00:02Z,cpu,usage,0.7
    """
)

UNSUPPORTED_ANNOTATED_CSV = textwrap.dedent(
    """\
    #datatype,string,long,dateTime:RFC3339,string,base64Binary
    ,result,table,_time,_measurement,_value
    ,_result,0,2024-01-01T00:00:00Z,cpu,QUJD
    """
)

MULTI_TABLE_RAW = (
    "#datatype,string,long,dateTime:RFC3339,string,string,double\r\n"
    ",result,table,_time,_measurement,_field,_value\r\n"
    ",_result,0,2024-01-01T00:00:00Z,cpu,usage,0.5\r\n"
    ",_result,0,2024-01-01T00:00:01Z,cpu,usage,0.6\r\n"
    "\r\n"
    "#datatype,string,long,dateTime:RFC3339,string,string,double\r\n"
    ",result,table,_time,_measurement,_field,_value\r\n"
    ",_result,1,2024-01-02T00:00:00Z,cpu,usage,0.7\r\n"
)


@pytest.fixture
def simple_annotated_csv(tmp_path):
    """Write a minimal annotated CSV and return its path."""
    path = tmp_path / "simple.csv"
    path.write_text(SIMPLE_ANNOTATED_CSV)
    return path


@pytest.fixture
def unsupported_annotated_csv(tmp_path):
    """Annotated CSV containing a type parflux does not support."""
    path = tmp_path / "unsupported.csv"
    path.write_text(UNSUPPORTED_ANNOTATED_CSV)
    return path


@pytest.fixture
def multi_table_raw(tmp_path):
    """Raw InfluxDB response with two \\r-separated annotated CSV tables."""
    path = tmp_path / "multi_table.txt"
    path.write_bytes(MULTI_TABLE_RAW.encode())
    return path


@pytest.fixture
def empty_raw(tmp_path):
    """Raw response file with no data (size <= 2 bytes)."""
    path = tmp_path / "empty.txt"
    path.write_bytes(b"")
    return path
