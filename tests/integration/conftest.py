"""Fixtures for live-InfluxDB integration tests.

These fixtures require a reachable InfluxDB v2 instance configured via the
standard ``INFLUXDB_V2_URL`` / ``INFLUXDB_V2_ORG`` / ``INFLUXDB_V2_TOKEN`` env
vars. Individual tests must also be marked ``integration``; the top-level
``tests/conftest.py`` will skip them unless ``PARFLUX_RUN_INTEGRATION=1``.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

REQUIRED_ENV_VARS = ("INFLUXDB_V2_URL", "INFLUXDB_V2_ORG", "INFLUXDB_V2_TOKEN")


@pytest.fixture(scope="session")
def influx_client():
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        pytest.skip(f"missing InfluxDB env vars: {', '.join(missing)}")

    client = InfluxDBClient.from_env_properties()
    if not client.ping():
        pytest.skip("InfluxDB is not reachable")
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def seeded_bucket(influx_client):
    """Create an ephemeral bucket, write a handful of points, yield its name."""
    buckets_api = influx_client.buckets_api()
    org = os.environ["INFLUXDB_V2_ORG"]
    bucket_name = f"parflux-test-{uuid.uuid4().hex[:8]}"

    bucket = buckets_api.create_bucket(bucket_name=bucket_name, org=org)
    try:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        now = datetime.now(tz=timezone.utc).replace(microsecond=0)
        points = [
            Point("cpu").tag("host", "h1").field("usage", 0.1 * i).time(now - timedelta(seconds=i)) for i in range(10)
        ]
        write_api.write(bucket=bucket_name, org=org, record=points)

        start = now - timedelta(minutes=1)
        end = now + timedelta(seconds=1)
        yield bucket_name, start, end, 10
    finally:
        buckets_api.delete_bucket(bucket)
