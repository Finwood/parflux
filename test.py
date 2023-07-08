import logging
from datetime import datetime, timedelta
from pathlib import Path

from parflux.session import Session

try:
    from dotenv import load_dotenv
except ImportError:
    pass
else:
    load_dotenv()


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    session = Session()
    hex_timestamp = f"{int(session.start.timestamp()):08X}"

    bucket = "cellkit-test"
    # for bucket in session.list_buckets():
    # measurements = list_measurements(client, bucket.name)
    # print(bucket.name)

    measurement = "sink"
    # for measurement in session.list_measurements(bucket):
    record_count = session.count_records_in_measurement(bucket, measurement)
    total = sum(record_count.values())
    if total:
        print(f"    {measurement}: {total}")
        session.download_measurement(
            bucket,
            measurement,
            Path(f"data/{bucket}/{measurement}/{hex_timestamp}.parquet"),
        )
