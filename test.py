from pathlib import Path
from parflux.session import Session
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
except ImportError:
    pass
else:
    load_dotenv()


if __name__ == "__main__":
    session = Session()
    hex_timestamp = f"{int(session.start.timestamp()):08X}"

    # bucket = "pyric"
    for bucket in sorted(session.list_buckets(), key=lambda b: b.updated_at):
        # measurements = list_measurements(client, bucket.name)
        print(bucket.name)

        # measurement = "reg.udral.physics.electricity.SourceTs.0.1"
        for measurement in session.list_measurements(bucket):
            record_count = session.count_records_in_measurement(bucket, measurement)
            total = sum(record_count.values())
            if total:
                print(f"    {measurement}: {total}")
                session.download_measurement(
                    bucket,
                    measurement,
                    Path(f"data/{bucket.name}/{measurement}/{hex_timestamp}.parquet"),
                )
