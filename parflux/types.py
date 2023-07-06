import dataclasses
from datetime import datetime, timedelta
from typing import Optional

import influxdb_client


@dataclasses.dataclass
class Bucket:
    id: str
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    retention: Optional[timedelta]

    @classmethod
    def from_openapi_model(cls, model: influxdb_client.Bucket):
        try:
            retention = (
                timedelta(seconds=model.retention_rules[0].every_seconds) or None
            )
        except IndexError:
            retention = None
        return cls(
            id=model.id,
            name=model.name,
            description=model.description,
            created_at=model.created_at,
            updated_at=model.updated_at,
            retention=retention,
        )
