from __future__ import annotations

from datetime import datetime, timedelta
from time import perf_counter_ns

from pydantic import BaseModel, Field


class ArgsBucket(BaseModel):
    args: str
    timestamp: datetime = Field(default_factory=datetime.now)
    ttl: timedelta | None = None

    def encode(self) -> str:
        return self.json(exclude_defaults=True)

    @classmethod
    def decode(cls, data: str) -> ArgsBucket:
        return cls.parse_raw(data)


class ResultBucket(BaseModel):
    data: str

    success: bool = True

    started_when: int = Field(...)
    finished_when: int = Field(default_factory=perf_counter_ns)

    exception: str | None = None
    timestamp: datetime = Field(default_factory=datetime.now)
    ttl: timedelta | None = None

    def encode(self) -> str:
        return self.json(exclude_defaults=True)

    @classmethod
    def decode(cls, data: str) -> ResultBucket:
        return cls.parse_raw(data)
