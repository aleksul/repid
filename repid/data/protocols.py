from __future__ import annotations

from datetime import datetime, timedelta
from enum import IntEnum
from typing import Protocol


class PrioritiesT(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class RoutingKeyT(Protocol):
    id_: str
    topic: str
    queue: str
    priority: int


class RetriesPropertiesT(Protocol):
    max_amount: int
    already_tried: int


class ResultPropertiesT(Protocol):
    id_: str
    ttl: timedelta | None


class DelayPropertiesT(Protocol):
    delay_until: datetime | None
    defer_by: timedelta | None
    cron: str | None
    next_execution_time: datetime | None


class ParametersT(Protocol):
    execution_timeout: timedelta
    result: ResultPropertiesT | None
    retries: RetriesPropertiesT | None
    delay: DelayPropertiesT | None
    timestamp: datetime
    ttl: timedelta | None

    def encode(self) -> str:
        """Encodes Payload object."""

    @classmethod
    def decode(cls, data: str) -> ParametersT:
        """Decodes Payload object."""

    @property
    def is_overdue(self) -> bool:
        """Is time-to-live expired?"""
        if self.ttl is None:
            return False
        return datetime.now() > self.timestamp + self.ttl

    @property
    def compute_next_execution_time(self) -> datetime | None:
        """Computes unix timestamp of the next execution time."""


class BucketT(Protocol):
    timestamp: datetime
    ttl: timedelta | None

    def encode(self) -> str:
        """Encodes Bucket object."""

    @classmethod
    def decode(cls, data: str) -> BucketT:
        """Decodes Bucket object."""
