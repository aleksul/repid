from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol, TypeVar
from uuid import uuid4

from repid.data.priorities import PrioritiesT
from repid.utils import VALID_ID, VALID_NAME

T_co = TypeVar("T_co", covariant=True)


class SerializableT(Protocol[T_co]):
    def encode(self) -> str:
        ...

    @classmethod
    def decode(cls, data: str) -> T_co:
        ...


class TimedT(Protocol):
    timestamp: datetime
    ttl: timedelta | None

    @property
    def is_overdue(self) -> bool:
        """Is time-to-live expired?"""
        if self.ttl is None:
            return False
        return datetime.now() > self.timestamp + self.ttl


class RoutingKeyT(Protocol):
    id_: str
    topic: str
    queue: str
    priority: int

    def __init__(
        self,
        *,
        id_: str | None = None,
        topic: str,
        queue: str = "default",
        priority: int = PrioritiesT.MEDIUM.value,
    ) -> None:
        if id_ is None:
            id_ = uuid4().hex
        else:
            if not VALID_ID.fullmatch(id_):
                raise ValueError("Invalid id.")
        self.id_ = id_

        if not VALID_NAME.fullmatch(topic):
            raise ValueError("Invalid topic.")
        self.topic = topic

        if not VALID_NAME.fullmatch(queue):
            raise ValueError("Invalid queue name.")
        self.queue = queue

        if self.priority < 0:
            raise ValueError("Invalid priority.")
        self.priority


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


class ParametersT(SerializableT, TimedT, Protocol):
    execution_timeout: timedelta
    result: ResultPropertiesT | None
    retries: RetriesPropertiesT | None
    delay: DelayPropertiesT | None

    @property
    def compute_next_execution_time(self) -> datetime | None:
        """Computes unix timestamp of the next execution time."""


class BucketT(SerializableT, TimedT, Protocol):
    pass


class MessageT(Protocol):
    key: RoutingKeyT
    payload: str
    parameters: ParametersT
