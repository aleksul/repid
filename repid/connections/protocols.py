from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from repid.data import Message


class RoutingKey(Protocol):
    id_: str
    queue: str
    topic: str
    priority: int


class RetriesProperties(Protocol):
    max_amount: int = 1
    already_tried: int = 0


class ResultProperties(Protocol):
    id_: str
    ttl: float | None = None


class DelayProperties(Protocol):
    delay_until: float | None = None
    defer_by: float | None = None
    cron: str | None = None


class Payload(Protocol):
    execution_timeout: float = 600.0  # sec
    args: bytes | str | None = None  # bytes => encoded data, str => id of the bucket
    result: ResultProperties | None = None
    retries: RetriesProperties | None = None
    delay: DelayProperties | None = None


class AdditionalParameters(Protocol):
    timestamp: float
    ttl: float | None = None
    next_execution_time: float | None = None


class Messaging(Protocol):
    async def consume(self, queue_name: str, topics: frozenset[str]) -> Message:
        """Consumes one message from the specified queue.
        Should respect the topics.
        Informs the broker that job execution is started."""

    async def enqueue(
        self,
        key: RoutingKey,
        payload: bytes | None,
        params: AdditionalParameters,
    ) -> None:
        """Appends the message to the queue."""

    async def reject(self, key: RoutingKey) -> None:
        """Infroms message broker that job needs to be rescheduled on another worker."""

    async def ack(self, key: RoutingKey) -> None:
        """Informs message broker that job execution succeed."""

    async def nack(self, key: RoutingKey) -> None:
        """Informs message broker that job execution failed."""

    async def requeue(
        self, key: RoutingKey, payload: bytes | None, params: AdditionalParameters
    ) -> None:
        """Re-queues the message with different body. Id must be the same."""

    async def queue_declare(self, queue_name: str) -> None:
        """Creates the specified queue."""

    async def queue_flush(self, queue_name: str) -> None:
        """Empties the queue. Doesn't delete the queue itself."""

    async def queue_delete(self, queue_name: str) -> None:
        """Deletes the queue with all of its messages."""


class Bucketing(Protocol):
    async def get_bucket(self, id_: str) -> bytes | None:
        """Retrivies the bucket."""

    async def store_bucket(self, id_: str, payload: bytes, *, ttl: float | None = None) -> None:
        """Stores the bucket."""

    async def delete_bucket(self, id_: str) -> None:
        """Deletes the bucket."""
