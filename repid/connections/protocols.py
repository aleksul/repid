from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from repid.data import AnyBucketT, Message


class Messaging(Protocol):
    supports_delayed_messages: bool

    def __init__(self, dsn: str) -> None:
        ...

    async def consume(self, queue_name: str, topics: frozenset[str]) -> Message:
        """Consumes one message from the specified queue.
        Should respect the topics.
        Informs the broker that job execution is started."""

    async def enqueue(self, message: Message) -> None:
        """Appends the message to the queue."""

    async def reject(self, message: Message) -> None:
        """Infroms message broker that job needs to be rescheduled on another worker."""

    async def ack(self, message: Message) -> None:
        """Informs message broker that job execution succeed."""

    async def nack(self, message: Message) -> None:
        """Informs message broker that job execution failed."""

    async def requeue(self, message: Message) -> None:
        """Re-queues the message with different body. Id must be the same."""
        await self.ack(message)
        await self.enqueue(message)

    async def queue_declare(self, queue_name: str) -> None:
        """Creates the specified queue."""

    async def queue_flush(self, queue_name: str) -> None:
        """Empties the queue. Doesn't delete the queue itself."""

    async def queue_delete(self, queue_name: str) -> None:
        """Deletes the queue with all of its messages."""


class Bucketing(Protocol):
    def __init__(self, dsn: str) -> None:
        ...

    async def get_bucket(self, id_: str) -> AnyBucketT | None:
        """Retrivies the bucket."""

    async def store_bucket(self, bucket: AnyBucketT) -> None:
        """Stores the bucket."""

    async def delete_bucket(self, id_: str) -> None:
        """Deletes the bucket."""
