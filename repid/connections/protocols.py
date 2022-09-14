from __future__ import annotations

from typing import TYPE_CHECKING, AsyncIterator, Protocol

if TYPE_CHECKING:
    from repid.data.protocols import BucketT, ParametersT, RoutingKeyT

EncodedPayloadT = str


class Messaging(Protocol):
    async def consume(
        self,
        queue_name: str,
        topics: frozenset[str] | None = None,
    ) -> AsyncIterator[tuple[RoutingKeyT, EncodedPayloadT, ParametersT]]:
        """Consumes messages from the specified queue.
        If topics are specified, each message will be checked for compliance with one of the topics.
        Otherwise every message from the queue can be consumed.
        Informs the broker that job execution is started."""

    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: EncodedPayloadT = "",
        params: ParametersT | None = None,
    ) -> None:
        """Appends the message to the queue."""

    async def reject(self, key: RoutingKeyT) -> None:
        """Infroms message broker that job needs to be rescheduled on another worker."""

    async def ack(self, key: RoutingKeyT) -> None:
        """Informs message broker that job execution succeed."""

    async def nack(self, key: RoutingKeyT) -> None:
        """Informs message broker that job execution failed."""

    async def requeue(
        self,
        key: RoutingKeyT,
        payload: EncodedPayloadT = "",
        params: ParametersT | None = None,
    ) -> None:
        """Re-queues the message with payload/parameters. Routing key must stay the same."""

    async def queue_declare(self, queue_name: str) -> None:
        """Creates the specified queue."""

    async def queue_flush(self, queue_name: str) -> None:
        """Empties the queue. Doesn't delete the queue itself."""

    async def queue_delete(self, queue_name: str) -> None:
        """Deletes the queue with all of its messages."""


class Bucketing(Protocol):
    async def get_bucket(self, id_: str) -> BucketT | None:
        """Retrivies the bucket."""

    async def store_bucket(self, id_: str, payload: BucketT) -> None:
        """Stores the bucket."""

    async def delete_bucket(self, id_: str) -> None:
        """Deletes the bucket."""
