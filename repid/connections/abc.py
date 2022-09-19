from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Iterable

from repid.data._buckets import ArgsBucket
from repid.data._key import RoutingKey
from repid.data._parameters import Parameters

if TYPE_CHECKING:
    from repid.data.protocols import BucketT, ParametersT, RoutingKeyT

EncodedPayloadT = str


class ConsumerT(ABC):
    @abstractmethod
    async def start(self) -> None:
        ...

    @abstractmethod
    async def stop(self) -> None:
        ...

    async def __aenter__(self) -> ConsumerT:
        await self.start()
        return self

    async def __aexit__(self, *exc: tuple) -> None:
        await self.stop()

    def __aiter__(self) -> ConsumerT:
        return self

    @abstractmethod
    async def __anext__(self) -> tuple[RoutingKeyT, EncodedPayloadT, ParametersT]:
        ...


class MessageBrokerT(ABC):
    ROUTING_KEY_CLASS: ClassVar[type[RoutingKeyT]] = RoutingKey
    PARAMETERS_CLASS: ClassVar[type[ParametersT]] = Parameters

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        ...

    @abstractmethod
    async def consume(
        self,
        queue_name: str,
        topics: Iterable[str] | None = None,
    ) -> ConsumerT:
        """Consumes messages from the specified queue.
        If topics are specified, each message will be checked for compliance with one of the topics.
        Otherwise every message from the queue can be consumed.
        Informs the broker that job execution is started."""

    @abstractmethod
    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: EncodedPayloadT = "",
        params: ParametersT | None = None,
    ) -> None:
        """Appends the message to the queue."""

    @abstractmethod
    async def reject(self, key: RoutingKeyT) -> None:
        """Infroms message broker that job needs to be rescheduled on another worker."""

    @abstractmethod
    async def ack(self, key: RoutingKeyT) -> None:
        """Informs message broker that job execution succeed."""

    @abstractmethod
    async def nack(self, key: RoutingKeyT) -> None:
        """Informs message broker that job execution failed."""

    @abstractmethod
    async def requeue(
        self,
        key: RoutingKeyT,
        payload: EncodedPayloadT = "",
        params: ParametersT | None = None,
    ) -> None:
        """Re-queues the message with payload/parameters. Routing key must stay the same."""

    @abstractmethod
    async def queue_declare(self, queue_name: str) -> None:
        """Creates the specified queue."""

    @abstractmethod
    async def queue_flush(self, queue_name: str) -> None:
        """Empties the queue. Doesn't delete the queue itself."""

    @abstractmethod
    async def queue_delete(self, queue_name: str) -> None:
        """Deletes the queue with all of its messages."""


class BucketBrokerT(ABC):
    BUCKET_CLASS: ClassVar[type[BucketT]] = ArgsBucket

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        ...

    @abstractmethod
    async def get_bucket(self, id_: str) -> BucketT | None:
        """Retrivies the bucket."""

    @abstractmethod
    async def store_bucket(self, id_: str, payload: BucketT) -> None:
        """Stores the bucket."""

    @abstractmethod
    async def delete_bucket(self, id_: str) -> None:
        """Deletes the bucket."""
