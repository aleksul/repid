from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Coroutine, Dict, Iterable

from repid.data._key import RoutingKey
from repid.data._parameters import Parameters
from repid.middlewares import middleware_wrapper

if TYPE_CHECKING:
    from repid.data.protocols import BucketT, ParametersT, RoutingKeyT

EncodedPayloadT = str
SignalEmitterT = Callable[[str, Dict[str, Any]], Coroutine]


class ConsumerT(ABC):
    __WRAPPED_METHODS__ = ("consume",)

    @abstractmethod
    async def start(self) -> None:
        """Start to consume messages from the queue."""

    async def pause(self) -> None:
        """Pause message consumption. Depending on the implementation, may do nothing."""
        return None

    async def unpause(self) -> None:
        """Unpause message consumption. Depending on the implementation, may do nothing."""
        return None

    @abstractmethod
    async def finish(self) -> None:
        """Finish to consume messages.
        Reject all messages (if any),
        which are currently related to this consumer."""

    @abstractmethod
    async def consume(self) -> tuple[RoutingKeyT, EncodedPayloadT, ParametersT]:
        """Consume one message. Consumer must be started."""

    async def __aenter__(self) -> ConsumerT:
        await self.start()
        return self

    async def __aexit__(self, *exc: tuple) -> None:
        await self.finish()

    def __aiter__(self) -> ConsumerT:
        return self

    async def __anext__(self) -> tuple[RoutingKeyT, EncodedPayloadT, ParametersT]:
        return await self.consume()

    def __new__(cls: type[ConsumerT], *args: tuple, **kwargs: dict) -> ConsumerT:
        inst = super().__new__(cls)

        for method in inst.__WRAPPED_METHODS__:
            setattr(inst, method, middleware_wrapper(getattr(inst, method)))

        return inst

    @property
    def _signal_emitter(self) -> SignalEmitterT | None:
        if not hasattr(self, "__repid_signal_emitter"):
            return None
        return self.__repid_signal_emitter

    @_signal_emitter.setter
    def _signal_emitter(self, signal_emitter: SignalEmitterT) -> None:
        self.__repid_signal_emitter = signal_emitter
        for method in self.__WRAPPED_METHODS__:
            getattr(self, method)._repid_signal_emitter = signal_emitter


class MessageBrokerT(ABC):
    ROUTING_KEY_CLASS: ClassVar[type[RoutingKeyT]] = RoutingKey
    PARAMETERS_CLASS: ClassVar[type[ParametersT]] = Parameters

    __WRAPPED_METHODS__ = (
        "enqueue",
        "reject",
        "ack",
        "nack",
        "requeue",
        "queue_declare",
        "queue_flush",
        "queue_delete",
    )

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
        # NOTE: implementation of this method must set consumer's _signal_emitter.

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
        """Informs message broker that job needs to be rescheduled on another worker."""

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

    def __new__(cls: type[MessageBrokerT], *args: tuple, **kwargs: dict) -> MessageBrokerT:
        inst = super().__new__(cls)

        for method in inst.__WRAPPED_METHODS__:
            setattr(inst, method, middleware_wrapper(getattr(inst, method)))

        return inst

    @property
    def _signal_emitter(self) -> SignalEmitterT | None:
        if not hasattr(self, "__repid_signal_emitter"):
            return None
        return self.__repid_signal_emitter

    @_signal_emitter.setter
    def _signal_emitter(self, signal_emitter: SignalEmitterT) -> None:
        self.__repid_signal_emitter = signal_emitter
        for method in self.__WRAPPED_METHODS__:
            getattr(self, method)._repid_signal_emitter = signal_emitter


class BucketBrokerT(ABC):
    BUCKET_CLASS: type[BucketT]

    __WRAPPED_METHODS__ = (
        "get_bucket",
        "store_bucket",
        "delete_bucket",
    )

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

    def __new__(cls: type[BucketBrokerT], *args: tuple, **kwargs: dict) -> BucketBrokerT:
        inst = super().__new__(cls)

        for method in inst.__WRAPPED_METHODS__:
            setattr(inst, method, middleware_wrapper(getattr(inst, method)))

        return inst

    @property
    def _signal_emitter(self) -> SignalEmitterT | None:
        if not hasattr(self, "__repid_signal_emitter"):
            return None
        return self.__repid_signal_emitter

    @_signal_emitter.setter
    def _signal_emitter(self, signal_emitter: SignalEmitterT) -> None:
        self.__repid_signal_emitter = signal_emitter
        for method in self.__WRAPPED_METHODS__:
            getattr(self, method)._repid_signal_emitter = signal_emitter
