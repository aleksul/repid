from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Iterable,
    TypeVar,
)

from repid.config import Config
from repid.message import MessageCategory
from repid.middlewares import middleware_wrapper

if TYPE_CHECKING:
    from repid.data.protocols import BucketT, ParametersT, ResultBucketT, RoutingKeyT

EncodedPayloadT = str
SignalEmitterT = Callable[[str, Dict[str, Any]], Coroutine]
WrappedABCSelf = TypeVar("WrappedABCSelf", bound="_WrappedABC")


class _WrappedABC(ABC):
    __WRAPPED_METHODS__: tuple[str, ...] = ()

    def __new__(
        cls: type[WrappedABCSelf],
        *args: Any,  # noqa: ARG003
        **kwargs: Any,  # noqa: ARG003
    ) -> WrappedABCSelf:
        inst = super().__new__(cls)

        for method in inst.__WRAPPED_METHODS__:
            setattr(inst, method, middleware_wrapper(getattr(inst, method)))

        return inst

    @property
    def _signal_emitter(self) -> SignalEmitterT | None:
        if not hasattr(self, "_signal_emitter_var"):
            return None
        return self._signal_emitter_var

    @_signal_emitter.setter
    def _signal_emitter(self, signal_emitter: SignalEmitterT) -> None:
        self._signal_emitter_var = signal_emitter
        for method in self.__WRAPPED_METHODS__:
            getattr(self, method)._repid_signal_emitter = signal_emitter


class ConsumerT(_WrappedABC):
    __WRAPPED_METHODS__ = ("consume",)

    @abstractmethod
    def __init__(
        self,
        broker: MessageBrokerT,
        queue_name: str,
        topics: Iterable[str] | None = None,
        max_unacked_messages: int | None = None,
        category: MessageCategory = MessageCategory.NORMAL,
    ) -> None: ...

    @abstractmethod
    async def start(self) -> None:
        """Start to consume messages from the queue."""

    @abstractmethod
    async def pause(self) -> None:
        """Pause message consumption."""

    @abstractmethod
    async def unpause(self) -> None:
        """Unpause message consumption."""

    @abstractmethod
    async def finish(self) -> None:
        """Finish to consume messages.
        Reject all messages (if any),
        which are currently related to this consumer."""

    @abstractmethod
    async def consume(self) -> tuple[RoutingKeyT, EncodedPayloadT, ParametersT]:
        """Consume one message. Consumer must be started."""

    async def __aenter__(self) -> ConsumerT:  # noqa: PYI034
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.finish()

    def __aiter__(self) -> ConsumerT:
        return self

    async def __anext__(self) -> tuple[RoutingKeyT, EncodedPayloadT, ParametersT]:
        return await self.consume()


class MessageBrokerT(_WrappedABC):
    CONSUMER_CLASS: ClassVar[type[ConsumerT]]

    ROUTING_KEY_CLASS: ClassVar[type[RoutingKeyT]]
    PARAMETERS_CLASS: ClassVar[type[ParametersT]]

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
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    def get_consumer(
        self,
        queue_name: str,
        topics: Iterable[str] | None = None,
        max_unacked_messages: int | None = None,
        category: MessageCategory = MessageCategory.NORMAL,
    ) -> ConsumerT:
        """Consumes messages from the specified queue.
        If topics are specified, each message will be checked for compliance with one of the topics.
        Otherwise every message from the queue can be consumed.
        Consumer should inform the broker that job execution is started.
        Some brokers can adjust their load based on max_unacked_messages parameter.
        """

        consumer = self.CONSUMER_CLASS(self, queue_name, topics, max_unacked_messages, category)
        consumer._signal_emitter = self._signal_emitter
        return consumer

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

    def __new__(  # noqa: PYI034
        cls: type[MessageBrokerT],
        *args: Any,  # noqa: ARG003
        **kwargs: Any,  # noqa: ARG003s
    ) -> MessageBrokerT:
        cls.ROUTING_KEY_CLASS = deepcopy(Config.ROUTING_KEY)
        cls.PARAMETERS_CLASS = deepcopy(Config.PARAMETERS)

        return super().__new__(cls)


class BucketBrokerT(_WrappedABC):
    BUCKET_CLASS: type[BucketT | ResultBucketT]

    __WRAPPED_METHODS__ = (
        "get_bucket",
        "store_bucket",
        "delete_bucket",
    )

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def get_bucket(self, id_: str) -> BucketT | None:
        """Retrivies the bucket."""

    @abstractmethod
    async def store_bucket(self, id_: str, payload: BucketT) -> None:
        """Stores the bucket."""

    @abstractmethod
    async def delete_bucket(self, id_: str) -> None:
        """Deletes the bucket."""

    def __new__(  # noqa: PYI034
        cls: type[BucketBrokerT],
        *args: Any,  # noqa: ARG003
        **kwargs: Any,  # noqa: ARG003
    ) -> BucketBrokerT:
        cls.BUCKET_CLASS = deepcopy(Config.BUCKET)

        return super().__new__(cls)
