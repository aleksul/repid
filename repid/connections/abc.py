from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, TypedDict

from repid.data.message import MessageData

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag
    from repid.data.concurrency import Concurrency


class MessageAction(StrEnum):
    acked = "acked"
    nacked = "nacked"
    rejected = "rejected"
    replied = "replied"


class BaseMessageT(Protocol):
    @property
    def payload(self) -> bytes: ...

    @property
    def headers(self) -> dict[str, str] | None: ...

    @property
    def content_type(self) -> str | None: ...

    @property
    def reply_to(self) -> str | None: ...


class SentMessageT(BaseMessageT, Protocol):
    pass


MessagePublisherT: TypeAlias = Callable[
    [str, MessageData, dict[str, Any] | None],
    Coroutine[Any, Any, Any],
]


class ReceivedMessageT(BaseMessageT, Protocol):
    @property
    def channel(self) -> str: ...

    @property
    def action(self) -> MessageAction | None: ...

    @property
    def is_acted_on(self) -> bool: ...

    @property
    def message_id(self) -> str | None:
        """Unique identifier of a message if provided by the message broker."""

    async def ack(self) -> None: ...

    async def nack(self) -> None: ...

    async def reject(self) -> None: ...

    @property
    def keep_alive_interval(self) -> float | None:
        """Recommended positive interval in seconds, or None if keep-alive is unnecessary."""

    async def keep_alive(self) -> bool | None:
        """Renew this delivery's lease.

        Return False if the broker adapter knows further renewals are unnecessary,
        for example after settlement or lease expiry. Repid then ends the renewal
        loop. Return None to keep the renewal schedule active.
        """

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Atomically (if supporter by the server) ack and reply to the message."""


class CapabilitiesT(TypedDict):
    supports_native_reply: bool
    supports_lightweight_pause: bool
    supports_keep_alive: bool


@dataclass(frozen=True, kw_only=True, slots=True)
class Demand:
    """Free capacity in an intake scope. None means the dimension is unlimited."""

    messages: int | None
    payload_bytes: int | None


class IntakeScope(Protocol):
    """Capacity the worker publishes for one scope: the whole worker or one channel."""

    @property
    def limits(self) -> Concurrency:
        """Most deliveries the worker runs at once in this scope.

        Size broker-side windows (link credit, unacknowledged limits) from it.
        """

    @property
    def demand(self) -> Demand:
        """Free capacity right now. Size fetches from it.

        Byte demand is advisory: payload sizes are only known after receipt,
        and the worker enforces its byte budget when admitting a delivery.
        """

    @property
    def ready(self) -> asyncio.Future[None]:
        """Done while the scope has room for another delivery.

        Otherwise it completes once room frees. Read it once per check and wait on
        that object: await it, pass it to `asyncio.wait`, or add a done callback.
        Cancelling a pending one affects only its reader.
        """


class ChannelIntake(IntakeScope, Protocol):
    def submit(self, message: ReceivedMessageT, /) -> None:
        """Hand a delivery over to the worker, which owns it from now on.

        Synchronous and never blocks. Submit while `ready` is done; surplus
        deliveries wait at admission. The worker settles every submitted message.
        """


class Intake(IntakeScope, Protocol):
    """The worker scope, with one intake per subscribed channel."""

    @property
    def channels(self) -> Mapping[str, ChannelIntake]: ...


class SubscriberT(Protocol):
    @property
    def is_active(self) -> bool: ...

    @property
    def task(self) -> asyncio.Task[object]: ...

    async def pause(self) -> None: ...

    async def resume(self) -> None: ...

    async def close(self) -> None: ...


class ServerT(Protocol):
    @property
    def host(self) -> str: ...

    @property
    def protocol(self) -> str: ...

    @property
    def pathname(self) -> str | None: ...

    @property
    def title(self) -> str | None: ...

    @property
    def summary(self) -> str | None: ...

    @property
    def description(self) -> str | None: ...

    @property
    def protocol_version(self) -> str | None: ...

    @property
    def variables(self) -> Mapping[str, ServerVariable] | None: ...

    @property
    def security(self) -> Sequence[Any] | None: ...

    @property
    def tags(self) -> Sequence[Tag] | None: ...

    @property
    def external_docs(self) -> ExternalDocs | None: ...

    @property
    def bindings(self) -> ServerBindingsObject | None: ...

    # capabilities

    @property
    def capabilities(self) -> CapabilitiesT: ...

    # connection lifecycle management

    @property
    def is_connected(self) -> bool: ...

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    def connection(self) -> AbstractAsyncContextManager[ServerT]: ...

    # message publishing

    async def publish(
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    # message receiving

    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT: ...
