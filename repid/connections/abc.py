from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any, Protocol, TypedDict

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag


class BaseMessageT(Protocol):
    @property
    def payload(self) -> bytes: ...

    @property
    def headers(self) -> dict[str, str] | None: ...

    @property
    def content_type(self) -> str | None: ...


class SentMessageT(BaseMessageT, Protocol):
    pass


class ReceivedMessageT(BaseMessageT, Protocol):
    @property
    def channel(self) -> str: ...

    @property
    def is_acted_on(self) -> bool: ...

    @property
    def message_id(self) -> str | None:
        """Unique identifier of a message if provided by the message broker."""

    async def ack(self) -> None: ...

    async def nack(self) -> None: ...

    async def reject(self) -> None: ...

    async def reply(
        self,
        *,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        channel: str | None = None,  # if None, message will be sent to the same channel
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Atomically (if supporter by the server) ack and reply to the message."""


class CapabilitiesT(TypedDict):
    supports_acknowledgments: bool
    supports_persistence: bool
    supports_reply: bool
    supports_lightweight_pause: bool


class SubscriberT(Protocol):
    @property
    def is_active(self) -> bool: ...

    @property
    def task(self) -> asyncio.Task: ...

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
