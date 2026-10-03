from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, TypedDict

from repid.data.message import MessageData
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag


class MessageAction(str, Enum):
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
        """Recommended seconds between keep_alive() calls, or None if not needed."""

    async def keep_alive(self) -> None:
        """Signal the broker that this message is still being processed."""

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
    supports_keep_alive: bool
    supports_worker_native_messages: bool
    supports_channel_native_messages: bool
    supports_worker_native_bytes: bool
    supports_channel_native_bytes: bool
    supports_worker_pause: bool
    supports_channel_pause: bool
    supports_worker_oversized_delivery: bool
    supports_channel_oversized_delivery: bool
    supports_worker_oversized_blocking: bool
    supports_channel_oversized_blocking: bool


def broker_capabilities(
    *,
    native_reply: bool = False,
    keep_alive: bool = False,
    worker_pause: bool = False,
    channel_pause: bool = False,
    native: bool = False,
) -> CapabilitiesT:
    """Conservative defaults; ``native`` is for the fully controlled in-memory broker."""
    return {
        "supports_native_reply": native_reply,
        "supports_keep_alive": keep_alive,
        "supports_worker_native_messages": native,
        "supports_channel_native_messages": native,
        "supports_worker_native_bytes": native,
        "supports_channel_native_bytes": native,
        "supports_worker_pause": worker_pause,
        "supports_channel_pause": channel_pause,
        "supports_worker_oversized_delivery": native,
        "supports_channel_oversized_delivery": native,
        "supports_worker_oversized_blocking": native,
        "supports_channel_oversized_blocking": native,
    }


def validate_native_flow(flow: NativeFlow, capabilities: CapabilitiesT) -> None:
    """Adapters may not silently ignore a resolved outstanding-delivery requirement."""
    for scope, window in [
        ("worker", flow.worker),
        *(("channel", value) for value in flow.channels.values()),
    ]:
        for dimension, cap in (
            ("messages", window.max_messages),
            ("bytes", window.max_payload_bytes),
        ):
            if cap is None:
                continue
            if not capabilities[f"supports_{scope}_native_{dimension}"]:  # type: ignore[literal-required]
                raise ValueError(f"Unsupported native {dimension} window at {scope} scope")
            if dimension == "bytes":
                mode = "delivery" if window.oversized_delivery == "deliver" else "blocking"
                if not capabilities[f"supports_{scope}_oversized_{mode}"]:  # type: ignore[literal-required]
                    raise ValueError(
                        f"Unsupported native-byte oversized {mode} mode at {scope} scope",
                    )


class SubscriberT(Protocol):
    @property
    def native_flow(self) -> NativeFlow:
        """Actual transport windows, including automatic fallback discovered at startup."""
        ...

    @property
    def is_active(self) -> bool: ...

    @property
    def task(self) -> asyncio.Task: ...

    async def pause(self, channel: str | None = None) -> None: ...

    async def resume(self, channel: str | None = None) -> None: ...

    async def stop(self) -> None: ...

    async def finish(self) -> None: ...


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
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
    ) -> SubscriberT: ...
