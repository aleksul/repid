"""Internal helper dataclasses for Pub/Sub protocol implementation."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT


@dataclass(slots=True, kw_only=True)
class ChannelConfig:
    """Internal configuration for a subscribed channel."""

    channel: str
    subscription_path: str
    callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]]


@dataclass(slots=True, kw_only=True)
class QueuedDelivery:
    """Internal wrapper for a queued message delivery."""

    callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]]
    message: ReceivedMessageT
