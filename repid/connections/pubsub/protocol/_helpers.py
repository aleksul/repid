"""Internal helper dataclasses for Pub/Sub protocol implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from repid.connections.abc import MessageCallbackT, ReceivedMessageT


@dataclass(slots=True, kw_only=True)
class ChannelConfig:
    """Internal configuration for a subscribed channel."""

    channel: str
    subscription_path: str
    callback: MessageCallbackT


@dataclass(slots=True, kw_only=True)
class QueuedDelivery:
    """Internal wrapper for a queued message delivery."""

    callback: MessageCallbackT
    message: ReceivedMessageT
