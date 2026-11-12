"""Internal helper dataclasses for Pub/Sub protocol implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from repid.connections.abc import MessageCallbackT, ReceivedMessageT
    from repid.data.concurrency import Concurrency


@dataclass(slots=True, kw_only=True)
class ChannelConfig:
    """Internal configuration for a subscribed channel."""

    channel: str
    subscription_path: str
    callback: MessageCallbackT
    concurrency: Concurrency | None = None
    client_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass(slots=True, kw_only=True)
class QueuedDelivery:
    """Internal wrapper for a queued message delivery."""

    callback: MessageCallbackT
    message: ReceivedMessageT
