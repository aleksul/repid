"""Internal helper dataclasses for Pub/Sub protocol implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import uuid4

from repid.data.concurrency import Concurrency

if TYPE_CHECKING:
    from repid.connections.abc import ChannelIntake


@dataclass(slots=True, kw_only=True)
class ChannelConfig:
    """Internal configuration for a subscribed channel."""

    channel: str
    subscription_path: str
    intake: ChannelIntake
    # Flow control for the stream: the worker's limits plus the server's prefetch.
    concurrency: Concurrency = field(default_factory=Concurrency)
    client_id: str = field(default_factory=lambda: str(uuid4()))
