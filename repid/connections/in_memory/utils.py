from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class DummyQueue:
    """A simple in-memory queue implementation."""

    queue: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    processing: set[Message] = field(default_factory=set)

    @dataclass(eq=False)
    class Message:
        """A message in the queue that implements BaseMessageT protocol."""

        payload: bytes
        headers: dict[str, str] | None = None
        content_type: str | None = None
        reply_to: str | None = None
        message_id: str | None = None
