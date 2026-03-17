from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class DummyQueue:
    """A simple in-memory queue implementation."""

    queue: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    processing: set[Message] = field(default_factory=set)

    @dataclass
    class Message:
        """A message in the queue that implements BaseMessageT protocol."""

        payload: bytes
        headers: dict[str, str] | None = None
        content_type: str | None = None
        message_id: str | None = None

        def __hash__(self) -> int:
            return hash(
                (
                    self.payload,
                    self.content_type,
                    tuple(sorted(self.headers.items())) if self.headers else None,
                    self.message_id,
                ),
            )

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, DummyQueue.Message):
                return False
            return (
                self.payload == other.payload
                and self.headers == other.headers
                and self.content_type == other.content_type
                and self.message_id == other.message_id
            )
