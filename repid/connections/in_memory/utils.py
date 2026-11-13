from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TypeVar

from repid._utils._typing import override

_Item = TypeVar("_Item")


class _DeliveryQueue(asyncio.Queue[_Item]):
    __slots__ = ("_readable",)

    def __init__(self) -> None:
        super().__init__()
        self._readable = asyncio.Event()

    @override
    def put_nowait(self, item: _Item) -> None:
        super().put_nowait(item)
        self._readable.set()

    @override
    def get_nowait(self) -> _Item:
        item = super().get_nowait()
        if self.empty():
            self._readable.clear()
        return item

    async def get_when_resumed(self, resumed: asyncio.Event) -> _Item:
        while True:
            await self._readable.wait()
            await resumed.wait()
            # Both events can change while the waiting task is being woken.
            if resumed.is_set() and not self.empty():
                return self.get_nowait()


@dataclass
class DummyQueue:
    """A simple in-memory queue implementation."""

    queue: _DeliveryQueue[Message] = field(default_factory=_DeliveryQueue)
    processing: set[Message] = field(default_factory=set)

    @dataclass(eq=False)
    class Message:
        """A message in the queue that implements BaseMessageT protocol."""

        payload: bytes
        headers: dict[str, str] | None = None
        content_type: str | None = None
        reply_to: str | None = None
        message_id: str | None = None
