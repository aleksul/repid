from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING, Iterable

from repid.connections.abc import ConsumerT
from repid.message import MessageCategory

if TYPE_CHECKING:
    from repid.connections.in_memory.message_broker import InMemoryMessageBroker
    from repid.connections.in_memory.utils import Message
    from repid.data.protocols import ParametersT, RoutingKeyT


class _InMemoryConsumer(ConsumerT):
    def __init__(
        self,
        broker: InMemoryMessageBroker,
        queue_name: str,
        topics: Iterable[str] | None,
        max_unacked_messages: int | None = None,  # noqa: ARG002
        category: MessageCategory = MessageCategory.NORMAL,
    ):
        self.broker = broker
        self.queue_name = queue_name
        self._queue = broker.queues[queue_name]
        self.topics = topics
        self.category = category

        self._paused = asyncio.Lock()
        self._started = False

        self.__category_to_consume = {
            MessageCategory.NORMAL: self.__consume_normal,
            MessageCategory.DELAYED: self.__consume_delayed,
            MessageCategory.DEAD: self.__consume_dead,
        }

    async def start(self) -> None:
        await asyncio.sleep(0)
        self._started = True
        await asyncio.sleep(0)

    async def pause(self) -> None:
        if not self._paused.locked():
            await self._paused.acquire()

    async def unpause(self) -> None:
        self._paused.release()

    async def finish(self) -> None:
        await asyncio.sleep(0)
        self._started = False
        while self._queue.processing:
            await self._queue.simple.put(self._queue.processing.pop())
        await asyncio.sleep(0)

    async def __update_delayed(self) -> None:
        await asyncio.sleep(0)
        now = datetime.now()
        pop_soon = []
        for time_, msgs in self._queue.delayed.items():
            if time_ < now:
                pop_soon.append(time_)
                for msg in msgs:
                    self._queue.simple.put_nowait(msg)
        [self._queue.delayed.pop(i) for i in pop_soon]
        await asyncio.sleep(0)

    async def __consume_normal(self) -> Message | None:
        with suppress(asyncio.TimeoutError):
            msg = await asyncio.wait_for(self._queue.simple.get(), timeout=1.0)
            if msg.parameters.is_overdue:  # ttl expired
                self._queue.dead.append(msg)
            elif self.topics and msg.key.topic not in self.topics:  # topics don't match
                self._queue.simple.put_nowait(msg)
            else:  # proper message
                return msg
        return None

    async def __consume_delayed(self) -> Message | None:
        if not self._queue.delayed:
            return None

        soonest = min(self._queue.delayed)

        if len(self._queue.delayed[soonest]) == 1:
            return self._queue.delayed.pop(soonest)[0]
        return self._queue.delayed[soonest].pop(0)

    async def __consume_dead(self) -> Message | None:
        if not self._queue.dead:
            return None

        return self._queue.dead.pop(0)

    async def consume(self) -> tuple[RoutingKeyT, str, ParametersT]:
        await asyncio.sleep(0)
        if not self._started:
            raise RuntimeError("Consumer wasn't started.")
        while self._paused.locked():
            await asyncio.sleep(0.1)

        _consume_fn = self.__category_to_consume[self.category]

        msg: Message | None
        while True:
            await self.__update_delayed()
            if (msg := await _consume_fn()) is not None:
                break
            await asyncio.sleep(0.001)

        self._queue.processing.add(msg)

        await asyncio.sleep(0)
        return (msg.key, msg.payload, msg.parameters)
