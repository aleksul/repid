from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING, Iterable

from repid.connections.abc import ConsumerT

if TYPE_CHECKING:
    from repid.connections.dummy.message_broker import DummyMessageBroker
    from repid.data.protocols import MessageT, ParametersT, RoutingKeyT


class _DummyConsumer(ConsumerT):
    def __init__(self, broker: DummyMessageBroker, queue_name: str, topics: Iterable[str] | None):
        self.broker = broker
        self.queue_name = queue_name
        self._queue = broker.queues[queue_name]
        self.topics = topics

        self._started = False

    async def start(self) -> None:
        await asyncio.sleep(0.1)
        self._started = True
        await asyncio.sleep(0.1)

    async def finish(self) -> None:
        await asyncio.sleep(0.1)
        self._started = False
        for msg in self._queue.processing:
            self._queue.processing.remove(msg)
            self._queue.simple.put_nowait(msg)
        await asyncio.sleep(0.1)

    async def __update_delayed(self) -> None:
        await asyncio.sleep(0.1)
        now = datetime.now()
        for time_, msgs in self._queue.delayed.items():
            if time_ > now:
                self._queue.delayed.pop(time_)
                for msg in msgs:
                    self._queue.simple.put_nowait(msg)
        await asyncio.sleep(0.1)

    async def consume(self) -> tuple[RoutingKeyT, str, ParametersT]:
        await asyncio.sleep(0.1)
        if not self._started:
            raise RuntimeError("Consumer wasn't started.")
        msg: MessageT
        while True:
            await self.__update_delayed()
            with suppress(asyncio.TimeoutError):
                msg = await asyncio.wait_for(self._queue.simple.get(), timeout=1.0)
                if msg.parameters.is_overdue:
                    self._queue.dead.append(msg)
                elif self.topics and msg.key.topic not in self.topics:
                    self._queue.simple.put_nowait(msg)
                else:
                    break
            await asyncio.sleep(0.1)
        self._queue.processing.add(msg)

        await asyncio.sleep(0.1)
        return (msg.key, msg.payload, msg.parameters)
