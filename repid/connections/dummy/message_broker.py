from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING, Iterable

from repid.connections.abc import ConsumerT, MessageBrokerT
from repid.connections.dummy.utils import DummyQueue, wait_until
from repid.data._message import Message
from repid.logger import logger
from repid.middlewares import InjectMiddleware

if TYPE_CHECKING:

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

    async def stop(self) -> None:
        await asyncio.sleep(0.1)
        self._started = False
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

    async def __anext__(self) -> tuple[RoutingKeyT, str, ParametersT]:
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


@InjectMiddleware
class DummyMessageBroker(MessageBrokerT):
    def __init__(self) -> None:
        self.queues: dict[str, DummyQueue] = {}

    async def connect(self) -> None:
        logger.info("Connecting to dummy message broker.")
        await asyncio.sleep(0.1)

    async def disconnect(self) -> None:
        logger.info("Disconnecting from dummy message broker.")
        await asyncio.sleep(0.1)

    async def consume(
        self,
        queue_name: str,
        topics: Iterable[str] | None = None,
    ) -> _DummyConsumer:
        logger.debug(
            "Consuming from queue '{queue_name}'.",
            extra=dict(queue_name=queue_name, topics=topics),
        )
        return _DummyConsumer(self, queue_name, topics)

    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Enqueueing message with id: {id_}.", extra=dict(id_=key.id_))
        await asyncio.sleep(0.1)

        delay: datetime | None = wait_until(params)

        msg = Message(key, payload, params or self.PARAMETERS_CLASS())
        if delay is not None:
            self.queues[key.queue].delayed.setdefault(delay, []).append(msg)
        else:
            self.queues[key.queue].simple.put_nowait(msg)

        await asyncio.sleep(0.1)

    async def reject(self, key: RoutingKeyT) -> None:
        logger.debug("Rejecting message with id: {id_}.", extra=dict(id_=key.id_))
        await asyncio.sleep(0.1)

        q = self.queues[key.queue]
        for msg in q.processing:
            if msg.key.id_ == key.id_:
                q.processing.remove(msg)
                q.simple.put_nowait(msg)
                break

        await asyncio.sleep(0.1)

    async def ack(self, key: RoutingKeyT) -> None:
        logger.debug("Acking message with id: {id_}.", extra=dict(id_=key.id_))
        await asyncio.sleep(0.1)

        q = self.queues[key.queue]
        for msg in q.processing:
            if msg.key.id_ == key.id_:
                q.processing.remove(msg)
                break

        await asyncio.sleep(0.1)

    async def nack(self, key: RoutingKeyT) -> None:
        logger.debug("Nacking message with id: {id_}.", extra=dict(id_=key.id_))
        await asyncio.sleep(0.1)

        q = self.queues[key.queue]
        for msg in q.processing:
            if msg.key.id_ == key.id_:
                q.processing.remove(msg)
                q.dead.append(msg)
                break

        await asyncio.sleep(0.1)

    async def requeue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Requeueing message with id: {id_}.", extra=dict(id_=key.id_))
        await self.ack(key)
        await self.enqueue(key, payload, params)

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug("Declaring queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        await asyncio.sleep(0.1)

        if queue_name not in self.queues:
            self.queues[queue_name] = DummyQueue()

        await asyncio.sleep(0.1)

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug("Flushing queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        await asyncio.sleep(0.1)

        if queue_name in self.queues:
            self.queues[queue_name] = DummyQueue()

        await asyncio.sleep(0.1)

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug("Deleting queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        await asyncio.sleep(0.1)

        self.queues.pop(queue_name, None)

        await asyncio.sleep(0.1)
