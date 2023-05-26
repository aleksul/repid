from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from repid.connections.abc import MessageBrokerT
from repid.connections.in_memory.consumer import _InMemoryConsumer
from repid.connections.in_memory.utils import DummyQueue, Message, wait_until
from repid.logger import logger

if TYPE_CHECKING:
    from datetime import datetime

    from repid.data.protocols import ParametersT, RoutingKeyT


class InMemoryMessageBroker(MessageBrokerT):
    CONSUMER_CLASS = _InMemoryConsumer

    def __init__(self) -> None:
        self.queues: dict[str, DummyQueue] = {}

    async def connect(self) -> None:
        logger.info("Connecting to in-memory message broker.")
        await asyncio.sleep(0)

    async def disconnect(self) -> None:
        logger.info("Disconnecting from in-memory message broker.")
        await asyncio.sleep(0)

    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Enqueueing message with id: {id_}.", extra={"id_": key.id_})
        await asyncio.sleep(0)

        delay: datetime | None = wait_until(params)

        msg = Message(key, payload, params or self.PARAMETERS_CLASS())
        if delay is not None:
            self.queues[key.queue].delayed.setdefault(delay, []).append(msg)
        else:
            self.queues[key.queue].simple.put_nowait(msg)

        await asyncio.sleep(0)

    async def reject(self, key: RoutingKeyT) -> None:
        logger.debug("Rejecting message with id: {id_}.", extra={"id_": key.id_})
        await asyncio.sleep(0)

        q = self.queues[key.queue]
        for msg in q.processing:
            if msg.key.id_ == key.id_:
                q.processing.remove(msg)
                q.simple.put_nowait(msg)
                break

        await asyncio.sleep(0)

    async def ack(self, key: RoutingKeyT) -> None:
        logger.debug("Acking message with id: {id_}.", extra={"id_": key.id_})
        await asyncio.sleep(0)

        q = self.queues[key.queue]
        for msg in q.processing:
            if msg.key.id_ == key.id_:
                q.processing.remove(msg)
                break

        await asyncio.sleep(0)

    async def nack(self, key: RoutingKeyT) -> None:
        logger.debug("Nacking message with id: {id_}.", extra={"id_": key.id_})
        await asyncio.sleep(0)

        q = self.queues[key.queue]
        for msg in q.processing:
            if msg.key.id_ == key.id_:
                q.processing.remove(msg)
                q.dead.append(msg)
                break

        await asyncio.sleep(0)

    async def requeue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Requeueing message with id: {id_}.", extra={"id_": key.id_})
        await self.ack(key)
        await self.enqueue(key, payload, params)

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug("Declaring queue '{queue_name}'.", extra={"queue_name": queue_name})
        await asyncio.sleep(0)

        if queue_name not in self.queues:
            self.queues[queue_name] = DummyQueue()

        await asyncio.sleep(0)

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug("Flushing queue '{queue_name}'.", extra={"queue_name": queue_name})
        await asyncio.sleep(0)

        if queue_name in self.queues:
            self.queues[queue_name] = DummyQueue()

        await asyncio.sleep(0)

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug("Deleting queue '{queue_name}'.", extra={"queue_name": queue_name})
        await asyncio.sleep(0)

        self.queues.pop(queue_name, None)

        await asyncio.sleep(0)


def DummyMessageBroker() -> InMemoryMessageBroker:  # noqa: N802
    from warnings import warn

    warn(
        "DummyMessageBroker was renamed to InMemoryMessageBroker.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return InMemoryMessageBroker()
