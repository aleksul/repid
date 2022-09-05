from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from repid.logger import logger
from repid.middlewares import InjectMiddleware
from repid.serializer import MessageSerializer

if TYPE_CHECKING:
    from repid.data import Message


@InjectMiddleware
class DummyMessaging:
    supports_delayed_messages = True

    def __init__(self, dsn: str) -> None:
        self.queues: dict[str, list[bytes]] = dict()

    async def consume(self, queue_name: str, topics: frozenset[str]) -> Message:
        logger.debug(
            "Consuming from queue '{queue_name}'.",
            extra=dict(queue_name=queue_name, topics=topics),
        )
        for queue in self.queues.values():
            for data in queue:
                message = MessageSerializer.decode(data)
                if message.topic in topics:
                    queue.remove(data)
                    return message
                await asyncio.sleep(0.1)
        await asyncio.sleep(1.0)
        return await self.consume(queue_name, topics)

    async def enqueue(self, message: Message) -> None:
        logger.debug("Enqueueing message with id: {id_}.", extra=dict(id_=message.id_))
        data = MessageSerializer.encode(message)
        self.queues[message.queue].append(data)

    async def reject(self, message: Message) -> None:
        logger.debug("Rejecting message with id: {id_}.", extra=dict(id_=message.id_))
        data = MessageSerializer.encode(message)
        self.queues[message.queue].insert(0, data)

    async def ack(self, message: Message) -> None:
        logger.debug("Acking message with id: {id_}.", extra=dict(id_=message.id_))

    async def nack(self, message: Message) -> None:
        logger.debug("Nacking message with id: {id_}.", extra=dict(id_=message.id_))

    async def requeue(self, message: Message) -> None:
        logger.debug("Requeueing message with id: {id_}.", extra=dict(id_=message.id_))
        data = MessageSerializer.encode(message)
        self.queues[message.queue].insert(0, data)

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug("Declaring queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        if queue_name not in self.queues:
            self.queues[queue_name] = []

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug("Flushing queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        self.queues[queue_name].clear()

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug("Deleting queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        self.queues.pop(queue_name, None)
