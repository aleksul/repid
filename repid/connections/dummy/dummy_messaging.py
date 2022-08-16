import logging
from typing import Dict, FrozenSet, List

import anyio

from repid.data import Message
from repid.middlewares import InjectMiddleware
from repid.serializer import MessageSerializer

logger = logging.getLogger(__name__)


@InjectMiddleware
class DummyMessaging:
    supports_delayed_messages = True

    def __init__(self, dsn: str) -> None:
        self.queues: Dict[str, List[bytes]] = dict()

    async def consume(self, queue_name: str, topics: FrozenSet[str]) -> Message:
        logger.debug(f"Consuming from {queue_name = }; {topics = }.")
        for queue in self.queues.values():
            for data in queue:
                message = MessageSerializer.decode(data)
                if message.topic in topics:
                    queue.remove(data)
                    return message
                await anyio.sleep(0.1)
        await anyio.sleep(1.0)
        return await self.consume(queue_name, topics)

    async def enqueue(self, message: Message) -> None:
        logger.debug(f"Enqueuing {message = }.")
        data = MessageSerializer.encode(message)
        self.queues[message.queue].append(data)

    async def reject(self, message: Message) -> None:
        logger.debug(f"Rejecting {message = }.")
        data = MessageSerializer.encode(message)
        self.queues[message.queue].insert(0, data)

    async def ack(self, message: Message) -> None:
        logger.debug(f"Acking {message = }.")

    async def nack(self, message: Message) -> None:
        logger.debug(f"Nacking {message = }.")

    async def requeue(self, message: Message) -> None:
        logger.debug(f"Requeueing {message = }.")
        data = MessageSerializer.encode(message)
        self.queues[message.queue].insert(0, data)

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug(f"Declaring queue {queue_name = }.")
        if queue_name not in self.queues:
            self.queues[queue_name] = []

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug(f"Flushing queue {queue_name = }.")
        self.queues[queue_name].clear()

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug(f"Deleting queue {queue_name = }.")
        self.queues.pop(queue_name, None)
