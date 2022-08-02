from typing import Iterable, Literal, Optional

import aio_pika as aiopika
from yarl import URL

from repid.connection import Messaging
from repid.data import (
    AnyMessageT,
    DeferredByMessage,
    DeferredCronMessage,
    DeferredMessage,
    Message,
    PrioritiesT,
    Serializer,
)
from repid.middlewares.wrapper import InjectMiddleware
from repid.utils import next_exec_time


@InjectMiddleware
class AMQPMessaging(Messaging):
    supports_delayed_messages = True
    queue_type: Literal["FIFO", "LIFO", "SIMPLE"] = "FIFO"

    def __init__(self, dsn: str) -> None:
        self.conn = aiopika.RobustConnection(URL(dsn))
        self._priorities = {PrioritiesT.LOW: 0, PrioritiesT.MEDIUM: 1, PrioritiesT.HIGH: 2}

    async def consume(
        self, queue_name: str, topics: Optional[Iterable[str]]
    ) -> Optional[AnyMessageT]:
        channel = await self.conn.channel()
        queue = await channel.get_queue(queue_name)
        message = await queue.get(no_ack=True, fail=False)
        if message is None:
            return None
        return Serializer.decode(message.body)  # type: ignore[return-value]

    async def enqueue(self, message: AnyMessageT) -> None:
        channel = await self.conn.channel()
        expiration = None
        if not isinstance(message, Message):
            expiration = message.delay_until or next_exec_time(message)
        await channel.default_exchange.publish(
            aiopika.Message(
                body=Serializer.encode(message),
                priority=self._priorities.get(message.priority, 1),
                expiration=expiration,
                message_id=message.id_,
                timestamp=message.timestamp,
            ),
            routing_key=f"{message.queue}{':delayed' if isinstance(message, DeferredMessage) else ''}",  # noqa: E501
        )

    async def ack(self, message: AnyMessageT) -> None:
        """Informs message broker that job execution succeed."""

    async def nack(self, message: AnyMessageT) -> None:
        """Informs message broker that job execution failed."""

    async def requeue(self, message: AnyMessageT) -> None:
        """Requeues the message."""

    async def queue_declare(self, queue_name: str) -> None:
        channel = await self.conn.channel()
        await channel.declare_queue(
            queue_name,
            arguments={"x-max-priority": 3},
        )
        await channel.declare_queue(
            f"{queue_name}:delayed",
            arguments={
                "x-max-priority": 3,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )

    async def queue_flush(self, queue_name: str) -> None:
        channel = await self.conn.channel()
        queue = await channel.get_queue(queue_name)
        delayed_queue = await channel.get_queue(f"{queue_name}:delayed")
        await queue.purge()
        await delayed_queue.purge()

    async def queue_delete(self, queue_name: str) -> None:
        channel = await self.conn.channel()
        await channel.queue_delete(queue_name)
        await channel.queue_delete(f"{queue_name}:delayed")

    async def maintenance(self) -> None:
        return
