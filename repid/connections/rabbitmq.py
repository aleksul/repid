import logging
from datetime import datetime
from typing import Dict, FrozenSet, Union

import aio_pika as aiopika
import anyio
from yarl import URL

from repid.data import Message
from repid.middlewares import InjectMiddleware
from repid.serializer import MessageSerializer

logger = logging.getLogger(__name__)


@InjectMiddleware
class RabbitMessaging:
    supports_delayed_messages = True
    queues_durable = True

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.__conn = aiopika.RobustConnection(URL(dsn))
        self.__id_to_deleviry_tag: Dict[str, int] = {}
        self.__channel: Union[aiopika.abc.AbstractChannel, None] = None

    async def _channel(self) -> aiopika.abc.AbstractChannel:
        if not self.__conn.connected.is_set():
            await self.__conn.connect()
        if self.__channel is not None:
            if self.__channel.is_closed:
                await self.__channel.reopen()
            return self.__channel
        self.__channel = await self.__conn.channel(publisher_confirms=False)
        return self.__channel

    async def consume(self, queue_name: str, topics: FrozenSet[str]) -> Message:
        logger.debug(f"Consuming from {queue_name = }; {topics = }.")
        channel = await self._channel()
        queue = await channel.get_queue(queue_name)
        while True:
            message = await queue.get(fail=False)
            if message is None:
                continue
            decoded: Message = MessageSerializer.decode(message.body)
            if decoded.topic not in topics:
                await message.reject(requeue=True)
                continue
            if decoded.is_overdue:
                await message.nack(requeue=False)
                continue
            if message.delivery_tag is not None:
                self.__id_to_deleviry_tag[decoded.id_] = message.delivery_tag
            return decoded

    async def enqueue(self, message: Message) -> None:
        logger.debug(f"Enqueuing {message = }.")
        channel = await self._channel()
        await channel.default_exchange.publish(
            aiopika.Message(
                body=MessageSerializer.encode(message),
                priority=message.priority - 1,
                expiration=datetime.fromtimestamp(message.delay_until)
                if message.delay_until is not None
                else None,
                message_id=message.id_,
                timestamp=message.timestamp,
            ),
            routing_key=f"{message.queue}{':delayed' if message.delay_until is not None else ''}",
        )

    async def ack(self, message: Message) -> None:
        logger.debug(f"Acking {message = }.")
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(f"Can't ack unknown delivery tag for {message.id_ = }.")
            return
        channel = await self._channel()
        await channel.channel.basic_ack(delivery_tag)

    async def nack(self, message: Message) -> None:
        logger.debug(f"Nacking {message = }.")
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(f"Can't nack unknown delivery tag for {message.id_ = }.")
            return
        channel = await self._channel()
        await channel.channel.basic_nack(delivery_tag, requeue=False)  # will trigger dlx

    async def reject(self, message: Message) -> None:
        logger.debug(f"Rejecting {message = }.")
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(f"Can't reject unknown delivery tag for {message.id_ = }.")
            return
        channel = await self._channel()
        await channel.channel.basic_reject(delivery_tag, requeue=True)

    async def requeue(self, message: Message) -> None:
        logger.debug(f"Requeueing {message = }.")
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(f"Can't requeue unknown delivery tag for {message.id_ = }.")
            return
        channel = await self._channel()
        async with channel.transaction():
            await channel.channel.basic_ack(delivery_tag)
            await channel.default_exchange.publish(
                aiopika.Message(
                    body=MessageSerializer.encode(message),
                    priority=message.priority - 1,
                    expiration=datetime.fromtimestamp(message.delay_until)
                    if message.delay_until is not None
                    else None,
                    message_id=message.id_,
                    timestamp=message.timestamp,
                ),
                routing_key=(
                    f"{message.queue}{':delayed' if message.delay_until is not None else ''}"
                ),
            )

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug(f"Declaring queue {queue_name = }.")
        channel = await self._channel()
        await channel.declare_queue(
            f"{queue_name}:dead",
            durable=True,
            arguments={
                "x-max-priority": 3,
            },
        )
        await channel.declare_queue(
            queue_name,
            durable=self.queues_durable,
            arguments={
                "x-max-priority": 3,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": f"{queue_name}:dead",
            },
        )
        await channel.declare_queue(
            f"{queue_name}:delayed",
            durable=self.queues_durable,
            arguments={
                "x-max-priority": 3,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug(f"Flushing queue {queue_name = }.")
        channel = await self._channel()

        async def _flush(queue_name: str) -> None:
            queue = await channel.get_queue(queue_name)
            await queue.purge()

        async with anyio.create_task_group() as tg:
            tg.start_soon(_flush, queue_name)
            tg.start_soon(_flush, f"{queue_name}:delayed")
            tg.start_soon(_flush, f"{queue_name}:dead")

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug(f"Deleting queue {queue_name = }.")
        channel = await self._channel()
        async with anyio.create_task_group() as tg:
            tg.start_soon(channel.queue_delete, queue_name)
            tg.start_soon(channel.queue_delete, f"{queue_name}:delayed")
            tg.start_soon(channel.queue_delete, f"{queue_name}:dead")
