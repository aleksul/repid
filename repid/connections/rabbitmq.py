from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

import aio_pika as aiopika
from yarl import URL

from repid.logger import logger
from repid.middlewares import InjectMiddleware
from repid.serializer import MessageSerializer

if TYPE_CHECKING:
    from repid.data import Message


@InjectMiddleware
class RabbitMessaging:
    supports_delayed_messages = True
    queues_durable = True

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.__conn = aiopika.RobustConnection(URL(dsn))
        self.__id_to_deleviry_tag: dict[str, int] = {}
        self.__channel: aiopika.abc.AbstractChannel | None = None

    async def _channel(self) -> aiopika.abc.AbstractChannel:
        if not self.__conn.connected.is_set():
            await self.__conn.connect()
        if self.__channel is not None:
            if self.__channel.is_closed:
                await self.__channel.reopen()
            return self.__channel
        self.__channel = await self.__conn.channel(publisher_confirms=False)
        return self.__channel

    async def consume(self, queue_name: str, topics: frozenset[str]) -> Message:
        logger.debug(
            "Consuming from queue '{queue_name}'.",
            extra=dict(queue_name=queue_name, topics=topics),
        )
        channel = await self._channel()
        queue = await channel.get_queue(queue_name)
        while True:
            message = await queue.get(fail=False)
            if message is None:  # means that the queue is empty/non-existent
                await asyncio.sleep(0.1)
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
        logger.debug("Enqueueing message with id: {id_}.", extra=dict(id_=message.id_))
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
        logger_extra = dict(id_=message.id_)
        logger.debug("Acking message with id: {id_}.", extra=logger_extra)
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(
                "Can't ack unknown delivery tag for message with id: {id_}.",
                extra=logger_extra,
            )
            return
        channel = await self._channel()
        await channel.channel.basic_ack(delivery_tag)

    async def nack(self, message: Message) -> None:
        logger_extra = dict(id_=message.id_)
        logger.debug("Nacking message with id: {id_}.", extra=logger_extra)
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(
                "Can't nack unknown delivery tag for message with id: {id_}.",
                extra=logger_extra,
            )
            return
        channel = await self._channel()
        await channel.channel.basic_nack(delivery_tag, requeue=False)  # will trigger dlx

    async def reject(self, message: Message) -> None:
        logger_extra = dict(id_=message.id_)
        logger.debug("Rejecting message with id: {id_}.", extra=logger_extra)
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(
                "Can't reject unknown delivery tag for message with id: {id_}.",
                extra=logger_extra,
            )
            return
        channel = await self._channel()
        await channel.channel.basic_reject(delivery_tag, requeue=True)

    async def requeue(self, message: Message) -> None:
        logger_extra = dict(id_=message.id_)
        logger.debug("Requeueing message with id: {id_}.", extra=logger_extra)
        if (delivery_tag := self.__id_to_deleviry_tag.pop(message.id_, None)) is None:
            logger.error(
                "Can't requeue unknown delivery tag for message with id: {id_}.",
                extra=logger_extra,
            )
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
        logger.debug("Declaring queue '{queue_name}'.", extra=dict(queue_name=queue_name))
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
        logger.debug("Flushing queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        channel = await self._channel()

        async def _flush(queue_name: str) -> None:
            queue = await channel.get_queue(queue_name)
            await queue.purge()

        await asyncio.gather(
            *[
                _flush(queue_name),
                _flush(f"{queue_name}:delayed"),
                _flush(f"{queue_name}:dead"),
            ]
        )

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug("Deleting queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        channel = await self._channel()
        await asyncio.gather(
            *[
                channel.queue_delete(queue_name),
                channel.queue_delete(f"{queue_name}:delayed"),
                channel.queue_delete(f"{queue_name}:dead"),
            ]
        )
