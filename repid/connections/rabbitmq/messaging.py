from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import TYPE_CHECKING, AsyncIterator
from uuid import uuid4

import aiormq
from aiormq.abc import Basic

from repid.connections.rabbitmq.protocols import MessageContent
from repid.connections.rabbitmq.utils import durable_message_decider, qnc, wait_until
from repid.data._key import RoutingKey
from repid.data._message import Message
from repid.data._parameters import Parameters
from repid.data.priorities import PrioritiesT
from repid.logger import logger
from repid.middlewares import InjectMiddleware

if TYPE_CHECKING:
    from repid.connections.rabbitmq.protocols import (
        DurableMessageDeciderT,
        QueueNameConstructorT,
    )
    from repid.data.protocols import MessageT, ParametersT, RoutingKeyT


@InjectMiddleware
class RabbitMessaging:
    ROUTING_KEY_CLASS: type[RoutingKeyT] = RoutingKey
    PARAMETERS_CLASS: type[ParametersT] = Parameters

    def __init__(
        self,
        dsn: str,
        *,
        queue_name_constructor: QueueNameConstructorT = qnc,
        is_durable_decider: DurableMessageDeciderT = durable_message_decider,
    ) -> None:
        self.dsn = dsn
        self.qnc = queue_name_constructor
        self.idd = is_durable_decider
        self.__connection: aiormq.abc.AbstractConnection | None = None
        self.__channel: aiormq.abc.AbstractChannel | None = None
        self.__id_to_delivery_tag: dict[str, int] = dict()

    async def _channel(self) -> aiormq.abc.AbstractChannel:
        if self.__connection is None:
            self.__connection = aiormq.Connection(self.dsn)
        if self.__channel is None:
            self.__channel = await self.__connection.channel(publisher_confirms=False)
        if self.__channel.is_closed:
            await self.__channel.open()
            return self.__channel
        return self.__channel

    async def consume(
        self,
        queue_name: str,
        topics: frozenset[str] | None = None,
    ) -> AsyncIterator[tuple[RoutingKeyT, str, ParametersT]]:
        logger.debug(
            "Consuming from queue '{queue_name}'.",
            extra=dict(queue_name=queue_name, topics=topics),
        )
        _queue: asyncio.Queue[MessageT] = asyncio.Queue()

        channel = await self._channel()

        async def on_message(message: aiormq.abc.DeliveredMessage) -> None:
            if message.header.properties.message_id is None:
                message.header.properties.message_id = uuid4().hex
            msg_id = message.header.properties.message_id

            if message.delivery_tag is None:
                logger.error(
                    "Can't process message (id: {id_}) with unknown delivery tag.",
                    extra=dict(id_=msg_id),
                )
                return

            msg_topic: str | None = None
            msg_queue: str = "default"

            if message.header.properties.headers is not None:
                msg_topic = message.header.properties.headers.get("topic", None)
                msg_queue = message.header.properties.headers.get("queue", "default")

            if msg_topic is None:
                await channel.basic_reject(message.delivery_tag)
                return

            if topics and msg_topic not in topics:
                await channel.basic_reject(message.delivery_tag)
                return

            decoded: MessageContent = json.loads(message.body)
            params = self.PARAMETERS_CLASS.decode(decoded["parameters"])

            if params.is_overdue:
                await channel.basic_nack(message.delivery_tag, requeue=False)

            self.__id_to_delivery_tag[msg_id] = message.delivery_tag

            await _queue.put(
                Message(
                    self.ROUTING_KEY_CLASS(
                        id_=msg_id,
                        topic=msg_topic,
                        queue=msg_queue,
                        priority=message.header.properties.priority or PrioritiesT.MEDIUM.value,
                    ),
                    decoded["payload"],
                    params,
                )
            )

        await channel.basic_consume(
            queue=queue_name,
            consumer_callback=on_message,
            no_ack=True,
        )

        while True:
            msg = await _queue.get()
            yield (msg.key, msg.payload, msg.parameters)

    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Enqueueing message with id: {id_}.", extra=dict(id_=key.id_))
        channel = await self._channel()

        body = MessageContent(
            payload=payload,
            parameters=params.encode() if params is not None else "",
        )

        exp: str | None = None
        if (delayed := wait_until(params)) is not None:
            millis = int(
                (delayed - datetime.now()).total_seconds() * 1000
            )  # milliseconds as an integer
            if millis > 0:
                exp = str(millis)

        confirmation = await channel.basic_publish(
            body=json.dumps(body).encode(),
            routing_key=self.qnc(key.queue, delayed=exp is not None),
            properties=aiormq.spec.Basic.Properties(
                message_id=key.id_,
                priority=key.priority,
                expiration=exp,
                delivery_mode=2 if self.idd(key) else 1,
                timestamp=params.timestamp if params is not None else None,
                headers=dict(queue=key.queue, topic=key.topic),
            ),
            mandatory=True,
        )
        if not isinstance(confirmation, Basic.Ack):
            raise ConnectionError("Message wasn't published.")

    async def ack(self, key: RoutingKeyT) -> None:
        logger_extra = dict(routing_key=key)
        logger.debug("Acking message ({routing_key}).", extra=logger_extra)
        if (delivery_tag := self.__id_to_delivery_tag.pop(key.id_, None)) is None:
            logger.error(
                "Can't ack unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        channel = await self._channel()
        await channel.basic_ack(delivery_tag)

    async def nack(self, key: RoutingKeyT) -> None:
        logger_extra = dict(routing_key=key)
        logger.debug("Nacking message ({routing_key}).", extra=logger_extra)
        if (delivery_tag := self.__id_to_delivery_tag.pop(key.id_, None)) is None:
            logger.error(
                "Can't nack unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        channel = await self._channel()
        await channel.basic_nack(delivery_tag, requeue=False)  # will trigger dlx

    async def reject(self, key: RoutingKeyT) -> None:
        logger_extra = dict(routing_key=key)
        logger.debug("Rejecting message ({routing_key}).", extra=logger_extra)
        if (delivery_tag := self.__id_to_delivery_tag.pop(key.id_, None)) is None:
            logger.error(
                "Can't reject unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        channel = await self._channel()
        await channel.basic_reject(delivery_tag, requeue=True)

    async def requeue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger_extra = dict(routing_key=key)
        logger.debug("Requeueing message ({routing_key}).", extra=logger_extra)
        channel = await self._channel()
        await channel.tx_select()
        try:
            await self.ack(key)
            await self.enqueue(key, payload, params)
        except Exception:
            logger.exception("Unsuccessful message requeue.")
            await channel.tx_rollback()
        else:
            await channel.tx_commit()

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug("Declaring queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        channel = await self._channel()
        await channel.queue_declare(
            f"{queue_name}:dead",
            durable=True,
            arguments={
                "x-max-priority": 9,
            },
        )
        await channel.queue_declare(
            queue_name,
            durable=True,
            arguments={
                "x-max-priority": 9,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": f"{queue_name}:dead",
            },
        )
        await channel.queue_declare(
            f"{queue_name}:delayed",
            durable=True,
            arguments={
                "x-max-priority": 9,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )

    async def queue_flush(self, queue_name: str) -> None:
        logger.debug("Flushing queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        channel = await self._channel()

        await asyncio.gather(
            *[
                channel.queue_purge(queue_name),
                channel.queue_purge(f"{queue_name}:delayed"),
                channel.queue_purge(f"{queue_name}:dead"),
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
