from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Iterable
from uuid import uuid4

import aiormq
import orjson
from aiormq.abc import Basic

from repid.connections.abc import ConsumerT, MessageBrokerT
from repid.connections.rabbitmq.utils import (
    MessageContent,
    durable_message_decider,
    qnc,
    wait_until,
)
from repid.data.priorities import PrioritiesT
from repid.logger import logger
from repid.middlewares import InjectMiddleware

if TYPE_CHECKING:
    from repid.connections.rabbitmq.protocols import (
        DurableMessageDeciderT,
        QueueNameConstructorT,
    )
    from repid.data.protocols import ParametersT, RoutingKeyT


class _RabbitConsumer(ConsumerT):
    def __init__(
        self,
        broker: RabbitBroker,
        queue_name: str,
        topics: Iterable[str] | None = None,
    ) -> None:
        self.broker = broker
        self.channel: aiormq.abc.AbstractChannel = broker._channel()
        self.queue_name = queue_name
        self.topics = topics
        self.queue: asyncio.Queue[tuple[RoutingKeyT, str, ParametersT]] = asyncio.Queue()
        self._consumer_tag: str | None = None

    async def __anext__(self) -> tuple[RoutingKeyT, str, ParametersT]:
        return await self.queue.get()

    async def start(self) -> None:
        confirmation = await self.channel.basic_consume(
            self.queue_name, self.on_message, no_ack=False
        )
        if not isinstance(confirmation, Basic.ConsumeOk):
            raise RuntimeError("Haven't started consumer properly.")
        self._consumer_tag = confirmation.consumer_tag

    async def stop(self) -> None:
        if self._consumer_tag is None:
            return
        confirmation = await self.channel.basic_cancel(self._consumer_tag)
        if not isinstance(confirmation, Basic.CancelOk):
            raise RuntimeError("Haven't stopped consumer propertly.")

    async def on_message(self, message: aiormq.abc.DeliveredMessage) -> None:
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
            await self.channel.basic_reject(message.delivery_tag)
            return

        if self.topics and msg_topic not in self.topics:
            await self.channel.basic_reject(message.delivery_tag)
            return

        decoded: MessageContent = orjson.loads(message.body)
        params = self.broker.PARAMETERS_CLASS.decode(decoded["parameters"])

        if params.is_overdue:
            await self.channel.basic_nack(message.delivery_tag, requeue=False)

        self.broker._id_to_delivery_tag[msg_id] = message.delivery_tag

        await self.queue.put(
            (
                self.broker.ROUTING_KEY_CLASS(
                    id_=msg_id,
                    topic=msg_topic,
                    queue=msg_queue,
                    priority=message.header.properties.priority or PrioritiesT.MEDIUM.value,
                ),
                decoded["payload"],
                params,
            )
        )


@InjectMiddleware
class RabbitBroker(MessageBrokerT):
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
        self._id_to_delivery_tag: dict[str, int] = dict()

    def _channel(self) -> aiormq.abc.AbstractChannel:
        if self.__channel is None:
            raise ConnectionError("Channel isn't set.")
        return self.__channel

    async def connect(self) -> None:
        if self.__connection is None:
            self.__connection = await aiormq.connect(self.dsn)
        if self.__channel is None:
            self.__channel = await self.__connection.channel()

    async def disconnect(self) -> None:
        if self.__connection is not None:
            await self.__connection.close()
        self.__channel = None
        self.__connection = None

    async def consume(
        self,
        queue_name: str,
        topics: Iterable[str] | None = None,
    ) -> _RabbitConsumer:
        logger.debug(
            "Consuming from queue '{queue_name}'.",
            extra=dict(queue_name=queue_name, topics=topics),
        )
        return _RabbitConsumer(self, queue_name, topics)

    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Enqueueing message with id: {id_}.", extra=dict(id_=key.id_))

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

        confirmation = await self._channel().basic_publish(
            body=orjson.dumps(body),
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
        if (delivery_tag := self._id_to_delivery_tag.pop(key.id_, None)) is None:
            logger.error(
                "Can't ack unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        await self._channel().basic_ack(delivery_tag)

    async def nack(self, key: RoutingKeyT) -> None:
        logger_extra = dict(routing_key=key)
        logger.debug("Nacking message ({routing_key}).", extra=logger_extra)
        if (delivery_tag := self._id_to_delivery_tag.pop(key.id_, None)) is None:
            logger.error(
                "Can't nack unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        await self._channel().basic_nack(delivery_tag, requeue=False)  # will trigger dlx

    async def reject(self, key: RoutingKeyT) -> None:
        logger_extra = dict(routing_key=key)
        logger.debug("Rejecting message ({routing_key}).", extra=logger_extra)
        if (delivery_tag := self._id_to_delivery_tag.pop(key.id_, None)) is None:
            logger.error(
                "Can't reject unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        await self._channel().basic_reject(delivery_tag, requeue=True)

    async def requeue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger_extra = dict(routing_key=key)
        logger.debug("Requeueing message ({routing_key}).", extra=logger_extra)
        await self.ack(key)
        await self.enqueue(key, payload, params)

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug("Declaring queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        channel = self._channel()
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
        channel = self._channel()

        await asyncio.gather(
            *[
                channel.queue_purge(queue_name),
                channel.queue_purge(f"{queue_name}:delayed"),
                channel.queue_purge(f"{queue_name}:dead"),
            ],
            return_exceptions=True,
        )

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug("Deleting queue '{queue_name}'.", extra=dict(queue_name=queue_name))
        channel = self._channel()
        await asyncio.gather(
            *[
                channel.queue_delete(queue_name),
                channel.queue_delete(f"{queue_name}:delayed"),
                channel.queue_delete(f"{queue_name}:dead"),
            ],
            return_exceptions=True,
        )
