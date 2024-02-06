from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, cast

import aiormq
from aiormq.abc import Basic

from repid._utils import JSON_ENCODER
from repid.connections.abc import MessageBrokerT
from repid.connections.rabbitmq.consumer import _RabbitConsumer
from repid.connections.rabbitmq.utils import (
    MessageContent,
    _Consumers,
    durable_message_decider,
    qnc,
    wait_until,
)
from repid.logger import logger

if TYPE_CHECKING:
    from aiormq.channel import Channel

    from repid.connections.rabbitmq.protocols import (
        DurableMessageDeciderT,
        QueueNameConstructorT,
    )
    from repid.data.protocols import ParametersT, RoutingKeyT


class RabbitMessageBroker(MessageBrokerT):
    CONSUMER_CLASS = _RabbitConsumer

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
        self._id_to_delivery_tag: dict[str, int] = {}

    @property
    def _channel(self) -> aiormq.abc.AbstractChannel:
        if self.__channel is None or self.__channel.is_closed:  # pragma: no cover
            raise ConnectionError("Channel isn't available.")
        return self.__channel

    async def connect(self) -> None:
        if self.__connection is None:
            self.__connection = await aiormq.connect(self.dsn)
        if self.__channel is None:
            self.__channel = await self.__connection.channel()
            cast("Channel", self.__channel).consumers = cast("dict[str, Callable]", _Consumers())

    async def disconnect(self) -> None:
        if self.__connection is not None:
            await self.__connection.close()
        self.__channel = None
        self.__connection = None

    async def enqueue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger.debug("Enqueueing message ({routing_key}).", extra={"routing_key": key})

        body = MessageContent(
            payload=payload,
            parameters=params.encode() if params is not None else "",
        )

        exp: str | None = None
        if (delayed := wait_until(params)) is not None:
            millis = int(
                (delayed - datetime.now()).total_seconds() * 1000,
            )  # milliseconds as an integer
            if millis > 0:
                exp = str(millis)

        confirmation = await self._channel.basic_publish(
            body=JSON_ENCODER.encode(body).encode(),
            routing_key=self.qnc(key.queue, delayed=exp is not None),
            properties=aiormq.spec.Basic.Properties(
                message_id=key.id_,
                priority=key.priority,
                expiration=exp,
                delivery_mode=2 if self.idd(key) else 1,
                timestamp=params.timestamp if params is not None else None,
                headers={"queue": key.queue, "topic": key.topic},
            ),
            mandatory=True,
        )
        if not isinstance(confirmation, Basic.Ack):  # pragma: no cover
            raise ConnectionError("Message wasn't published.")

    async def ack(self, key: RoutingKeyT) -> None:
        logger_extra = {"routing_key": key}
        logger.debug("Acking message ({routing_key}).", extra=logger_extra)
        if (
            delivery_tag := self._id_to_delivery_tag.pop(key.id_, None)
        ) is None:  # pragma: no cover
            logger.error(
                "Can't ack unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        await self._channel.basic_ack(delivery_tag)

    async def nack(self, key: RoutingKeyT) -> None:
        logger_extra = {"routing_key": key}
        logger.debug("Nacking message ({routing_key}).", extra=logger_extra)
        if (
            delivery_tag := self._id_to_delivery_tag.pop(key.id_, None)
        ) is None:  # pragma: no cover
            logger.error(
                "Can't nack unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        await self._channel.basic_nack(delivery_tag, requeue=False)  # will trigger dlx

    async def reject(self, key: RoutingKeyT) -> None:
        logger_extra = {"routing_key": key}
        logger.debug("Rejecting message ({routing_key}).", extra=logger_extra)
        if (
            delivery_tag := self._id_to_delivery_tag.pop(key.id_, None)
        ) is None:  # pragma: no cover
            logger.error(
                "Can't reject unknown delivery tag for message ({routing_key}).",
                extra=logger_extra,
            )
            return
        await self._channel.basic_reject(delivery_tag, requeue=True)

    async def requeue(
        self,
        key: RoutingKeyT,
        payload: str = "",
        params: ParametersT | None = None,
    ) -> None:
        logger_extra = {"routing_key": key}
        logger.debug("Requeueing message ({routing_key}).", extra=logger_extra)
        await self.ack(key)
        await self.enqueue(key, payload, params)

    async def queue_declare(self, queue_name: str) -> None:
        logger.debug("Declaring queue '{queue_name}'.", extra={"queue_name": queue_name})
        channel = self._channel
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
        logger.debug("Flushing queue '{queue_name}'.", extra={"queue_name": queue_name})
        channel = self._channel

        await asyncio.gather(
            *[
                channel.queue_purge(queue_name),
                channel.queue_purge(f"{queue_name}:delayed"),
                channel.queue_purge(f"{queue_name}:dead"),
            ],
        )

    async def queue_delete(self, queue_name: str) -> None:
        logger.debug("Deleting queue '{queue_name}'.", extra={"queue_name": queue_name})
        channel = self._channel
        await asyncio.gather(
            *[
                channel.queue_delete(queue_name),
                channel.queue_delete(f"{queue_name}:delayed"),
                channel.queue_delete(f"{queue_name}:dead"),
            ],
        )
