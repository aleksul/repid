from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Iterable
from uuid import uuid4

import aiormq
import orjson
from aiormq.abc import Basic

from repid.connections.abc import ConsumerT
from repid.data.priorities import PrioritiesT
from repid.logger import logger

if TYPE_CHECKING:
    from repid.connections.rabbitmq.message_broker import RabbitMessageBroker
    from repid.connections.rabbitmq.utils import MessageContent
    from repid.data.protocols import ParametersT, RoutingKeyT


class _RabbitConsumer(ConsumerT):
    def __init__(
        self,
        broker: RabbitMessageBroker,
        queue_name: str,
        topics: Iterable[str] | None = None,
        max_unacked_messages: int | None = None,
    ) -> None:
        self.broker = broker
        self.queue_name = queue_name
        self.topics = topics
        self.queue: asyncio.Queue[tuple[RoutingKeyT, str, ParametersT]] = asyncio.Queue()
        self.max_unacked_messages = 0 if max_unacked_messages is None else max_unacked_messages
        self._consumer_tag: str | None = None
        self.__is_paused: bool = False

    async def consume(self) -> tuple[RoutingKeyT, str, ParametersT]:
        return await self.queue.get()

    async def start(self) -> None:
        self.__is_paused = False
        await self.broker._channel().basic_qos(
            prefetch_size=0,
            prefetch_count=self.max_unacked_messages,
        )
        confirmation = await self.broker._channel().basic_consume(
            self.queue_name, self.on_new_message, no_ack=False
        )
        if not isinstance(confirmation, Basic.ConsumeOk):
            logger.error("Consumer wasn't started properly.")
        else:
            self._consumer_tag = confirmation.consumer_tag

    async def pause(self) -> None:
        self.__is_paused = True
        await self.broker._channel().basic_qos(
            prefetch_size=0,
            prefetch_count=1,
        )

    async def unpause(self) -> None:
        self.__is_paused = False
        await self.broker._channel().basic_qos(
            prefetch_size=0,
            prefetch_count=self.max_unacked_messages,
        )

    async def finish(self) -> None:
        self.__is_paused = True
        if self._consumer_tag is None:
            return
        confirmation = await self.broker._channel().basic_cancel(self._consumer_tag)
        if not isinstance(confirmation, Basic.CancelOk):
            logger.error(
                "Consumer (tag: {tag}) wasn't stopped properly.",
                extra=dict(tag=self._consumer_tag),
            )
        rejects = []
        while self.queue.qsize() > 0:
            key, _, _ = self.queue.get_nowait()
            tag = self.broker._id_to_delivery_tag.pop(key.id_, None)
            if tag is not None:
                rejects.append(self.broker._channel().basic_reject(tag))
            else:
                logger.error(
                    "Can't reject unknown delivery tag for message ({routing_key}) "
                    "while finishing consumer.",
                    extra=dict(routing_key=key),
                )
        await asyncio.gather(*rejects)

    async def on_new_message(self, message: aiormq.abc.DeliveredMessage) -> None:
        # get or set message id
        if message.header.properties.message_id is None:
            message.header.properties.message_id = uuid4().hex
        msg_id = message.header.properties.message_id

        # get rabbitmq delivery tag
        if message.delivery_tag is None:
            logger.error(
                "Can't process message (id: {id_}) with unknown delivery tag.",
                extra=dict(id_=msg_id),
            )
            return

        # if consumer is paused - reject the message
        if self.__is_paused:
            await self.broker._channel().basic_reject(message.delivery_tag)
            return

        msg_topic: str | None = None
        msg_queue: str = "default"

        # get message topic and queue
        if message.header.properties.headers is not None:
            msg_topic = message.header.properties.headers.get("topic", None)  # type: ignore[assignment]  # noqa: E501
            msg_queue = message.header.properties.headers.get("queue", "default")  # type: ignore[assignment]  # noqa: E501

        # reject the message with no topic set (== message wasn't scheduled by repid-like producer)
        if msg_topic is None:
            await asyncio.sleep(0.1)  # poison message fix
            await self.broker._channel().basic_reject(message.delivery_tag)
            return

        # reject the message if the topic isn't in the range of specified
        if self.topics and msg_topic not in self.topics:
            await asyncio.sleep(0.1)  # poison message fix
            await self.broker._channel().basic_reject(message.delivery_tag)
            return

        # decode message payload (rabbitmq abstraction level)
        decoded: MessageContent = orjson.loads(message.body)
        # decode message params (repid abstraction level)
        params = self.broker.PARAMETERS_CLASS.decode(decoded["parameters"])

        # put message to a dead queue if it's overdue
        if params.is_overdue:
            await self.broker._channel().basic_nack(message.delivery_tag, requeue=False)

        # save delivery tag for the future
        self.broker._id_to_delivery_tag[msg_id] = message.delivery_tag

        # create a key object and put message in in-memory queue to be picked up soon
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
