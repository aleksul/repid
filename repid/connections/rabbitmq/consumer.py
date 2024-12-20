from __future__ import annotations

import asyncio
import json
import weakref
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import aiormq
from aiormq.abc import Basic

from repid.connections.abc import ConsumerT
from repid.connections.rabbitmq.utils import _Consumers
from repid.data.priorities import PrioritiesT
from repid.logger import logger
from repid.message import MessageCategory

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
        category: MessageCategory = MessageCategory.NORMAL,
    ) -> None:
        self.broker = broker
        self.queue_name = queue_name
        self.topics = topics
        self.queue: asyncio.Queue[tuple[RoutingKeyT, str, ParametersT]] = asyncio.Queue()
        self.max_unacked_messages = 0 if max_unacked_messages is None else max_unacked_messages
        self.category = category
        self.server_side_cancel_event = asyncio.Event()
        self._consumer_tag: str | None = None
        self.__is_paused: bool = False
        self.__is_consuming: bool = False
        self.__channel: aiormq.abc.AbstractChannel | None = None

    def _force_reset(self) -> None:  # pragma: no cover
        self._consumer_tag = None
        self.__is_paused = False
        self.__is_consuming = False
        self.server_side_cancel_event.clear()
        while not self.queue.empty():
            self.queue.get_nowait()

    async def _get_or_open_channel(self) -> aiormq.abc.AbstractChannel:
        if self.__channel is not None:
            if not self.__channel.is_closed:
                return self.__channel
            logger.error(  # pragma: no cover
                "RabbitMQ has closed consumer channel (number: {number}), reopening.",
                extra={"number": self.__channel.number},
            )
            self._force_reset()  # pragma: no cover
        connection = await self.broker._get_connection()
        self.__channel = await connection.channel()
        # patch consumers dict to hook into server-side cancel event
        cast("aiormq.Channel", self.__channel).consumers = cast("dict[str, Callable]", _Consumers())
        return self.__channel

    def _get_channel_if_opened(self) -> aiormq.abc.AbstractChannel | None:
        if self.__channel is not None:
            if not self.__channel.is_closed:
                return self.__channel
            logger.error(  # pragma: no cover
                "Cannot act on closed consumer channel (number: {number}).",
                extra={"number": self.__channel.number},
            )
        return None

    async def consume(self) -> tuple[RoutingKeyT, str, ParametersT]:
        # fast-path without task creation
        if not self.queue.empty():
            return self.queue.get_nowait()

        while True:
            # allow consume to be interrupted by server side cancel event
            get_task = asyncio.create_task(self.queue.get())
            server_side_cancel_wait_task = asyncio.create_task(self.server_side_cancel_event.wait())

            # wait for a message or for server side cancel
            try:
                _, pending = await asyncio.wait(
                    {get_task, server_side_cancel_wait_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.CancelledError:
                # if we got cancellation while waiting on our tasks - cancel the tasks
                get_task.cancel()
                server_side_cancel_wait_task.cancel()
                raise

            # cancel unfinished tasks
            for p in pending:
                p.cancel()

            # if we finished getting a message - return it
            if get_task.done() and not get_task.cancelled():
                return get_task.result()

            # restart consumer if it was running and we got cancellation
            if self.server_side_cancel_event.is_set() and self.__is_consuming:
                logger.error(
                    "RabbitMQ has terminated consumer (tag: {tag}) server-side.",
                    extra={"tag": self._consumer_tag},
                )
                # clear the queue - those messages are probably already rejected on RabbitMQ side
                while not self.queue.empty():
                    self.queue.get_nowait()  # pragma: no cover
                # attempt to restart the consumer
                await self.start()
                # if restart was successful - we can try to consume again
                continue
            if self.server_side_cancel_event.is_set():
                self.server_side_cancel_event.clear()
                continue

    async def start(self) -> None:
        self.__is_consuming = True
        self.server_side_cancel_event.clear()
        channel = await self._get_or_open_channel()
        await channel.basic_qos(
            prefetch_size=0,
            prefetch_count=self.max_unacked_messages,
        )
        confirmation = await channel.basic_consume(
            self.broker.qnc(
                self.queue_name,
                delayed=self.category == MessageCategory.DELAYED,
                dead=self.category == MessageCategory.DEAD,
            ),
            self.on_new_message,
            no_ack=False,
        )
        if not isinstance(confirmation, Basic.ConsumeOk):  # pragma: no cover
            self.__is_consuming = False
            raise ConnectionError("Consumer wasn't started properly.")
        self._consumer_tag = confirmation.consumer_tag

    async def pause(self) -> None:
        self.__is_paused = True
        if (channel := self._get_channel_if_opened()) is None:
            return
        await channel.basic_qos(
            prefetch_size=0,
            prefetch_count=1,
        )

    async def unpause(self) -> None:
        self.__is_paused = False
        if (channel := self._get_channel_if_opened()) is None:
            return  # pragma: no cover
        await channel.basic_qos(
            prefetch_size=0,
            prefetch_count=self.max_unacked_messages,
        )

    async def finish(self) -> None:
        self.__is_consuming = False
        if self._consumer_tag is None:
            return
        if (channel := self._get_channel_if_opened()) is None:
            self._force_reset()  # pragma: no cover
            return  # pragma: no cover
        confirmation = await channel.basic_cancel(self._consumer_tag)
        if not isinstance(confirmation, Basic.CancelOk):  # pragma: no cover
            logger.error(
                "Consumer (tag: {tag}) wasn't stopped properly.",
                extra={"tag": self._consumer_tag},
            )
        rejects = []
        while not self.queue.empty():
            key, _, _ = self.queue.get_nowait()
            tag_and_channel_ref = self.broker._id_to_delivery_tag.pop(key.id_, None)
            if (
                tag_and_channel_ref is not None
                and (tag := tag_and_channel_ref[0])
                and (channel_ref := tag_and_channel_ref[1]()) is not None
                and channel is channel_ref
            ):
                rejects.append(channel.basic_reject(tag))
            else:  # pragma: no cover
                logger.error(
                    "Can't reject unknown delivery tag for message ({routing_key}) "
                    "while finishing consumer.",
                    extra={"routing_key": key},
                )
        await asyncio.gather(*rejects)

    async def on_new_message(self, message: aiormq.abc.DeliveredMessage) -> None:
        # get or set message id
        if message.header.properties.message_id is None:  # pragma: no cover
            message.header.properties.message_id = uuid4().hex
        msg_id = message.header.properties.message_id

        # get rabbitmq delivery tag
        if message.delivery_tag is None:  # pragma: no cover
            logger.error(
                "Can't process message (id: {id_}) with unknown delivery tag.",
                extra={"id_": msg_id},
            )
            return

        channel = message.channel

        # if consumer is paused or is not set to consume messages - reject the message
        if self.__is_paused or not self.__is_consuming:
            await asyncio.sleep(0.1)
            await channel.basic_reject(message.delivery_tag)
            logger.debug("Consumer is paused or is not set to consume message.")
            return

        msg_topic: str | None = None
        msg_queue: str = "default"

        # get message topic and queue
        if message.header.properties.headers is not None:
            msg_topic = message.header.properties.headers.get("topic", None)  # type: ignore[assignment]
            msg_queue = message.header.properties.headers.get("queue", "default")  # type: ignore[assignment]

        # reject the message with no topic set (== message wasn't scheduled by repid-like producer)
        if msg_topic is None:  # pragma: no cover
            await asyncio.sleep(0.1)  # poison message fix
            await channel.basic_reject(message.delivery_tag)
            logger.debug(
                "Message has no topic set or message wasn't scheduled by repid-like producer.",
            )
            return

        # reject the message if the topic isn't in the range of specified
        if self.topics and msg_topic not in self.topics:
            await asyncio.sleep(0.1)  # poison message fix
            await channel.basic_reject(message.delivery_tag)
            logger.debug(
                "Unknown message's topic. Check if Actor name matches Job or message topic name.",
            )
            return

        # decode message payload (rabbitmq abstraction level)
        decoded: MessageContent = json.loads(message.body)
        # decode message params (repid abstraction level)
        params = self.broker.PARAMETERS_CLASS.decode(decoded["parameters"])

        # put message to a dead queue if it's overdue
        if params.is_overdue and self.category == MessageCategory.NORMAL:
            await channel.basic_nack(message.delivery_tag, requeue=False)
            logger.debug("Message is overdue, placing it in dlx.")
            return

        # save delivery tag for the future
        self.broker._id_to_delivery_tag[msg_id] = (message.delivery_tag, weakref.ref(channel))

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
            ),
        )
