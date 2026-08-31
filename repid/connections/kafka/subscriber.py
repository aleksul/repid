from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any

from aiokafka import OffsetAndMetadata
from aiokafka.structs import TopicPartition

from repid.connections._subscriber import SubscriberDispatcher
from repid.connections.kafka.message import KafkaReceivedMessage

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.connections.kafka.message_broker import KafkaServer
    from repid.connections.kafka.protocols import AIOKafkaConsumerProtocol, ConsumerRecordProtocol

logger = logging.getLogger("repid.connections.kafka")


class KafkaSubscriber:
    def __init__(
        self,
        server: KafkaServer,
        consumer: AIOKafkaConsumerProtocol,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        dispatcher: SubscriberDispatcher | None = None,
    ) -> None:
        self._server = server
        self._consumer = consumer
        self._channels_to_callbacks = channels_to_callbacks
        self._dispatcher = dispatcher or SubscriberDispatcher()

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()

        self._offset_tracker: dict[TopicPartition, dict[int, bool]] = {}  # type: ignore[no-any-unimported]
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._task = asyncio.create_task(self._consume_loop())

    @property
    def is_active(self) -> bool:
        return not self._closed and not self._task.done()

    @property
    def task(self) -> asyncio.Task[Any]:
        return self._task

    async def pause(self) -> None:
        if not self._paused_event.is_set():
            return
        self._paused_event.clear()
        self._consumer.pause(*self._consumer.assignment())

    async def resume(self) -> None:
        if self._paused_event.is_set():
            return
        self._paused_event.set()
        self._consumer.resume(*self._consumer.assignment())

    async def close(self) -> None:
        self._closed = True
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        background_tasks = tuple(self._background_tasks)
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

        try:
            await self._consumer.stop()
        except Exception as exc:
            logger.exception("subscriber.close.error", exc_info=exc)

    async def _consume_loop(self) -> None:
        native_limit = self._dispatcher.native_message_limit()
        max_records = None if native_limit is None else max(native_limit, 1)

        while not self._closed:
            await self._paused_event.wait()

            result = await self._consumer.getmany(timeout_ms=1000, max_records=max_records)

            for tp, messages in result.items():
                if tp not in self._offset_tracker:
                    self._offset_tracker[tp] = {}

                for msg in messages:
                    self._offset_tracker[tp][msg.offset] = False

                    await self._process_message(msg, tp)

    async def _mark_complete(  # type: ignore[no-any-unimported]
        self,
        r: ConsumerRecordProtocol,
        tp: TopicPartition,
    ) -> None:
        self._offset_tracker[tp][r.offset] = True

        # Find the highest contiguous completed offset
        highest_completed = -1
        for offset in sorted(self._offset_tracker[tp].keys()):
            if self._offset_tracker[tp][offset]:
                highest_completed = offset
            else:
                break

        if highest_completed >= 0:
            await self._consumer.commit(
                {
                    tp: OffsetAndMetadata(highest_completed + 1, ""),
                },
            )
            # Clean up completed offsets
            for offset in list(self._offset_tracker[tp].keys()):
                if offset <= highest_completed:
                    del self._offset_tracker[tp][offset]

    @staticmethod
    async def _run_callback(
        callback: Callable[[ReceivedMessageT], Coroutine[None, None, None]] | None,
        message: ReceivedMessageT,
    ) -> None:
        if callback is None:
            return
        try:
            await callback(message)
        except Exception as exc:
            logger.exception("consumer.error.unexpected", exc_info=exc)
            if not message.is_acted_on:
                await message.nack()

    async def _process_message(  # type: ignore[no-any-unimported]
        self,
        record: ConsumerRecordProtocol,
        tp: TopicPartition,
    ) -> None:
        msg = KafkaReceivedMessage(
            server=self._server,
            record=record,
            mark_complete_callback=lambda r: self._mark_complete(r, tp),
        )
        lease = await self._dispatcher.reserve(msg)
        if lease is None:
            return
        task = asyncio.create_task(
            self._dispatcher.run_admitted(
                lease,
                msg,
                partial(self._run_callback, self._channels_to_callbacks.get(record.topic)),
            ),
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
