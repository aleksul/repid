from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any

from aiokafka import OffsetAndMetadata
from aiokafka.structs import TopicPartition

from repid.connections._subscriber import AdmittedTaskTracker, SubscriberDispatcher
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
        dispatcher: SubscriberDispatcher,
    ) -> None:
        self._server = server
        self._consumer = consumer
        self._channels_to_callbacks = channels_to_callbacks
        self._dispatcher = dispatcher

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()
        self._paused_channels: set[str] = set()

        self._offset_tracker: dict[TopicPartition, dict[int, bool]] = {}  # type: ignore[no-any-unimported]
        self._completion_locks: dict[TopicPartition, asyncio.Lock] = {}  # type: ignore[no-any-unimported]
        self._admitted_tasks = AdmittedTaskTracker()
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
        # Per-channel pauses survive a global resume.
        self._consumer.resume(
            *(tp for tp in self._consumer.assignment() if tp.topic not in self._paused_channels),
        )

    async def pause_channel(self, channel: str) -> None:
        self._paused_channels.add(channel)
        self._consumer.pause(
            *(tp for tp in self._consumer.assignment() if tp.topic == channel),
        )

    async def resume_channel(self, channel: str) -> None:
        self._paused_channels.discard(channel)
        self._consumer.resume(
            *(tp for tp in self._consumer.assignment() if tp.topic == channel),
        )

    async def stop(self) -> None:
        """Stop intake: cancel the consume loop without touching callbacks."""
        if self._closed:
            return
        self._closed = True
        self._task.cancel()

    async def finish(self) -> None:
        """Cancel remaining callbacks, drain their cleanup, stop the consumer."""
        await self.stop()
        try:
            await self._admitted_tasks.cancel_and_drain()
            if asyncio.current_task() is not self._task:
                await asyncio.gather(self._task, return_exceptions=True)
        finally:
            try:
                await self._consumer.stop()
            except Exception as exc:
                logger.exception("subscriber.close.error", exc_info=exc)

    async def _consume_loop(self) -> None:
        max_records = self._dispatcher.native_message_limit()

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
        lock = self._completion_locks.setdefault(tp, asyncio.Lock())
        async with lock:
            tracker = self._offset_tracker.get(tp)
            if tracker is None or r.offset not in tracker:
                return
            tracker[r.offset] = True

            # Find the highest contiguous completed offset.
            highest_completed = -1
            for offset in sorted(tracker):
                if tracker[offset]:
                    highest_completed = offset
                else:
                    break

            if highest_completed >= 0:
                await self._consumer.commit(
                    {
                        tp: OffsetAndMetadata(highest_completed + 1, ""),
                    },
                )
                # Retain completed offsets when commit fails so a later acknowledgement retries.
                for offset in list(tracker):
                    if offset <= highest_completed:
                        del tracker[offset]

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
        self._admitted_tasks.start(
            self._dispatcher,
            lease,
            msg,
            partial(self._run_callback, self._channels_to_callbacks.get(record.topic)),
        )
