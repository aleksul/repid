from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any

from aiokafka import OffsetAndMetadata
from aiokafka.structs import TopicPartition

from repid.connections._buffer import SubmissionBuffer, stop_task
from repid.connections.abc import SubscriberT
from repid.connections.kafka.message import KafkaReceivedMessage
from repid.limits import UNLIMITED_NATIVE_FLOW, NativeFlow

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.connections.kafka.message_broker import KafkaServer
    from repid.connections.kafka.protocols import AIOKafkaConsumerProtocol, ConsumerRecordProtocol

logger = logging.getLogger("repid.connections.kafka")


class KafkaSubscriber(SubscriberT):
    def __init__(
        self,
        server: KafkaServer,
        consumer: AIOKafkaConsumerProtocol,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        native_flow: NativeFlow = UNLIMITED_NATIVE_FLOW,
    ) -> None:
        self._server = server
        self._native_flow = native_flow
        self._consumer = consumer
        self._channels_to_callbacks = channels_to_callbacks
        self._finished = False

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()

        self._offset_tracker: dict[TopicPartition, dict[int, bool]] = {}  # type: ignore[no-any-unimported]
        self._commit_lock = asyncio.Lock()
        self._task = asyncio.create_task(self._consume_loop())

    @property
    def native_flow(self) -> NativeFlow:
        return self._native_flow

    @property
    def is_active(self) -> bool:
        return not self._closed and not self._task.done()

    @property
    def task(self) -> asyncio.Task[Any]:
        return self._task

    async def pause(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("Kafka supports worker pause only")
        if not self._paused_event.is_set():
            return
        self._paused_event.clear()
        self._consumer.pause(*self._consumer.assignment())

    async def resume(self, channel: str | None = None) -> None:
        if channel is not None:
            raise ValueError("Kafka supports worker pause only")
        if self._paused_event.is_set():
            return
        self._paused_event.set()
        self._consumer.resume(*self._consumer.assignment())

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._consumer.pause(*self._consumer.assignment())
        await stop_task(self._task)

    async def finish(self) -> None:
        try:
            await self.stop()
        finally:
            if not self._finished:
                await self._consumer.stop()
                self._finished = True

    async def _consume_loop(self) -> None:
        buffer = SubmissionBuffer()
        try:
            while not self._closed:
                await self._paused_event.wait()
                result = await self._consumer.getmany(timeout_ms=1000, max_records=100)
                messages = []
                for tp, records in result.items():
                    tracker = self._offset_tracker.setdefault(tp, {})
                    for record in records:
                        tracker[record.offset] = False
                        callback = self._channels_to_callbacks.get(record.topic)
                        message = KafkaReceivedMessage(
                            server=self._server,
                            record=record,
                            mark_complete_callback=partial(self._mark_complete, tp=tp),
                        )
                        messages.append((message, callback))
                buffer.track(message for message, _ in messages)
                for message, callback in messages:
                    await self._paused_event.wait()
                    if callback is not None:
                        await buffer.submit(message, callback)
                await asyncio.sleep(0)
        finally:
            await buffer.dispose()

    async def _mark_complete(  # type: ignore[no-any-unimported]
        self,
        r: ConsumerRecordProtocol,
        tp: TopicPartition,
    ) -> None:
        async with self._commit_lock:
            await self._commit_completed(r, tp)

    async def _commit_completed(  # type: ignore[no-any-unimported]
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
            await self._consumer.commit({tp: OffsetAndMetadata(highest_completed + 1, "")})
            for offset in list(self._offset_tracker[tp].keys()):
                if offset <= highest_completed:
                    del self._offset_tracker[tp][offset]
