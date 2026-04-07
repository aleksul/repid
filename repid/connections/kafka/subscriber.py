from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from aiokafka import OffsetAndMetadata
from aiokafka.structs import TopicPartition

from repid.connections.abc import SubscriberT
from repid.connections.kafka.message import KafkaReceivedMessage

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
        concurrency_limit: int | None = None,
    ) -> None:
        self._server = server
        self._consumer = consumer
        self._channels_to_callbacks = channels_to_callbacks
        self._concurrency_limit = concurrency_limit

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()

        self._semaphore = (
            asyncio.Semaphore(concurrency_limit)
            if concurrency_limit and concurrency_limit > 0
            else None
        )

        self._offset_tracker: dict[TopicPartition, dict[int, bool]] = {}  # type: ignore[no-any-unimported]
        self._background_tasks: set[asyncio.Task[Any]] = set()
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
        tasks_to_await = list(self._background_tasks)
        for task in tasks_to_await:
            task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await self._task

        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)

        try:
            await self._consumer.stop()
        except Exception as exc:
            logger.exception("subscriber.close.error", exc_info=exc)

    async def _consume_loop(self) -> None:
        try:
            while not self._closed:
                await self._paused_event.wait()

                result = await self._consumer.getmany(timeout_ms=1000)

                for tp, messages in result.items():
                    if tp not in self._offset_tracker:
                        self._offset_tracker[tp] = {}

                    for msg in messages:
                        self._offset_tracker[tp][msg.offset] = False

                        if self._semaphore:
                            await self._semaphore.acquire()

                        task = asyncio.create_task(self._process_message(msg, tp))
                        self._background_tasks.add(task)
                        task.add_done_callback(self._background_tasks.discard)
        except asyncio.CancelledError:
            pass

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

        try:
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
        finally:
            if self._semaphore:
                self._semaphore.release()

    async def _process_message(  # type: ignore[no-any-unimported]
        self,
        record: ConsumerRecordProtocol,
        tp: TopicPartition,
    ) -> None:
        msg: KafkaReceivedMessage | None = None
        try:
            msg = KafkaReceivedMessage(
                server=self._server,
                record=record,
                mark_complete_callback=lambda r: self._mark_complete(r, tp),
            )

            callback = self._channels_to_callbacks.get(record.topic)
            if callback:
                try:
                    await callback(msg)
                except Exception as exc:
                    logger.exception("consumer.error.unexpected", exc_info=exc)
                    if not msg.is_acted_on:
                        await msg.nack()
        except Exception as exc:  # pragma: no cover
            logger.exception("consumer.error.message_processing", exc_info=exc)
        finally:
            # If the message was never instantiated, or wasn't acted on, we must release the semaphore.
            # (If it was acted on, the _mark_complete_callback handled the semaphore release.)
            if self._semaphore and (msg is None or not msg.is_acted_on):
                self._semaphore.release()
