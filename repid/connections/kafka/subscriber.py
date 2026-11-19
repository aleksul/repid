from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING

from aiokafka import OffsetAndMetadata
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition

from repid.connections.abc import MessageCallbackT, SubscriberT
from repid.connections.kafka.message import KafkaReceivedMessage

if TYPE_CHECKING:
    from repid.connections.kafka.message_broker import KafkaServer
    from repid.connections.kafka.protocols import AIOKafkaConsumerProtocol, ConsumerRecordProtocol

logger = logging.getLogger("repid.connections.kafka")


@dataclass(slots=True)
class _PartitionDeliveryState:
    offsets: dict[int, bool] = field(default_factory=dict)
    callbacks: set[asyncio.Task[None]] = field(default_factory=set)
    # Dispatch generation per tracked offset; only its latest delivery may settle it.
    generations: dict[int, int] = field(default_factory=dict)
    # Tracked offsets whose settlement failed; a refetch dispatches them again.
    abandoned: set[int] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    commit: asyncio.Task[None] | None = None
    owned: bool = True
    # Next offset this assignment has committed; earlier records are never redispatched.
    committed: int = 0


class KafkaSubscriber(ConsumerRebalanceListener, SubscriberT):
    def __init__(
        self,
        server: KafkaServer,
        consumer: AIOKafkaConsumerProtocol,
        channels_to_callbacks: dict[str, MessageCallbackT],
        max_records: int | None = None,
    ) -> None:
        self._server = server
        self._consumer = consumer
        self._channels_to_callbacks = channels_to_callbacks
        self._max_records = max_records

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()

        self._partitions: dict[TopicPartition, _PartitionDeliveryState] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        try:
            if self._channels_to_callbacks:
                self._consumer.subscribe(tuple(self._channels_to_callbacks), listener=self)
            await self._consumer.start()
        except BaseException:
            try:
                await self._consumer.stop()
            except BaseException as exc:
                logger.exception("subscriber.start.cleanup_error", exc_info=exc)
            raise
        self._task = asyncio.create_task(self._consume_loop())

    @property
    def is_active(self) -> bool:
        return not self._closed and self._task is not None and not self._task.done()

    @property
    def task(self) -> asyncio.Task[None]:
        if self._task is None:  # pragma: no cover
            raise RuntimeError("Kafka subscriber has not started.")
        return self._task

    async def pause(self) -> None:
        self._paused_event.clear()
        self._update_intake()

    async def resume(self) -> None:
        if self._paused_event.is_set():
            return
        self._paused_event.set()
        self._update_intake()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for state in self._partitions.values():
            state.owned = False
        self._partitions.clear()

        tasks_to_await = self._background_tasks - {asyncio.current_task()}
        if self._task is not None:
            tasks_to_await.add(self._task)
        try:
            for task in tasks_to_await:
                task.cancel()
            if tasks_to_await:
                await asyncio.gather(*tasks_to_await, return_exceptions=True)
        finally:
            try:
                await self._consumer.stop()
            except Exception as exc:
                logger.exception("subscriber.close.error", exc_info=exc)
            finally:
                if self in self._server._active_subscribers:
                    self._server._active_subscribers.remove(self)

    async def _consume_loop(self) -> None:
        try:
            while not self._closed:
                # Poll even while paused so admitted work keeps its assignment.
                self._update_intake()
                result = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self._max_records,
                )

                for tp, messages in result.items():
                    state = self._partitions.get(tp)
                    if state is None:
                        state = self._partitions[tp] = _PartitionDeliveryState()
                    # Track the full batch before any callback can complete.
                    fresh = self._track_offsets(state, [msg.offset for msg in messages])

                    for msg in messages:
                        if msg.offset not in fresh:
                            continue
                        task = asyncio.create_task(self._process_message(msg, tp, state))
                        self._background_tasks.add(task)
                        state.callbacks.add(task)
                        task.add_done_callback(partial(self._intake_finished, state))
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _track_offsets(state: _PartitionDeliveryState, offsets: list[int]) -> set[int]:
        """Track dispatchable offsets in ascending order and return them.

        A position reset can fetch offsets again while their first delivery is
        still in flight or awaiting commit, or after this assignment committed
        them. The first delivery keeps ownership, so refetched records are not
        dispatched a second time. A record whose settlement failed is dispatched
        again, and the new delivery takes over its settlement.
        """
        tracked = state.offsets
        committed = state.committed
        abandoned = state.abandoned
        fresh = [
            offset
            for offset in offsets
            if offset >= committed and (offset not in tracked or offset in abandoned)
        ]
        rewound = bool(tracked) and bool(fresh) and fresh[0] < next(reversed(tracked))
        generations = state.generations
        for offset in fresh:
            abandoned.discard(offset)
            generations[offset] = generations.get(offset, 0) + 1
        tracked.update(dict.fromkeys(fresh, False))
        if rewound:
            state.offsets = dict(sorted(tracked.items()))
        return set(fresh)

    def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        # Late settlements belong to the revoked assignment even when Kafka
        # immediately gives us the same partition and redelivers its records.
        for tp in revoked:
            state = self._partitions.pop(tp, None)
            if state is not None:
                state.owned = False
                for task in state.callbacks:
                    task.cancel()
                if state.commit is not None:
                    state.commit.cancel()

    def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:  # noqa: ARG002
        self._update_intake()

    def _update_intake(self) -> None:
        # Keep polling other topics while this topic's last batch is waiting
        # at admission. Shared fetch settings still govern the poll itself.
        assigned = self._consumer.assignment()
        paused = self._consumer.paused() & assigned
        busy_topics = {tp.topic for tp, state in self._partitions.items() if state.callbacks}
        busy = (
            {tp for tp in assigned if tp.topic in busy_topics}
            if not self._closed and self._paused_event.is_set()
            else assigned
        )
        if pause := busy - paused:
            self._consumer.pause(*pause)
        if resume := paused - busy:
            self._consumer.resume(*resume)

    def _intake_finished(self, state: _PartitionDeliveryState, task: asyncio.Task[None]) -> None:
        self._background_tasks.discard(task)
        state.callbacks.discard(task)
        if state.owned and not state.callbacks and self._paused_event.is_set():
            self._update_intake()

    async def _mark_complete(
        self,
        offset: int,
        tp: TopicPartition,
        state: _PartitionDeliveryState,
    ) -> None:
        if not state.owned or offset not in state.offsets:
            return
        # Mark completion before queueing for the lock, so the commit issued by
        # whichever caller holds it next covers every offset finished meanwhile.
        state.offsets[offset] = True
        state.abandoned.discard(offset)
        async with state.lock:
            # A cancelled caller can leave its offset RPC in flight. Finish
            # that operation before issuing another commit for this partition.
            if state.commit is not None:
                await asyncio.wait({state.commit})
            # A commit that succeeded while we waited already covered this offset.
            # After a failed one the offset is still tracked, so commit it again.
            if not state.owned or offset not in state.offsets:
                return
            completed: list[int] = []
            # Offsets are kept in ascending order, so the committable prefix
            # is the dict's leading run of completed offsets.
            for pending_offset in state.offsets:
                if not state.offsets[pending_offset]:
                    break
                completed.append(pending_offset)
            if completed:
                task = asyncio.create_task(self._commit(tp, state, completed))
                state.commit = task
                self._background_tasks.add(task)
                task.add_done_callback(partial(self._commit_finished, state))
                await asyncio.wait({task})
                if state.owned:
                    task.result()

    async def _commit(
        self,
        tp: TopicPartition,
        state: _PartitionDeliveryState,
        completed: list[int],
    ) -> None:
        await self._consumer.commit({tp: OffsetAndMetadata(completed[-1] + 1, "")})
        state.committed = completed[-1] + 1
        for offset in completed:
            del state.offsets[offset]
            state.generations.pop(offset, None)

    def _commit_finished(self, state: _PartitionDeliveryState, task: asyncio.Task[None]) -> None:
        if state.commit is task:
            state.commit = None
        self._background_tasks.discard(task)
        if not task.cancelled():
            task.exception()  # The caller may have been cancelled before observing the result.

    async def _process_message(
        self,
        record: ConsumerRecordProtocol,
        tp: TopicPartition,
        state: _PartitionDeliveryState,
    ) -> None:
        try:
            offset = record.offset
            generation = state.generations[offset]

            def is_owned() -> bool:
                return state.owned and state.generations.get(offset) == generation

            def abandon() -> None:
                if is_owned() and state.offsets.get(offset) is False:
                    state.abandoned.add(offset)

            msg = KafkaReceivedMessage(
                server=self._server,
                record=record,
                mark_complete_callback=lambda: self._mark_complete(offset, tp, state),
                is_owned=is_owned,
                on_settlement_failed=abandon,
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
