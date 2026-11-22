from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING

from aiokafka import OffsetAndMetadata
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition

from repid.connections._submission import _SubmissionQueue
from repid.connections.abc import Intake, SubscriberT
from repid.connections.kafka.message import KafkaReceivedMessage

if TYPE_CHECKING:
    from repid.connections.kafka.message_broker import KafkaServer
    from repid.connections.kafka.protocols import AIOKafkaConsumerProtocol, ConsumerRecordProtocol

logger = logging.getLogger("repid.connections.kafka")


@dataclass(slots=True)
class _PartitionDeliveryState:
    offsets: dict[int, bool] = field(default_factory=dict)
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
        intake: Intake,
        max_records: int = 1000,
        prefetch: int = 0,
    ) -> None:
        self._server = server
        self._consumer = consumer
        self._intake = intake
        self._max_records = max_records
        self._prefetch = prefetch
        # Fetched records the worker has no room for yet. Records of a revoked
        # partition are dropped instead of submitted.
        self._submissions: dict[str, _SubmissionQueue[KafkaReceivedMessage]] = {
            topic: _SubmissionQueue(
                channel_intake,
                keep_alive=False,
                is_stale=lambda message: not message.is_owned,
            )
            for topic, channel_intake in intake.channels.items()
        }
        self._capacity_watches: set[str] = set()

        self._closed = False
        self._paused_event = asyncio.Event()
        self._paused_event.set()

        self._partitions: dict[TopicPartition, _PartitionDeliveryState] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        try:
            if self._submissions:
                self._consumer.subscribe(tuple(self._submissions), listener=self)
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
        for submissions in self._submissions.values():
            submissions.pause()
        self._update_intake()

    async def resume(self) -> None:
        if self._paused_event.is_set():
            return
        self._paused_event.set()
        for submissions in self._submissions.values():
            submissions.resume()
        self._update_intake()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for state in self._partitions.values():
            state.owned = False
        self._partitions.clear()
        # Held records stay uncommitted, so the group redelivers them.
        for submissions in self._submissions.values():
            submissions.close()

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
                    max_records=self._fetch_size(),
                )

                for tp, messages in result.items():
                    state = self._partitions.get(tp)
                    if state is None:
                        state = self._partitions[tp] = _PartitionDeliveryState()
                    # Track the full batch before any delivery can complete.
                    fresh = self._track_offsets(state, [msg.offset for msg in messages])
                    submissions = self._submissions.get(tp.topic)
                    if submissions is None:  # pragma: no cover
                        continue

                    for msg in messages:
                        if msg.offset in fresh:
                            submissions.put(self._make_message(msg, tp, state))
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
                if state.commit is not None:
                    state.commit.cancel()

    def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:  # noqa: ARG002
        self._update_intake()

    def _room(self, topic: str) -> int | None:
        """Records this topic may fetch now, or None without a message limit."""
        demand = self._intake.channels[topic].demand.messages
        if demand is None:
            return None
        return demand + self._prefetch - len(self._submissions[topic])

    def _fetch_size(self) -> int:
        # Busy topics are paused. The shared poll may return any open topic's
        # records, so size it for the open topic with the least room. A topic
        # without a message limit is always open and does not shrink the poll.
        size: int | None = None
        for topic in self._submissions:
            room = self._room(topic)
            if room is None:
                size = size or self._max_records
            elif room > 0:
                size = min(room, size or self._max_records)
        # With every topic paused, a partition resumed mid-poll takes one record.
        return 1 if size is None and self._submissions else size or self._max_records

    def _update_intake(self) -> None:
        # Keep polling other topics while this topic has no room for more
        # records. Shared fetch settings still govern the poll itself.
        assigned = self._consumer.assignment()
        paused = self._consumer.paused() & assigned
        if self._closed or not self._paused_event.is_set():
            busy = assigned
        else:
            busy_topics = set()
            for topic in self._submissions:
                room = self._room(topic)
                if room is not None and room <= 0:
                    busy_topics.add(topic)
                    self._watch_capacity(topic)
            busy = {tp for tp in assigned if tp.topic in busy_topics}
        if pause := busy - paused:
            self._consumer.pause(*pause)
        if resume := paused - busy:
            self._consumer.resume(*resume)

    def _watch_capacity(self, topic: str) -> None:
        if topic in self._capacity_watches:
            return
        ready = self._intake.channels[topic].ready
        if ready.done():
            return
        self._capacity_watches.add(topic)
        ready.add_done_callback(partial(self._capacity_freed, topic))

    def _capacity_freed(self, topic: str, _ready: asyncio.Future[None]) -> None:
        self._capacity_watches.discard(topic)
        # Submit held records first, so the room computed below is current.
        self._submissions[topic].drain()
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

    def _make_message(
        self,
        record: ConsumerRecordProtocol,
        tp: TopicPartition,
        state: _PartitionDeliveryState,
    ) -> KafkaReceivedMessage:
        offset = record.offset
        generation = state.generations[offset]

        def is_owned() -> bool:
            return state.owned and state.generations.get(offset) == generation

        def abandon() -> None:
            if is_owned() and state.offsets.get(offset) is False:
                state.abandoned.add(offset)

        return KafkaReceivedMessage(
            server=self._server,
            record=record,
            mark_complete_callback=lambda: self._mark_complete(offset, tp, state),
            is_owned=is_owned,
            on_settlement_failed=abandon,
        )
