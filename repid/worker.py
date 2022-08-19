from __future__ import annotations

import logging
import signal
from itertools import cycle
from typing import Any, Callable

import anyio

from repid import ArgsBucket, Connection, Queue, Repid, ResultBucket
from repid.actor import Actor
from repid.data import Message
from repid.utils import unix_time

logger = logging.getLogger(__name__)


class Worker:
    __slots__ = (
        "_conn",
        "actors",
        "gracefull_shutdown_time",
        "messages_limit",
        "_processed",
    )

    def __init__(
        self,
        gracefull_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        _connection: Connection | None = None,
    ):
        self._conn = _connection or Repid.get_default_connection()
        self.actors: dict[str, Actor] = {}
        self.gracefull_shutdown_time = gracefull_shutdown_time
        self._processed = 0
        self.messages_limit = messages_limit

    @property
    def topics(self) -> frozenset[str]:
        return frozenset(self.actors.keys())

    @property
    def messages_processed(self) -> int:
        return self._processed

    def actor(self, name: str | None = None, queue: str = "default") -> Callable:
        def decorator(fn: Callable) -> Callable:
            a = Actor(fn, name=name, queue=queue)
            self.actors[a.name] = a
            return fn

        return decorator

    async def _get_message_args(self, message: Message) -> tuple[tuple, dict[str, Any]]:
        if message.simple_args or message.simple_kwargs:
            return (message.simple_args or (), message.simple_kwargs or {})
        elif message.args_bucket_id:
            if self._conn.args_bucketer is None:
                raise ConnectionError("No args bucketer provided.")
            bucket = await self._conn.args_bucketer.get_bucket(message.args_bucket_id)
            if bucket is None:
                logger.error(f"No bucket found for id = {message.args_bucket_id}.")
            if isinstance(bucket, ArgsBucket):
                return (bucket.args or (), bucket.kwargs or {})
        return ((), {})

    async def _set_result(self, result: ResultBucket) -> None:
        if self._conn.results_bucketer is None:
            logger.error("No results bucketer provided for returned data.")
            return
        await self._conn.results_bucketer.store_bucket(result)

    async def _process_message(self, message: Message) -> None:
        actor = self.actors[message.topic]
        args, kwargs = await self._get_message_args(message)

        actor._TIME_LIMIT.set(message.execution_timeout)
        result = await actor(*args, **kwargs)

        # rescheduling (retry)
        if not result.success and message.tried + 1 < message.retries:
            message._increment_retry()
            # message.delay_until = None  # TODO: exponential backoff
            await self._conn.messager.requeue(message)
        # rescheduling (deferred)
        elif message.defer_by is not None or message.cron is not None:
            message._prepare_reschedule()
            await self._conn.messager.requeue(message)
        # ack
        elif result.success:
            await self._conn.messager.ack(message)
        # nack
        else:
            await self._conn.messager.nack(message)

        # return result
        if message.result_bucket_id is not None:
            result_bucket = ResultBucket(
                id_=message.result_bucket_id,
                data=result.data,
                success=result.success,
                started_when=result.started_when,
                finished_when=result.finished_when,
                exception=f"{type(result.exception)}: {result.exception}"
                if result.exception is not None
                else None,
                timestamp=unix_time(),
                ttl=message.result_bucket_ttl,
            )
            await self._set_result(result_bucket)

    async def __process_message_with_cancellation(self, message: Message) -> None:
        try:
            await self._process_message(message)
        except anyio.get_cancelled_exc_class():
            with anyio.CancelScope(shield=True):
                await self._conn.messager.reject(message)
            raise
        return None

    @staticmethod
    async def __stop_on_event(
        scope: anyio.CancelScope,
        on_event: anyio.Event,
    ) -> None:
        await on_event.wait()
        scope.cancel()

    @staticmethod
    async def __listen_signal(
        scope: anyio.CancelScope,
        on_event: anyio.Event,
        shutdown_time: float,
    ) -> None:
        with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            await signals.__anext__()
            on_event.set()
            with anyio.CancelScope(shield=True):
                await anyio.sleep(shutdown_time)
                scope.cancel()

    async def __listen_signal_with_stop(
        self,
        scope: anyio.CancelScope,
        on_stop_consume: anyio.Event,
    ) -> None:
        async with anyio.create_task_group() as tg:
            tg.start_soon(
                self.__stop_on_event,
                tg.cancel_scope,
                on_stop_consume,
            )
            tg.start_soon(
                self.__listen_signal,
                scope,
                on_stop_consume,
                self.gracefull_shutdown_time,
            )

    async def _declare_queues(self, queue_names: set[str]) -> None:
        async with anyio.create_task_group() as tg:
            for q in queue_names:
                my_q = Queue(q)
                tg.start_soon(my_q.declare)

    async def run(self) -> None:
        queue_names = {q.queue for q in self.actors.values()}
        await self._declare_queues(queue_names)
        topics_by_queue = {
            q: frozenset(t for t in self.topics if self.actors[t].queue == q) for q in queue_names
        }
        queue_iter = cycle(queue_names)
        stop_consume_event = anyio.Event()
        async with anyio.create_task_group() as tg:
            tg.start_soon(self.__listen_signal_with_stop, tg.cancel_scope, stop_consume_event)
            while not stop_consume_event.is_set() and self.messages_limit > self._processed:
                queue = next(queue_iter)
                message = await self._conn.messager.consume(queue, topics_by_queue[queue])
                tg.start_soon(self.__process_message_with_cancellation, message)
                self._processed += 1
            stop_consume_event.set()
