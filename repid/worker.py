from __future__ import annotations

import asyncio
import signal
from itertools import cycle
from typing import TYPE_CHECKING, Any, Callable

from repid import ArgsBucket, Connection, Queue, Repid, ResultBucket
from repid.actor import Actor, ActorContext, ActorContexVar
from repid.logger import logger
from repid.utils import unix_time

if TYPE_CHECKING:
    from repid.data import Message


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
    def queues(self) -> frozenset[str]:
        return frozenset(q.queue for q in self.actors.values())

    @property
    def topics_by_queue(self) -> dict[str, frozenset[str]]:
        topics = self.topics
        return {q: frozenset(t for t in topics if self.actors[t].queue == q) for q in self.queues}

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
            logger_extra = dict(id_=message.args_bucket_id)
            if self._conn.args_bucketer is None:
                logger.error("No args bucketer provided.", extra=logger_extra)
                return ((), {})
            bucket = await self._conn.args_bucketer.get_bucket(message.args_bucket_id)
            if bucket is None:
                logger.error("No bucket found for id: {id_}.", extra=logger_extra)
                return ((), {})
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

        ActorContexVar.set(
            ActorContext(
                message_id=message.id_,
                time_limit=message.execution_timeout,
            )
        )
        result = await actor(*args, **kwargs)

        # rescheduling (retry)
        if not result.success and message.tried + 1 < message.retries:
            message._prepare_retry(actor.retry_policy(message.tried))
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

    async def __process_message_with_cancellation(
        self, message: Message, cancel_event: asyncio.Event
    ) -> None:
        await asyncio.wait(  # type: ignore[type-var]
            {
                asyncio.create_task(cancel_event.wait()),
                asyncio.create_task(self._process_message(message)),
            },
            return_when=asyncio.FIRST_COMPLETED,
        )
        if cancel_event.is_set():
            await self._conn.messager.reject(message)

    async def run(self) -> None:
        queue_names = self.queues
        await asyncio.wait(
            {asyncio.create_task(Queue(q).declare()) for q in queue_names},
            return_when=asyncio.ALL_COMPLETED,
        )
        topics_by_queue = self.topics_by_queue
        queue_iter = cycle(queue_names)

        stop_consume_event = asyncio.Event()
        cancel_event = asyncio.Event()
        wait_stop_consume = asyncio.create_task(stop_consume_event.wait())

        tasks: set[asyncio.Task] = set()

        loop = asyncio.get_running_loop()

        def signal_handler() -> None:
            stop_consume_event.set()

            async def wait_before_cancel() -> None:
                await asyncio.sleep(self.gracefull_shutdown_time)
                cancel_event.set()

            tasks.add(asyncio.create_task(wait_before_cancel()))

        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)

        while self.messages_limit > self._processed:
            queue = next(queue_iter)
            consume = asyncio.create_task(
                self._conn.messager.consume(queue, topics_by_queue[queue])
            )
            await asyncio.wait(  # type: ignore[type-var]
                {wait_stop_consume, consume},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_consume_event.is_set():
                consume.cancel()
                break
            message = consume.result()
            t = asyncio.create_task(self.__process_message_with_cancellation(message, cancel_event))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
            self._processed += 1
        await asyncio.gather(*tasks)
