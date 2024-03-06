from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Iterable

from repid._processor import _Processor
from repid.health_check_server import HealthCheckStatus
from repid.logger import logger
from repid.main import Repid

if TYPE_CHECKING:
    from repid.actor import ActorData
    from repid.connection import Connection
    from repid.connections import ConsumerT
    from repid.data import ParametersT, RoutingKeyT
    from repid.health_check_server import HealthCheckServer


class _Runner(_Processor):
    def __init__(
        self,
        max_tasks: int = float("inf"),  # type: ignore[assignment]
        tasks_concurrency_limit: int = 1000,
        health_check_server: HealthCheckServer | None = None,
        _connection: Connection | None = None,
    ):
        self._conn = _connection or Repid.get_magic_connection()

        self.tasks: set[asyncio.Task] = set()

        self.stop_consume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()

        self.max_tasks = max_tasks
        self.tasks_concurrency_limit = tasks_concurrency_limit
        self.limiter = asyncio.Semaphore(tasks_concurrency_limit)
        self._tasks_processed = 0

        self.health_check_server = health_check_server

        super().__init__(self._conn)

    @property
    def max_tasks_hit(self) -> bool:
        return (
            self.max_tasks
            - self._tasks_processed
            - (self.tasks_concurrency_limit - self.limiter._value)
            <= 0
        )

    @property
    def cancel_event_task(self) -> asyncio.Task:
        if not hasattr(self, "_cancel_event_task"):
            self._cancel_event_task = asyncio.create_task(self.cancel_event.wait())
        return self._cancel_event_task

    @property
    def stop_consume_event_task(self) -> asyncio.Task:
        if not hasattr(self, "_stop_consume_event_task"):
            self._stop_consume_event_task = asyncio.create_task(self.stop_consume_event.wait())
        return self._stop_consume_event_task

    def _task_callback(self, task: asyncio.Task) -> None:
        self.tasks.discard(task)
        self.limiter.release()
        self._tasks_processed += 1
        if self.max_tasks_hit:
            self.stop_consume_event.set()

    async def _process_with_event(
        self,
        actor: ActorData,
        key: RoutingKeyT,
        payload: str,
        parameters: ParametersT,
    ) -> None:
        process_task = asyncio.create_task(self.process(actor, key, payload, parameters))
        await asyncio.wait(
            {self.cancel_event_task, process_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if self.cancel_event.is_set():
            process_task.cancel()
            await self._conn.message_broker.reject(key)
            return
        await process_task

    async def _run_consumer(
        self,
        consumer: ConsumerT,
        actors: dict[str, ActorData],
    ) -> None:
        async for key, payload, params in consumer:
            actor = actors[key.topic]
            if self.limiter.locked():
                await consumer.pause()
                await self.limiter.acquire()
                await consumer.unpause()
            else:
                await self.limiter.acquire()
            t = asyncio.create_task(self._process_with_event(actor, key, payload, params))
            self.tasks.add(t)
            t.add_done_callback(self._task_callback)

    async def run_one_queue(
        self,
        queue_name: str,
        topics: Iterable[str],
        actors: dict[str, ActorData],
    ) -> ConsumerT:
        consumer = self._conn.message_broker.get_consumer(
            queue_name,
            topics,
            self.tasks_concurrency_limit,
        )
        await consumer.start()
        consume_task = asyncio.create_task(self._run_consumer(consumer, actors))
        await asyncio.wait(
            {self.stop_consume_event_task, consume_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if (
            consume_task.done()
            and not consume_task.cancelled()
            and (exc := consume_task.exception()) is not None
        ):
            logger.critical(
                "Error while running consumer on queue '{queue_name}'.",
                extra={"queue_name": queue_name},
                exc_info=exc,
            )
            if self.health_check_server is not None:
                self.health_check_server.health_status = HealthCheckStatus.UNHEALTHY
        if self.stop_consume_event.is_set():
            consume_task.cancel()
        await consumer.pause()
        return consumer
