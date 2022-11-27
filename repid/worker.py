from __future__ import annotations

import asyncio
import signal
from typing import TYPE_CHECKING, Callable

from repid._runner import _Runner
from repid.main import DEFAULT_CONNECTION
from repid.queue import Queue

if TYPE_CHECKING:
    from repid.actor import ActorData
    from repid.connection import Connection
    from repid.router import Router


class Worker:
    def __init__(
        self,
        routers: list[Router] | None = None,
        gracefull_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        tasks_limit: int = 1000,
        _connection: Connection | None = None,
    ):
        self._conn = _connection or DEFAULT_CONNECTION.get()

        self.actors: dict[str, ActorData] = {}
        self._queues_to_routes: dict[str, set[str]] = {}

        if routers is not None:
            for router in routers:
                self.include_router(router)

        self.tasks_limit = tasks_limit
        self.gracefull_shutdown_time = gracefull_shutdown_time
        self.messages_limit = messages_limit

    def include_router(self, router: Router) -> None:
        self.actors.update(router.actors)
        queues = self._queues_to_routes.keys()
        for queue_name, topics in router.topics_by_queue.items():
            if queue_name in queues:
                self._queues_to_routes[queue_name].update(topics)
            else:
                self._queues_to_routes[queue_name] = set(topics)

    def _signal_handler_constructor(self, runner: _Runner) -> Callable[[], None]:
        def signal_handler() -> None:
            runner.stop_consume_event.set()

            async def wait_before_cancel() -> None:
                await asyncio.sleep(self.gracefull_shutdown_time)
                runner.cancel_event.set()

            t = asyncio.create_task(wait_before_cancel())
            runner.tasks.add(t)
            t.add_done_callback(runner.tasks.discard)
            loop = asyncio.get_running_loop()
            loop.remove_signal_handler(signal.SIGINT)
            loop.remove_signal_handler(signal.SIGTERM)

        return signal_handler

    async def run(self) -> _Runner:
        runner = _Runner(
            max_tasks=self.messages_limit,
            tasks_concurrency_limit=self.tasks_limit,
            _connection=self._conn,
        )

        if not self.actors:
            return runner

        await asyncio.wait(
            {
                asyncio.create_task(Queue(queue_name).declare())
                for queue_name in self._queues_to_routes
            },
            return_when=asyncio.ALL_COMPLETED,
        )

        loop = asyncio.get_running_loop()
        signal_handler = self._signal_handler_constructor(runner)
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)

        queue_consume_tasks: set[asyncio.Task] = set()

        for queue_name in self._queues_to_routes:
            t = asyncio.create_task(
                runner(
                    queue_name,
                    self._queues_to_routes[queue_name],
                    self.actors,
                )
            )
            queue_consume_tasks.add(t)

        if queue_consume_tasks:
            await asyncio.wait({*queue_consume_tasks}, return_when=asyncio.ALL_COMPLETED)

        if runner.tasks:
            await asyncio.wait(runner.tasks, return_when=asyncio.ALL_COMPLETED)

        runner.stop_consume_event.set()
        runner.cancel_event.set()

        return runner
