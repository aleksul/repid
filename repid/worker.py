from __future__ import annotations

import asyncio
import signal
from typing import TYPE_CHECKING, Callable

from repid._runner import _Runner
from repid.main import Repid
from repid.queue import Queue
from repid.router import Router, RouterDefaults

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.connections.abc import ConsumerT


class Worker(Router):
    def __init__(
        self,
        routers: list[Router] | None = None,
        gracefull_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        tasks_limit: int = 1000,
        handle_signals: list[signal.Signals] | None = None,
        router_defaults: RouterDefaults | None = None,
        auto_declare: bool = True,
        _connection: Connection | None = None,
    ):
        super().__init__(defaults=router_defaults)

        self._conn = _connection or Repid.get_magic_connection()

        if routers is not None:
            for router in routers:
                self.include_router(router)

        self.tasks_limit = tasks_limit
        self.gracefull_shutdown_time = gracefull_shutdown_time
        self.messages_limit = messages_limit
        self.handle_signals = (
            [signal.SIGINT, signal.SIGTERM] if handle_signals is None else handle_signals
        )
        self.auto_declare = auto_declare

    async def declare_all_queues(self) -> None:
        await asyncio.wait(
            {
                asyncio.create_task(Queue(queue_name, _connection=self._conn).declare())
                for queue_name in self.topics_by_queue
            },
            return_when=asyncio.ALL_COMPLETED,
        )

    async def run(self) -> _Runner:
        runner = _Runner(
            max_tasks=self.messages_limit,
            tasks_concurrency_limit=self.tasks_limit,
            _connection=self._conn,
        )

        if not self.actors:
            return runner

        if self.auto_declare:
            await self.declare_all_queues()

        loop = asyncio.get_running_loop()
        signal_handler = self._signal_handler_constructor(runner)
        for sig in self.handle_signals:
            loop.add_signal_handler(sig, signal_handler)

        consumer_tasks: set[asyncio.Task] = set()

        for queue_name in self.topics_by_queue:
            t = asyncio.create_task(
                runner.run_one_queue(
                    queue_name,
                    self.topics_by_queue[queue_name],
                    self.actors,
                )
            )
            consumer_tasks.add(t)

        consumers: set[ConsumerT] = set()
        if consumer_tasks:
            consumer_futures, _ = await asyncio.wait(
                consumer_tasks, return_when=asyncio.ALL_COMPLETED
            )
            for ft in consumer_futures:
                if ft.exception() is None:
                    consumers.add(ft.result())

        if runner.tasks:
            await asyncio.wait(runner.tasks, return_when=asyncio.ALL_COMPLETED)

        if consumers:
            await asyncio.wait(
                {asyncio.create_task(c.finish()) for c in consumers},
                return_when=asyncio.ALL_COMPLETED,
            )

        runner.stop_consume_event.set()
        runner.cancel_event.set()

        return runner

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
            for sig in self.handle_signals:
                loop.remove_signal_handler(sig)

        return signal_handler
