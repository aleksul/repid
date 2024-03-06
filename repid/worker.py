from __future__ import annotations

import asyncio
import signal
from typing import TYPE_CHECKING, Callable

from repid._runner import _Runner
from repid.health_check_server import HealthCheckServer
from repid.main import Repid
from repid.queue import Queue
from repid.router import Router, RouterDefaults

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.connections.abc import ConsumerT
    from repid.health_check_server import HealthCheckServerSettings


class Worker(Router):
    def __init__(
        self,
        routers: list[Router] | None = None,
        *,
        graceful_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        tasks_limit: int = 1000,
        handle_signals: list[signal.Signals] | None = None,
        router_defaults: RouterDefaults | None = None,
        auto_declare: bool = True,
        run_health_check_server: bool = False,
        health_check_server_settings: HealthCheckServerSettings | None = None,
        _connection: Connection | None = None,
    ):
        super().__init__(defaults=router_defaults)

        self._conn = _connection or Repid.get_magic_connection()

        if routers is not None:
            for router in routers:
                self.include_router(router)

        self.tasks_limit = tasks_limit
        self.graceful_shutdown_time = graceful_shutdown_time
        self.messages_limit = messages_limit
        self.handle_signals = (
            [signal.SIGINT, signal.SIGTERM] if handle_signals is None else handle_signals
        )
        self.auto_declare = auto_declare
        self.health_check_server = None
        if run_health_check_server:
            self.health_check_server = HealthCheckServer(health_check_server_settings)

    async def declare_all_queues(self) -> None:
        await asyncio.gather(
            *(
                Queue(queue_name, _connection=self._conn).declare()
                for queue_name in self.topics_by_queue
            ),
        )

    async def run(self) -> _Runner:  # noqa: C901
        if self.health_check_server is not None:
            await self.health_check_server.start()

        runner = _Runner(
            max_tasks=self.messages_limit,
            tasks_concurrency_limit=self.tasks_limit,
            health_check_server=self.health_check_server,
            _connection=self._conn,
        )

        if not self.actors:
            if self.health_check_server is not None:  # pragma: no cover
                await self.health_check_server.stop()
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
                ),
            )
            consumer_tasks.add(t)

        consumers: set[ConsumerT] = set()
        if consumer_tasks:
            consumer_futures, _ = await asyncio.wait(
                consumer_tasks,
                return_when=asyncio.ALL_COMPLETED,
            )
            for ft in consumer_futures:
                if ft.exception() is None:
                    consumers.add(ft.result())

        runner.stop_consume_event.set()

        if runner.tasks:
            await asyncio.wait(runner.tasks, return_when=asyncio.ALL_COMPLETED)

        if consumers:
            await asyncio.wait(
                {asyncio.create_task(c.finish()) for c in consumers},
                return_when=asyncio.ALL_COMPLETED,
            )

        runner.cancel_event.set()

        if self.health_check_server is not None:
            await self.health_check_server.stop()

        return runner

    def _signal_handler_constructor(self, runner: _Runner) -> Callable[[], None]:
        def signal_handler() -> None:
            runner.stop_consume_event.set()

            async def wait_before_cancel() -> None:
                await asyncio.sleep(self.graceful_shutdown_time)
                runner.cancel_event.set()

            t = asyncio.create_task(wait_before_cancel())
            runner.tasks.add(t)
            t.add_done_callback(runner.tasks.discard)

            loop = asyncio.get_running_loop()
            for sig in self.handle_signals:
                loop.remove_signal_handler(sig)

        return signal_handler
