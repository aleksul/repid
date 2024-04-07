from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING

from repid._runner import _Runner
from repid.health_check_server import HealthCheckServer
from repid.logger import logger
from repid.main import Repid
from repid.queue import Queue
from repid.router import Router, RouterDefaults

if TYPE_CHECKING:
    from repid.connection import Connection
    from repid.health_check_server import HealthCheckServerSettings


class Worker(Router):
    def __init__(
        self,
        routers: Iterable[Router] | None = None,
        *,
        graceful_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        tasks_limit: int = 1000,
        handle_signals: Iterable[signal.Signals] | None = None,
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
        self.graceful_consumer_finish_time = 5.0
        self.graceful_health_check_server_finish_time = 1.0
        self.messages_limit = messages_limit
        self.handle_signals = (
            frozenset([signal.SIGINT, signal.SIGTERM] if handle_signals is None else handle_signals)
            if sys.platform != "emscripten"
            else frozenset()
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

    async def run(self) -> _Runner:
        logger.info("Starting to run worker.")

        if self.health_check_server is not None:
            await self.health_check_server.start()

        runner = _Runner(
            max_tasks=self.messages_limit,
            tasks_concurrency_limit=self.tasks_limit,
            health_check_server=self.health_check_server,
            _connection=self._conn,
        )

        if not self.actors or not self.topics_by_queue:
            logger.info("Exiting worker, as there are no actors to run.")
            if self.health_check_server is not None:  # pragma: no cover
                await self.health_check_server.stop()
            return runner

        if self.auto_declare:
            await self.declare_all_queues()

        loop = asyncio.get_running_loop()
        self._register_signals(loop, runner)

        logger.info("Starting consumers.")
        try:
            consumers = await asyncio.gather(
                *(
                    runner.run_one_queue(
                        queue_name,
                        self.topics_by_queue[queue_name],
                        self.actors,
                    )
                    for queue_name in self.topics_by_queue
                ),
            )
        except asyncio.CancelledError as exc:
            logger.critical("Worker was cancelled.", exc_info=exc)
            raise

        await runner.finish_gracefully(timeout=self.graceful_shutdown_time)

        if consumers:
            logger.info("Gracefully finishing consumers.")
            await asyncio.wait_for(
                asyncio.gather(*(c.finish() for c in consumers)),
                timeout=self.graceful_consumer_finish_time,
            )

        if self.health_check_server is not None:
            await asyncio.wait_for(
                self.health_check_server.stop(),
                timeout=self.graceful_health_check_server_finish_time,
            )

        self._unregister_signals(loop)

        logger.info("Exiting worker run.")

        return runner

    def _register_signals(self, loop: asyncio.AbstractEventLoop, runner: _Runner) -> None:
        def signal_handler() -> None:
            logger.info("Received signal, stopping runner.")
            runner.sync_stop_wait_and_cancel(self.graceful_shutdown_time)
            self._unregister_signals(loop)

        if self.handle_signals:
            logger.debug(
                "Registering signal ({signals}) handlers.",
                extra={"signals": self.handle_signals},
            )
        for sig in self.handle_signals:
            loop.add_signal_handler(sig, signal_handler)

    def _unregister_signals(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in self.handle_signals:
            loop.remove_signal_handler(sig)
