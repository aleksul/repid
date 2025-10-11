from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING

from repid._runner import _Runner
from repid.health_check_server import HealthCheckServer
from repid.logger import logger
from repid.router import Router

if TYPE_CHECKING:
    from repid.connections.abc import ServerT
    from repid.health_check_server import HealthCheckServerSettings


class _Worker:
    def __init__(
        self,
        server: ServerT,
        router: Router,
        graceful_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        tasks_limit: int = 1000,
        register_signals: Iterable[signal.Signals] | None = None,
        health_check_server: HealthCheckServerSettings | None = None,
    ):
        self.server: ServerT = server
        self.centralized_router = router

        self.tasks_limit: int = tasks_limit
        self.messages_limit: int = messages_limit

        self.graceful_shutdown_time: float = graceful_shutdown_time
        self.graceful_consumer_finish_time: float = 5.0
        self.graceful_health_check_server_finish_time: float = 1.0

        self.register_signals: frozenset[signal.Signals] = (
            frozenset(
                [signal.SIGINT, signal.SIGTERM] if register_signals is None else register_signals,
            )
            if sys.platform != "emscripten"
            else frozenset()
        )

        self.health_check_server: HealthCheckServer | None = None
        if health_check_server is not None:
            self.health_check_server = HealthCheckServer(health_check_server)

    async def run(self) -> _Runner:
        logger.info("Starting to run worker.")

        if self.health_check_server is not None:
            await self.health_check_server.start()

        runner = _Runner(
            server=self.server,
            max_tasks=self.messages_limit,
            tasks_concurrency_limit=self.tasks_limit,
            health_check_server=self.health_check_server,
        )

        if not self.centralized_router.actors:
            logger.info("Exiting worker, as there are no actors to run.")
            if self.health_check_server is not None:  # pragma: no cover
                await self.health_check_server.stop()
            return runner

        loop = asyncio.get_running_loop()
        self._register_signals(loop, runner)

        logger.debug("Starting consumer.")

        try:
            await runner.run(
                channels_to_actors=self.centralized_router._actors_per_channel_address,
                graceful_termination_timeout=self.graceful_shutdown_time,
            )
        except asyncio.CancelledError as exc:
            logger.critical("Worker was cancelled.", exc_info=exc)
            raise

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
            runner.stop_consume_event.set()
            self._unregister_signals(loop)

        if self.register_signals:
            logger.debug(
                "Registering signal ({signals}) handlers.",
                extra={"signals": self.register_signals},
            )
        for sig in self.register_signals:
            loop.add_signal_handler(sig, signal_handler)

    def _unregister_signals(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in self.register_signals:
            loop.remove_signal_handler(sig)
