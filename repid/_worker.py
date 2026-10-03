from __future__ import annotations

import asyncio
import logging
import signal
import sys
import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal

from repid._runner import _Runner
from repid._utils import NotSet
from repid._utils.not_set import _NotSet
from repid.asyncapi_server import AsyncAPIServer
from repid.data.actor import ActorExecutionContext
from repid.health_check_server import HealthCheckServer, HealthCheckStatus
from repid.limits import IntakeControl, LimitPolicyT, MessageLimits
from repid.router import _MaterializedRouter

logger = logging.getLogger("repid")

if TYPE_CHECKING:
    from repid.asyncapi import AsyncAPI3Schema
    from repid.asyncapi_server import AsyncAPIServerSettings
    from repid.health_check_server import HealthCheckServerSettings


class _Worker:
    def __init__(  # noqa: PLR0917
        self,
        actor_context: ActorExecutionContext,
        router: _MaterializedRouter,
        graceful_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        tasks_limit: int | _NotSet = NotSet,
        limits: MessageLimits | None = None,
        limit_policies: Iterable[LimitPolicyT] = (),
        intake_control: IntakeControl | None = None,
        actor_limits_propagation: Literal["auto", "off"] = "auto",
        register_signals: Iterable[signal.Signals] | None = None,
        health_check_server: HealthCheckServerSettings | None = None,
        asyncapi_server: AsyncAPIServerSettings | None = None,
        asyncapi_schema: AsyncAPI3Schema | None = None,
    ):
        self.actor_context = actor_context
        self.server = actor_context.server
        self.centralized_router = router

        if not isinstance(tasks_limit, _NotSet):
            if limits is not None:
                raise ValueError("Supply either tasks_limit or limits, not both")
            warnings.warn("tasks_limit is deprecated; use limits", DeprecationWarning, stacklevel=2)
            limits = MessageLimits(max_messages=tasks_limit)
        self.limits = limits if limits is not None else MessageLimits(max_messages=1000)
        self.tasks_limit = self.limits.max_messages
        self.limit_policies = tuple(limit_policies)
        self.intake_control = intake_control
        self.actor_limits_propagation = actor_limits_propagation
        self.messages_limit: int = messages_limit

        self.graceful_shutdown_time: float = graceful_shutdown_time
        self.graceful_consumer_finish_time: float = 5.0
        self.graceful_health_check_server_finish_time: float = 1.0
        self.graceful_asyncapi_server_finish_time: float = 1.0

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

        self.asyncapi_server: AsyncAPIServer | None = None
        if asyncapi_server is not None:
            if asyncapi_schema is None:  # pragma: no cover
                raise ValueError("AsyncAPI schema is required if AsyncAPI server is enabled.")
            self.asyncapi_server = AsyncAPIServer(asyncapi_schema, asyncapi_server)

    async def run(self) -> _Runner:
        logger.info(
            "worker.run.start",
            extra={
                "tasks_limit": self.tasks_limit,
                "messages_limit": self.messages_limit,
                "graceful_shutdown_time": self.graceful_shutdown_time,
            },
        )
        loop = asyncio.get_running_loop()
        try:
            runner = _Runner(
                actor_context=self.actor_context,
                max_tasks=self.messages_limit,
                limits=self.limits,
                limit_policies=self.limit_policies,
                intake_control=self.intake_control,
                actor_limits_propagation=self.actor_limits_propagation,
                health_check_server=self.health_check_server,
            )
            if self.health_check_server is not None:
                await self.health_check_server.start()
            if self.asyncapi_server is not None:
                await self.asyncapi_server.start()
            if not self.centralized_router.actors:
                logger.info("worker.run.exit.no_actors")
                return runner
            self._register_signals(loop, runner)
            logger.info("worker.consumer.start")
            await runner.run(
                channels_to_actors=self.centralized_router._actors_per_channel_address,
                channel_declarations=self.centralized_router.channel_declarations,
                graceful_termination_timeout=self.graceful_shutdown_time,
            )
        except asyncio.CancelledError as exc:
            logger.critical("worker.cancelled", exc_info=exc)
            raise
        except Exception:
            if self.health_check_server is not None:
                self.health_check_server.health_status = HealthCheckStatus.UNHEALTHY
            raise
        finally:
            self._unregister_signals(loop)
            if self.health_check_server is not None:
                await asyncio.wait_for(
                    self.health_check_server.stop(),
                    timeout=self.graceful_health_check_server_finish_time,
                )
            if self.asyncapi_server is not None:
                await asyncio.wait_for(
                    self.asyncapi_server.stop(),
                    timeout=self.graceful_asyncapi_server_finish_time,
                )
        logger.info("worker.run.exit")
        return runner

    def _register_signals(self, loop: asyncio.AbstractEventLoop, runner: _Runner) -> None:
        def signal_handler() -> None:
            logger.info("worker.signal.stop")
            runner.stop_consume_event.set()
            self._unregister_signals(loop)

        if self.register_signals:
            logger.debug("worker.signal.register", extra={"signals": self.register_signals})
        for sig in self.register_signals:
            loop.add_signal_handler(sig, signal_handler)

    def _unregister_signals(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in self.register_signals:
            loop.remove_signal_handler(sig)
