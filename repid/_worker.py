from __future__ import annotations

import asyncio
import logging
import signal
import sys
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from repid._runner import _Runner
from repid.asyncapi_server import AsyncAPIServer
from repid.data.actor import ActorExecutionContext
from repid.health_check_server import HealthCheckServer
from repid.limits import (
    ActorLimitsPropagation,
    MessageLimits,
    validate_actor_limits_propagation,
)
from repid.router import _MaterializedRouter

logger = logging.getLogger("repid")

if TYPE_CHECKING:
    from repid.asyncapi import AsyncAPI3Schema
    from repid.asyncapi_server import AsyncAPIServerSettings
    from repid.health_check_server import HealthCheckServerSettings
    from repid.limits import LimitPolicyT


class _Worker:
    def __init__(  # noqa: PLR0917
        self,
        actor_context: ActorExecutionContext,
        router: _MaterializedRouter,
        limits: MessageLimits,
        limit_policies: Sequence[LimitPolicyT] = (),
        graceful_shutdown_time: float = 25.0,
        # Lifetime cap on processed messages — not a concurrency cap; concurrent
        # in-flight caps live in `limits.max_messages` / `max_payload_bytes`.
        messages_limit: float = float("inf"),
        register_signals: Iterable[signal.Signals] | None = None,
        health_check_server: HealthCheckServerSettings | None = None,
        asyncapi_server: AsyncAPIServerSettings | None = None,
        asyncapi_schema: AsyncAPI3Schema | None = None,
        actor_limits_propagation: ActorLimitsPropagation = "sum",
    ):
        validate_actor_limits_propagation(actor_limits_propagation)
        if not (isinstance(messages_limit, float) and messages_limit == float("inf")):
            if not isinstance(messages_limit, int) or isinstance(messages_limit, bool):
                raise TypeError("messages_limit must be a non-negative integer or infinity.")
            if messages_limit < 0:
                raise ValueError("messages_limit must be a non-negative integer or infinity.")

        self.actor_context = actor_context
        self.centralized_router = router

        self.limits = limits
        self.limit_policies = tuple(limit_policies)
        self.messages_limit: int | float = messages_limit
        self.actor_limits_propagation = actor_limits_propagation

        self.graceful_shutdown_time: float = graceful_shutdown_time
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
                "limits": self.limits,
                "limit_policies": self.limit_policies,
                "messages_limit": self.messages_limit,
                "graceful_shutdown_time": self.graceful_shutdown_time,
            },
        )

        runner = _Runner(
            actor_context=self.actor_context,
            limits=self.limits,
            limit_policies=self.limit_policies,
            max_tasks=self.messages_limit,
            health_check_server=self.health_check_server,
            channel_limits={
                channel.address: channel.limits
                for channel in self.centralized_router.channels
                if channel.limits is not None
            },
            channel_limit_policies={
                channel.address: channel.limit_policies
                for channel in self.centralized_router.channels
                if channel.limit_policies
            },
            actor_limits_propagation=self.actor_limits_propagation,
        )

        loop = asyncio.get_running_loop()
        registered_signals: tuple[signal.Signals, ...] = ()
        started_health_check_server: HealthCheckServer | None = None
        started_asyncapi_server: AsyncAPIServer | None = None
        try:
            if self.health_check_server is not None:
                await self.health_check_server.start()
                started_health_check_server = self.health_check_server

            if self.asyncapi_server is not None:
                await self.asyncapi_server.start()
                started_asyncapi_server = self.asyncapi_server

            if not self.centralized_router.actors:
                logger.info("worker.run.exit.no_actors")
                return runner

            registered_signals = self._register_signals(loop, runner)

            logger.info("worker.consumer.start")
            await runner.run(
                channels_to_actors=self.centralized_router._actors_per_channel_address,
                graceful_termination_timeout=self.graceful_shutdown_time,
            )
        except asyncio.CancelledError as exc:
            logger.critical("worker.cancelled", exc_info=exc)
            raise
        finally:
            await self._cleanup_resources(
                loop,
                registered_signals,
                started_health_check_server,
                started_asyncapi_server,
            )

        logger.info("worker.run.exit")

        return runner

    async def _cleanup_resources(
        self,
        loop: asyncio.AbstractEventLoop,
        registered_signals: tuple[signal.Signals, ...],
        health_check_server: HealthCheckServer | None,
        asyncapi_server: AsyncAPIServer | None,
    ) -> None:
        cleanup_errors: list[BaseException] = []
        if registered_signals:
            try:
                self._unregister_signals(loop, registered_signals)
            except BaseException as exc:  # noqa: BLE001
                cleanup_errors.append(exc)

        if health_check_server is not None:
            try:
                await asyncio.wait_for(
                    health_check_server.stop(),
                    timeout=self.graceful_health_check_server_finish_time,
                )
            except BaseException as exc:  # noqa: BLE001
                cleanup_errors.append(exc)

        if asyncapi_server is not None:
            try:
                await asyncio.wait_for(
                    asyncapi_server.stop(),
                    timeout=self.graceful_asyncapi_server_finish_time,
                )
            except BaseException as exc:  # noqa: BLE001
                cleanup_errors.append(exc)

        if cleanup_errors:
            for cleanup_error in cleanup_errors[1:]:
                logger.exception("worker.cleanup.error", exc_info=cleanup_error)
            raise cleanup_errors[0]

    def _register_signals(
        self,
        loop: asyncio.AbstractEventLoop,
        runner: _Runner,
    ) -> tuple[signal.Signals, ...]:
        def signal_handler() -> None:
            logger.info("worker.signal.stop")
            runner.stop_consume_event.set()
            self._unregister_signals(loop)

        if self.register_signals:
            logger.debug("worker.signal.register", extra={"signals": self.register_signals})
        registered: list[signal.Signals] = []
        try:
            for sig in self.register_signals:
                loop.add_signal_handler(sig, signal_handler)
                registered.append(sig)
        except BaseException:
            try:
                self._unregister_signals(loop, registered)
            except BaseException as exc:
                logger.exception("worker.signal.rollback.error", exc_info=exc)
            raise
        return tuple(registered)

    def _unregister_signals(
        self,
        loop: asyncio.AbstractEventLoop,
        signals: Iterable[signal.Signals] | None = None,
    ) -> None:
        errors: list[BaseException] = []
        for sig in self.register_signals if signals is None else signals:
            try:
                loop.remove_signal_handler(sig)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
        if errors:
            for unregistration_error in errors[1:]:
                logger.exception("worker.signal.unregister.error", exc_info=unregistration_error)
            raise errors[0]
