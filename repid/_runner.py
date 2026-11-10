from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine, Mapping
from functools import partial
from typing import TYPE_CHECKING

from repid._concurrency import _DEFAULT_WORKER_CONCURRENCY, Concurrency
from repid._message_lifecycle import _MessageLifecycle
from repid.connections.abc import SubscriberT
from repid.data.actor import ActorExecutionContext
from repid.health_check_server import HealthCheckStatus

logger = logging.getLogger("repid")

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData
    from repid.health_check_server import HealthCheckServer


class _Runner:
    """Single-use runner enforcing worker and channel budgets before scheduling actors."""

    def __init__(
        self,
        *,
        actor_context: ActorExecutionContext,
        max_tasks: int = float("inf"),  # pyrefly: ignore[bad-function-definition]
        concurrency: Concurrency = _DEFAULT_WORKER_CONCURRENCY,
        channel_concurrency: Mapping[str, Concurrency] | None = None,
        health_check_server: HealthCheckServer | None = None,
        max_unrouted_retries: int = 10,
    ) -> None:
        self.actor_context = actor_context
        self.server = actor_context.server
        self._concurrency = concurrency
        self._channel_concurrency = dict(channel_concurrency or {})
        self._server_subscriber: SubscriberT | None = None
        self._subscriber_paused = False
        self._health_check_server = health_check_server
        self.max_unrouted_retries = max_unrouted_retries
        self._unrouted_seen_counts: dict[tuple[str, str], int] = {}
        self.stop_consume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()
        self._messages = _MessageLifecycle(
            actor_context=actor_context,
            concurrency=concurrency,
            channel_concurrency=channel_concurrency,
            max_tasks=max_tasks,
            stop_consume_event=self.stop_consume_event,
            cancel_event=self.cancel_event,
        )

    @property
    def processed(self) -> int:
        return self._messages.processed

    @property
    def max_tasks_hit(self) -> bool:
        return self._messages.max_tasks_hit

    async def _settle_unrouted(self, message: ReceivedMessageT) -> None:
        logger.warning("actor.route.not_found", extra={"channel": message.channel})
        msg_id = message.message_id
        if msg_id is not None:
            key = (message.channel, msg_id)
            count = self._unrouted_seen_counts.get(key, 0) + 1
            if count >= self.max_unrouted_retries:
                self._unrouted_seen_counts.pop(key, None)
                logger.error(
                    "actor.route.poison_message",
                    extra={"channel": message.channel, "message_id": msg_id},
                )
                await message.nack()
                return
            self._unrouted_seen_counts[key] = count
        await message.reject()

    async def _message_handler(self, actors: list[ActorData], message: ReceivedMessageT) -> None:
        await self._messages.submit(actors, message, self._settle_unrouted)

    async def _subscribe(self, channels_to_actors: dict[str, list[ActorData]]) -> SubscriberT:
        callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]] = {
            channel: partial(self._message_handler, actors)
            for channel, actors in channels_to_actors.items()
        }
        return await self.server.subscribe(
            channels_to_callbacks=callbacks,
            concurrency=self._concurrency,
            channel_concurrency=self._channel_concurrency,
        )

    async def run(
        self,
        channels_to_actors: dict[str, list[ActorData]],
        graceful_termination_timeout: float,
        cancellation_timeout: float = 1.0,
    ) -> None:
        if self.max_tasks_hit:
            return
        self._messages.start()
        try:
            self._server_subscriber = await self._subscribe(channels_to_actors)
            subscriber_task = self._server_subscriber.task
            await self._wait_for_stop(subscriber_task)
            if (
                subscriber_task.done()
                and not subscriber_task.cancelled()
                and (exc := subscriber_task.exception()) is not None
            ):
                logger.critical("runner.consumer.error", exc_info=exc)
                if self._health_check_server is not None:
                    self._health_check_server.health_status = HealthCheckStatus.UNHEALTHY
        finally:
            self._messages.stop()
            shutdown = asyncio.create_task(
                self._shutdown(
                    graceful_termination_timeout,
                    cancellation_timeout,
                ),
            )
            try:
                await asyncio.shield(shutdown)
            except asyncio.CancelledError:
                await asyncio.shield(shutdown)
                raise

    async def _wait_for_stop(self, subscriber_task: asyncio.Task[object]) -> None:
        stop = asyncio.create_task(self.stop_consume_event.wait())
        quota = asyncio.create_task(self._messages.quota_reached.wait())
        try:
            done, _ = await asyncio.wait(
                {stop, quota, subscriber_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if done == {quota}:
                await self._pause_subscriber()
                await asyncio.wait({stop, subscriber_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stop.cancel()
            quota.cancel()
            await asyncio.gather(stop, quota, return_exceptions=True)

    async def _pause_subscriber(self) -> None:
        if self._server_subscriber is None or self._subscriber_paused:
            return
        try:
            await self._server_subscriber.pause()
            self._subscriber_paused = True
        except Exception as exc:
            logger.exception("runner.subscriber.pause.error", exc_info=exc)

    async def _shutdown(self, grace: float, cancellation_timeout: float) -> None:
        logger.debug("runner.shutdown.start")
        drained = False
        try:
            await self._pause_subscriber()
            drained = await self._messages.drain(grace, cancellation_timeout)
        finally:
            if self._server_subscriber is not None:
                if drained:
                    await self._close_subscriber(self._server_subscriber)
                else:
                    # Some adapters join callback tasks in close(). Give them a chance to
                    # close even when a callback is still stuck after cancellation.
                    close = asyncio.create_task(self._close_subscriber(self._server_subscriber))
                    _, pending = await asyncio.wait({close}, timeout=cancellation_timeout)
                    if pending:
                        logger.error("runner.subscriber.close.timeout")
        logger.debug("runner.shutdown.complete")

    @staticmethod
    async def _close_subscriber(subscriber: SubscriberT) -> None:
        try:
            await subscriber.close()
        except Exception as exc:
            logger.exception("runner.subscriber.close.error", exc_info=exc)
