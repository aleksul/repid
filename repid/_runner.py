from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from functools import partial
from typing import TYPE_CHECKING, Any

from repid.connections.abc import ServerT, SubscriberT
from repid.health_check_server import HealthCheckStatus
from repid.logger import logger

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData
    from repid.health_check_server import HealthCheckServer
    from repid.serializer import SerializerT


ActorResultT = Any


async def _actor_execution(
    message: ReceivedMessageT,
    actor: ActorData,
    server: ServerT,
    default_serializer: SerializerT,
) -> ActorResultT:
    args, kwargs = await actor.converter.convert_inputs(
        message=message,
        actor=actor,
        server=server,
        default_serializer=default_serializer,
    )
    return await actor.fn(*args, **kwargs)


async def _actor_run(
    actor: ActorData,
    message: ReceivedMessageT,
    server: ServerT,
    default_serializer: SerializerT,
) -> ActorResultT | Exception:
    if (
        not message.is_acted_on  # theoretically a server can automatically ack the message on receive
        and actor.confirmation_mode == "ack_first"
    ):
        await message.ack()

    logger_extra = {
        "actor_name": actor.name,
        "time_limit": actor.timeout,
        "message_id": message.message_id,
    }

    exception = None
    result = None

    try:
        if actor.timeout is None or actor.timeout <= 0 or actor.timeout == float("inf"):
            result = await actor.middleware_pipeline(
                partial(_actor_execution, server=server, default_serializer=default_serializer),
                message,
                actor,
            )
        else:
            result = await asyncio.wait_for(
                actor.middleware_pipeline(
                    partial(_actor_execution, server=server, default_serializer=default_serializer),
                    message,
                    actor,
                ),
                timeout=actor.timeout,
            )

    except Exception as exc:  # noqa: BLE001
        exception = exc
        logger.debug(
            "Error inside of an actor '{actor_name}' on message {message_id}.",
            extra=logger_extra,
            exc_info=exc,
        )
    else:
        logger.debug(
            "Actor '{actor_name}' finished successfully on message {message_id}.",
            extra=logger_extra,
        )

    if not message.is_acted_on:
        if actor.confirmation_mode == "auto":
            if exception is None:
                await message.ack()
            else:
                await message.nack()
        elif actor.confirmation_mode == "always_ack":
            await message.ack()
        elif actor.confirmation_mode == "manual":
            logger.warning(
                "Actor '{actor_name}' is in 'manual' confirmation mode, "
                "but the message is not acknowledged.",
                extra=logger_extra,
            )

    return exception if exception is not None else result


async def _actor_run_with_cancel_event_and_callback(
    actor: ActorData,
    message: ReceivedMessageT,
    server: ServerT,
    default_serializer: SerializerT,
    cancel_event: asyncio.Event,
    cancel_event_task: asyncio.Task,
    callback: Callable[[], Awaitable],
) -> None:
    process_task = asyncio.create_task(_actor_run(actor, message, server, default_serializer))
    await asyncio.wait(
        {cancel_event_task, process_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    if cancel_event.is_set():
        process_task.cancel()
        if not message.is_acted_on:
            await message.reject()
        return
    await process_task
    await callback()


class _Runner:
    """State-management class for consuming messages and ensuring tasks are getting processed.
    It ensures proper concurrency limit with semaphore. It can also track amount of processed tasks.
    It has 2 events, which are used to create a graceful shutdown: stop_consume_event closes inflow
    of new messages, and cancel_event cancels all currently running tasks. Objects of this class
    are single-use only."""

    __slots__ = (
        "_cancel_event_task",
        "_health_check_server",
        "_limiter",
        "_processed",
        "_server_subscriber",
        "_server_subscriber_concurrency_unpause_threshold",
        "_server_subscriber_pause_lock",
        "_server_subscriber_was_paused",
        "_stop_consume_event_task",
        "_tasks",
        "_tasks_concurrency_limit",
        "cancel_event",
        "default_serializer",
        "max_tasks",
        "server",
        "stop_consume_event",
    )

    def __init__(
        self,
        *,
        server: ServerT,
        max_tasks: int = float("inf"),  # type: ignore[assignment]
        tasks_concurrency_limit: int = 1000,
        concurrency_unpause_percent: float = 0.1,  # 10 percent
        health_check_server: HealthCheckServer | None = None,
        default_serializer: SerializerT,
    ):
        self.server = server
        self._server_subscriber: SubscriberT | None = None
        self._server_subscriber_concurrency_unpause_threshold = max(
            math.ceil(tasks_concurrency_limit * concurrency_unpause_percent),
            1,
        )
        if self._server_subscriber_concurrency_unpause_threshold > tasks_concurrency_limit:
            raise ValueError(
                "Subscriber will never unpause, because unpause threshold is higher than concurrency limit.",
            )
        self._server_subscriber_was_paused = False
        self._server_subscriber_pause_lock = asyncio.Lock()

        self._processed = 0

        self._tasks: set[asyncio.Task] = set()

        self.stop_consume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()

        self.max_tasks = max_tasks
        self._tasks_concurrency_limit = tasks_concurrency_limit
        self._limiter = asyncio.Semaphore(tasks_concurrency_limit)

        self._health_check_server = health_check_server

        self.default_serializer = default_serializer

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def max_tasks_hit(self) -> bool:
        return (
            self.max_tasks
            - self._processed
            - (self._tasks_concurrency_limit - self._limiter._value)
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
        self._tasks.discard(task)
        self._limiter.release()
        self._processed += 1
        if self.max_tasks_hit:
            self.stop_consume_event.set()

    async def _actor_run_callback(self) -> None:
        if (
            self._server_subscriber_was_paused
            and self._server_subscriber is not None
            and self._tasks_concurrency_limit - self._limiter._value
            > self._server_subscriber_concurrency_unpause_threshold
        ):
            async with self._server_subscriber_pause_lock:
                if self._server_subscriber_was_paused:  # double check inside of the lock
                    await self._server_subscriber.resume()
                    self._server_subscriber_was_paused = False

    async def _message_handler(self, actors: list[ActorData], message: ReceivedMessageT) -> None:
        actor = next(filter(lambda actor: actor.routing_strategy(message), actors), None)
        if actor is None:
            # TODO: after the same message is seen multiple times - nack it instead of reject
            logger.warning(
                "No actor found for message on channel '{channel}'.",
                extra={"channel": message.channel},
            )
            await message.reject()
            return

        if (
            self._limiter.locked()
            and self.server.capabilities["supports_lightweight_pause"]
            and self._server_subscriber is not None
        ):
            async with self._server_subscriber_pause_lock:
                if not self._server_subscriber_was_paused:
                    await self._server_subscriber.pause()
                    self._server_subscriber_was_paused = True
            await self._limiter.acquire()
        else:
            await self._limiter.acquire()

        t = asyncio.create_task(
            _actor_run_with_cancel_event_and_callback(
                actor,
                message,
                self.server,
                self.default_serializer,
                self.cancel_event,
                self.cancel_event_task,
                self._actor_run_callback,
            ),
        )
        self._tasks.add(t)
        t.add_done_callback(self._task_callback)

    async def run(
        self,
        channels_to_actors: dict[str, list[ActorData]],
        graceful_termination_timeout: float,
        cancellation_timeout: float = 1.0,
    ) -> None:
        self._server_subscriber = await self.server.subscribe(
            channels_to_callbacks={
                channel: partial(self._message_handler, actors)
                for channel, actors in channels_to_actors.items()
            },
            concurrency_limit=self._tasks_concurrency_limit,
        )
        subscriber_task = self._server_subscriber.task
        await asyncio.wait(
            {self.stop_consume_event_task, subscriber_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if (
            subscriber_task.done()
            and not subscriber_task.cancelled()
            and (exc := subscriber_task.exception()) is not None
        ):
            logger.critical("Error while running consumer.", exc_info=exc)
            if self._health_check_server is not None:
                self._health_check_server.health_status = HealthCheckStatus.UNHEALTHY

        logger.debug("Gracefully finishing runner.")
        try:
            await self._server_subscriber.pause()
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Error while pausing subscriber during graceful shutdown.",
                exc_info=exc,
            )

        self.stop_consume_event.set()
        if self._tasks:
            _, pending = await asyncio.wait(
                self._tasks,
                return_when=asyncio.ALL_COMPLETED,
                timeout=graceful_termination_timeout,
            )
            if pending:
                logger.error("Some tasks timeouted when gracefully finishing runner.")
        self.cancel_event.set()

        # Give cancelled tasks a moment to clean up (reject messages, etc.)
        if self._tasks:
            logger.debug("Some tasks are still running even after cancellation, waiting for them.")
            await asyncio.wait(
                self._tasks,
                return_when=asyncio.ALL_COMPLETED,
                timeout=cancellation_timeout,
            )
            if self._tasks:
                logger.error("Some tasks did not finish even after cancellation and timeout.")

        try:
            await self._server_subscriber.close()
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Error while closing subscriber during graceful shutdown.",
                exc_info=exc,
            )

        logger.debug("Runner finished gracefully.")
