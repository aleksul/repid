from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from functools import partial
from typing import TYPE_CHECKING

from repid._concurrency import _DEFAULT_WORKER_CONCURRENCY, _Budget
from repid._deadlines import _Deadlines
from repid._delivery import _Delivery
from repid._intake import _Intake
from repid.data.concurrency import Concurrency
from repid.health_check_server import HealthCheckStatus

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT, ServerT, SubscriberT
    from repid.data.actor import ActorData, ActorExecutionContext
    from repid.health_check_server import HealthCheckServer

logger = logging.getLogger("repid")


async def _actor_execution(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> object:
    args, kwargs = await actor.converter.convert_inputs(
        message=message,
        actor=actor,
        actor_context=actor_context,
    )
    return await actor.fn(*args, **kwargs)


async def _apply_confirmation_action(message: ReceivedMessageT, action: object) -> bool:
    if action == "reject":
        await message.reject()
    elif action == "nack":
        await message.nack()
    elif action == "ack":
        await message.ack()
    else:
        return False
    return True


async def _actor_execution_with_confirmation(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
    deadlines: _Deadlines,
    stop_renewal: Callable[[], None] | None = None,
) -> object:
    """Wraps `_actor_execution` to ack/nack the message immediately after the actor fn runs,
    so middlewares unwinding above this leaf can observe `message.action`."""
    try:
        return await _execute_and_confirm(message, actor, actor_context, deadlines)
    finally:
        # Settlements awaited by the leaf have finished. Stop renewing even if
        # the adapter's keep_alive does not report settled deliveries itself.
        if stop_renewal is not None and message.is_acted_on:
            stop_renewal()


async def _execute_and_confirm(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
    deadlines: _Deadlines,
) -> object:
    timeout = actor.timeout
    try:
        if timeout is None or timeout <= 0 or timeout == float("inf"):
            result = await _actor_execution(message, actor, actor_context)
        else:
            async with deadlines.timeout(timeout):
                result = await _actor_execution(message, actor, actor_context)
    except Exception as exc:
        if message.is_acted_on:
            raise
        if actor.confirmation_mode in ("auto", "manual", "manual_explicit"):
            error_action = (
                actor.on_error if isinstance(actor.on_error, str) else actor.on_error(exc)
            )
            await _apply_confirmation_action(message, error_action)
        elif actor.confirmation_mode == "always_ack":
            await message.ack()
        raise

    if message.is_acted_on:
        return result
    if actor.confirmation_mode in ("auto", "always_ack"):
        await message.ack()
    elif actor.confirmation_mode == "manual_explicit":
        if await _apply_confirmation_action(message, result) or result == "no_action":
            return result
        raise ValueError(
            f"Actor '{actor.name}' with confirmation_mode='manual_explicit' "
            f"returned an invalid value: {result!r}. Expected one of: "
            "'ack', 'nack', 'reject', 'no_action'.",
        )
    return result


def _broker_interval(message: ReceivedMessageT, server: ServerT) -> float | None:
    return message.keep_alive_interval if server.capabilities["supports_keep_alive"] else None


def _processing_interval(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
) -> float | None:
    keep_alive = actor.keep_alive
    if keep_alive is False or not actor_context.server.capabilities["supports_keep_alive"]:
        return None
    if isinstance(keep_alive, (int, float)) and not isinstance(keep_alive, bool):
        return keep_alive
    return message.keep_alive_interval


async def _run_actor_pipeline(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
    stop_renewal: Callable[[], None] | None = None,
    deadlines: _Deadlines | None = None,
) -> object:
    logger_extra = {
        "actor_name": actor.name,
        "time_limit": actor.timeout,
        "message_id": message.message_id,
    }

    if (
        not message.is_acted_on  # theoretically a server can automatically ack the message on receive
        and actor.confirmation_mode == "ack_first"
    ):
        try:
            await message.ack()
        except Exception as exc:
            # A failed settlement belongs to this delivery, not the worker:
            # skip the actor and leave the message for redelivery.
            logger.warning("actor.ack_first.error", extra=logger_extra, exc_info=exc)
            return exc

    exception = None
    result = None

    leaf = partial(
        _actor_execution_with_confirmation,
        actor_context=actor_context,
        stop_renewal=stop_renewal,
        # Callers outside a runner get timeouts of their own.
        deadlines=deadlines if deadlines is not None else _Deadlines(),
    )

    try:
        result = await actor.middleware_pipeline(leaf, message, actor)
    except Exception as exc:
        exception = exc
        logger.debug("actor.run.error", extra=logger_extra, exc_info=exc)
    else:
        logger.debug("actor.run.success", extra=logger_extra)

    if not message.is_acted_on and actor.confirmation_mode == "manual":
        logger.warning("actor.ack.manual.unacknowledged", extra=logger_extra)

    return exception if exception is not None else result


def _route(actors: list[ActorData], message: ReceivedMessageT) -> ActorData | None:
    for actor in actors:
        if actor.routing_strategy(message):
            return actor
    return None


class _Runner:
    """Single-use runner enforcing worker and channel budgets before scheduling actors."""

    def __init__(
        self,
        *,
        actor_context: ActorExecutionContext,
        max_tasks: int | None = None,
        concurrency: Concurrency = _DEFAULT_WORKER_CONCURRENCY,
        channel_concurrency: Mapping[str, Concurrency] | None = None,
        health_check_server: HealthCheckServer | None = None,
        max_unrouted_retries: int = 10,
    ) -> None:
        self._actor_context = actor_context
        self.server = actor_context.server
        channel_concurrency = channel_concurrency or {}
        self._budget = _Budget(concurrency, channel_concurrency)
        self._intake = _Intake(self._budget)
        self._deadlines = _Deadlines()
        self._measure_payload = concurrency.payload_bytes is not None or any(
            limits.payload_bytes is not None for limits in channel_concurrency.values()
        )
        self._max_tasks = max_tasks
        self._committed = 0
        self.processed = 0
        self._server_subscriber: SubscriberT | None = None
        self._subscriber_paused = False
        self._health_check_server = health_check_server
        self.max_unrouted_retries = max_unrouted_retries
        self._unrouted_seen_counts: dict[tuple[str, str], int] = {}
        self.stop_consume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()
        self.quota_reached = asyncio.Event()
        self._cancel_waiter: asyncio.Task[None] | None = None
        # Work the drain waits for: one task per submitted delivery, from admission
        # through processing, and rejections issued after a task ended.
        self._tasks: dict[_Delivery, asyncio.Task[None]] = {}
        self._cleanup_tasks: set[asyncio.Task[None]] = set()

    @property
    def max_tasks_hit(self) -> bool:
        return self._max_tasks is not None and self._committed >= self._max_tasks

    @property
    def _accepting(self) -> bool:
        return not self.max_tasks_hit and not self.stop_consume_event.is_set()

    def _stop(self) -> None:
        self.stop_consume_event.set()
        self._close_admission()

    def _close_admission(self) -> None:
        self._budget.close()
        self._intake.close()

    async def run(
        self,
        channels_to_actors: dict[str, list[ActorData]],
        graceful_termination_timeout: float,
        cancellation_timeout: float = 1.0,
    ) -> None:
        if self.max_tasks_hit:
            return
        self._cancel_waiter = asyncio.create_task(self._cancel_processing())
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
            self._stop()
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

    async def _subscribe(self, channels_to_actors: dict[str, list[ActorData]]) -> SubscriberT:
        for channel, actors in channels_to_actors.items():
            self._intake.add_channel(channel, partial(self._submit, actors))
        return await self.server.subscribe(intake=self._intake)

    # Admission

    def _submit(self, actors: list[ActorData], message: ReceivedMessageT) -> None:
        size = len(message.payload) if self._measure_payload else 0
        channel = message.channel
        self._intake.charge(channel, size)
        # Built here so it adopts a renewal the subscriber offers while submitting.
        delivery = _Delivery(message, self._budget, size)
        coroutine = self._handle(actors, delivery)
        try:
            task = asyncio.create_task(coroutine)
        except BaseException:
            # The subscriber keeps the message; do not leak the adopted renewal.
            coroutine.close()
            delivery.stop_renewal()
            self._intake.release(channel, size)
            raise
        # The task removes itself when it finishes. Only shutdown cancels these
        # tasks, and it cleans up after one that never got to run. An eager task
        # factory may already have run it to completion.
        if not task.done():
            self._tasks[delivery] = task

    async def _handle(self, actors: list[ActorData], delivery: _Delivery) -> None:
        delivery.entered = True
        message = delivery.message
        try:
            actor = None
            try:
                if self._accepting:
                    actor = _route(actors, message)
                    if actor is None:
                        await self._settle_unrouted(delivery)
                        return

                    if not delivery.try_acquire():
                        await self._wait_for_capacity(delivery)

                    if not (delivery.held and self._accepting):
                        actor = None
            except asyncio.CancelledError:
                await self._reject_and_release(delivery)
                raise
            except BaseException:
                await self._reject_and_release(delivery, renew=False)
                raise
            if actor is None:
                await self._reject_and_release(delivery)
                return
            self._commit()
            await self._process(actor, delivery)
        except Exception as exc:
            # Nothing awaits this task, so its failure ends here.
            logger.error("runner.processing.error", exc_info=exc)
        finally:
            # Release in the task itself so a waiting reader resumes a loop hop sooner.
            self._intake.release(message.channel, delivery.size)
            # Absent when an eager task factory finished it inside create_task.
            self._tasks.pop(delivery, None)
            # The first actor to finish after the quota starts the graceful shutdown.
            if delivery.started and self.max_tasks_hit:
                self.stop_consume_event.set()

    def _commit(self) -> None:
        # A delivery consumes the lifetime quota once it is admitted for processing.
        self._committed += 1
        if self.max_tasks_hit:
            self._close_admission()
            self.quota_reached.set()

    async def _wait_for_capacity(self, delivery: _Delivery) -> None:
        delivery.renew_at(_broker_interval(delivery.message, self.server))
        await delivery.acquire()

    async def _settle_unrouted(self, delivery: _Delivery) -> None:
        message = delivery.message
        delivery.renew_at(_broker_interval(message, self.server))
        logger.warning("actor.route.not_found", extra={"channel": message.channel})
        if self._is_poison(message):
            logger.error(
                "actor.route.poison_message",
                extra={"channel": message.channel, "message_id": message.message_id},
            )
            await message.nack()
        else:
            await message.reject()
        await delivery.finish()

    def _is_poison(self, message: ReceivedMessageT) -> bool:
        if (msg_id := message.message_id) is None:
            return False
        key = (message.channel, msg_id)
        count = self._unrouted_seen_counts.get(key, 0) + 1
        if count >= self.max_unrouted_retries:
            self._unrouted_seen_counts.pop(key, None)
            return True
        self._unrouted_seen_counts[key] = count
        return False

    async def _reject_and_release(self, delivery: _Delivery, *, renew: bool = True) -> None:
        try:
            message = delivery.message
            if not message.is_acted_on:
                delivery.renew_at(_broker_interval(message, self.server) if renew else None)
                await delivery.reject()
        finally:
            await delivery.finish()

    # Processing

    async def _process(self, actor: ActorData, delivery: _Delivery) -> None:
        delivery.started = True
        message = delivery.message
        try:
            delivery.renew_at(_processing_interval(actor, message, self._actor_context))
            try:
                await _run_actor_pipeline(
                    actor,
                    message,
                    self._actor_context,
                    delivery.stop_renewal,
                    self._deadlines,
                )
            except asyncio.CancelledError:
                if delivery.shutdown_rejection is None:
                    # A settlement reserved at cancellation may have failed
                    # and released its action before propagating cancellation.
                    await delivery.reject()
                raise
            finally:
                if delivery.shutdown_rejection is not None:
                    await asyncio.wait({delivery.shutdown_rejection})
        except Exception:
            # Stop admitting messages before freeing the slot for a waiting delivery.
            self._stop()
            raise
        finally:
            try:
                await delivery.finish()
            finally:
                self.processed += 1

    def _prestart_cancelled(self, delivery: _Delivery, _task: asyncio.Task[None]) -> None:
        # A task cancelled before its first step never runs its own cleanup.
        del self._tasks[delivery]
        self._intake.release(delivery.message.channel, delivery.size)
        cleanup = asyncio.create_task(self._reject_and_release(delivery))
        self._cleanup_tasks.add(cleanup)
        cleanup.add_done_callback(partial(self._cleanup_done, prestart_delivery=delivery))

    def _cleanup_done(
        self,
        task: asyncio.Task[None],
        *,
        prestart_delivery: _Delivery | None = None,
    ) -> None:
        self._cleanup_tasks.discard(task)
        if task.cancelled():
            # The cleanup may have been cancelled before it could run its finally block.
            if prestart_delivery is not None:
                prestart_delivery.stop_renewal()
                prestart_delivery.release()
        elif (exc := task.exception()) is not None:
            logger.error("runner.rejection.error", exc_info=exc)
            self._stop()

    # Shutdown

    async def _cancel_processing(self) -> None:
        await self.cancel_event.wait()
        self._stop()
        for delivery, task in list(self._tasks.items()):
            self._cancel(delivery, task)

    def _cancel(self, delivery: _Delivery, task: asyncio.Task[None]) -> None:
        # Registered tasks are still running: each removes itself as it finishes.
        # Return the delivery even if actor code suppresses cancellation.
        if delivery.started and not delivery.message.is_acted_on:
            rejection = asyncio.create_task(delivery.reject())
            delivery.shutdown_rejection = rejection
            self._cleanup_tasks.add(rejection)
            rejection.add_done_callback(self._cleanup_done)
        task.cancel()
        if not delivery.entered:
            task.add_done_callback(partial(self._prestart_cancelled, delivery))

    async def _wait_until_idle(self, timeout: float) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while pending_work := {*self._tasks.values(), *self._cleanup_tasks}:
            _, pending = await asyncio.wait(pending_work, timeout=max(0, deadline - loop.time()))
            if pending:
                return False
        return True

    async def _drain(self, grace: float, cancellation_timeout: float) -> bool:
        """Return whether all message work drained within the two shutdown windows."""
        try:
            if not await self._wait_until_idle(grace):
                logger.error("runner.shutdown.tasks_timeout")

            self.cancel_event.set()
            if await self._wait_until_idle(cancellation_timeout):
                return True

            logger.error("runner.shutdown.tasks_unfinished")
            for delivery in self._tasks:
                delivery.stop_renewal()
            for cleanup in self._cleanup_tasks:
                cleanup.cancel()
            return False
        finally:
            if self._cancel_waiter is not None:
                self._cancel_waiter.cancel()
                await asyncio.gather(self._cancel_waiter, return_exceptions=True)

    async def _wait_for_stop(self, subscriber_task: asyncio.Task[object]) -> None:
        stop = asyncio.create_task(self.stop_consume_event.wait())
        quota = asyncio.create_task(self.quota_reached.wait())
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
            drained = await self._drain(grace, cancellation_timeout)
        finally:
            if self._server_subscriber is not None:
                if drained:
                    await self._close_subscriber(self._server_subscriber)
                else:
                    # Give the subscriber a bounded chance to close even when a
                    # delivery is still stuck after cancellation.
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
