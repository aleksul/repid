from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any

from repid._concurrency import _ConcurrencyController, _Reservation
from repid._keep_alive import LeaseRenewal

if TYPE_CHECKING:
    from collections.abc import Mapping

    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData, ActorExecutionContext
    from repid.data.concurrency import Concurrency

logger = logging.getLogger("repid")


ActorResultT = Any


async def _actor_execution(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> ActorResultT:
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
) -> ActorResultT:
    """Wraps `_actor_execution` to ack/nack the message immediately after the actor fn runs,
    so middlewares unwinding above this leaf can observe `message.action`."""
    timeout = actor.timeout
    try:
        if timeout is None or timeout <= 0 or timeout == float("inf"):
            result = await _actor_execution(message, actor, actor_context)
        else:
            async with asyncio.timeout(timeout):
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


def _processing_interval(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
) -> float | None:
    if actor.keep_alive is False:
        return None
    interval: float | None
    if isinstance(actor.keep_alive, (int, float)) and not isinstance(actor.keep_alive, bool):
        interval = actor.keep_alive
    else:
        interval = message.keep_alive_interval
    if interval is not None and actor_context.server.capabilities["supports_keep_alive"]:
        return interval
    return None


async def _actor_run(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
) -> ActorResultT | Exception:
    delivery = _Delivery(message)
    try:
        delivery.renew_at(_processing_interval(actor, message, actor_context))
        return await _run_actor_pipeline(actor, message, actor_context)
    finally:
        await delivery.finish()


async def _run_actor_pipeline(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
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

    leaf = partial(
        _actor_execution_with_confirmation,
        actor_context=actor_context,
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


@dataclass(slots=True)
class _Delivery:
    """Hold capacity until processing or rejection finishes."""

    message: ReceivedMessageT
    renewal: LeaseRenewal | None = None
    reservation: _Reservation | None = None
    started: bool = False
    shutdown_rejection: asyncio.Task[None] | None = None

    def renew_at(self, interval: float | None) -> None:
        if self.renewal is None:
            if interval is None:
                return
            self.renewal = LeaseRenewal(self.message)
        self.renewal.set_interval(interval)

    async def finish(self) -> None:
        try:
            if self.renewal is not None:
                await self.renewal.close()
        finally:
            self.release()

    def release(self) -> None:
        if self.reservation is not None:
            self.reservation.release()

    async def reject(self) -> None:
        if not self.message.is_acted_on:
            await self.message.reject()


class _MessageLifecycle:
    """Own delivery admission, processing, and drain for a single runner."""

    __slots__ = (
        "_actor_context",
        "_cancel_event",
        "_cancel_waiter",
        "_capacity",
        "_committed",
        "_max_tasks",
        "_measure_payload",
        "_prestart_cleanup",
        "_processing",
        "_stop_consume_event",
        "_submissions",
        "processed",
        "quota_reached",
    )

    def __init__(
        self,
        *,
        actor_context: ActorExecutionContext,
        concurrency: Concurrency,
        channel_concurrency: Mapping[str, Concurrency] | None,
        max_tasks: int,
        stop_consume_event: asyncio.Event,
        cancel_event: asyncio.Event,
    ) -> None:
        self._actor_context = actor_context
        self._capacity = _ConcurrencyController(concurrency, channel_concurrency)
        self._max_tasks = max_tasks
        self._committed = 0
        self.processed = 0
        self.quota_reached = asyncio.Event()
        self._stop_consume_event = stop_consume_event
        self._cancel_event = cancel_event
        self._cancel_waiter: asyncio.Task[None] | None = None
        self._measure_payload = concurrency.payload_bytes is not None or any(
            limits.payload_bytes is not None for limits in (channel_concurrency or {}).values()
        )
        self._submissions: dict[asyncio.Future[None], asyncio.Task[object] | None] = {}
        self._processing: dict[asyncio.Task[None], _Delivery] = {}
        self._prestart_cleanup: set[asyncio.Task[None]] = set()

    @property
    def max_tasks_hit(self) -> bool:
        return self._committed >= self._max_tasks

    @property
    def _accepting(self) -> bool:
        return not self.max_tasks_hit and not self._stop_consume_event.is_set()

    def stop(self) -> None:
        self._stop_consume_event.set()
        self._capacity.close()

    async def submit(
        self,
        actors: list[ActorData],
        message: ReceivedMessageT,
        unrouted: Callable[[ReceivedMessageT], Awaitable[None]],
    ) -> None:
        delivery = _Delivery(message, renewal=LeaseRenewal.claim_for(message))
        try:
            if self._accepting:
                for actor in actors:
                    if actor.routing_strategy(message):
                        break
                else:
                    await self._track_submission(self._settle_unrouted(delivery, unrouted))
                    return

                channel = message.channel
                size = len(message.payload) if self._measure_payload else 0
                delivery.reservation = self._capacity.try_acquire(channel, size)
                if delivery.reservation is None:
                    await self._track_submission(self._wait_for_capacity(delivery, channel, size))

                if delivery.reservation is not None and self._accepting:
                    self._start_processing(actor, delivery)
                    return
        except asyncio.CancelledError:
            await self._track_submission(self._reject_and_release(delivery))
            raise
        except BaseException:
            await self._track_submission(self._reject_and_release(delivery, renew=False))
            raise
        await self._track_submission(self._reject_and_release(delivery))

    @staticmethod
    async def _settle_unrouted(
        delivery: _Delivery,
        unrouted: Callable[[ReceivedMessageT], Awaitable[None]],
    ) -> None:
        await unrouted(delivery.message)
        await delivery.finish()

    async def _reject_and_release(self, delivery: _Delivery, *, renew: bool = True) -> None:
        try:
            message = delivery.message
            if not message.is_acted_on:
                delivery.renew_at(self._broker_interval(message) if renew else None)
                await delivery.reject()
        finally:
            await delivery.finish()

    async def _track_submission(self, operation: Awaitable[None]) -> None:
        # The adapter can run a submission inline in a long-lived reader task.
        # Track the completion of the individual operation for shutdown.
        completed = asyncio.get_running_loop().create_future()
        self._submissions[completed] = asyncio.current_task()
        try:
            await operation
        finally:
            del self._submissions[completed]
            completed.set_result(None)

    def _broker_interval(self, message: ReceivedMessageT) -> float | None:
        if not self._actor_context.server.capabilities["supports_keep_alive"]:
            return None
        interval = message.keep_alive_interval
        return interval if interval is not None and interval > 0 else None

    async def _wait_for_capacity(self, delivery: _Delivery, channel: str, size: int) -> None:
        delivery.renew_at(self._broker_interval(delivery.message))
        delivery.reservation = await self._capacity.acquire(channel, size)

    def start(self) -> None:
        self._cancel_waiter = asyncio.create_task(self._cancel_processing())

    async def _cancel_processing(self) -> None:
        await self._cancel_event.wait()
        self.stop()
        for task, delivery in self._processing.items():
            if not task.done():
                # Return the delivery even if actor code suppresses cancellation.
                if delivery.started and not delivery.message.is_acted_on:
                    delivery.shutdown_rejection = asyncio.create_task(delivery.reject())
                task.cancel()

    async def _process(self, actor: ActorData, delivery: _Delivery) -> None:
        delivery.started = True
        message = delivery.message
        try:
            delivery.renew_at(_processing_interval(actor, message, self._actor_context))
            try:
                await _run_actor_pipeline(actor, message, self._actor_context)
            except asyncio.CancelledError:
                if delivery.shutdown_rejection is None:
                    # A settlement reserved at cancellation may have failed
                    # and released its action before propagating cancellation.
                    await delivery.reject()
                raise
            finally:
                if delivery.shutdown_rejection is not None:
                    await delivery.shutdown_rejection
        except Exception:
            # Stop admitting messages before freeing the slot for a waiting delivery.
            self.stop()
            raise
        finally:
            try:
                await delivery.finish()
            finally:
                self.processed += 1

    def _start_processing(self, actor: ActorData, delivery: _Delivery) -> None:
        coroutine = self._process(actor, delivery)
        try:
            task = asyncio.create_task(coroutine)
        except BaseException:
            coroutine.close()
            raise

        # A scheduled delivery consumes the lifetime quota even if cancelled before start.
        self._processing[task] = delivery
        task.add_done_callback(self._processing_done)
        self._committed += 1
        if self.max_tasks_hit:
            self._capacity.close()
            self.quota_reached.set()

    def _processing_done(self, task: asyncio.Task[None]) -> None:
        delivery = self._processing.pop(task)
        if not delivery.started:
            cleanup = asyncio.create_task(self._reject_and_release(delivery))
            self._prestart_cleanup.add(cleanup)
            cleanup.add_done_callback(partial(self._cleanup_done, delivery))
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("runner.processing.error", exc_info=exc)
        if self.max_tasks_hit:
            self._stop_consume_event.set()

    def _cleanup_done(self, delivery: _Delivery, task: asyncio.Task[None]) -> None:
        self._prestart_cleanup.discard(task)
        if task.cancelled():
            # The cleanup may have been cancelled before it could run its finally block.
            if delivery.renewal is not None:
                delivery.renewal.cancel()
            delivery.release()
        elif (exc := task.exception()) is not None:
            logger.error("runner.rejection.error", exc_info=exc)
            self.stop()

    async def _wait_until_idle(self, timeout: float) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while (
            pending_work := self._submissions.keys()
            | self._processing.keys()
            | self._prestart_cleanup
        ):
            _, pending = await asyncio.wait(pending_work, timeout=max(0, deadline - loop.time()))
            if pending:
                return False
        return True

    async def drain(self, grace: float, cancellation_timeout: float) -> bool:
        """Return whether all message work drained within the two shutdown windows."""
        try:
            if not await self._wait_until_idle(grace):
                logger.error("runner.shutdown.tasks_timeout")

            self._cancel_event.set()
            if await self._wait_until_idle(cancellation_timeout):
                return True

            logger.error("runner.shutdown.tasks_unfinished")
            for task in self._submissions.values():
                if task is not None:
                    task.cancel()
            for cleanup in self._prestart_cleanup:
                cleanup.cancel()
            return False
        finally:
            if self._cancel_waiter is not None:
                self._cancel_waiter.cancel()
                await asyncio.gather(self._cancel_waiter, return_exceptions=True)
