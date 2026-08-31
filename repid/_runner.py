from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Callable, Coroutine, Sequence
from contextlib import asynccontextmanager, suppress
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from repid.admission import (
    ExecutionAdmission,
    ReservablePolicyT,
    dispose_oversized,
    release_leases,
    reserve_limit_policies,
)
from repid.connections import SubscriberDispatcher
from repid.connections._subscriber import _keep_alive_loop
from repid.connections.abc import ReceivedMessageT, SubscriberT
from repid.data.actor import ActorExecutionContext
from repid.health_check_server import HealthCheckStatus
from repid.limits import (
    ActorLimitsPropagation,
    MessageLimits,
    OversizedReservationError,
    ReservationLeaseT,
)

logger = logging.getLogger("repid")

if TYPE_CHECKING:
    from repid.data.actor import ActorData
    from repid.health_check_server import HealthCheckServer
    from repid.limits import LimitPolicyT


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


@asynccontextmanager
async def _keep_alive_during(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> AsyncGenerator[None]:
    if actor.keep_alive is False:
        yield
        return

    interval = (
        actor.keep_alive
        if isinstance(actor.keep_alive, (int, float)) and not isinstance(actor.keep_alive, bool)
        else message.keep_alive_interval
    )
    if (
        not actor_context.server.capabilities["supports_keep_alive"]
        or interval is None
        or interval <= 0
    ):
        yield
        return

    keepalive_task = asyncio.create_task(_keep_alive_loop(message, interval))
    try:
        yield
    finally:
        keepalive_task.cancel()
        await asyncio.gather(keepalive_task, return_exceptions=True)


async def _confirm_error(message: ReceivedMessageT, actor: ActorData, exc: BaseException) -> None:
    if message.is_acted_on:
        return
    if actor.confirmation_mode in ("auto", "manual", "manual_explicit"):
        action = actor.on_error if isinstance(actor.on_error, str) else actor.on_error(exc)
        if action == "reject":
            await message.reject()
        elif action == "nack":
            await message.nack()
        elif action == "ack":
            await message.ack()
    elif actor.confirmation_mode == "always_ack":
        await message.ack()


async def _actor_execution_with_confirmation(  # noqa: C901
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> ActorResultT:
    """Run the actor and confirm before middleware unwinds."""
    try:
        if actor.timeout is None or actor.timeout <= 0 or actor.timeout == float("inf"):
            result = await _actor_execution(message, actor, actor_context)
        else:
            result = await asyncio.wait_for(
                _actor_execution(message, actor, actor_context),
                timeout=actor.timeout,
            )
    except Exception as exc:
        await _confirm_error(message, actor, exc)
        raise
    else:
        if not message.is_acted_on:
            if actor.confirmation_mode in ("auto", "always_ack"):
                await message.ack()
            elif actor.confirmation_mode == "manual_explicit":
                if result == "reject":
                    await message.reject()
                elif result == "nack":
                    await message.nack()
                elif result == "ack":
                    await message.ack()
                elif result == "no_action":
                    pass
                else:
                    raise ValueError(
                        f"Actor '{actor.name}' with confirmation_mode='manual_explicit' "
                        f"returned an invalid value: {result!r}. Expected one of: "
                        "'ack', 'nack', 'reject', 'no_action'.",
                    )
        return result


async def _actor_run(
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
    leaf = partial(_actor_execution_with_confirmation, actor_context=actor_context)
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


async def _cancel_and_reject(process_task: asyncio.Task[Any], message: ReceivedMessageT) -> None:
    process_task.cancel()
    with suppress(asyncio.CancelledError):
        await process_task
    if not message.is_acted_on:
        await message.reject()


async def _actor_run_with_cancel_event(
    actor: ActorData,
    message: ReceivedMessageT,
    actor_context: ActorExecutionContext,
    cancel_event: asyncio.Event,
    cancel_event_task: asyncio.Task,
) -> None:
    if cancel_event.is_set():
        if not message.is_acted_on:
            await message.reject()
        return

    process_task = asyncio.create_task(_actor_run(actor, message, actor_context))
    try:
        await asyncio.wait({cancel_event_task, process_task}, return_when=asyncio.FIRST_COMPLETED)
        if cancel_event.is_set():
            await _cancel_and_reject(process_task, message)
            return
        await process_task
    except asyncio.CancelledError:
        await _cancel_and_reject(process_task, message)
        raise


class _Runner:
    """Consume and route messages while managing admission and shutdown.

    ``stop_consume_event`` closes intake. ``cancel_event`` cancels work that did
    not finish during graceful shutdown. Runner instances are single-use.
    """

    __slots__ = (
        "_admission",
        "_cancel_event_task",
        "_health_check_server",
        "_processed",
        "_stop_consume_event_task",
        "_tasks",
        "_unrouted_seen_counts",
        "actor_context",
        "cancel_event",
        "max_tasks",
        "max_unrouted_retries",
        "server",
        "stop_consume_event",
    )

    def __init__(
        self,
        *,
        actor_context: ActorExecutionContext,
        limits: MessageLimits,
        limit_policies: Sequence[LimitPolicyT] = (),
        max_tasks: float = float("inf"),
        health_check_server: HealthCheckServer | None = None,
        max_unrouted_retries: int = 10,
        channel_limits: dict[str, MessageLimits] | None = None,
        channel_limit_policies: dict[str, Sequence[LimitPolicyT]] | None = None,
        actor_limits_propagation: ActorLimitsPropagation = "sum",
    ) -> None:
        self.server = actor_context.server

        self._processed = 0
        self.max_unrouted_retries = max_unrouted_retries
        self._unrouted_seen_counts: dict[tuple[str, str], int] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self.stop_consume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()
        self.max_tasks = max_tasks

        self._health_check_server = health_check_server
        self.actor_context = actor_context
        self._admission = ExecutionAdmission(
            server=self.server,
            limits=limits,
            limit_policies=limit_policies,
            channel_limits=channel_limits,
            channel_limit_policies=channel_limit_policies,
            actor_limits_propagation=actor_limits_propagation,
        )

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def max_tasks_hit(self) -> bool:
        return self.max_tasks - self._processed - len(self._tasks) <= 0

    @property
    def _can_admit(self) -> bool:
        """Whether another message may be admitted into processing."""
        return not self.stop_consume_event.is_set() and not self.max_tasks_hit

    async def _reject_and_stop(self, message: ReceivedMessageT) -> None:
        """Refuse a message and stop admitting further ones."""
        self.stop_consume_event.set()
        if not message.is_acted_on:
            await message.reject()

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

    def _task_callback(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        self._processed += 1
        if self.max_tasks_hit:
            self.stop_consume_event.set()

    async def _message_handler(self, actors: list[ActorData], message: ReceivedMessageT) -> None:
        actor = next((actor for actor in actors if actor.routing_strategy(message)), None)
        if actor is None:
            await self._handle_unrouted(message)
            return

        if not self._can_admit:
            await self._reject_and_stop(message)
            return

        task = asyncio.create_task(self._run_routed(actor, message))
        self._tasks.add(task)
        task.add_done_callback(self._task_callback)
        await task

    async def _handle_unrouted(self, message: ReceivedMessageT) -> None:
        logger.warning("actor.route.not_found", extra={"channel": message.channel})
        msg_id = message.message_id
        if msg_id is None:
            await message.reject()
            return
        key = (message.channel, msg_id)
        count = self._unrouted_seen_counts.get(key, 0) + 1
        if count >= self.max_unrouted_retries:
            self._unrouted_seen_counts.pop(key, None)
            logger.error(
                "actor.route.poison_message",
                extra={"channel": message.channel, "message_id": msg_id},
            )
            await message.nack()
        else:
            self._unrouted_seen_counts[key] = count
            await message.reject()

    def _mark_fatal(self, event: str, exc: BaseException) -> None:
        logger.critical(event, exc_info=exc)
        if self._health_check_server is not None:
            self._health_check_server.health_status = HealthCheckStatus.UNHEALTHY

    async def _on_reservation_wait(
        self,
        waited_channels: list[str],
        channel: str,
        policy: ReservablePolicyT,
    ) -> None:
        waited_channels.extend(await self._admission.on_wait(channel, policy))

    async def _reserve_leases(
        self,
        actor: ActorData,
        message: ReceivedMessageT,
    ) -> tuple[ReservationLeaseT, ...] | None:
        """Hold every reservation needed to run ``message`` through ``actor``.

        Returns the leases, or ``None`` when the message was already disposed of
        (oversized policy failure or fatal reservation error).
        Re-raises ``CancelledError`` after rejecting the message.
        """
        waited_channels: list[str] = []
        leases: tuple[ReservationLeaseT, ...] = ()
        on_wait = partial(self._on_reservation_wait, waited_channels, message.channel)
        try:
            try:
                reservations = self._admission.reservations(actor, message)
            except OversizedReservationError:
                raise
            except Exception as exc:  # noqa: BLE001
                await _confirm_error(message, actor, exc)
                return None
            leases = await reserve_limit_policies(reservations, on_wait)
            return leases
        except asyncio.CancelledError:
            if not message.is_acted_on:
                await message.reject()
            raise
        except OversizedReservationError as exc:
            await dispose_oversized(message, exc.action)
            return None
        except Exception as exc:  # noqa: BLE001
            self._mark_fatal("runner.reservation.error", exc)
            await self._reject_and_stop(message)
            return None
        finally:
            try:
                await self._admission.on_ready(waited_channels)
            except asyncio.CancelledError:
                await release_leases(leases)
                if not message.is_acted_on:
                    await message.reject()
                raise
            except Exception as exc:  # noqa: BLE001
                self._mark_fatal("runner.execution_backpressure.resume.error", exc)
                self.stop_consume_event.set()

    async def _run_routed(
        self,
        actor: ActorData,
        message: ReceivedMessageT,
    ) -> None:
        if (
            not message.is_acted_on  # A server may acknowledge a message when it receives it.
            and actor.confirmation_mode == "ack_first"
        ):
            await message.ack()

        leases: tuple[ReservationLeaseT, ...] | None = None
        try:
            async with _keep_alive_during(message, actor, self.actor_context):
                leases = await self._reserve_leases(actor, message)
                if leases is None:
                    return

                await _actor_run_with_cancel_event(
                    actor,
                    message,
                    self.actor_context,
                    self.cancel_event,
                    self.cancel_event_task,
                )
        finally:
            if leases is not None:
                try:
                    await release_leases(leases)
                except Exception as exc:  # noqa: BLE001
                    self._mark_fatal("runner.reservation.release.error", exc)
                    self.stop_consume_event.set()

    async def run(
        self,
        channels_to_actors: dict[str, list[ActorData]],
        graceful_termination_timeout: float,
        cancellation_timeout: float = 1.0,
    ) -> None:
        dispatcher = SubscriberDispatcher(
            self._admission.limits,
            self._admission.prepare(channels_to_actors),
            active=False,
        )
        self._admission.dispatcher = dispatcher
        callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]] = {
            channel: cast(
                Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                partial(self._message_handler, actors),
            )
            for channel, actors in channels_to_actors.items()
        }
        subscriber = await self.server.subscribe(
            channels_to_callbacks=callbacks,
            dispatcher=dispatcher,
        )
        self._admission.server_subscriber = subscriber
        try:
            self._admission.validate_backpressure()
        except BaseException:
            await subscriber.stop()
            await subscriber.finish()
            raise
        if self.max_tasks_hit:
            self.stop_consume_event.set()
        dispatcher.activate()
        subscriber_task = subscriber.task
        try:
            await asyncio.wait(
                {self.stop_consume_event_task, subscriber_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if (
                subscriber_task.done()
                and not subscriber_task.cancelled()
                and (exc := subscriber_task.exception()) is not None
            ):
                self._mark_fatal("runner.consumer.error", exc)
        finally:
            shutdown = asyncio.create_task(
                self._shutdown(subscriber, graceful_termination_timeout, cancellation_timeout),
            )
            try:
                await asyncio.shield(shutdown)
            except asyncio.CancelledError:
                await asyncio.shield(shutdown)
                raise

    async def _shutdown(
        self,
        subscriber: SubscriberT,
        graceful_termination_timeout: float,
        cancellation_timeout: float,
    ) -> None:
        logger.debug("runner.shutdown.start")
        self.stop_consume_event.set()
        await self._admission.shutdown_backpressure()
        try:
            # Stop intake first so no new work is admitted while in-flight
            # callbacks finish naturally during the graceful phase.
            await subscriber.stop()
        except Exception as exc:
            logger.exception("runner.subscriber.stop.error", exc_info=exc)

        if self._tasks:
            _, pending = await asyncio.wait(
                self._tasks,
                return_when=asyncio.ALL_COMPLETED,
                timeout=graceful_termination_timeout,
            )
            if pending:
                logger.error("runner.shutdown.tasks_timeout")
        self.cancel_event.set()
        if self._tasks:
            logger.debug("runner.shutdown.tasks_pending")
            for task in tuple(self._tasks):
                task.cancel()
            await asyncio.wait(
                self._tasks,
                return_when=asyncio.ALL_COMPLETED,
                timeout=cancellation_timeout,
            )
            if self._tasks:
                logger.error("runner.shutdown.tasks_unfinished")
        await self._finish_subscriber_bounded(subscriber, cancellation_timeout)
        logger.debug("runner.shutdown.complete")

    async def _finish_subscriber_bounded(
        self,
        subscriber: SubscriberT,
        cancellation_timeout: float,
    ) -> None:
        """Give `finish()` a bounded, uncancellable window.

        `finish()` cancels whatever is still running inside the subscriber and
        drains retained cleanup; on timeout it is abandoned, not cancelled —
        it keeps settling retained work (requeues, lease release) in background.
        """
        finish_task = asyncio.create_task(subscriber.finish())
        finish_error_handled = False

        def _log_abandoned_finish(task: asyncio.Task[None]) -> None:
            # The guard is unreachable in practice: this callback is registered
            # before `wait_for`'s shield callback, so it always runs while the
            # exception (if any) has not been re-raised into `wait_for` yet.
            if finish_error_handled:  # pragma: no cover
                return
            if not task.cancelled() and task.exception() is not None:
                logger.exception("runner.subscriber.finish.error", exc_info=task.exception())

        finish_task.add_done_callback(_log_abandoned_finish)
        try:
            await asyncio.wait_for(
                asyncio.shield(finish_task),
                timeout=cancellation_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("runner.subscriber.finish.timeout")
        except Exception as exc:
            finish_error_handled = True
            logger.exception("runner.subscriber.finish.error", exc_info=exc)
