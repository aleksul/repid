from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass, field
from functools import partial
from inspect import iscoroutine
from typing import TYPE_CHECKING, Any

from repid._capacity import Capacity, Pool
from repid._limit_resolution import (
    ResolvedChannel,
    native_flow,
    pause_strategy,
    resolve_channels,
    resolve_control,
)
from repid.connections.abc import SubscriberT
from repid.data.actor import ActorExecutionContext
from repid.data.channel import Channel
from repid.health_check_server import HealthCheckStatus
from repid.limits import ActorLimits, IntakeControl, LimitPolicyT, MessageLimits, NativeFlow

logger = logging.getLogger("repid")

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData
    from repid.health_check_server import HealthCheckServer


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


async def _keep_alive_loop(message: ReceivedMessageT, interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        if message.is_acted_on:
            return
        try:
            await message.keep_alive()
        except Exception:  # noqa: BLE001
            logger.warning("message.keep_alive.error", extra={"message_id": message.message_id})


async def _run_with_keepalive(
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> ActorResultT:
    if actor.keep_alive is False:
        return await _actor_execution(message, actor, actor_context)

    interval = (
        actor.keep_alive
        if isinstance(actor.keep_alive, (int, float)) and not isinstance(actor.keep_alive, bool)
        else message.keep_alive_interval
    )
    if not actor_context.server.capabilities["supports_keep_alive"] or interval is None:
        return await _actor_execution(message, actor, actor_context)

    keepalive_task = asyncio.create_task(_keep_alive_loop(message, interval))
    try:
        return await _actor_execution(message, actor, actor_context)
    finally:
        keepalive_task.cancel()
        with suppress(asyncio.CancelledError):
            await keepalive_task


async def _actor_execution_with_confirmation(  # noqa: C901, PLR0912
    message: ReceivedMessageT,
    actor: ActorData,
    actor_context: ActorExecutionContext,
) -> ActorResultT:
    """Wraps `_actor_execution` to ack/nack the message immediately after the actor fn runs,
    so middlewares unwinding above this leaf can observe `message.action`."""
    try:
        if actor.timeout is None or actor.timeout <= 0 or actor.timeout == float("inf"):
            result = await _run_with_keepalive(message, actor, actor_context)
        else:
            result = await asyncio.wait_for(
                _run_with_keepalive(message, actor, actor_context),
                timeout=actor.timeout,
            )
    except Exception as exc:
        if not message.is_acted_on:
            if actor.confirmation_mode in ("auto", "manual", "manual_explicit"):
                error_action = (
                    actor.on_error if isinstance(actor.on_error, str) else actor.on_error(exc)
                )
                if error_action == "reject":
                    await message.reject()
                elif error_action == "nack":
                    await message.nack()
                elif error_action == "ack":
                    await message.ack()
            elif actor.confirmation_mode == "always_ack":
                await message.ack()
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


@dataclass(eq=False)
class _Delivery:
    message: ReceivedMessageT
    actor: ActorData
    size: int
    intake: tuple[Pool, ...]
    state: str = "delivered"
    renewal: asyncio.Task | None = None
    task: asyncio.Task | None = None
    reasons: set[object] = field(default_factory=set)
    admitted: bool = False
    wait_attempt: object | None = None


class _Runner:
    """Own deliveries independently of subscriber intake and actor execution."""

    def __init__(
        self,
        *,
        actor_context: ActorExecutionContext,
        max_tasks: int = float("inf"),  # type: ignore[assignment]
        tasks_concurrency_limit: int = 1000,
        limits: MessageLimits | None = None,
        intake_control: IntakeControl | None = None,
        limit_policies: tuple[LimitPolicyT, ...] = (),
        actor_limits_propagation: str = "auto",
        health_check_server: HealthCheckServer | None = None,
        max_unrouted_retries: int = 10,
    ):
        if actor_limits_propagation not in ("auto", "off"):
            raise ValueError("actor_limits_propagation must be auto or off")
        self.actor_context = actor_context
        self.server = actor_context.server
        self.max_tasks = max_tasks
        self.limits = (
            limits if limits is not None else MessageLimits(max_messages=tasks_concurrency_limit)
        )
        self.control = resolve_control(self.limits, intake_control)
        self.policies = limit_policies
        self.propagation = actor_limits_propagation
        self.max_unrouted_retries = max_unrouted_retries
        self._health_check_server = health_check_server
        self._unrouted_seen_counts: dict[str, int] = {}
        self._processed = 0
        self._committed = 0
        self._quota_closed = False
        self._capacity = Capacity()
        self._worker_pool = Pool(self.limits)
        self._channel_pools: dict[str, Pool] = {}
        self._execution_pools: dict[int, Pool] = {}
        self._order: dict[int, int] = {}
        self._channels: dict[str, ResolvedChannel] = {}
        self._flow = NativeFlow()
        self._callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]] = {}
        self._tasks: set[asyncio.Task] = set()
        self._deliveries: set[_Delivery] = set()
        self._submissions: set[asyncio.Task] = set()
        self._server_subscriber: SubscriberT | None = None
        self._pressure: dict[object, str | None] = {}
        self._numeric_pressure: set[tuple[str | None, str]] = set()
        self._paused: set[str | None] = set()
        self._pause_lock = asyncio.Lock()
        self._replacement: asyncio.Task | None = None
        self._intake_replacing = False
        self._changed = asyncio.Event()
        self._generation = 0
        self._failure: BaseException | None = None
        self.stop_consume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()
        self.delivered_messages = 0
        self.delivered_payload_bytes = 0
        self.admitted_messages = 0
        self.admitted_payload_bytes = 0
        self.processing_messages = 0
        self.processing_payload_bytes = 0

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def max_tasks_hit(self) -> bool:
        return self._processed + self._committed >= self.max_tasks

    def _notify(self) -> None:
        self._generation += 1
        self._changed.set()

    def _fail(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
            logger.error("runner.failure", exc_info=error)
        if self._health_check_server is not None:
            self._health_check_server.health_status = HealthCheckStatus.UNHEALTHY
        self.stop_consume_event.set()
        self._notify()

    def _register(self, resource: ActorLimits | LimitPolicyT) -> None:
        identity = id(resource)
        if identity not in self._order:
            self._order[identity] = len(self._order)
        if isinstance(resource, ActorLimits):
            self._execution_pools.setdefault(identity, Pool(resource))

    async def _unrouted(self, message: ReceivedMessageT) -> None:
        logger.warning("actor.route.not_found", extra={"channel": message.channel})
        msg_id = message.message_id
        if msg_id is not None:
            count = self._unrouted_seen_counts.get(msg_id, 0) + 1
            if count >= self.max_unrouted_retries:
                self._unrouted_seen_counts.pop(msg_id, None)
                logger.error(
                    "actor.route.poison_message",
                    extra={"channel": message.channel, "message_id": msg_id},
                )
                await message.nack()
                return
            self._unrouted_seen_counts[msg_id] = count
        await message.reject()

    async def _oversized(self, delivery: _Delivery) -> bool:
        channel = self._channels.get(delivery.message.channel)
        limits = [
            self.limits,
            channel.limits if channel else MessageLimits(),
            *delivery.actor.execution_limits,
        ]
        seen: set[int] = set()
        for index, limit in enumerate(limits):
            if id(limit) in seen:
                continue
            seen.add(id(limit))
            if limit.max_payload_bytes is None or delivery.size <= limit.max_payload_bytes:
                continue
            try:
                action = (
                    limit.on_oversized_payload(delivery.message)
                    if callable(limit.on_oversized_payload)
                    else limit.on_oversized_payload
                )
                if action not in ("run_alone", "reject", "nack"):
                    if iscoroutine(action):
                        action.close()
                    raise ValueError("Invalid oversized action")
            except Exception:  # noqa: BLE001
                logger.error(
                    "message.oversized.callback_error",
                    extra={
                        "scope": (
                            ("worker", "channel", *delivery.actor.execution_limit_scopes)[index]
                            if index < len(delivery.actor.execution_limit_scopes) + 2
                            else "execution"
                        ),
                        "message_id": delivery.message.message_id,
                    },
                )
                action = "nack"
            if action != "run_alone":
                await getattr(delivery.message, action)()
                return True
        return False

    async def _message_handler(self, actors: list[ActorData], message: ReceivedMessageT) -> None:  # noqa: C901, PLR0912, PLR0915
        # Ownership transfers at entry, before routing, renewal, or admission awaits.
        submission = asyncio.current_task()
        if submission is not None:
            self._submissions.add(submission)
        renewal = None
        delivery = None
        self.delivered_messages += 1
        self.delivered_payload_bytes += len(message.payload)
        try:
            interval = message.keep_alive_interval
            if self.server.capabilities["supports_keep_alive"] and interval is not None:
                renewal = asyncio.create_task(_keep_alive_loop(message, interval))
            if self.stop_consume_event.is_set() or self._quota_closed or self._intake_replacing:
                await message.reject()
                return
            actor = next(
                (candidate for candidate in actors if candidate.routing_strategy(message)),
                None,
            )
            if actor is None:
                await self._unrouted(message)
                return
            channel_pool = self._channel_pools.get(message.channel)
            intake = (
                (self._worker_pool, channel_pool)
                if channel_pool is not None
                else (self._worker_pool,)
            )
            delivery = _Delivery(
                message,
                actor,
                len(message.payload),
                intake,
                renewal=renewal,
                task=submission,
            )
            self._deliveries.add(delivery)
            if await self._oversized(delivery):
                return
            await self._reconcile()
            delivery.state = "waiting_intake"
            await self._capacity.acquire(
                intake,
                delivery.size,
                partial(self._intake_wait, delivery),
            )
            delivery.admitted = True
            self.admitted_messages += 1
            self.admitted_payload_bytes += delivery.size
            self._clear_waits(delivery)
            if self.stop_consume_event.is_set() or self._quota_closed or self._intake_replacing:
                await message.reject()
                return
            self._committed += 1
            delivery.state = "admitted"
            task = asyncio.create_task(self._process(delivery))
            delivery.task = task
            self._tasks.add(task)
            task.add_done_callback(self._task_done)
            if self.max_tasks_hit:
                self._quota_closed = True
                self._notify()
            await self._reconcile()
        except asyncio.CancelledError:
            if (
                delivery is None
                or delivery.state not in ("admitted", "waiting_execution", "processing", "finished")
            ) and not message.is_acted_on:
                await message.reject()
            raise
        except Exception as exc:
            if (
                delivery is None
                or delivery.state not in ("admitted", "waiting_execution", "processing", "finished")
            ) and not message.is_acted_on:
                await message.reject()
            self._fail(exc)
            raise
        finally:
            if submission is not None:
                self._submissions.discard(submission)
            if delivery is None or delivery.task is submission:
                if delivery is not None:
                    await self._release(delivery)
                else:
                    self.delivered_messages -= 1
                    self.delivered_payload_bytes -= len(message.payload)
                    await self._cancel_renewal(renewal)
                    await self._reconcile()
            self._notify()

    def _task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self._fail(task.exception())  # type: ignore[arg-type]
        self._notify()

    async def _intake_wait(self, delivery: _Delivery) -> None:
        pool = self._worker_pool
        count, byte = pool.limits.max_messages, pool.limits.max_payload_bytes
        worker_full = (count is not None and pool.messages >= count) or (
            byte is not None
            and (
                (delivery.size > byte and pool.messages > 0)
                or (delivery.size <= byte and pool.payload_bytes + delivery.size > byte)
            )
        )
        await self._wait(delivery, None if worker_full else delivery.message.channel)

    async def _wait(self, delivery: _Delivery, scope: str | None) -> None:
        reason = object()
        delivery.reasons.add(reason)
        self._pressure[reason] = scope
        await self._reconcile()

    def _attempt_wait(
        self,
        delivery: _Delivery,
        scope: str | None,
    ) -> Callable[[], Awaitable[None]]:
        notified = False
        attempt = object()
        delivery.wait_attempt = attempt

        async def on_wait() -> None:
            nonlocal notified
            if not notified and delivery.wait_attempt is attempt:
                notified = True
                await self._wait(delivery, scope)

        return on_wait

    def _clear_waits(self, delivery: _Delivery) -> None:
        for reason in delivery.reasons:
            self._pressure.pop(reason, None)
        delivery.reasons.clear()

    async def _process(self, delivery: _Delivery) -> None:  # noqa: C901, PLR0912, PLR0915
        message, actor = delivery.message, delivery.actor
        started = False
        try:
            delivery.state = "waiting_execution"
            channel = self._channels.get(message.channel)
            resource_list: list[ActorLimits | LimitPolicyT] = list(actor.execution_limits)
            resource_list.extend(self.policies)
            resource_list.extend(channel.policies if channel else ())
            resource_list.extend(actor.limit_policies)
            resources = {id(resource): resource for resource in resource_list}
            for resource in resources.values():
                self._register(resource)
            async with AsyncExitStack() as stack:
                for identity in sorted(resources, key=self._order.__getitem__):
                    resource = resources[identity]
                    scope = (
                        None
                        if any(resource is policy for policy in self.policies)
                        else message.channel
                    )
                    on_wait = self._attempt_wait(delivery, scope)
                    try:
                        if isinstance(resource, ActorLimits):
                            await stack.enter_async_context(
                                self._capacity.reserve(
                                    self._execution_pools[identity],
                                    delivery.size,
                                    on_wait,
                                ),
                            )
                        else:
                            await stack.enter_async_context(
                                resource.reserve(message, actor, on_wait),
                            )
                    finally:
                        delivery.wait_attempt = None
                        self._clear_waits(delivery)
                        await self._reconcile()
                await self._cancel_renewal(delivery.renewal)
                delivery.renewal = None
                if self.stop_consume_event.is_set():
                    if not message.is_acted_on:
                        await message.reject()
                    return
                # No await between the shutdown check and processing-start transition.
                delivery.state = "processing"
                started = True
                self.processing_messages += 1
                self.processing_payload_bytes += delivery.size
                await _actor_run(actor, message, self.actor_context)
        except asyncio.CancelledError:
            if not message.is_acted_on:
                await message.reject()
            raise
        except Exception as exc:  # noqa: BLE001
            if not message.is_acted_on:
                await message.reject()
            self._fail(exc)
        finally:
            if started:
                self.processing_messages -= 1
                self.processing_payload_bytes -= delivery.size
                self._processed += 1
            self._committed -= 1
            delivery.state = "finished"
            await self._release(delivery)
            if self._quota_closed and self._processed >= self.max_tasks:
                self.stop_consume_event.set()
            self._notify()

    @staticmethod
    async def _cancel_renewal(task: asyncio.Task | None) -> None:
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _release(self, delivery: _Delivery) -> None:
        self._clear_waits(delivery)
        if delivery.admitted:
            await self._capacity.release(delivery.intake, delivery.size)
            self.admitted_messages -= 1
            self.admitted_payload_bytes -= delivery.size
            delivery.admitted = False
        self._deliveries.discard(delivery)
        self.delivered_messages -= 1
        self.delivered_payload_bytes -= delivery.size
        await self._cancel_renewal(delivery.renewal)
        logger.debug(
            "runner.accounting",
            extra={
                "delivered": self.delivered_messages,
                "admitted": self.admitted_messages,
                "processing": self.processing_messages,
                "delivered_over_cap": max(
                    0,
                    self.delivered_messages - (self.limits.max_messages or self.delivered_messages),
                ),
            },
        )
        await self._reconcile()
        self._notify()

    def _numeric(self) -> None:
        for address, control in [
            (None, self.control),
            *((address, channel.control) for address, channel in self._channels.items()),
        ]:
            messages = (
                self.delivered_messages
                if address is None
                else sum(1 for d in self._deliveries if d.message.channel == address)
            )
            payload = (
                self.delivered_payload_bytes
                if address is None
                else sum(d.size for d in self._deliveries if d.message.channel == address)
            )
            flow = (
                self._server_subscriber.native_flow
                if self._server_subscriber is not None
                else self._flow
            )
            window = flow.worker if address is None else flow.channels.get(address)
            for dimension, config, usage in (
                ("messages", control.messages, messages),
                ("bytes", control.payload_bytes, payload),
            ):
                key = (address, dimension)
                native = (
                    (window.max_messages if dimension == "messages" else window.max_payload_bytes)
                    if window
                    else None
                )
                if key in self._numeric_pressure:
                    if usage <= (config.resume_at or 0):
                        self._numeric_pressure.remove(key)
                elif (
                    config.pause_at is not None
                    and usage >= config.pause_at
                    and native != config.pause_at
                ):
                    self._numeric_pressure.add(key)

    async def _reconcile(self) -> None:
        self._numeric()
        if (
            self._server_subscriber is None
            or self.stop_consume_event.is_set()
            or self._intake_replacing
        ):
            return
        async with self._pause_lock:
            scopes = set(self._pressure.values()) | {scope for scope, _ in self._numeric_pressure}
            desired: set[str | None] = set()
            for scope in scopes:
                control = self.control if scope is None else self._channels[scope].control
                strategy = pause_strategy(control, self.server.capabilities)
                if strategy == "resubscribe":
                    self._intake_replacing = True
                    self._replacement = asyncio.create_task(self._replace(self._server_subscriber))
                    return
                if strategy == "channel_pause" or (
                    strategy == "worker_pause"
                    and not self.server.capabilities["supports_worker_pause"]
                ):
                    desired.update(
                        self._channels if scope is None or strategy == "worker_pause" else (scope,),
                    )
                elif strategy == "worker_pause":
                    desired.add(None)
            if None in desired:
                desired = {None}
            try:
                # Establish new pauses before removing old ones.
                for scope in desired - self._paused:
                    await self._server_subscriber.pause(scope)
                for scope in self._paused - desired:
                    await self._server_subscriber.resume(scope)
                self._paused = desired
            except Exception as exc:
                self._fail(exc)
                raise

    async def _replace(self, subscriber: SubscriberT) -> None:
        try:
            await subscriber.stop()
            self._notify()
            while not self.stop_consume_event.is_set():
                generation = self._generation
                self._numeric()
                if not self._tasks and not self._pressure and not self._numeric_pressure:
                    break
                await self._wait_changed(generation)
            if self.stop_consume_event.is_set():
                return
            await subscriber.finish()
            self._paused.clear()
            self._server_subscriber = await self.server.subscribe(
                channels_to_callbacks=self._callbacks,
                native_flow=self._flow,
            )
            self._intake_replacing = False
            await self._reconcile()
            self._notify()
        except Exception as exc:  # noqa: BLE001
            self._fail(exc)

    async def _wait_changed(self, generation: int) -> None:
        async def change() -> None:
            if self._generation == generation:
                self._changed.clear()
                await self._changed.wait()

        changed = asyncio.create_task(change())
        stopped = asyncio.create_task(self.stop_consume_event.wait())
        try:
            await asyncio.wait({changed, stopped}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            changed.cancel()
            stopped.cancel()
            await asyncio.gather(changed, stopped, return_exceptions=True)

    def _configure(
        self,
        actors: dict[str, list[ActorData]],
        declarations: list[Channel] | None,
    ) -> None:
        if declarations is None:
            declarations = [
                actor.channel or Channel(address=address)
                for address, candidates in actors.items()
                for actor in candidates
            ]
        self._channels = resolve_channels(declarations, actors, self.control, self.propagation)  # type: ignore[arg-type]
        self._channel_pools = {
            address: Pool(channel.limits) for address, channel in self._channels.items()
        }
        self._flow = native_flow(
            self.limits,
            self.control,
            self._channels,
            self.server.capabilities,
        )
        for policy in self.policies:
            self._register(policy)
        for address, candidates in actors.items():
            for policy in self._channels[address].policies:
                self._register(policy)
            for actor in candidates:
                for numeric_pool in actor.execution_limits:
                    self._register(numeric_pool)
                for actor_policy in actor.limit_policies:
                    self._register(actor_policy)
        for scope, control, policies in [
            (None, self.control, self.policies),
            *(
                (
                    address,
                    channel.control,
                    (
                        *channel.policies,
                        *(p for actor in actors[address] for p in actor.limit_policies),
                    ),
                )
                for address, channel in self._channels.items()
            ),
        ]:
            triggers = (
                control.messages.pause_at is not None
                or control.payload_bytes.pause_at is not None
                or policies
                or (scope is not None and any(actor.execution_limits for actor in actors[scope]))
            )
            if (
                control.on_unavailable == "error"
                and triggers
                and pause_strategy(control, self.server.capabilities) is None
            ):
                raise ValueError(f"No available pause strategy for {scope or 'worker'}")
        self._callbacks = {
            address: partial(self._message_handler, candidates)
            for address, candidates in actors.items()
        }

    async def run(
        self,
        channels_to_actors: dict[str, list[ActorData]],
        graceful_termination_timeout: float,
        cancellation_timeout: float = 1.0,
        channel_declarations: list[Channel] | None = None,
    ) -> None:
        self._configure(channels_to_actors, channel_declarations)
        if self.max_tasks <= 0:
            return
        try:
            self._server_subscriber = await self.server.subscribe(
                channels_to_callbacks=self._callbacks,
                native_flow=self._flow,
            )
            await self._reconcile()
            while not self.stop_consume_event.is_set():
                generation = self._generation
                subscriber = self._server_subscriber
                if not self._intake_replacing and not self._quota_closed and subscriber.task.done():
                    if not subscriber.task.cancelled() and subscriber.task.exception() is not None:
                        self._fail(subscriber.task.exception())  # type: ignore[arg-type]
                    else:
                        self.stop_consume_event.set()
                    break
                if self._quota_closed and not self._intake_replacing:
                    await subscriber.stop()
                changed = asyncio.create_task(self._wait_changed(generation))
                try:
                    waiters = (
                        {changed}
                        if self._intake_replacing or self._quota_closed
                        else {changed, subscriber.task}
                    )
                    await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    changed.cancel()
                    await asyncio.gather(changed, return_exceptions=True)
        finally:
            await self._shutdown(graceful_termination_timeout, cancellation_timeout)
        if self._failure is not None:
            raise self._failure

    async def _shutdown(self, grace: float, cancellation_timeout: float) -> None:  # noqa: C901, PLR0912
        self.stop_consume_event.set()
        self._notify()
        if self._replacement is not None and not self._replacement.done():
            self._replacement.cancel()
            await asyncio.gather(self._replacement, return_exceptions=True)
        if self._server_subscriber is not None:
            try:
                await self._server_subscriber.stop()
            except Exception as exc:  # noqa: BLE001
                self._fail(exc)
        waiting = {
            d.task for d in self._deliveries if d.state != "processing" and d.task is not None
        }
        waiting.update(self._submissions)
        for task in waiting:
            task.cancel()
        if waiting:
            await asyncio.gather(*waiting, return_exceptions=True)
        # A Task cancelled before its first turn never enters its coroutine finally.
        for delivery in tuple(self._deliveries):
            if (
                delivery.state == "admitted"
                and delivery.task is not None
                and delivery.task.cancelled()
            ):
                if not delivery.message.is_acted_on:
                    await delivery.message.reject()
                self._committed -= 1
                delivery.state = "disposed"
                await self._release(delivery)
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=grace)
            if pending:
                logger.error("runner.shutdown.tasks_timeout")
            self.cancel_event.set()
            for task in pending:
                task.cancel()
            if pending:
                _, unfinished = await asyncio.wait(pending, timeout=cancellation_timeout)
                if unfinished:
                    logger.error("runner.shutdown.tasks_unfinished")
                    self._fail(TimeoutError("Processing did not stop within cancellation_timeout"))
        if self._server_subscriber is not None:
            try:
                await self._server_subscriber.finish()
            except Exception as exc:  # noqa: BLE001
                self._fail(exc)
