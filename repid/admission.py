"""Admission-control runtime: reservation machinery, intake gates, backpressure.

This module *enforces* the policies declared in :mod:`repid.limits`. It owns
everything that runs while a worker is processing: fixed-capacity gates,
reservation leases and ordering, the intake gate consulted by broker
subscribers, oversized-message disposal, custom limit policies, and the
mapping of execution-capacity waits onto broker pauses.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from functools import partial
from typing import TYPE_CHECKING, TypeAlias

from repid.connections.abc import ChannelPausableSubscriberT, NativeFlowControlSubscriberT
from repid.limits import (
    ActorLimits,
    ActorLimitsPropagation,
    BackpressurePolicy,
    BackpressureResource,
    LimitPolicyT,
    Limits,
    MessageLimits,
    OversizedPayloadAction,
    OversizedPayloadPolicy,
    OversizedReservationError,
    ReservationLeaseT,
    dedupe_by_identity,
    resolve_oversized_payload_action,
    validate_cost,
    validate_oversized_payload_action,
)

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT, ServerT, SubscriberT
    from repid.data.actor import ActorData

logger = logging.getLogger("repid")


class _FixedCapacityGate:
    """Local fixed-capacity reservation backend for built-in resource costs."""

    __slots__ = (
        "_capacity",
        "_condition",
        "_exclusive",
        "_exclusive_waiters",
        "_ordinary_claims",
        "_used",
        "action",
        "resource",
    )

    def __init__(
        self,
        capacity: int,
        *,
        resource: BackpressureResource = "messages",
        oversized_payload_action: OversizedPayloadAction = "run_alone",
    ) -> None:
        self._capacity = validate_cost(capacity, "capacity")
        self.action = validate_oversized_payload_action(oversized_payload_action)
        self.resource = resource
        self._condition = asyncio.Condition()
        self._exclusive = False
        self._exclusive_waiters = 0
        self._ordinary_claims = 0
        self._used = 0

    async def reserve(  # noqa: C901, PLR0912
        self,
        cost: int,
        on_wait: Callable[[], Awaitable[None]],
        *,
        resume_at: float = 1.0,
    ) -> ReservationLeaseT:
        cost = validate_cost(cost, "cost")
        resume_threshold = math.floor(self._capacity * resume_at)
        oversized = cost > self._capacity
        if oversized and self.action != "run_alone":
            raise OversizedReservationError(self.action)

        waited = False
        drained = False
        queued_exclusive = False
        try:
            while True:
                async with self._condition:
                    if waited and not oversized and self._used <= resume_threshold:
                        drained = True
                    if (not waited or oversized or drained) and self._can_reserve(
                        cost,
                        oversized=oversized,
                    ):
                        if oversized:
                            self._exclusive = True
                            if queued_exclusive:
                                self._exclusive_waiters -= 1
                                queued_exclusive = False
                        else:
                            self._ordinary_claims += 1
                            self._used += cost
                        return _ReservationLease(
                            lambda: self._release(cost=cost, oversized=oversized),
                        )
                    if oversized and not queued_exclusive:
                        self._exclusive_waiters += 1
                        queued_exclusive = True
                if not waited:
                    await on_wait()
                    waited = True
                async with self._condition:
                    if not oversized and not drained:
                        await self._condition.wait_for(
                            lambda: self._used <= resume_threshold,
                        )
                    else:
                        await self._condition.wait_for(
                            lambda: self._can_reserve(cost, oversized=oversized),
                        )
        finally:
            if queued_exclusive:
                async with self._condition:
                    self._exclusive_waiters -= 1
                    self._condition.notify_all()

    def _can_reserve(self, cost: int, *, oversized: bool) -> bool:
        if oversized:
            return not self._exclusive and self._ordinary_claims == 0
        return (
            not self._exclusive
            and self._exclusive_waiters == 0
            and self._used + cost <= self._capacity
        )

    async def _release(self, *, cost: int, oversized: bool) -> None:
        async with self._condition:
            if oversized:
                self._exclusive = False
            else:
                self._ordinary_claims -= 1
                self._used -= cost
            self._condition.notify_all()


ReservablePolicyT = LimitPolicyT | _FixedCapacityGate
ReservationCallT: TypeAlias = Callable[
    [Callable[[], Awaitable[None]]],
    Awaitable[ReservationLeaseT],
]
ReservationRequestT: TypeAlias = tuple[ReservablePolicyT, ReservationCallT]


class _ReservationLease:
    """Idempotent lease: ``release`` runs at most once and shields its completion.

    Serves both as a single claim (from a built-in gate or custom limit policy)
    and as a composite claim wrapping several leases at once (see
    ``IntakeGate.reserve``), so callers always release through one interface.
    """

    __slots__ = ("_release", "_release_task")

    def __init__(self, release: Callable[[], Awaitable[None]]) -> None:
        self._release = release
        self._release_task: asyncio.Future[None] | None = None

    async def release(self) -> None:
        if self._release_task is None:
            self._release_task = asyncio.ensure_future(self._release())
        await asyncio.shield(self._release_task)


async def release_leases(leases: Iterable[ReservationLeaseT]) -> None:
    error: BaseException | None = None
    for lease in reversed(tuple(leases)):
        try:
            await lease.release()
        except BaseException as exc:
            if error is None:
                error = exc
            else:
                logger.warning("lease.release.suppressed", exc_info=exc)
    if error is not None:
        raise error


async def reserve_limit_policies(
    reservations: Iterable[ReservationRequestT],
    on_wait: Callable[[ReservablePolicyT], Awaitable[None]],
) -> tuple[ReservationLeaseT, ...]:
    """Reserve unique policies in one process-wide order, cleaning up on failure."""
    unique: dict[int, ReservationRequestT] = {}
    for policy, reserve in reservations:
        unique.setdefault(id(policy), (policy, reserve))
    leases: list[ReservationLeaseT] = []
    try:
        for policy, reserve in sorted(unique.values(), key=lambda request: id(request[0])):
            leases.append(await reserve(partial(on_wait, policy)))
    except BaseException:
        await release_leases(leases)
        raise
    return tuple(leases)


async def dispose_oversized(message: ReceivedMessageT, action: OversizedPayloadAction) -> None:
    """Dispose of a fetched message using the resolved oversized payload action."""
    if action == "nack":
        await message.nack()
    elif action == "reject":
        await message.reject()


def _tightest(*caps: int | None) -> int | None:
    values = [cap for cap in caps if cap is not None]
    return min(values) if values else None


class _FixedCapacityGates:
    """Per-policy fixed-capacity gates, built once and cached by identity.

    ``Limits`` compares by identity, so instances are safe dictionary keys;
    caching them also keeps each gate alive for the registry's lifetime.
    """

    def __init__(self) -> None:
        self._gates: dict[Limits, tuple[_FixedCapacityGate | None, _FixedCapacityGate | None]] = {}

    def for_policy(
        self,
        limits: MessageLimits | ActorLimits,
    ) -> tuple[_FixedCapacityGate | None, _FixedCapacityGate | None]:
        cached = self._gates.get(limits)
        if cached is None:
            cached = (
                _FixedCapacityGate(limits.max_messages, resource="messages")
                if limits.max_messages is not None
                else None,
                _FixedCapacityGate(limits.max_payload_bytes, resource="payload_bytes")
                if limits.max_payload_bytes is not None
                else None,
            )
            self._gates[limits] = cached
        return cached


class IntakeGate:
    """Local count and payload accounting for one subscription."""

    def __init__(
        self,
        limits: MessageLimits | None = None,
        channel_limits: Mapping[str, MessageLimits] | None = None,
    ) -> None:
        self._limits = limits
        self._channel_limits = dict(channel_limits) if channel_limits is not None else {}
        # Resolve an oversized payload disposition before reserving. Individual gates
        # then use exclusive run-alone mechanics for the selected disposition.
        self._gates = _FixedCapacityGates()

    def native_message_limit(self, channel: str | None = None) -> int | None:
        policies = (self._limits,) if channel is None else self._policies(channel)
        return _tightest(*(p.max_messages for p in policies if p is not None))

    def native_payload_limit(self, channel: str | None = None) -> int | None:
        policies = (self._limits,) if channel is None else self._policies(channel)
        return _tightest(*(p.max_payload_bytes for p in policies if p is not None))

    def _policies(self, channel: str) -> tuple[MessageLimits | None, MessageLimits | None]:
        return (self._limits, self._channel_limits.get(channel))

    async def reserve(self, message: ReceivedMessageT) -> ReservationLeaseT:
        reservations: list[ReservationRequestT] = []
        policies = self._policies(message.channel)
        # A channel override wins over the worker policy. This remains stable
        # regardless of the identity-based ordering used for deadlock avoidance.
        for policy in reversed(policies):
            if policy is None:
                continue
            is_oversized = (
                policy.max_payload_bytes is not None
                and len(
                    message.payload,
                )
                > policy.max_payload_bytes
            )
            if is_oversized:
                action = resolve_oversized_payload_action(policy.oversized_payload_action, message)
                if action != "run_alone":
                    raise OversizedReservationError(action)
                break
        for policy in policies:
            if policy is None:
                continue
            message_gate, payload_gate = self._gates.for_policy(policy)
            if message_gate is not None:
                reservations.append((message_gate, partial(message_gate.reserve, 1)))
            if payload_gate is not None:
                reservations.append(
                    (payload_gate, partial(payload_gate.reserve, len(message.payload))),
                )
        leases = await reserve_limit_policies(reservations, lambda _: _noop())
        return _ReservationLease(lambda: release_leases(leases))


async def _noop() -> None:
    return None


class _BackpressureController:
    """Resolve ordered backpressure policies and coordinate pause lifecycles."""

    def __init__(
        self,
        *,
        server: ServerT,
        default: BackpressurePolicy,
        overrides: Mapping[str, BackpressurePolicy],
    ) -> None:
        self._server = server
        self._default = default
        self._overrides = dict(overrides)
        self.subscriber: SubscriberT | None = None
        self._lock = asyncio.Lock()
        self._waiters: dict[str, int] = {}
        self._paused_channels: set[str] = set()
        self._global_paused = False
        self._warned_unavailable: set[str] = set()

    def resolve(self, channel: str) -> BackpressurePolicy:
        return self._overrides.get(channel, self._default)

    def tracks_waiters(self, channel: str) -> bool:
        return bool(self.resolve(channel).strategies)

    def _supports_native(
        self,
        channel: str,
        resource: BackpressureResource | None,
    ) -> bool:
        if self.subscriber is None or resource is None:
            return False
        return isinstance(
            self.subscriber,
            NativeFlowControlSubscriberT,
        ) and self.subscriber.supports_native_flow_control(channel, resource)

    def _strategy_available(
        self,
        strategy: str,
        channel: str,
        resource: BackpressureResource | None,
    ) -> bool:
        capabilities = self._server.capabilities
        if strategy == "native":
            return self._supports_native(channel, resource)
        if strategy == "channel_pause":
            return capabilities["supports_channel_pause"] and isinstance(
                self.subscriber,
                ChannelPausableSubscriberT,
            )
        if strategy == "worker_pause":
            return self.subscriber is not None and capabilities["supports_lightweight_pause"]
        return strategy == "resubscribe" and self.subscriber is not None

    def _select_strategy(
        self,
        channel: str,
        resource: BackpressureResource | None,
    ) -> str | None:
        return next(
            (
                strategy
                for strategy in self.resolve(channel).strategies
                if self._strategy_available(strategy, channel, resource)
            ),
            None,
        )

    def resume_at(self, channel: str, resource: BackpressureResource) -> float:
        strategy = self._select_strategy(channel, resource)
        if strategy in ("channel_pause", "worker_pause", "resubscribe"):
            return self.resolve(channel).resume_at
        return 1.0

    def require_capability(
        self,
        resources: Mapping[str, Iterable[BackpressureResource | None]],
    ) -> None:
        """Fail strict policies when no configured strategy can handle a possible wait."""
        for channel, channel_resources in resources.items():
            policy = self.resolve(channel)
            if policy.on_unavailable != "error":
                continue
            for resource in channel_resources:
                if self._select_strategy(channel, resource) is not None:
                    continue
                raise ValueError(
                    f"backpressure on channel {channel!r} has no available strategy "
                    f"for {resource or 'custom limit policies'}.",
                )

    def can_control_without_native(self, channel: str) -> bool:
        capabilities = self._server.capabilities
        strategies = self.resolve(channel).strategies
        return (
            ("channel_pause" in strategies and capabilities["supports_channel_pause"])
            or ("worker_pause" in strategies and capabilities["supports_lightweight_pause"])
            or "resubscribe" in strategies
        )

    async def on_wait(  # noqa: C901
        self,
        channels: Iterable[str],
        resource: BackpressureResource | None,
    ) -> list[str]:
        """Apply the first available strategy and return waits needing release."""
        subscriber = self.subscriber
        pause_channels: list[str] = []
        global_pause = False
        tracked: list[str] = []
        unavailable: list[str] = []
        try:
            async with self._lock:
                for channel in channels:
                    action = self._wait_action(channel, resource)
                    if action in ("channel", "global", "paused"):
                        self._waiters[channel] = self._waiters.get(channel, 0) + 1
                        tracked.append(channel)
                    if action == "channel":
                        pause_channels.append(channel)
                    elif action == "global":
                        global_pause = True
                    elif action == "warn":
                        unavailable.append(channel)
            if subscriber is None:
                return tracked
            for channel in pause_channels:
                if isinstance(subscriber, ChannelPausableSubscriberT):
                    await subscriber.pause_channel(channel)
            if global_pause:
                await subscriber.pause()
            for channel in unavailable:
                logger.warning(
                    "runner.execution_backpressure.unavailable",
                    extra={"channel": channel},
                )
            return tracked
        except BaseException:
            try:
                await self.on_ready(tracked)
            except Exception as exc:
                logger.exception("runner.execution_backpressure.rollback.error", exc_info=exc)
            raise

    def _wait_action(
        self,
        channel: str,
        resource: BackpressureResource | None,
    ) -> str | None:
        """Select and record the first available strategy for one waiter."""
        configured = self.resolve(channel)
        strategy = self._select_strategy(channel, resource)
        if strategy == "native":
            return "native"
        if strategy is not None:
            if self._global_paused or channel in self._paused_channels:
                return "paused"
            if strategy == "channel_pause":
                self._paused_channels.add(channel)
                return "channel"
            self._global_paused = True
            return "global"

        if configured.strategies and channel not in self._warned_unavailable:
            self._warned_unavailable.add(channel)
            return "warn"
        return None

    async def on_ready(self, channels: Iterable[str]) -> None:
        """Record one reservation finishing its wait per channel, resuming intake."""
        subscriber = self.subscriber
        resume_channels: list[str] = []
        global_resume = False
        async with self._lock:
            for channel in channels:
                remaining = self._waiters.get(channel, 1) - 1
                if remaining > 0:
                    self._waiters[channel] = remaining
                    continue
                self._waiters.pop(channel, None)
                if subscriber is None:
                    continue
                if channel in self._paused_channels:
                    self._paused_channels.remove(channel)
                    resume_channels.append(channel)
                elif self._global_paused and not self._waiters:
                    self._global_paused = False
                    global_resume = True
        if subscriber is not None:
            for channel in resume_channels:
                if isinstance(subscriber, ChannelPausableSubscriberT):
                    await subscriber.resume_channel(channel)
            if global_resume:
                await subscriber.resume()


class ExecutionAdmission:
    """Worker-side execution admission: limits, custom policies, and backpressure.

    One instance is built per worker run from the worker's limits, per-channel
    overrides, and the channels/actors materialized by the router. The runner
    delegates to it for everything about *what* to reserve and *how* backpressure
    maps onto broker channels, keeping the runner as pure orchestration.
    """

    def __init__(
        self,
        *,
        server: ServerT,
        limits: MessageLimits,
        limit_policies: Sequence[LimitPolicyT] = (),
        channel_limits: dict[str, MessageLimits] | None = None,
        channel_limit_policies: Mapping[str, Sequence[LimitPolicyT]] | None = None,
        actor_limits_propagation: ActorLimitsPropagation = "sum",
    ) -> None:
        self._limits = limits
        self._limit_policies = tuple(dedupe_by_identity(limit_policies))
        self._channel_limits = channel_limits or {}
        self._channel_limit_policies = {
            channel: tuple(dedupe_by_identity(policies))
            for channel, policies in (channel_limit_policies or {}).items()
        }
        self._actor_limits_propagation = actor_limits_propagation
        # Resolve strict oversized payload actions by actor scope before reservation. Gates
        # then use exclusive run-alone semantics, avoiding identity-based ordering
        # from deciding which strict action is raised.
        self._execution_gates = _FixedCapacityGates()
        # Keyed by object identity: custom policies may be unhashable or define
        # value equality. Their owning scopes keep them alive for the run.
        self._limit_policy_channels: dict[int, set[str]] = {}
        self._channels_to_actors: dict[str, list[ActorData]] = {}
        self._backpressure = _BackpressureController(
            server=server,
            default=limits.backpressure or BackpressurePolicy(),
            overrides={
                channel: policy.backpressure
                for channel, policy in self._channel_limits.items()
                if policy.backpressure is not None
            },
        )

    @property
    def limits(self) -> MessageLimits:
        return self._limits

    @property
    def server_subscriber(self) -> SubscriberT | None:
        return self._backpressure.subscriber

    @server_subscriber.setter
    def server_subscriber(self, subscriber: SubscriberT | None) -> None:
        self._backpressure.subscriber = subscriber

    def prepare(
        self,
        channels_to_actors: dict[str, list[ActorData]],
    ) -> dict[str, MessageLimits]:
        """Adopt the run's channels and return the per-channel intake limits."""
        self._channels_to_actors = channels_to_actors
        self._map_limit_policy_channels()
        self._warn_unbounded_inflight()
        merged: dict[str, MessageLimits] = {}
        for channel, actors in channels_to_actors.items():
            explicit = self._channel_limits.get(channel)
            propagated_messages, propagated_bytes = self._propagated_actor_limits(actors)
            if explicit is None and propagated_messages is None and propagated_bytes is None:
                continue
            merged[channel] = MessageLimits(
                max_messages=_tightest(
                    self._limits.max_messages,
                    explicit.max_messages if explicit is not None else None,
                    propagated_messages,
                ),
                max_payload_bytes=_tightest(
                    explicit.max_payload_bytes if explicit is not None else None,
                    propagated_bytes,
                ),
                oversized_payload_action=self._intake_action(explicit, propagated_bytes),
                backpressure=explicit.backpressure if explicit is not None else None,
            )
        return merged

    def resolved_backpressure(self, channel: str) -> BackpressurePolicy:
        return self._backpressure.resolve(channel)

    def validate_backpressure(self) -> None:  # noqa: C901, PLR0912
        """Validate strict policies against the active subscriber."""
        worker_resources: set[BackpressureResource | None] = set()
        if self._limits.max_messages is not None:
            worker_resources.add("messages")
        if self._limits.max_payload_bytes is not None:
            worker_resources.add("payload_bytes")
        if self._limit_policies:
            worker_resources.add(None)
        resources = {channel: set(worker_resources) for channel in self._channels_to_actors}
        for channel, limits in self._channel_limits.items():
            if channel not in resources:
                continue
            if limits.max_messages is not None:
                resources[channel].add("messages")
            if limits.max_payload_bytes is not None:
                resources[channel].add("payload_bytes")
        for channel, policies in self._channel_limit_policies.items():
            if channel in resources and policies:
                resources[channel].add(None)
        for channel, actors in self._channels_to_actors.items():
            for actor in actors:
                for actor_limits in actor.limits:
                    if actor_limits.max_messages is not None:
                        resources[channel].add("messages")
                    if actor_limits.max_payload_bytes is not None:
                        resources[channel].add("payload_bytes")
                if actor.limit_policies:
                    resources[channel].add(None)
        for channel_resources in resources.values():
            if not channel_resources:
                channel_resources.add(None)
        self._backpressure.require_capability(resources)

    def reservations(
        self,
        actor: ActorData,
        message: ReceivedMessageT,
    ) -> list[ReservationRequestT]:
        """Reservations a message must hold before executing through ``actor``."""
        reservations: list[ReservationRequestT] = []
        for actor_limits in reversed(actor.limits):
            if (
                actor_limits.max_payload_bytes is None
                or len(message.payload) <= actor_limits.max_payload_bytes
            ):
                continue
            action = resolve_oversized_payload_action(
                actor_limits.oversized_payload_action,
                message,
            )
            if action != "run_alone":
                raise OversizedReservationError(action)
            break
        policies = [
            *self._limit_policies,
            *actor.limit_policies,
            *self._channel_limit_policies.get(message.channel, ()),
        ]
        for policy in dedupe_by_identity(policies):
            reservations.append((policy, partial(policy.reserve, message, actor)))
        # Only actor-owned caps become execution gates; worker- and channel-level
        # caps gate intake instead (see IntakeGate).
        for limits in dedupe_by_identity(actor.limits):
            message_gate, payload_gate = self._execution_gates.for_policy(limits)
            if message_gate is not None:
                reservations.append(
                    (
                        message_gate,
                        partial(
                            message_gate.reserve,
                            1,
                            resume_at=self._resume_at(message.channel, message_gate),
                        ),
                    ),
                )
            if payload_gate is not None:
                reservations.append(
                    (
                        payload_gate,
                        partial(
                            payload_gate.reserve,
                            len(message.payload),
                            resume_at=self._resume_at(message.channel, payload_gate),
                        ),
                    ),
                )
        return reservations

    def _resume_at(self, channel: str, gate: _FixedCapacityGate) -> float:
        channels = self._limit_policy_channels.get(id(gate), {channel})
        # A shared gate resumes at the most conservative boundary of the
        # channels that actually pause; native and buffering policies return 1.
        return min(self._backpressure.resume_at(c, gate.resource) for c in channels)

    async def on_wait(self, channel: str, policy: ReservablePolicyT) -> list[str]:
        """Pause intake for every channel a waiting limit policy feeds.

        Returns the affected channels so the caller can resume them once the
        reservation stops waiting.
        """
        channels = self._limit_policy_channels.get(id(policy), {channel})
        affected = [c for c in channels if self._backpressure.tracks_waiters(c)]
        resource = policy.resource if isinstance(policy, _FixedCapacityGate) else None
        return await self._backpressure.on_wait(affected, resource)

    async def on_ready(self, channels: Iterable[str]) -> None:
        await self._backpressure.on_ready(channels)

    def _map_limit_policy_channels(self) -> None:
        all_channels = set(self._channels_to_actors)
        for policy in self._limit_policies:
            self._limit_policy_channels.setdefault(id(policy), set()).update(all_channels)
        for channel, policies in self._channel_limit_policies.items():
            for policy in policies:
                self._limit_policy_channels.setdefault(id(policy), set()).add(channel)
        for channel, actors in self._channels_to_actors.items():
            for actor in actors:
                for policy in actor.limit_policies:
                    self._limit_policy_channels.setdefault(id(policy), set()).add(channel)
                # Only actor-owned caps have execution gates (see `reservations`).
                for limits in dedupe_by_identity(actor.limits):
                    for gate in self._execution_gates.for_policy(limits):
                        if gate is not None:
                            self._limit_policy_channels.setdefault(id(gate), set()).add(channel)

    def _propagated_actor_limits(
        self,
        actors: Iterable[ActorData],
    ) -> tuple[int | None, int | None]:
        """Aggregate finite actor caps without constraining uncapped actors."""
        if self._actor_limits_propagation == "off":
            return None, None
        actor_list = tuple(actors)
        unique = {limits for actor in actor_list for limits in actor.limits}
        messages = (
            None
            if any(
                all(limits.max_messages is None for limits in actor.limits) for actor in actor_list
            )
            else sum(limits.max_messages for limits in unique if limits.max_messages is not None)
            or None
        )
        payload_bytes = (
            None
            if any(
                all(limits.max_payload_bytes is None for limits in actor.limits)
                for actor in actor_list
            )
            else sum(
                limits.max_payload_bytes
                for limits in unique
                if limits.max_payload_bytes is not None
            )
            or None
        )
        return messages, payload_bytes

    def _intake_action(
        self,
        explicit: MessageLimits | None,
        propagated_payload_bytes: int | None,
    ) -> OversizedPayloadPolicy:
        if explicit is not None and explicit.max_payload_bytes is not None:
            return explicit.oversized_payload_action
        if propagated_payload_bytes is not None:
            if self._limits.max_payload_bytes is not None:
                return self._limits.oversized_payload_action
            return "run_alone"
        if explicit is not None and explicit.max_messages is not None:
            return explicit.oversized_payload_action
        if self._limits.max_messages is not None:
            return self._limits.oversized_payload_action
        return "run_alone"

    def _channel_has_intake_limit(self, channel: str) -> bool:
        if self._limits.max_messages is not None or self._limits.max_payload_bytes is not None:
            return True
        explicit = self._channel_limits.get(channel)
        propagated = self._propagated_actor_limits(self._channels_to_actors.get(channel, ()))
        return (
            explicit is not None
            and (explicit.max_messages is not None or explicit.max_payload_bytes is not None)
        ) or any(value is not None for value in propagated)

    def _warn_unbounded_inflight(self) -> None:
        policies = [
            *self._limit_policies,
            *(policy for values in self._channel_limit_policies.values() for policy in values),
            *(
                policy
                for actors in self._channels_to_actors.values()
                for actor in actors
                for policy in actor.limit_policies
            ),
        ]
        for policy in dedupe_by_identity(policies):
            channels = self._limit_policy_channels[id(policy)]
            if all(
                self._channel_has_intake_limit(channel)
                or self._backpressure.can_control_without_native(channel)
                for channel in channels
            ):
                continue
            logger.warning(
                "runner.limits.unbounded_inflight",
                extra={"limit_policy": policy},
            )
