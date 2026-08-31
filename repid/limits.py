"""Admission-control vocabulary: built-in limits, custom policies, and contracts.

This module holds everything users declare. Runtime machinery that enforces
these policies lives in :mod:`repid.admission`.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, TypeAlias, TypeVar, get_args

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData

_T = TypeVar("_T")


OversizedPayloadAction: TypeAlias = Literal["run_alone", "nack", "reject"]


class OversizedPayloadPolicyT(Protocol):
    def __call__(self, message: ReceivedMessageT) -> OversizedPayloadAction: ...


OversizedPayloadPolicy: TypeAlias = OversizedPayloadAction | OversizedPayloadPolicyT
_VALID_OVERSIZED_PAYLOAD_ACTIONS: tuple[str, ...] = get_args(OversizedPayloadAction)

ActorLimitsPropagation: TypeAlias = Literal["sum", "off"]
BackpressureResource: TypeAlias = Literal["messages", "payload_bytes"]
BackpressureStrategy: TypeAlias = Literal[
    "native",
    "channel_pause",
    "worker_pause",
    "resubscribe",
]
BackpressureFallback: TypeAlias = Literal["buffer", "error"]


@dataclass(frozen=True, slots=True, kw_only=True)
class BackpressurePolicy:
    """Ordered upstream controls, fallback, and pause resume boundary."""

    strategies: Sequence[BackpressureStrategy] = (
        "native",
        "channel_pause",
        "worker_pause",
    )
    on_unavailable: BackpressureFallback = "buffer"
    resume_at: float = 0.75

    def __post_init__(self) -> None:
        strategies = tuple(self.strategies)
        invalid = tuple(s for s in strategies if s not in get_args(BackpressureStrategy))
        if invalid:
            raise ValueError(f"unknown backpressure strategies: {invalid!r}.")
        if self.on_unavailable not in get_args(BackpressureFallback):
            raise ValueError("on_unavailable must be 'buffer' or 'error'.")
        if not isinstance(self.resume_at, (int, float)) or isinstance(self.resume_at, bool):
            raise TypeError("resume_at must be a number between 0 and 1.")
        if not 0 <= self.resume_at <= 1:
            raise ValueError("resume_at must be between 0 and 1.")
        object.__setattr__(self, "strategies", tuple(dict.fromkeys(strategies)))


class ReservationLeaseT(Protocol):
    """One acquired resource claim."""

    async def release(self) -> None:
        """Release this claim. Calling this more than once is harmless."""


class LimitPolicyT(Protocol):
    """An application-defined resource limit policy.

    The policy owns both pricing and reservation. It may use asynchronous I/O
    to calculate a cost or acquire capacity. If it must wait, it calls
    ``on_wait`` once before blocking. Repid resumes intake after ``reserve``
    returns, so a policy may wait for a lower usage boundary before returning
    when pause and resume operations are expensive.
    """

    async def reserve(
        self,
        message: ReceivedMessageT,
        actor: ActorData,
        on_wait: Callable[[], Awaitable[None]],
    ) -> ReservationLeaseT:
        """Reserve capacity for ``message`` and return an idempotent lease."""


def validate_cost(value: int, name: str) -> int:
    """Validate one non-negative integer cost or capacity."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be a non-negative integer.")
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")
    return value


def validate_oversized_payload_action(value: OversizedPayloadAction) -> OversizedPayloadAction:
    if value not in _VALID_OVERSIZED_PAYLOAD_ACTIONS:
        raise ValueError("oversized payload action must be 'run_alone', 'nack', or 'reject'.")
    return value


def validate_oversized_payload_policy(value: OversizedPayloadPolicy) -> OversizedPayloadPolicy:
    if callable(value):
        return value
    return validate_oversized_payload_action(value)


def resolve_oversized_payload_action(
    policy: OversizedPayloadPolicy,
    message: ReceivedMessageT,
) -> OversizedPayloadAction:
    """Resolve a fixed or message-dependent oversized payload policy."""
    action = policy(message) if callable(policy) else policy
    if inspect.isawaitable(action):
        if inspect.iscoroutine(action):
            action.close()
        raise TypeError("oversized_payload_action policies must be synchronous.")
    return validate_oversized_payload_action(action)


def validate_actor_limits_propagation(value: ActorLimitsPropagation) -> ActorLimitsPropagation:
    if value not in get_args(ActorLimitsPropagation):
        raise ValueError("actor_limits_propagation must be 'sum' or 'off'.")
    return value


def dedupe_by_identity(items: Iterable[_T]) -> Iterable[_T]:
    """Yield objects once by identity, preserving their first occurrence."""
    seen: set[int] = set()
    for item in items:
        if id(item) not in seen:
            seen.add(id(item))
            yield item


def _validate_numeric_limits(
    max_messages: int | None,
    max_payload_bytes: int | None,
    oversized_payload_action: OversizedPayloadPolicy,
) -> None:
    if max_messages is not None:
        validate_cost(max_messages, "max_messages")
        if max_messages == 0:
            raise ValueError("max_messages must be a positive integer or None.")
    if max_payload_bytes is not None:
        validate_cost(max_payload_bytes, "max_payload_bytes")
        if max_payload_bytes == 0:
            raise ValueError("max_payload_bytes must be a positive integer or None.")
    validate_oversized_payload_policy(oversized_payload_action)


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class Limits:
    """Built-in numeric limits attached to a worker, channel, router, or actor.

    ``max_messages`` and ``max_payload_bytes`` cap concurrent work.
    ``oversized_payload_action`` applies only to ``max_payload_bytes``. It may
    be a fixed action or a function that chooses an action from the message.

    Instances compare by identity, so reusing one instance shares capacity.
    Application-defined behavior is attached separately with ``limit_policies``.
    """

    max_messages: int | None = None
    max_payload_bytes: int | None = None
    oversized_payload_action: OversizedPayloadPolicy = "run_alone"

    def __post_init__(self) -> None:
        _validate_numeric_limits(
            self.max_messages,
            self.max_payload_bytes,
            self.oversized_payload_action,
        )


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class ActorLimits(Limits):
    """Execution limits attached to a router or actor."""


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class MessageLimits(Limits):
    """Intake limits attached to a worker or channel."""

    backpressure: BackpressurePolicy | None = None

    def __post_init__(self) -> None:
        Limits.__post_init__(self)
        if self.backpressure is not None and not isinstance(
            self.backpressure,
            BackpressurePolicy,
        ):
            raise TypeError("backpressure must be a BackpressurePolicy or None.")


class OversizedReservationError(Exception):
    def __init__(self, action: OversizedPayloadAction) -> None:
        self.action = action
        super().__init__(f"Oversized payload requires {action!r}.")
