"""Local capacity budgets and independent broker intake controls."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Protocol, TypeAlias

from repid._utils import NotSet
from repid._utils.not_set import _NotSet

if TYPE_CHECKING:
    from repid.connections.abc import ReceivedMessageT
    from repid.data.actor import ActorData

OversizedPayloadAction: TypeAlias = Literal["run_alone", "reject", "nack"]
OnOversizedPayloadT: TypeAlias = (
    OversizedPayloadAction | Callable[["ReceivedMessageT"], OversizedPayloadAction]
)
NativeMode: TypeAlias = Literal["auto", "off"] | int
PauseStrategy: TypeAlias = Literal["channel_pause", "worker_pause", "resubscribe"]


def _positive(value: int | None, name: str) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
        raise ValueError(f"{name} must be a positive integer or None")


@dataclass(frozen=True, kw_only=True, slots=True)
class MessageLimits:
    max_messages: int | None = None
    max_payload_bytes: int | None = None
    on_oversized_payload: OnOversizedPayloadT = "run_alone"

    def __post_init__(self) -> None:
        _positive(self.max_messages, "max_messages")
        _positive(self.max_payload_bytes, "max_payload_bytes")
        if not callable(self.on_oversized_payload) and self.on_oversized_payload not in (
            "run_alone",
            "reject",
            "nack",
        ):
            raise ValueError("Invalid on_oversized_payload action")


@dataclass(frozen=True, kw_only=True, slots=True)
class ActorLimits(MessageLimits):
    """One identity-shared execution pool within a worker."""


def _validate_intake(
    native: NativeMode | _NotSet,
    pause_at: int | None,
    resume_at: int | None,
) -> None:
    if not isinstance(native, _NotSet) and native not in ("auto", "off"):
        if native is None:
            raise ValueError("native must be auto, off, or a positive integer")
        _positive(native, "native")  # type: ignore[arg-type]
    _positive(pause_at, "pause_at")
    if resume_at is not None and (
        isinstance(resume_at, bool) or not isinstance(resume_at, int) or resume_at < 0
    ):
        raise ValueError("resume_at must be a nonnegative integer")
    if pause_at is not None and resume_at is not None and resume_at >= pause_at:
        raise ValueError("resume_at must be less than pause_at")


@dataclass(frozen=True, kw_only=True, slots=True)
class MessageCountIntake:
    native: NativeMode | _NotSet = NotSet
    pause_at: int | None = None
    resume_at: int | None = None

    def __post_init__(self) -> None:
        _validate_intake(self.native, self.pause_at, self.resume_at)


@dataclass(frozen=True, kw_only=True, slots=True)
class PayloadByteIntake(MessageCountIntake):
    oversized_delivery: Literal["deliver", "allow_blocked"] | None = None

    def __post_init__(self) -> None:
        _validate_intake(self.native, self.pause_at, self.resume_at)
        if self.oversized_delivery not in (None, "deliver", "allow_blocked"):
            raise ValueError("Invalid oversized_delivery mode")


@dataclass(frozen=True, kw_only=True, slots=True)
class IntakeControl:
    messages: MessageCountIntake | None = None
    payload_bytes: PayloadByteIntake | None = None
    pause_strategies: tuple[PauseStrategy, ...] | None = None
    on_unavailable: Literal["buffer", "error"] | None = None

    def __post_init__(self) -> None:
        if self.messages is not None and type(self.messages) is not MessageCountIntake:
            raise TypeError("messages must be MessageCountIntake")
        if self.payload_bytes is not None and not isinstance(self.payload_bytes, PayloadByteIntake):
            raise TypeError("payload_bytes must be PayloadByteIntake")
        if self.pause_strategies is not None:
            object.__setattr__(self, "pause_strategies", tuple(self.pause_strategies))
            if any(
                s not in ("channel_pause", "worker_pause", "resubscribe")
                for s in self.pause_strategies
            ):
                raise ValueError("Invalid pause strategy")
            if len(set(self.pause_strategies)) != len(self.pause_strategies):
                raise ValueError("Duplicate pause strategy")
        if self.on_unavailable not in (None, "buffer", "error"):
            raise ValueError("Invalid on_unavailable action")


class LimitPolicyT(Protocol):
    def reserve(
        self,
        message: ReceivedMessageT,
        actor: ActorData,
        on_capacity_exhausted: Callable[[], Awaitable[None]],
    ) -> AbstractAsyncContextManager[None]: ...


@dataclass(frozen=True, kw_only=True, slots=True)
class NativeWindow:
    max_messages: int | None = None
    max_payload_bytes: int | None = None
    oversized_delivery: Literal["deliver", "allow_blocked"] = "deliver"
    # Preserve provenance for transport settings discovered on existing consumers.
    messages_automatic: bool = False

    def __post_init__(self) -> None:
        _positive(self.max_messages, "native max_messages")
        _positive(self.max_payload_bytes, "native max_payload_bytes")
        if self.oversized_delivery not in ("deliver", "allow_blocked"):
            raise ValueError("Invalid native oversized_delivery mode")


@dataclass(frozen=True, kw_only=True, slots=True)
class NativeFlow:
    worker: NativeWindow = NativeWindow()
    channels: Mapping[str, NativeWindow] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "channels", MappingProxyType(dict(self.channels)))


UNLIMITED_NATIVE_FLOW = NativeFlow()
