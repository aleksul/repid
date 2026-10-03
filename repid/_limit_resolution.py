"""Resolve local capacities and broker guarantees before starting intake."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from fractions import Fraction
from typing import TYPE_CHECKING, Literal

from repid._utils.not_set import _NotSet
from repid.limits import (
    IntakeControl,
    MessageCountIntake,
    MessageLimits,
    NativeFlow,
    NativeWindow,
    PauseStrategy,
    PayloadByteIntake,
)

if TYPE_CHECKING:
    from repid.connections.abc import CapabilitiesT
    from repid.data import ActorData, Channel
    from repid.limits import LimitPolicyT

logger = logging.getLogger("repid")


@dataclass(frozen=True)
class ResolvedIntake:
    messages: MessageCountIntake
    payload_bytes: PayloadByteIntake
    pause_strategies: tuple[PauseStrategy, ...]
    on_unavailable: Literal["buffer", "error"]


@dataclass(frozen=True)
class ResolvedChannel:
    limits: MessageLimits
    control: ResolvedIntake
    policies: tuple[LimitPolicyT, ...]


def resolve_control(
    limits: MessageLimits,
    control: IntakeControl | None,
    parent: ResolvedIntake | None = None,
) -> ResolvedIntake:
    control = control or IntakeControl()

    def child(*, byte: bool) -> MessageCountIntake:
        own = control.payload_bytes if byte else control.messages
        inherited = (parent.payload_bytes if byte else parent.messages) if parent else None
        cap = limits.max_payload_bytes if byte else limits.max_messages
        mode = own.native if own else None
        if mode is None or isinstance(mode, _NotSet):
            mode = inherited.native if inherited else "auto"
            if isinstance(mode, int):
                mode = "auto"  # Absolute worker values never become per-channel requirements.
        pause = own.pause_at if own and own.pause_at is not None else cap
        resume = (
            own.resume_at
            if own and own.resume_at is not None
            else (pause * 3 // 4 if pause else None)
        )
        if pause is None and resume is not None:
            raise ValueError("resume_at requires pause_at or a finite local cap")
        if pause is not None and cap is not None and pause > cap:
            raise ValueError("pause_at exceeds the effective local cap")
        if byte:
            delivery = own.oversized_delivery if isinstance(own, PayloadByteIntake) else None
            if delivery is None:
                delivery = parent.payload_bytes.oversized_delivery if parent else "deliver"
            return PayloadByteIntake(
                native=mode,
                pause_at=pause,
                resume_at=resume,
                oversized_delivery=delivery,
            )
        return MessageCountIntake(native=mode, pause_at=pause, resume_at=resume)

    return ResolvedIntake(
        messages=child(byte=False),
        payload_bytes=child(byte=True),  # type: ignore[arg-type]
        pause_strategies=control.pause_strategies
        if control.pause_strategies is not None
        else (parent.pause_strategies if parent else ("channel_pause", "worker_pause")),
        on_unavailable=control.on_unavailable or (parent.on_unavailable if parent else "buffer"),
    )


def propagated_capacity(actors: list[ActorData], field: str) -> int | None:
    """A safe weighted-cover bound for nominal capacity, without an optimization dependency."""
    pools: dict[int, tuple[int, set[int]]] = {}
    minima: list[int] = []
    for index, actor in enumerate(actors):
        caps = []
        for pool in actor.execution_limits:
            cap = getattr(pool, field)
            if cap is not None:
                caps.append(cap)
                pools.setdefault(id(pool), (cap, set()))[1].add(index)
        if not caps:
            return None
        minima.append(min(caps))
    if not minima:
        return None
    remaining = set(range(len(actors)))
    cover = 0
    while remaining:
        choices = [
            (Fraction(cap, len(covered & remaining)), order, cap, covered)
            for order, (cap, covered) in enumerate(pools.values())
            if covered & remaining
        ]
        _, _, cap, covered = min(choices, key=lambda item: (item[0], item[1]))
        cover += cap
        remaining -= covered
    return min(cover, sum(minima))


def resolve_channels(
    declarations: list[Channel],
    actors: dict[str, list[ActorData]],
    worker_control: ResolvedIntake,
    propagation: Literal["auto", "off"],
) -> dict[str, ResolvedChannel]:
    result: dict[str, ResolvedChannel] = {}
    for declaration in declarations:
        limits = declaration.limits or MessageLimits()
        values = {}
        for field in ("max_messages", "max_payload_bytes"):
            declared = getattr(limits, field)
            propagated = (
                propagated_capacity(actors[declaration.address], field)
                if propagation == "auto"
                else None
            )
            values[field] = (
                min(v for v in (declared, propagated) if v is not None)
                if declared is not None or propagated is not None
                else None
            )
        effective = MessageLimits(**values, on_oversized_payload=limits.on_oversized_payload)
        resolved = ResolvedChannel(
            limits=effective,
            control=resolve_control(effective, declaration.intake_control, worker_control),
            policies=declaration.limit_policies,
        )
        previous = result.get(declaration.address)
        if previous is not None and (
            previous.limits != resolved.limits
            or previous.control != resolved.control
            or tuple(map(id, previous.policies)) != tuple(map(id, resolved.policies))
        ):
            raise ValueError(
                f"Conflicting limits or intake settings for channel {declaration.address!r}",
            )
        result[declaration.address] = resolved
    return result


def resolve_native(
    limits: MessageLimits,
    control: ResolvedIntake,
    capabilities: CapabilitiesT,
    scope: Literal["worker", "channel"],
    address: str | None = None,
) -> NativeWindow:
    values: list[int | None] = []
    for dimension, config, cap in (
        ("messages", control.messages, limits.max_messages),
        ("bytes", control.payload_bytes, limits.max_payload_bytes),
    ):
        requested = (
            cap if config.native == "auto" else (None if config.native == "off" else config.native)
        )
        supported = capabilities[f"supports_{scope}_native_{dimension}"]  # type: ignore[literal-required]
        reason = None
        if not supported:
            reason = "unsupported scope or outstanding-delivery guarantee"
        if dimension == "bytes" and supported:
            delivery = capabilities[f"supports_{scope}_oversized_delivery"]  # type: ignore[literal-required]
            blocked = capabilities[f"supports_{scope}_oversized_blocking"]  # type: ignore[literal-required]
            if control.payload_bytes.oversized_delivery == "allow_blocked":
                supported = blocked
            else:
                supported = delivery
            if not supported:
                reason = "unsupported oversized-delivery mode"
        if requested is not None and not supported and config.native != "auto":
            raise ValueError(
                f"Explicit native {dimension} window is unsupported at {scope} scope ({address})",
            )
        value = requested if supported else None
        values.append(value)  # type: ignore[arg-type]
        logger.info(
            "worker.intake.resolved",
            extra={
                "scope": scope,
                "channel": address,
                "dimension": dimension,
                "local_cap": cap,
                "native_cap": value,
                "pause_at": config.pause_at,
                "resume_at": config.resume_at,
                "fallback_reason": reason if requested is not None and value is None else None,
            },
        )
    return NativeWindow(
        max_messages=values[0],
        max_payload_bytes=values[1],
        oversized_delivery=control.payload_bytes.oversized_delivery or "deliver",
        messages_automatic=control.messages.native == "auto" and values[0] is not None,
    )


def pause_strategy(control: ResolvedIntake, capabilities: CapabilitiesT) -> PauseStrategy | None:
    for strategy in control.pause_strategies:
        if (
            strategy == "resubscribe"
            or (strategy == "channel_pause" and capabilities["supports_channel_pause"])
            or (
                strategy == "worker_pause"
                and (
                    capabilities["supports_worker_pause"] or capabilities["supports_channel_pause"]
                )
            )
        ):
            return strategy
    return None


def native_flow(
    worker: MessageLimits,
    control: ResolvedIntake,
    channels: dict[str, ResolvedChannel],
    capabilities: CapabilitiesT,
) -> NativeFlow:
    return NativeFlow(
        worker=resolve_native(worker, control, capabilities, "worker"),
        channels={
            address: resolve_native(
                channel.limits,
                channel.control,
                capabilities,
                "channel",
                address,
            )
            for address, channel in channels.items()
        },
    )
