from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from repid import ActorLimits, IntakeControl, MessageCountIntake, MessageLimits, PayloadByteIntake
from repid._limit_resolution import propagated_capacity, resolve_control, resolve_native
from repid.connections._buffer import SubmissionBuffer
from repid.connections.abc import broker_capabilities, validate_native_flow
from repid.data import ActorData
from repid.limits import NativeFlow, NativeWindow


@pytest.mark.parametrize(
    ("factory", "kwargs"),
    [
        (MessageLimits, {"on_oversized_payload": "drop"}),
        (MessageCountIntake, {"native": None}),
        (PayloadByteIntake, {"oversized_delivery": "unknown"}),
        (IntakeControl, {"payload_bytes": MessageCountIntake()}),
        (IntakeControl, {"pause_strategies": ["native"]}),
        (IntakeControl, {"pause_strategies": ["worker_pause", "worker_pause"]}),
        (IntakeControl, {"on_unavailable": "ignore"}),
        (NativeWindow, {"oversized_delivery": "unknown"}),
    ],
)
def test_invalid_configuration_values(factory: Any, kwargs: dict[str, Any]) -> None:
    with pytest.raises((ValueError, TypeError)):
        factory(**kwargs)


@pytest.mark.parametrize(
    ("limits", "control"),
    [
        (MessageLimits(), IntakeControl(messages=MessageCountIntake(resume_at=1))),
        (MessageLimits(max_messages=2), IntakeControl(messages=MessageCountIntake(pause_at=3))),
    ],
)
def test_resolved_thresholds_must_fit_local_capacity(
    limits: MessageLimits,
    control: IntakeControl,
) -> None:
    with pytest.raises(ValueError, match=r"requires|exceeds"):
        resolve_control(limits, control)


def test_independent_weighted_cover_and_unbounded_paths() -> None:
    shared = ActorLimits(max_messages=3, max_payload_bytes=100)
    private = ActorLimits(max_messages=2, max_payload_bytes=80)
    actors = cast(
        list[ActorData],
        [Mock(execution_limits=(shared, private)), Mock(execution_limits=(shared,))],
    )
    assert propagated_capacity(actors, "max_messages") == 3
    assert propagated_capacity(actors, "max_payload_bytes") == 100
    assert propagated_capacity([], "max_messages") is None
    assert propagated_capacity([*actors, Mock(execution_limits=())], "max_messages") is None


@pytest.mark.parametrize("mode", ["deliver", "allow_blocked"])
def test_unsupported_native_byte_modes_fail_explicit_and_fall_back_auto(mode: Any) -> None:
    caps = broker_capabilities(native=True)
    caps["supports_worker_oversized_delivery"] = False
    caps["supports_worker_oversized_blocking"] = False
    limits = MessageLimits(max_payload_bytes=100)
    control = resolve_control(
        limits,
        IntakeControl(payload_bytes=PayloadByteIntake(oversized_delivery=mode)),
    )
    assert resolve_native(limits, control, caps, "worker").max_payload_bytes is None
    control = resolve_control(
        limits,
        IntakeControl(payload_bytes=PayloadByteIntake(native=100, oversized_delivery=mode)),
    )
    with pytest.raises(ValueError, match="Explicit native bytes"):
        resolve_native(limits, control, caps, "worker")
    flow = NativeFlow(worker=NativeWindow(max_payload_bytes=100, oversized_delivery=mode))
    with pytest.raises(ValueError, match="Unsupported native-byte oversized"):
        validate_native_flow(flow, caps)


async def test_adapter_buffer_renews_waiting_deliveries_until_handoff() -> None:
    renewed, submitted = asyncio.Event(), asyncio.Event()
    message = Mock(
        is_acted_on=False,
        keep_alive_interval=0.001,
        keep_alive=AsyncMock(side_effect=renewed.set),
        reject=AsyncMock(),
    )
    buffer = SubmissionBuffer()
    buffer.track((message,))
    await asyncio.wait_for(renewed.wait(), 2)

    async def submit(delivery: Any) -> None:
        assert delivery is message
        assert not buffer.owned
        submitted.set()

    await buffer.submit(message, submit)
    renewals = message.keep_alive.await_count
    await buffer.dispose()
    assert submitted.is_set()
    assert message.keep_alive.await_count == renewals
    message.reject.assert_not_awaited()


async def test_buffer_disposal_attempts_all_messages_and_propagates_failure() -> None:
    bad = Mock(
        is_acted_on=False,
        keep_alive_interval=None,
        reject=AsyncMock(side_effect=RuntimeError("settlement")),
    )
    good = Mock(is_acted_on=False, keep_alive_interval=None, reject=AsyncMock())
    buffer = SubmissionBuffer()
    buffer.track((bad, good))
    with pytest.raises(RuntimeError, match="settlement"):
        await buffer.dispose()
    good.reject.assert_awaited_once()
    await buffer.dispose()
