from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from types import SimpleNamespace
from typing import Any, cast

import pytest

from repid.admission import IntakeGate, _FixedCapacityGate, release_leases, reserve_limit_policies
from repid.connections._subscriber import SubscriberDispatcher
from repid.limits import (
    ActorLimits,
    BackpressurePolicy,
    MessageLimits,
    OversizedReservationError,
    ReservationLeaseT,
    validate_actor_limits_propagation,
)


class _Lease:
    def __init__(self, released: list[str], name: str) -> None:
        self._released = released
        self._name = name

    async def release(self) -> None:
        self._released.append(self._name)


class _LimitPolicy:
    def __init__(self, name: str, events: list[str], fail: bool = False) -> None:
        self.name = name
        self.events = events
        self.fail = fail

    async def reserve(
        self,
        message: Any,
        actor: Any,
        on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
    ) -> ReservationLeaseT:
        await asyncio.sleep(0)  # Cost calculation may use asynchronous I/O.
        self.events.append(f"reserve:{self.name}:{message.cost}:{actor.name}")
        if self.fail:
            raise RuntimeError(self.name)
        return _Lease(self.events, self.name)


def _message(payload: bytes = b"x", channel: str = "orders") -> Any:
    return SimpleNamespace(channel=channel, payload=payload, cost=1)


def _request(policy: _LimitPolicy) -> Any:
    return (
        policy,
        partial(policy.reserve, _message(), SimpleNamespace(name="actor")),
    )


def test_backpressure_policy_defines_the_complete_ladder() -> None:
    assert BackpressurePolicy().strategies == (
        "native",
        "channel_pause",
        "worker_pause",
    )
    assert BackpressurePolicy().resume_at == 0.75

    custom = BackpressurePolicy(
        strategies=("worker_pause", "channel_pause", "worker_pause"),
    )
    assert custom.strategies == ("worker_pause", "channel_pause")

    with pytest.raises(ValueError, match="strategies"):
        BackpressurePolicy(strategies=("unknown",))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="on_unavailable"):
        BackpressurePolicy(strategies=(), on_unavailable="drop")  # type: ignore[arg-type]
    for resume_at in (True, 1.5):
        with pytest.raises((TypeError, ValueError), match="resume_at"):
            BackpressurePolicy(resume_at=resume_at)


@pytest.mark.parametrize("limits_type", [MessageLimits, ActorLimits])
def test_limits_reject_custom_policy_buckets(limits_type: type) -> None:
    with pytest.raises(TypeError, match="buckets"):
        limits_type(buckets=(_LimitPolicy("quota", []),))


@pytest.mark.parametrize("limits_type", [MessageLimits, ActorLimits])
@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"max_messages": True}, id="boolean_message_capacity"),
        pytest.param({"max_messages": 0}, id="zero_message_capacity"),
        pytest.param({"max_payload_bytes": -1}, id="negative_payload_capacity"),
        pytest.param({"max_payload_bytes": 0}, id="zero_payload_capacity"),
        pytest.param({"oversized_payload_action": "drop"}, id="unknown_oversized_payload_action"),
    ],
)
def test_limits_rejects_invalid_configuration(limits_type: type, kwargs: dict[str, Any]) -> None:
    with pytest.raises((TypeError, ValueError)):
        limits_type(**kwargs)


async def test_reserve_limit_policies_deduplicates_and_supports_async_pricing() -> None:
    events: list[str] = []
    policy = _LimitPolicy("quota", events)

    leases = await reserve_limit_policies(
        [_request(policy), _request(policy)],
        lambda _: asyncio.sleep(0),
    )
    await leases[0].release()

    assert events == ["reserve:quota:1:actor", "quota"]


async def test_release_leases_keeps_releasing_after_errors() -> None:
    class BrokenLease:
        async def release(self) -> None:
            raise RuntimeError("release failed")

    with pytest.raises(RuntimeError, match="release failed"):
        await release_leases((BrokenLease(), BrokenLease()))


async def test_reserve_intake_applies_fixed_and_dynamic_actions() -> None:
    class Message:
        channel = "orders"
        payload = b"xx"
        is_acted_on = False

        def __init__(self) -> None:
            self.actions: list[str] = []

        async def nack(self) -> None:
            self.actions.append("nack")

        async def reject(self) -> None:
            self.actions.append("reject")

    policies: tuple[Any, ...] = (
        "nack",
        "reject",
        lambda message: "nack" if message.channel == "orders" else "reject",
    )
    for policy in policies:
        message = Message()
        dispatcher = SubscriberDispatcher(
            MessageLimits(max_payload_bytes=1, oversized_payload_action=policy),
        )

        assert await dispatcher.reserve(message) is None  # type: ignore[arg-type]
        assert message.actions == ["nack" if callable(policy) else policy]


async def test_intake_run_alone_and_async_policy_validation() -> None:
    message = _message(b"oversized")
    lease = await IntakeGate(MessageLimits(max_payload_bytes=1)).reserve(message)
    await lease.release()

    async def async_policy(message: Any) -> str:  # noqa: ARG001
        return "nack"

    gate = IntakeGate(
        MessageLimits(max_payload_bytes=1, oversized_payload_action=cast(Any, async_policy)),
    )
    with pytest.raises(TypeError, match="synchronous"):
        await gate.reserve(message)


async def test_subscriber_dispatcher_waits_for_activation() -> None:
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1), active=False)
    reservation = asyncio.create_task(dispatcher.reserve(_message()))

    await asyncio.sleep(0)
    assert not reservation.done()
    dispatcher.activate()
    lease = await reservation
    assert lease is not None
    await lease.release()


async def test_subscriber_dispatcher_reserves_and_runs_callback() -> None:
    message = _message()
    message.is_acted_on = False
    dispatcher = SubscriberDispatcher(
        MessageLimits(max_messages=1, max_payload_bytes=10),
    )
    assert dispatcher.native_message_limit() == 1
    assert dispatcher.native_payload_limit() == 10
    assert dispatcher.native_message_limit("orders") == 1
    assert dispatcher.native_payload_limit("orders") == 10
    scoped = SubscriberDispatcher(
        MessageLimits(max_messages=10),
        {"orders": MessageLimits(max_messages=2)},
    )
    assert scoped.native_message_limit() == 10
    assert scoped.native_message_limit("orders") == 2
    release_callback = asyncio.Event()

    async def run(_: Any) -> None:
        await release_callback.wait()

    async def dispatch(current: Any) -> None:
        lease = await dispatcher.reserve(current)
        if lease is not None:
            await dispatcher.run_admitted(lease, current, run)

    first = asyncio.create_task(dispatch(message))
    await asyncio.sleep(0)
    second = asyncio.create_task(dispatch(_message()))
    await asyncio.sleep(0)
    assert not first.done()
    assert not second.done()

    release_callback.set()
    await asyncio.gather(first, second)


async def test_fixed_capacity_gate_waits_and_releases_idempotently() -> None:
    async def on_wait() -> None:
        waited.set()

    gate = _FixedCapacityGate(1)
    waited = asyncio.Event()
    first = await gate.reserve(1, on_wait)
    second_task = asyncio.create_task(gate.reserve(1, on_wait))

    await waited.wait()
    assert not second_task.done()
    await first.release()
    second = await second_task
    await second.release()
    await second.release()
    with pytest.raises(TypeError, match="cost"):
        await gate.reserve(True, on_wait)

    exclusive_gate = _FixedCapacityGate(1)
    ordinary = await exclusive_gate.reserve(1, on_wait)
    oversized_task = asyncio.create_task(exclusive_gate.reserve(2, on_wait))
    await asyncio.sleep(0)
    assert not oversized_task.done()
    await ordinary.release()
    oversized_lease = await oversized_task
    blocked_task = asyncio.create_task(exclusive_gate.reserve(1, on_wait))
    await asyncio.sleep(0)
    assert not blocked_task.done()
    await oversized_lease.release()
    await (await blocked_task).release()

    oversized = _FixedCapacityGate(1, oversized_payload_action="nack")
    with pytest.raises(OversizedReservationError, match="nack"):
        await oversized.reserve(2, on_wait)


async def test_fixed_capacity_gate_waits_for_resume_boundary() -> None:
    gate = _FixedCapacityGate(4)

    async def on_wait() -> None:
        await asyncio.sleep(0)

    leases = [await gate.reserve(1, on_wait) for _ in range(4)]
    waiting = asyncio.create_task(gate.reserve(1, on_wait, resume_at=0.5))

    await asyncio.sleep(0)
    await leases[0].release()
    await asyncio.sleep(0)
    assert not waiting.done()

    await leases[1].release()
    admitted = await waiting
    await admitted.release()
    for lease in leases[2:]:
        await lease.release()


async def test_fixed_capacity_gate_gives_waiting_oversized_claim_priority() -> None:
    gate = _FixedCapacityGate(2)
    oversized_waiting = asyncio.Event()

    async def on_oversized_wait() -> None:
        oversized_waiting.set()

    async def on_ordinary_wait() -> None:
        return None

    first = await gate.reserve(1, on_ordinary_wait)
    oversized_task = asyncio.create_task(gate.reserve(3, on_oversized_wait))
    await oversized_waiting.wait()

    ordinary_task = asyncio.create_task(gate.reserve(1, on_ordinary_wait))
    await asyncio.sleep(0)
    assert not ordinary_task.done()

    await first.release()
    oversized = await oversized_task
    assert not ordinary_task.done()

    await oversized.release()
    await (await ordinary_task).release()


async def test_fixed_capacity_gate_removes_cancelled_oversized_waiter() -> None:
    gate = _FixedCapacityGate(2)
    oversized_waiting = asyncio.Event()

    async def on_oversized_wait() -> None:
        oversized_waiting.set()

    async def on_ordinary_wait() -> None:
        return None

    first = await gate.reserve(1, on_ordinary_wait)
    oversized_task = asyncio.create_task(gate.reserve(3, on_oversized_wait))
    await oversized_waiting.wait()
    oversized_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await oversized_task

    second = await gate.reserve(1, on_ordinary_wait)
    await second.release()
    await first.release()


async def test_intake_gate_holds_capacity_until_its_lease_releases() -> None:
    intake_gate = IntakeGate(MessageLimits(max_messages=1))
    first = await intake_gate.reserve(_message(b"first"))
    second_task = asyncio.create_task(intake_gate.reserve(_message(b"second")))

    await asyncio.sleep(0)
    assert not second_task.done()
    await first.release()
    await (await second_task).release()


async def test_intake_gate_applies_worker_and_channel_caps() -> None:
    intake_gate = IntakeGate(
        MessageLimits(max_messages=2),
        {"orders": MessageLimits(max_messages=1)},
    )

    assert intake_gate.native_message_limit("orders") == 1
    assert intake_gate.native_message_limit("other") == 2
    assert intake_gate.native_payload_limit("orders") is None

    first = await intake_gate.reserve(_message(b"first"))
    blocked_task = asyncio.create_task(intake_gate.reserve(_message(b"second")))
    await asyncio.sleep(0)
    assert not blocked_task.done()
    await first.release()
    await (await blocked_task).release()


async def test_intake_gate_shares_reused_limits_across_channels() -> None:
    shared = MessageLimits(max_messages=1)
    intake_gate = IntakeGate(channel_limits={"orders": shared, "reports": shared})
    first = await intake_gate.reserve(_message(channel="orders"))
    second_task = asyncio.create_task(intake_gate.reserve(_message(channel="reports")))

    await asyncio.sleep(0)
    assert not second_task.done()
    await first.release()
    await (await second_task).release()


async def test_limit_policy_reservations_use_stable_order_and_reverse_cleanup() -> None:
    events: list[str] = []
    policies = [
        _LimitPolicy("one", events),
        _LimitPolicy("two", events),
        _LimitPolicy("three", events),
    ]
    failing = max(policies, key=id)
    failing.fail = True

    with pytest.raises(RuntimeError, match=failing.name):
        await reserve_limit_policies(
            [_request(policy) for policy in reversed(policies)],
            lambda _: asyncio.sleep(0),
        )

    ordered = sorted(policies, key=id)
    acquired = [policy.name for policy in ordered if policy is not failing]
    assert [event for event in events if event.startswith("reserve:")] == [
        f"reserve:{policy.name}:1:actor" for policy in ordered
    ]
    assert events[-len(acquired) :] == list(reversed(acquired))


def test_actor_limits_propagation_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="actor_limits_propagation"):
        validate_actor_limits_propagation(cast(Any, "sometimes"))


async def test_intake_gate_prefers_channel_oversized_payload_action() -> None:
    intake_gate = IntakeGate(
        MessageLimits(max_payload_bytes=2, oversized_payload_action="nack"),
        {"orders": MessageLimits(max_payload_bytes=1, oversized_payload_action="reject")},
    )

    with pytest.raises(OversizedReservationError, match="'reject'"):
        await intake_gate.reserve(_message(b"too large"))
