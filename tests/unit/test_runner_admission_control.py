from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from repid import ActorLimits, BackpressurePolicy, MessageLimits, Router
from repid._runner import _confirm_error, _Runner
from repid.admission import IntakeGate, _FixedCapacityGate
from repid.connections.in_memory import InMemoryServer
from repid.data import ActorExecutionContext
from repid.health_check_server import HealthCheckServer, HealthCheckStatus
from repid.limits import OversizedReservationError, ReservationLeaseT
from repid.serializer import default_serializer


def _context(server: Any) -> ActorExecutionContext:
    return ActorExecutionContext(
        server=server,
        publish=AsyncMock(),
        default_serializer=default_serializer,
    )


def _actor(*, limits: ActorLimits) -> Any:
    router = Router()

    @router.actor(limits=limits)
    async def actor() -> None:
        pass

    return router.actors[0]


def _message(payload: bytes = b"{}") -> Mock:
    message = Mock()
    message.channel = "jobs"
    message.payload = payload
    message.message_id = "id"
    message.is_acted_on = False
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    message.reject = AsyncMock()
    return message


async def test_confirm_error_ignores_already_confirmed_message() -> None:
    actor = _actor(limits=ActorLimits())
    message = _message()
    message.is_acted_on = True

    await _confirm_error(message, actor, RuntimeError())

    message.nack.assert_not_awaited()


async def test_runner_marks_unhealthy_and_rejects_reservation_backend_failure() -> None:
    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
        ) -> ReservationLeaseT:
            raise RuntimeError("backend unavailable")

    server = InMemoryServer()
    health = HealthCheckServer()
    runner = _Runner(
        actor_context=_context(server),
        limits=MessageLimits(),
        limit_policies=(LimitPolicy(),),
        health_check_server=health,
    )
    message = _message()

    await runner._run_routed(_actor(limits=ActorLimits()), message)

    assert health.health_status == HealthCheckStatus.UNHEALTHY
    assert runner.stop_consume_event.is_set()
    message.reject.assert_awaited_once()


async def test_ack_first_confirms_before_policy_reservation() -> None:
    class LimitPolicy:
        async def reserve(self, message: object, actor: object, on_wait: object) -> Any:  # noqa: ARG002
            raise RuntimeError("backend unavailable")

    router = Router()

    @router.actor(confirmation_mode="ack_first", limit_policies=(LimitPolicy(),))
    async def actor() -> None:
        raise AssertionError("reservation failure must prevent execution")

    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())
    message = _message()

    async def ack() -> None:
        message.is_acted_on = True

    message.ack.side_effect = ack
    await runner._run_routed(router.actors[0], message)

    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()


async def test_runner_marks_unhealthy_when_reservation_release_fails() -> None:
    class Lease:
        async def release(self) -> None:
            raise RuntimeError("release unavailable")

    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],  # noqa: ARG002
        ) -> ReservationLeaseT:
            return Lease()

    server = InMemoryServer()
    health = HealthCheckServer()
    runner = _Runner(
        actor_context=_context(server),
        limits=MessageLimits(),
        limit_policies=(LimitPolicy(),),
        health_check_server=health,
    )
    message = _message()
    actor = _actor(limits=ActorLimits())

    await runner._run_routed(actor, message)
    runner.cancel_event.set()

    assert health.health_status == HealthCheckStatus.UNHEALTHY
    assert runner.stop_consume_event.is_set()


def test_runner_actor_run_alone_builds_an_exclusive_reservation() -> None:
    actor = _actor(limits=ActorLimits(max_payload_bytes=1))
    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())

    assert runner._admission.reservations(actor, _message())


def test_runner_prefers_innermost_oversized_payload_action_over_reservation_order() -> None:
    outer = ActorLimits(max_payload_bytes=1, oversized_payload_action="nack")
    inner = ActorLimits(max_payload_bytes=1, oversized_payload_action="reject")
    actor = replace(_actor(limits=inner), limits=(outer, inner))
    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())

    with pytest.raises(OversizedReservationError, match="'reject'"):
        runner._admission.reservations(actor, _message())


async def test_runner_execution_wait_handles_wait_missing_and_global_backpressure() -> None:
    class Subscriber:
        def __init__(self) -> None:
            self.paused: list[str] = []
            self.resumed: list[str] = []
            self.pause = AsyncMock()
            self.resume = AsyncMock()

        async def pause_channel(self, channel: str) -> None:
            self.paused.append(channel)

        async def resume_channel(self, channel: str) -> None:
            self.resumed.append(channel)

    policy = object()
    server = Mock()
    server.capabilities = {
        "supports_channel_pause": False,
        "supports_lightweight_pause": True,
        "supports_keep_alive": False,
        "supports_native_reply": False,
    }
    runner = _Runner(
        actor_context=_context(server),
        limits=MessageLimits(),
        channel_limits={"waiting": MessageLimits(backpressure=BackpressurePolicy(strategies=()))},
    )
    subscriber = Subscriber()
    runner._admission.server_subscriber = subscriber  # type: ignore[assignment]

    waiting_waits = await runner._admission.on_wait("waiting", cast(Any, policy))
    assert waiting_waits == []

    runner._admission.server_subscriber = None
    missing_waits = await runner._admission.on_wait("missing", cast(Any, policy))
    assert await runner._admission.on_wait("missing", cast(Any, policy)) == []
    await runner._admission.on_ready(missing_waits)

    runner._admission.server_subscriber = subscriber  # type: ignore[assignment]
    global_waits = await runner._admission.on_wait("global", cast(Any, policy))
    global_waits += await runner._admission.on_wait("global", cast(Any, policy))
    await runner._admission.on_ready(global_waits)
    await runner._admission.on_ready(global_waits)

    subscriber.pause.assert_awaited_once()
    subscriber.resume.assert_awaited_once()

    orphaned_waits = await runner._admission.on_wait("global", cast(Any, policy))
    runner._admission.server_subscriber = None
    await runner._admission.on_ready(orphaned_waits)


async def test_runner_logs_resume_failure_without_losing_reservations() -> None:
    class Lease:
        async def release(self) -> None:
            pass

    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Callable[[], Awaitable[None]],
        ) -> ReservationLeaseT:
            await on_wait()
            return Lease()

    class Subscriber:
        async def pause_channel(self, channel: str) -> None:
            pass

        async def resume_channel(self, channel: str) -> None:  # noqa: ARG002
            raise RuntimeError("resume failed")

    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(),
        limit_policies=(LimitPolicy(),),
    )
    runner._admission.server_subscriber = Subscriber()  # type: ignore[assignment]

    await runner._run_routed(_actor(limits=ActorLimits()), _message())
    runner.cancel_event.set()


async def test_runner_rolls_back_backpressure_when_pause_is_cancelled() -> None:
    pause_started = asyncio.Event()

    class Subscriber:
        async def pause_channel(self, channel: str) -> None:  # noqa: ARG002
            pause_started.set()
            await asyncio.Future()

        async def resume_channel(self, channel: str) -> None:
            raise RuntimeError(channel)

    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())
    runner._admission.server_subscriber = Subscriber()  # type: ignore[assignment]
    waiting = asyncio.create_task(runner._admission.on_wait("jobs", cast(Any, object())))
    await pause_started.wait()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert runner._admission._backpressure._waiters == {}


async def test_runner_execution_wait_resumes_channel_after_waiters_cancel() -> None:
    class Subscriber:
        def __init__(self) -> None:
            self.paused: list[str] = []
            self.resumed: list[str] = []

        async def pause_channel(self, channel: str) -> None:
            self.paused.append(channel)

        async def resume_channel(self, channel: str) -> None:
            self.resumed.append(channel)

    server = InMemoryServer()
    runner = _Runner(actor_context=_context(server), limits=MessageLimits())
    subscriber = Subscriber()
    runner._admission.server_subscriber = subscriber  # type: ignore[assignment]
    waits = await runner._admission.on_wait("jobs", cast(Any, object()))
    waits += await runner._admission.on_wait("jobs", cast(Any, object()))
    await runner._admission.on_ready(waits)
    await runner._admission.on_ready(waits)

    assert subscriber.paused == ["jobs"]
    assert subscriber.resumed == ["jobs"]


def _actors_for_channel(*actor_limits: Any) -> list[Any]:
    router = Router()
    for index, limits in enumerate(actor_limits):

        @router.actor(name=f"actor{index}", limits=limits)
        async def actor() -> None:
            pass

    return [replace(actor, channel_address="jobs") for actor in router.actors]


def test_runner_propagates_sum_of_actor_limits_to_channel_intake() -> None:
    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())
    actors = _actors_for_channel(ActorLimits(max_messages=3), ActorLimits(max_messages=4))

    merged = runner._admission.prepare({"jobs": actors})

    assert merged["jobs"].max_messages == 7
    assert merged["jobs"].max_payload_bytes is None
    assert merged["jobs"].oversized_payload_action == "run_alone"


def test_runner_propagation_off_keeps_intake_unchanged() -> None:
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(),
        actor_limits_propagation="off",
    )
    actors = _actors_for_channel(ActorLimits(max_messages=3))

    assert runner._admission.prepare({"jobs": actors}) == {}


def test_runner_propagation_requires_every_actor_to_have_a_finite_field_cap() -> None:
    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())
    actors = _actors_for_channel(
        ActorLimits(max_messages=None),
        ActorLimits(max_messages=None, max_payload_bytes=None),
    )

    assert runner._admission.prepare({"jobs": actors}) == {}

    mixed = _actors_for_channel(ActorLimits(max_messages=1), None)
    assert runner._admission.prepare({"jobs": mixed}) == {}

    independently_capped = _actors_for_channel(
        ActorLimits(max_messages=2),
        ActorLimits(max_messages=3, max_payload_bytes=9),
    )
    merged = runner._admission.prepare({"jobs": independently_capped})
    assert merged["jobs"].max_messages == 5
    assert merged["jobs"].max_payload_bytes is None


def test_runner_propagation_min_merges_with_explicit_channel_limits() -> None:
    explicit = MessageLimits(max_messages=5, oversized_payload_action="nack")
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(),
        channel_limits={"jobs": explicit},
    )
    actors = _actors_for_channel(ActorLimits(max_messages=7))

    merged = runner._admission.prepare({"jobs": actors})

    assert merged["jobs"].max_messages == 5
    assert merged["jobs"].oversized_payload_action == "nack"


def test_runner_shared_actor_limits_count_once_per_channel() -> None:
    shared = ActorLimits(max_messages=3)
    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())
    actors = _actors_for_channel(shared, shared)

    merged = runner._admission.prepare({"jobs": actors})

    assert merged["jobs"].max_messages == 3


def test_runner_propagated_payload_uses_worker_oversized_policy() -> None:
    worker = MessageLimits(max_payload_bytes=10, oversized_payload_action="nack")
    runner = _Runner(actor_context=_context(InMemoryServer()), limits=worker)
    actors = _actors_for_channel(ActorLimits(max_payload_bytes=5))

    merged = runner._admission.prepare({"jobs": actors})

    assert merged["jobs"].oversized_payload_action == "nack"


def test_runner_worker_cap_shrinks_propagated_channel_intake() -> None:
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(max_messages=4),
    )
    actors = _actors_for_channel(ActorLimits(max_messages=7))

    merged = runner._admission.prepare({"jobs": actors})

    assert merged["jobs"].max_messages == 4


async def test_runner_preserves_worker_payload_action_with_channel_count_limit() -> None:
    worker_limits = MessageLimits(max_payload_bytes=1, oversized_payload_action="nack")
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=worker_limits,
        channel_limits={"jobs": MessageLimits(max_messages=5)},
    )

    merged = runner._admission.prepare({"jobs": []})
    gate = IntakeGate(worker_limits, merged)

    with pytest.raises(OversizedReservationError) as exc_info:
        await gate.reserve(_message(b"xx"))

    assert exc_info.value.action == "nack"


async def test_runner_keeps_worker_and_channel_payload_actions_separate() -> None:
    worker_limits = MessageLimits(max_payload_bytes=10, oversized_payload_action="reject")
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=worker_limits,
        channel_limits={
            "jobs": MessageLimits(max_payload_bytes=100, oversized_payload_action="nack"),
        },
    )

    merged = runner._admission.prepare({"jobs": []})
    gate = IntakeGate(worker_limits, merged)

    with pytest.raises(OversizedReservationError) as worker_exc:
        await gate.reserve(_message(b"x" * 50))
    with pytest.raises(OversizedReservationError) as channel_exc:
        await gate.reserve(_message(b"x" * 101))

    assert worker_exc.value.action == "reject"
    assert channel_exc.value.action == "nack"


def test_runner_resolved_backpressure_prefers_channel_value_over_worker_default() -> None:
    worker_policy = BackpressurePolicy(strategies=())
    channel_policy = BackpressurePolicy(on_unavailable="error")
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(backpressure=worker_policy),
        channel_limits={"jobs": MessageLimits(backpressure=channel_policy)},
    )

    assert runner._admission.resolved_backpressure("jobs") is channel_policy
    assert runner._admission.resolved_backpressure("other") is worker_policy


async def test_runner_auto_uses_native_flow_before_pause() -> None:
    class Subscriber:
        pause = AsyncMock()
        resume = AsyncMock()

        def supports_native_flow_control(self, channel: str, resource: str) -> bool:
            return channel == "jobs" and resource == "messages"

        async def pause_channel(self, channel: str) -> None:  # noqa: ARG002
            raise AssertionError("native flow must win")

        async def resume_channel(self, channel: str) -> None:  # noqa: ARG002
            raise AssertionError("native flow must win")

    runner = _Runner(actor_context=_context(InMemoryServer()), limits=MessageLimits())
    subscriber = Subscriber()
    runner._admission.server_subscriber = subscriber  # type: ignore[assignment]

    waits = await runner._admission.on_wait("jobs", _FixedCapacityGate(1))
    await runner._admission.on_ready(waits)

    assert waits == []
    subscriber.pause.assert_not_awaited()
    subscriber.resume.assert_not_awaited()


async def test_runner_native_wait_does_not_delay_unrelated_global_resume() -> None:
    class Subscriber:
        def __init__(self) -> None:
            self.pause = AsyncMock()
            self.resume = AsyncMock()

        def supports_native_flow_control(self, channel: str, resource: str) -> bool:
            return channel == "native" and resource == "messages"

    server = Mock()
    server.capabilities = {
        "supports_channel_pause": False,
        "supports_lightweight_pause": True,
        "supports_keep_alive": False,
        "supports_native_reply": False,
    }
    runner = _Runner(actor_context=_context(server), limits=MessageLimits())
    subscriber = Subscriber()
    runner._admission.server_subscriber = subscriber  # type: ignore[assignment]

    global_wait = await runner._admission.on_wait("global", cast(Any, object()))
    native_wait = await runner._admission.on_wait("native", _FixedCapacityGate(1))
    await runner._admission.on_ready(global_wait)

    assert native_wait == []
    subscriber.pause.assert_awaited_once()
    subscriber.resume.assert_awaited_once()


def test_runner_uses_resume_boundary_only_for_pause_strategies() -> None:
    actor = _actor(limits=ActorLimits(max_messages=4))

    def configured_resume_at(policy: BackpressurePolicy, subscriber: object) -> float:
        runner = _Runner(
            actor_context=_context(InMemoryServer()),
            limits=MessageLimits(backpressure=policy),
        )
        runner._admission.prepare({"jobs": [actor]})
        runner._admission.server_subscriber = cast(Any, subscriber)
        reservation = runner._admission.reservations(actor, _message())[0][1]
        return cast(float, cast(Any, reservation).keywords["resume_at"])

    class NativeSubscriber:
        def supports_native_flow_control(self, channel: str, resource: str) -> bool:  # noqa: ARG002
            return True

    assert configured_resume_at(BackpressurePolicy(), object()) == 0.75
    assert configured_resume_at(BackpressurePolicy(resume_at=0.5), object()) == 0.5
    assert configured_resume_at(BackpressurePolicy(resume_at=0.5), NativeSubscriber()) == 1.0
    assert configured_resume_at(BackpressurePolicy(strategies=(), resume_at=0.5), object()) == 1.0


async def test_runner_custom_policy_respects_strategy_order() -> None:
    class Subscriber:
        def __init__(self) -> None:
            self.pause = AsyncMock()
            self.resume = AsyncMock()
            self.pause_channel = AsyncMock()
            self.resume_channel = AsyncMock()

    policy = BackpressurePolicy(
        strategies=("worker_pause", "channel_pause"),
        on_unavailable="error",
    )
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(backpressure=policy),
    )
    subscriber = Subscriber()
    runner._admission.server_subscriber = subscriber  # type: ignore[assignment]

    waits = await runner._admission.on_wait("jobs", _FixedCapacityGate(1))
    await runner._admission.on_ready(waits)

    subscriber.pause.assert_awaited_once()
    subscriber.resume.assert_awaited_once()
    subscriber.pause_channel.assert_not_awaited()


async def test_runner_buffer_backpressure_never_pauses_even_when_pausable() -> None:
    class Subscriber:
        def __init__(self) -> None:
            self.paused: list[str] = []
            self.pause = AsyncMock()

        async def pause_channel(self, channel: str) -> None:
            self.paused.append(channel)

    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(backpressure=BackpressurePolicy(strategies=())),
    )
    subscriber = Subscriber()
    runner._admission.server_subscriber = subscriber  # type: ignore[assignment]

    waits = await runner._admission.on_wait("jobs", cast(Any, object()))

    assert waits == []
    assert subscriber.paused == []
    subscriber.pause.assert_not_awaited()


def test_runner_native_backpressure_is_strict() -> None:
    actor = _actor(limits=ActorLimits(max_messages=1))
    runner = _Runner(
        actor_context=_context(InMemoryServer()),
        limits=MessageLimits(
            max_payload_bytes=2,
            backpressure=BackpressurePolicy(strategies=("native",), on_unavailable="error"),
        ),
        channel_limits={
            "jobs": MessageLimits(max_payload_bytes=1),
            "unused": MessageLimits(max_payload_bytes=1),
        },
    )
    runner._admission.prepare({"jobs": [actor]})
    runner._admission.server_subscriber = cast(Any, object())

    with pytest.raises(ValueError, match="no available strategy"):
        runner._admission.validate_backpressure()

    native = Mock()
    native.supports_native_flow_control = Mock(return_value=True)
    runner._admission.server_subscriber = native
    runner._admission.validate_backpressure()
    native.supports_native_flow_control.assert_any_call("jobs", "messages")
    native.supports_native_flow_control.assert_any_call("jobs", "payload_bytes")


async def test_runner_pause_backpressure_requires_pausable_broker() -> None:
    class NoPauseServer(InMemoryServer):
        @property
        def capabilities(self) -> Any:
            return {
                "supports_native_reply": True,
                "supports_lightweight_pause": False,
                "supports_channel_pause": False,
                "supports_keep_alive": False,
            }

    server = NoPauseServer()
    runner = _Runner(
        actor_context=_context(server),
        limits=MessageLimits(backpressure=BackpressurePolicy(on_unavailable="error")),
    )

    async with server.connection():
        with pytest.raises(ValueError, match="no available strategy"):
            await runner.run(
                {"jobs": [_actor(limits=ActorLimits(max_messages=1))]},
                graceful_termination_timeout=0,
            )


async def test_runner_resubscribe_backpressure_is_explicit() -> None:
    class ResubscribeServer(InMemoryServer):
        @property
        def capabilities(self) -> Any:
            return {
                "supports_native_reply": True,
                "supports_lightweight_pause": False,
                "supports_channel_pause": False,
                "supports_keep_alive": False,
            }

    subscriber = Mock(pause=AsyncMock(), resume=AsyncMock())
    runner = _Runner(
        actor_context=_context(ResubscribeServer()),
        limits=MessageLimits(
            backpressure=BackpressurePolicy(
                strategies=("native", "channel_pause", "worker_pause", "resubscribe"),
                on_unavailable="error",
            ),
        ),
    )
    runner._admission.prepare({"jobs": []})
    runner._admission.server_subscriber = subscriber

    waits = await runner._admission.on_wait("jobs", cast(Any, object()))
    await runner._admission.on_ready(waits)

    subscriber.pause.assert_awaited_once()
    subscriber.resume.assert_awaited_once()

    auto_subscriber = Mock(pause=AsyncMock(), resume=AsyncMock())
    auto = _Runner(actor_context=_context(ResubscribeServer()), limits=MessageLimits())
    auto._admission.server_subscriber = auto_subscriber
    auto_waits = await auto._admission.on_wait("jobs", cast(Any, object()))
    await auto._admission.on_ready(auto_waits)
    auto_subscriber.pause.assert_not_awaited()


async def test_runner_pause_backpressure_accepts_lightweight_pause_alone() -> None:
    class LightweightPauseServer(InMemoryServer):
        @property
        def capabilities(self) -> Any:
            return {
                "supports_native_reply": True,
                "supports_lightweight_pause": True,
                "supports_channel_pause": False,
                "supports_keep_alive": False,
            }

    runner = _Runner(
        actor_context=_context(LightweightPauseServer()),
        limits=MessageLimits(backpressure=BackpressurePolicy(on_unavailable="error")),
    )
    runner._admission.prepare({"jobs": []})

    runner._admission.server_subscriber = Mock(pause=AsyncMock(), resume=AsyncMock())
    waits = await runner._admission.on_wait("jobs", cast(Any, object()))
    await runner._admission.on_ready(waits)

    assert waits == ["jobs"]
    runner._admission.server_subscriber.pause.assert_awaited_once()


class _ListHandler(logging.Handler):
    def __init__(self, records: list[logging.LogRecord]) -> None:
        super().__init__()
        self._records = records

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append(record)


@contextmanager
def _capture_repid_logs() -> Iterator[list[logging.LogRecord]]:
    records: list[logging.LogRecord] = []
    handler = _ListHandler(records)
    repid_logger = logging.getLogger("repid")
    repid_logger.addHandler(handler)
    try:
        yield records
    finally:
        repid_logger.removeHandler(handler)


def test_runner_warns_only_when_limit_policies_cannot_be_bounded() -> None:
    class LimitPolicy:
        async def reserve(
            self,
            message: object,  # noqa: ARG002
            actor: object,  # noqa: ARG002
            on_wait: Any,  # noqa: ARG002
        ) -> Any:
            raise AssertionError

    class NoPauseServer(InMemoryServer):
        @property
        def capabilities(self) -> Any:
            return {
                "supports_native_reply": True,
                "supports_lightweight_pause": False,
                "supports_channel_pause": False,
                "supports_keep_alive": False,
            }

    def warnings_for(
        server: Any,
        limits: MessageLimits,
        limit_policies: tuple[LimitPolicy, ...],
    ) -> int:
        runner = _Runner(
            actor_context=_context(server),
            limits=limits,
            limit_policies=limit_policies,
        )
        runner._admission.prepare({"jobs": []})
        with _capture_repid_logs() as records:
            runner._admission._warn_unbounded_inflight()
        return [r.message for r in records].count("runner.limits.unbounded_inflight")

    policy = LimitPolicy()
    uncapped = MessageLimits()
    buffer_uncapped = MessageLimits(backpressure=BackpressurePolicy(strategies=()))
    pause_uncapped = MessageLimits(backpressure=BackpressurePolicy(on_unavailable="error"))
    buffer_capped = MessageLimits(max_messages=1, backpressure=BackpressurePolicy(strategies=()))

    assert warnings_for(InMemoryServer(), buffer_uncapped, (policy,)) == 1
    assert warnings_for(NoPauseServer(), uncapped, (policy,)) == 1
    assert warnings_for(InMemoryServer(), uncapped, (policy,)) == 0
    assert warnings_for(InMemoryServer(), pause_uncapped, (policy,)) == 0
    assert warnings_for(InMemoryServer(), buffer_capped, (policy,)) == 0
