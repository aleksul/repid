from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import pytest

from repid import ActorLimits, BackpressurePolicy, MessageLimits, Repid, Router
from repid._runner import _Runner
from repid.admission import ExecutionAdmission, IntakeGate
from repid.connections import SubscriberDispatcher
from repid.connections.in_memory import InMemoryServer
from repid.connections.sqs.message import SqsReceivedMessage
from repid.connections.sqs.subscriber import SqsSubscriber
from repid.data import ActorExecutionContext, MessageData
from repid.serializer import default_serializer


async def test_dispatcher_renews_pre_admission_delivery_and_stops_on_handoff() -> None:
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    first = Mock(channel="jobs", payload=b"first", keep_alive_interval=None, is_acted_on=False)
    first_lease = await dispatcher.reserve(first)
    assert first_lease is not None
    second = Mock(channel="jobs", payload=b"second", keep_alive_interval=0.001, is_acted_on=False)
    second.keep_alive = AsyncMock()

    pending = asyncio.create_task(dispatcher.reserve(second))
    await asyncio.sleep(0.02)
    assert second.keep_alive.await_count > 0

    await first_lease.release()
    second_lease = await asyncio.wait_for(pending, timeout=1)
    assert second_lease is not None
    renewals_at_handoff = second.keep_alive.await_count
    await asyncio.sleep(0.01)
    assert second.keep_alive.await_count == renewals_at_handoff
    await second_lease.release()


@pytest.mark.parametrize("interval", [pytest.param(0, id="zero"), pytest.param(-1, id="negative")])
async def test_dispatcher_skips_non_positive_pre_admission_keepalive(interval: float) -> None:
    dispatcher = SubscriberDispatcher()
    message = Mock(
        channel="jobs",
        payload=b"message",
        keep_alive_interval=interval,
        is_acted_on=False,
    )
    message.keep_alive = AsyncMock()

    dispatcher.start_keep_alive(message)
    await asyncio.sleep(0)

    message.keep_alive.assert_not_called()
    assert id(message) not in dispatcher._pre_admission_keepalives
    assert not dispatcher._keepalive_tasks


async def test_dispatcher_cancellation_while_stopping_keepalive_releases_lease() -> None:
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    stopping = asyncio.Event()

    async def stop_keep_alive(message: object) -> None:  # noqa: ARG001
        stopping.set()
        await asyncio.Future()

    dispatcher.stop_keep_alive = stop_keep_alive  # type: ignore[method-assign]
    reserving = asyncio.create_task(
        dispatcher.reserve(Mock(channel="jobs", payload=b"message", keep_alive_interval=None)),
    )
    await stopping.wait()
    reserving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reserving

    dispatcher.stop_keep_alive = SubscriberDispatcher.stop_keep_alive.__get__(dispatcher)  # type: ignore[method-assign]
    next_lease = await asyncio.wait_for(
        dispatcher.reserve(Mock(channel="jobs", payload=b"next", keep_alive_interval=None)),
        timeout=1,
    )
    assert next_lease is not None
    await next_lease.release()


async def test_cancelled_worker_stops_actor_and_releases_policy() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("memory", server, is_default=True)
    started = asyncio.Event()
    finished = asyncio.Event()
    released = asyncio.Event()

    class Lease:
        async def release(self) -> None:
            released.set()

    policy = Mock(reserve=AsyncMock(return_value=Lease()))
    router = Router(limit_policies=(policy,))

    @router.actor
    async def actor() -> None:
        started.set()
        try:
            await asyncio.Future()
        finally:
            finished.set()

    app.include_router(router)
    async with server.connection():
        await server.publish(
            channel="default",
            message=MessageData(payload=b"{}", headers={"topic": "actor"}),
        )
        worker = asyncio.create_task(
            app.run_worker(graceful_shutdown_time=0, register_signals=[]),
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        try:
            assert finished.is_set()
            assert released.is_set()
            assert not server.queues["default"].queue.empty()
        finally:
            for subscriber in tuple(server._subscribers):
                await subscriber.stop()


async def test_zero_message_limit_exits_without_delivery() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("memory", server, is_default=True)
    router = Router()

    @router.actor
    async def actor() -> None:
        raise AssertionError("zero messages means no actor executions")

    app.include_router(router)
    async with server.connection():
        try:
            result = await asyncio.wait_for(
                app.run_worker(messages_limit=0, register_signals=[]),
                timeout=0.1,
            )
            assert result.processed == 0
        finally:
            for subscriber in tuple(server._subscribers):
                await subscriber.stop()


async def test_unrouted_message_can_be_nacked_on_first_delivery() -> None:
    server = InMemoryServer()
    runner = _Runner(
        actor_context=ActorExecutionContext(
            server=server,
            publish=AsyncMock(),
            default_serializer=default_serializer,
        ),
        limits=MessageLimits(),
        max_unrouted_retries=1,
    )
    message = Mock(message_id="poison", channel="jobs", nack=AsyncMock())
    await runner._message_handler([], message)
    message.nack.assert_awaited_once()


@pytest.mark.parametrize(
    "strategy",
    [pytest.param("channel_pause", id="channel"), pytest.param("worker_pause", id="worker")],
)
async def test_backpressure_serializes_resume_before_new_pause(strategy: Any) -> None:
    resume_started = asyncio.Event()
    allow_resume = asyncio.Event()
    events: list[str] = []

    async def pause(*args: object) -> None:  # noqa: ARG001
        events.append("pause")

    async def resume(*args: object) -> None:  # noqa: ARG001
        resume_started.set()
        await allow_resume.wait()
        events.append("resume")

    subscriber = Mock(
        pause=AsyncMock(side_effect=pause),
        resume=AsyncMock(side_effect=resume),
        pause_channel=AsyncMock(side_effect=pause),
        resume_channel=AsyncMock(side_effect=resume),
    )
    admission = ExecutionAdmission(
        server=InMemoryServer(),
        limits=MessageLimits(backpressure=BackpressurePolicy(strategies=(strategy,))),
    )
    admission.server_subscriber = subscriber
    first = await admission.on_wait("jobs", cast(Any, object()))
    resuming = asyncio.create_task(admission.on_ready(first))
    await resume_started.wait()
    waiting = asyncio.create_task(admission.on_wait("jobs", cast(Any, object())))
    await asyncio.sleep(0)
    allow_resume.set()
    await resuming
    second = await waiting
    assert events == ["pause", "resume", "pause"]
    await admission.on_ready(second)


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"limits": ActorLimits(max_messages=1)}, id="wrong_limit_scope"),
        pytest.param({"limits": None}, id="none_is_not_omission"),
        pytest.param({"tasks_limit": 1.5}, id="fractional_alias"),
        pytest.param({"tasks_limit": None}, id="none_alias"),
    ],
)
async def test_worker_rejects_invalid_limits_instead_of_silently_using_defaults(
    kwargs: dict[str, Any],
) -> None:
    app = Repid()
    app.servers.register_server("memory", InMemoryServer(), is_default=True)
    with pytest.raises(TypeError):
        await app.run_worker(register_signals=[], **kwargs)


async def test_sqs_stop_cancels_poll_and_finish_cancels_callback() -> None:
    polling = asyncio.Event()
    poll_cancelled = asyncio.Event()
    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()
    calls = 0

    async def receive(**kwargs: object) -> dict[str, Any]:  # noqa: ARG001
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"Messages": [{"MessageId": "id", "ReceiptHandle": "receipt", "Body": "x"}]}
        polling.set()
        try:
            await asyncio.Future()
        finally:
            poll_cancelled.set()
        return {}

    async def callback(message: object) -> None:  # noqa: ARG001
        callback_started.set()
        try:
            await asyncio.Future()
        finally:
            callback_cancelled.set()

    server = Mock(
        _client=Mock(
            receive_message=AsyncMock(side_effect=receive),
            change_message_visibility=AsyncMock(),
        ),
        _get_queue_url=AsyncMock(return_value="queue"),
        _batch_size=1,
        _receive_wait_time_seconds=1,
        _visibility_timeout=30,
        _active_subscribers=set(),
    )
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    subscriber = SqsSubscriber(server, {"jobs": callback}, dispatcher=dispatcher)
    await asyncio.wait_for(polling.wait(), timeout=1)
    await asyncio.wait_for(callback_started.wait(), timeout=1)

    await subscriber.stop()
    await asyncio.wait_for(poll_cancelled.wait(), timeout=1)
    # stop() only cancels intake; the in-flight callback keeps running.
    assert not callback_cancelled.is_set()

    await subscriber.finish()
    await asyncio.wait_for(callback_cancelled.wait(), timeout=1)

    lease = await asyncio.wait_for(
        dispatcher.reserve(Mock(channel="jobs", payload=b"x", keep_alive_interval=None)),
        timeout=0.1,
    )
    assert lease is not None
    await lease.release()


async def test_sqs_close_drains_unprocessed_requeues_only_when_requested() -> None:
    requeue_started = asyncio.Event()
    allow_requeue = asyncio.Event()

    async def change_visibility(**kwargs: object) -> None:  # noqa: ARG001
        requeue_started.set()
        await allow_requeue.wait()

    server = Mock(
        _client=Mock(change_message_visibility=AsyncMock(side_effect=change_visibility)),
        _visibility_timeout=30,
        _active_subscribers=set(),
    )
    subscriber = SqsSubscriber(server, {}, dispatcher=SubscriberDispatcher())

    messages = [
        SqsReceivedMessage(
            server,
            "jobs",
            "queue",
            {"MessageId": "id", "ReceiptHandle": "receipt", "Body": "x"},
            30,
        ),
    ]

    subscriber._reject_unprocessed(messages)
    await requeue_started.wait()
    await subscriber.stop()
    draining_finish = asyncio.create_task(subscriber.finish())
    await asyncio.sleep(0)
    assert not draining_finish.done()
    allow_requeue.set()
    await draining_finish

    requeue_started = asyncio.Event()
    allow_requeue = asyncio.Event()
    subscriber = SqsSubscriber(server, {}, dispatcher=SubscriberDispatcher())

    messages = [
        SqsReceivedMessage(
            server,
            "jobs",
            "queue",
            {"MessageId": "second", "ReceiptHandle": "receipt", "Body": "x"},
            30,
        ),
    ]
    subscriber._reject_unprocessed(messages)
    await requeue_started.wait()
    await asyncio.wait_for(subscriber.stop(), timeout=1)
    assert server._client.change_message_visibility.await_count == 2
    allow_requeue.set()
    while subscriber._requeue_tasks:
        await asyncio.sleep(0)


async def test_cancellation_during_graceful_drain_still_closes_subscriber() -> None:
    server = InMemoryServer()
    actor_started = asyncio.Event()
    actor_finished = asyncio.Event()
    router = Router()

    @router.actor
    async def actor() -> None:
        actor_started.set()
        try:
            await asyncio.Future()
        finally:
            actor_finished.set()

    runner = _Runner(
        actor_context=ActorExecutionContext(
            server=server,
            publish=AsyncMock(),
            default_serializer=default_serializer,
        ),
        limits=MessageLimits(),
    )
    async with server.connection():
        await server.publish(
            channel="default",
            message=MessageData(payload=b"{}", headers={"topic": "actor"}),
        )
        running = asyncio.create_task(
            runner.run(router._actors_per_channel_address, graceful_termination_timeout=0.05),
        )
        await asyncio.wait_for(actor_started.wait(), timeout=1)
        subscriber = next(iter(server._subscribers))
        stopped = asyncio.Event()
        stop_intake_original = subscriber.stop

        async def stop_intake() -> None:
            await stop_intake_original()
            stopped.set()

        with patch.object(subscriber, "stop", side_effect=stop_intake):
            runner.stop_consume_event.set()
            await asyncio.wait_for(stopped.wait(), timeout=1)
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
        try:
            assert actor_finished.is_set()
            assert not server._subscribers
        finally:
            for subscriber in tuple(server._subscribers):
                await subscriber.stop()
                await subscriber.finish()


async def test_intake_gate_native_limit_independence_without_worker_limits() -> None:
    empty_gate = IntakeGate()
    assert empty_gate.native_limit_is_independent("jobs", ("jobs",), "messages") is False

    gated = IntakeGate(MessageLimits(max_messages=1))
    assert gated.native_limit_is_independent("jobs", ("jobs",), "messages") is True
    assert gated.native_limit_is_independent("jobs", ("jobs",), "payload_bytes") is False


async def test_intake_gate_native_limit_uniformity_without_worker_limits() -> None:
    empty_gate = IntakeGate()
    assert empty_gate.native_limit_is_uniform(("jobs",), "messages") is False
    assert empty_gate.native_limit_is_uniform(("jobs",), "payload_bytes") is False

    gated = IntakeGate(MessageLimits(max_messages=1, max_payload_bytes=64))
    assert gated.native_limit_is_uniform(("jobs",), "messages") is True
    assert gated.native_limit_is_uniform(("jobs",), "payload_bytes") is True


async def test_execution_admission_dispatcher_defaults_to_none() -> None:
    runner = _Runner(
        actor_context=ActorExecutionContext(
            server=InMemoryServer(),
            publish=AsyncMock(),
            default_serializer=default_serializer,
        ),
        limits=MessageLimits(max_messages=1),
    )

    assert runner._admission.dispatcher is None
