from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import nats
import pytest
from nats.js.api import AckPolicy, ConsumerConfig

from repid import IntakeControl, MessageCountIntake, MessageLimits
from repid._limit_resolution import resolve_control, resolve_native
from repid.connections.nats.message_broker import NatsServer, NatsSubscriber
from repid.limits import NativeFlow, NativeWindow


def push_server(*, capacity: int = 1000, ack_wait: float | None = 30) -> tuple[Any, Any]:
    config = ConsumerConfig(
        deliver_subject="delivery",
        deliver_group="jobs_group",
        max_ack_pending=capacity,
        ack_wait=ack_wait,
    )
    info = Mock(config=config)
    sub = Mock(
        _sub=Mock(drain=AsyncMock()),
        unsubscribe=AsyncMock(),
        consumer_info=AsyncMock(return_value=info),
    )
    server = NatsServer("nats://localhost")
    server._js = Mock(
        find_stream_name_by_subject=AsyncMock(return_value="stream"),
        consumer_info=AsyncMock(return_value=info),
        subscribe=AsyncMock(return_value=sub),
    )
    return server, sub


def raw_message() -> Any:
    return Mock(
        data=b"payload",
        headers=None,
        metadata=None,
        in_progress=AsyncMock(),
        ack=AsyncMock(),
        nak=AsyncMock(),
        term=AsyncMock(),
    )


async def ready(subscriber: NatsSubscriber) -> None:
    await asyncio.wait_for(subscriber._ready.wait(), 2)
    if subscriber.task.done():
        await subscriber.task


@pytest.mark.parametrize("capacity", [1, 1000, -1])
async def test_existing_push_consumer_is_preserved(capacity: int) -> None:
    server, sub = push_server(capacity=capacity)
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()})
    await ready(subscriber)
    options = server._js.subscribe.await_args.kwargs
    assert options["queue"] == options["durable"] == "jobs_group"
    assert options["manual_ack"] is True
    assert 1 <= options["pending_msgs_limit"] <= 1000
    assert options["config"].max_ack_pending == capacity
    await subscriber.stop()
    sub._sub.drain.assert_awaited_once()
    sub.unsubscribe.assert_awaited_once()
    assert subscriber._configs
    await subscriber.stop()
    await subscriber.finish()
    await subscriber.finish()
    assert not subscriber._configs
    sub.unsubscribe.assert_awaited_once()


@pytest.mark.parametrize("requested", [None, 3])
async def test_new_consumer_communicates_resolved_capacity(requested: int | None) -> None:
    server, _ = push_server(capacity=requested or 1000)
    server._js.consumer_info.side_effect = nats.js.errors.NotFoundError
    flow = NativeFlow(channels={"jobs": NativeWindow(max_messages=requested)})
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()}, flow)
    await ready(subscriber)
    assert server._js.subscribe.await_args.kwargs["config"].max_ack_pending == (requested or 1000)
    await subscriber.finish()


@pytest.mark.parametrize("actual", [1, 1000, -1])
async def test_explicit_shared_window_conflict_fails_before_subscription(actual: int) -> None:
    server, _ = push_server(capacity=actual)
    subscriber = NatsSubscriber(
        server,
        {"jobs": AsyncMock()},
        NativeFlow(channels={"jobs": NativeWindow(max_messages=3)}),
    )
    with pytest.raises(ValueError, match="Explicit native messages"):
        await asyncio.wait_for(subscriber.task, 2)
    server._js.subscribe.assert_not_awaited()
    with pytest.raises(ValueError, match="Explicit native messages"):
        await subscriber.finish()


@pytest.mark.parametrize("automatic", [True, False])
async def test_existing_window_fallback_and_stricter_window(automatic: bool, caplog: Any) -> None:
    server, _ = push_server(capacity=1000 if automatic else 1)
    subscriber = NatsSubscriber(
        server,
        {"jobs": AsyncMock()},
        NativeFlow(channels={"jobs": NativeWindow(max_messages=3, messages_automatic=True)}),
    )
    with caplog.at_level("INFO"):
        await ready(subscriber)
    assert server._js.subscribe.await_args.kwargs["config"].max_ack_pending == (
        1000 if automatic else 1
    )
    assert server._js.subscribe.await_args.kwargs["pending_msgs_limit"] == (
        1000 if automatic else 1
    )
    assert subscriber.native_flow.channels["jobs"].max_messages == (None if automatic else 1)
    if automatic:
        assert any(getattr(record, "fallback_reason", None) for record in caplog.records)
    await subscriber.finish()


@pytest.mark.parametrize("failure", ["missing", "pull", "group", "info", "raced"])
async def test_push_startup_failures(failure: str) -> None:
    server, sub = push_server()
    error: type[Exception] = ValueError
    if failure == "missing":
        server._js = None
        error = ConnectionError
    elif failure == "pull":
        server._js.consumer_info.return_value.config.deliver_subject = None
    elif failure == "group":
        server._js.consumer_info.return_value.config.deliver_group = "other"
    elif failure == "info":
        server._js.consumer_info.side_effect = RuntimeError("unavailable")
        error = RuntimeError
    else:
        sub.consumer_info.return_value = Mock(config=ConsumerConfig())
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()})
    with pytest.raises(error):
        await asyncio.wait_for(subscriber.task, 2)
    with pytest.raises(error):
        await subscriber.finish()
    if failure == "raced":
        sub.unsubscribe.assert_awaited_once()


async def test_buffer_is_bounded_and_renewed_during_pause() -> None:
    server, sub = push_server(capacity=1, ack_wait=0.003)
    handed_off = asyncio.Event()
    callback = AsyncMock(side_effect=lambda _: handed_off.set())
    subscriber = NatsSubscriber(server, {"jobs": callback})
    await ready(subscriber)
    receive = server._js.subscribe.await_args.kwargs["cb"]
    await subscriber.pause()
    first, overflow = raw_message(), raw_message()
    renewed = asyncio.Event()
    first.in_progress.side_effect = renewed.set
    await receive(first)
    # Dispatcher may have taken a message before pause, but at most one remains queued.
    await receive(overflow)
    await asyncio.wait_for(renewed.wait(), 2)
    assert len(subscriber._buffers["jobs"].owned) == 1
    overflow.nak.assert_awaited_once()
    callback.assert_not_awaited()
    await subscriber.resume()
    await asyncio.wait_for(handed_off.wait(), 2)
    assert not subscriber._buffers["jobs"].owned
    first.term.assert_not_awaited()
    await subscriber.resume()
    assert server._js.subscribe.await_count == 2
    await subscriber.finish()
    assert sub.unsubscribe.await_count == 2


async def test_stop_disposes_buffer_but_keeps_handed_off_settlement_resources() -> None:
    server, _ = push_server(capacity=2)
    started = asyncio.Event()
    delivered: list[Any] = []

    async def callback(message: Any) -> None:
        delivered.append(message)
        started.set()
        await asyncio.Event().wait()

    subscriber = NatsSubscriber(server, {"jobs": callback})
    await ready(subscriber)
    receive = server._js.subscribe.await_args.kwargs["cb"]
    owned, buffered, late = raw_message(), raw_message(), raw_message()
    await receive(owned)
    await asyncio.wait_for(started.wait(), 2)
    await receive(buffered)
    await subscriber.stop()
    buffered.nak.assert_awaited_once()
    owned.term.assert_not_awaited()
    await delivered[0].keep_alive()
    await delivered[0].ack()
    owned.in_progress.assert_awaited_once()
    owned.ack.assert_awaited_once()
    await subscriber.resume()
    await receive(late)
    late.nak.assert_awaited_once()
    await subscriber.finish()


@pytest.mark.parametrize("failure", ["overflow", "renewal", "late"])
async def test_callback_and_renewal_errors_surface(failure: str) -> None:
    server, _ = push_server(capacity=1, ack_wait=0.003)
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()})
    await ready(subscriber)
    receive = server._js.subscribe.await_args.kwargs["cb"]
    await subscriber.pause()
    first = raw_message()
    if failure == "renewal":
        first.in_progress.side_effect = RuntimeError("renewal")
    await receive(first)
    if failure == "overflow":
        overflow = raw_message()
        overflow.nak.side_effect = RuntimeError("overflow")
        await receive(overflow)
    elif failure == "late":
        await subscriber.stop()
        late = raw_message()
        late.nak.side_effect = RuntimeError("late")
        await receive(late)
        with pytest.raises(RuntimeError, match="late"):
            await subscriber.finish()
        return
    with pytest.raises(RuntimeError, match=failure):
        await asyncio.wait_for(subscriber.task, 2)
    with pytest.raises(RuntimeError, match=failure):
        await subscriber.finish()


async def test_stop_attempts_all_buffer_cleanup_after_detach_failure() -> None:
    server, sub = push_server()
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()})
    await ready(subscriber)
    sub._sub.drain.side_effect = RuntimeError("drain")
    buffer = subscriber._buffers["jobs"]
    with patch.object(buffer, "dispose", new_callable=AsyncMock) as dispose:
        dispose.side_effect = RuntimeError("dispose")
        with pytest.raises(RuntimeError, match="dispose"):
            await subscriber.stop()
        dispose.assert_awaited_once()
    sub._sub.drain.side_effect = None
    await subscriber.finish()


async def test_nats_slow_consumer_is_a_worker_failure() -> None:
    server, _ = push_server()
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()})
    server._active_subscribers.add(subscriber)
    await ready(subscriber)
    await server._intake_error(RuntimeError("connection warning"))
    assert not subscriber.task.done()
    error = nats.errors.SlowConsumerError("jobs", "", 1, Mock())
    await server._intake_error(error)
    with pytest.raises(nats.errors.SlowConsumerError):
        await asyncio.wait_for(subscriber.task, 2)
    with pytest.raises(nats.errors.SlowConsumerError):
        await subscriber.finish()


def test_resolution_preserves_automatic_window_provenance() -> None:
    limits = MessageLimits(max_messages=3)
    caps = NatsServer("nats://localhost").capabilities
    for mode in ("auto", 3):
        control = resolve_control(limits, IntakeControl(messages=MessageCountIntake(native=mode)))
        resolved = resolve_native(limits, control, caps, "channel", "jobs")
        assert resolved.max_messages == 3
        assert resolved.messages_automatic == (mode == "auto")


async def test_sdk_handoff_without_renewal_interval() -> None:
    server, _ = push_server(ack_wait=None)
    submitted = asyncio.Event()
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock(side_effect=lambda _: submitted.set())})
    await ready(subscriber)
    message = raw_message()
    await server._js.subscribe.await_args.kwargs["cb"](message)
    await asyncio.wait_for(submitted.wait(), 2)
    message.in_progress.assert_not_awaited()
    await subscriber.finish()


@pytest.mark.parametrize("automatic", [False, True])
async def test_no_ack_consumer_cannot_supply_count_guarantee(automatic: bool) -> None:
    server, _ = push_server(capacity=3)
    server._js.consumer_info.return_value.config.ack_policy = AckPolicy.NONE
    subscriber = NatsSubscriber(
        server,
        {"jobs": AsyncMock()},
        NativeFlow(
            channels={
                "jobs": NativeWindow(max_messages=3, messages_automatic=automatic),
            },
        ),
    )
    if automatic:
        await ready(subscriber)
        assert subscriber.native_flow.channels["jobs"].max_messages is None
        await subscriber.finish()
    else:
        with pytest.raises(ValueError, match="ack_policy"):
            await asyncio.wait_for(subscriber.task, 2)
        server._js.subscribe.assert_not_awaited()
        with pytest.raises(ValueError, match="ack_policy"):
            await subscriber.finish()


async def test_buffer_renewal_uses_first_backoff_deadline() -> None:
    server, _ = push_server()
    server._js.consumer_info.return_value.config.backoff = [0.003, 30]
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()})
    await ready(subscriber)
    await subscriber.pause()
    message = raw_message()
    renewed = asyncio.Event()
    message.in_progress.side_effect = renewed.set
    await server._js.subscribe.await_args.kwargs["cb"](message)
    await asyncio.wait_for(renewed.wait(), 2)
    await subscriber.finish()
