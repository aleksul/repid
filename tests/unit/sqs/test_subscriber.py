from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from repid.connections._subscriber import SubscriberDispatcher
from repid.connections.abc import ReceivedMessageT
from repid.connections.sqs.message_broker import SqsServer
from repid.connections.sqs.subscriber import SqsSubscriber


def _server(client: Any) -> MagicMock:
    return MagicMock(
        spec=SqsServer,
        _client=client,
        _get_queue_url=AsyncMock(return_value="queue"),
        _batch_size=10,
        _receive_wait_time_seconds=0,
        _visibility_timeout=30,
    )


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", new=MagicMock())
async def test_consume_channel_breaks_intake_loop_when_shutdown_mid_dispatch() -> None:
    """Covers subscriber.py line 203: break out of the dispatch loop mid-batch."""
    client = AsyncMock()
    server = _server(client)
    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, dispatcher=SubscriberDispatcher())

    subscriber._active = True
    subscriber._paused_event.set()

    gate = asyncio.Event()
    reserve_calls: list[Any] = []

    orig_reserve = SubscriberDispatcher.reserve

    async def gated_reserve(self: Any, message: Any) -> Any:
        reserve_calls.append(message)
        await gate.wait()
        return await orig_reserve(self, message)

    async def receive_message(**_: object) -> dict[str, list[dict[str, str]]]:
        return {
            "Messages": [
                {"MessageId": "1", "ReceiptHandle": "h1", "Body": "aGk="},
                {"MessageId": "2", "ReceiptHandle": "h2", "Body": "aGk="},
            ],
        }

    client.receive_message = AsyncMock(side_effect=receive_message)

    with patch.object(SubscriberDispatcher, "reserve", gated_reserve):
        consume_task = asyncio.create_task(subscriber._consume_channel("test"))
        for _ in range(20):
            await asyncio.sleep(0)
            if len(reserve_calls) == 1:
                break

        assert len(reserve_calls) == 1

        # Shutting down while the first intake reservation is still pending must
        # break out of the dispatch loop without reserving intake for the
        # remaining fetched messages.
        subscriber._shutdown_event.set()
        gate.set()
        for _ in range(20):
            await asyncio.sleep(0)
            if consume_task.done():
                break
        await consume_task

    assert len(reserve_calls) == 1
    assert subscriber._shutdown_event.is_set()


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", new=MagicMock())
async def test_consume_channel_rejects_unprocessed_messages_when_cancelled() -> None:
    """Covers subscriber.py lines 227-228: requeue fetched messages on cancel."""
    client = AsyncMock()
    server = _server(client)
    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, dispatcher=SubscriberDispatcher())

    subscriber._active = True
    subscriber._paused_event.set()

    received = asyncio.Event()
    release = asyncio.Event()
    gate = asyncio.Event()

    async def receive_message(**_: object) -> dict[str, list[dict[str, str]]]:
        received.set()
        await release.wait()
        return {"Messages": [{"MessageId": "1", "ReceiptHandle": "h", "Body": "aGk="}]}

    client.receive_message = AsyncMock(side_effect=receive_message)

    orig_reserve = SubscriberDispatcher.reserve

    async def gated_reserve(self: Any, message: Any) -> Any:
        await gate.wait()
        return await orig_reserve(self, message)

    with patch.object(SubscriberDispatcher, "reserve", gated_reserve):
        consume_task = asyncio.create_task(subscriber._consume_channel("test"))
        await received.wait()
        release.set()
        for _ in range(20):
            await asyncio.sleep(0)

        # Cancel while intake is reserved: the fetched, unadmitted message must
        # be scheduled for rejection before the cancellation propagates.
        consume_task.cancel()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await consume_task

    assert subscriber._requeue_tasks
    requeue_tasks = tuple(subscriber._requeue_tasks)
    await asyncio.gather(*requeue_tasks)
    assert not subscriber._requeue_tasks
    client.change_message_visibility.assert_awaited_once()


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", new=MagicMock())
async def test_channel_pause_stops_one_channel_only() -> None:
    client = AsyncMock()
    server = _server(client)
    subscriber = SqsSubscriber(
        server,
        {"a": AsyncMock(), "b": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    subscriber._active = True
    subscriber._paused_event.set()

    await subscriber.pause_channel("a")
    assert not subscriber._channel_paused_events["a"].is_set()
    assert subscriber._channel_paused_events["b"].is_set()

    receive_started = asyncio.Event()

    blocked: asyncio.Future[dict[str, Any]] = asyncio.Future()

    async def receive_message(**_: object) -> dict[str, Any]:
        receive_started.set()
        return await blocked

    client.receive_message = AsyncMock(side_effect=receive_message)

    paused_task = asyncio.create_task(subscriber._consume_channel("a"))
    for _ in range(20):
        await asyncio.sleep(0)
        if paused_task.done():
            break
    # The paused channel never issues a receive.
    assert not receive_started.is_set()
    assert not paused_task.done()

    await subscriber.resume_channel("a")
    for _ in range(20):
        await asyncio.sleep(0)
        if receive_started.is_set():
            break
    assert receive_started.is_set()

    # A global pause re-arms every channel; a global resume releases them.
    await subscriber.pause()
    assert not subscriber._channel_paused_events["a"].is_set()
    assert not subscriber._channel_paused_events["b"].is_set()
    await subscriber.resume()
    assert subscriber._channel_paused_events["a"].is_set()
    assert subscriber._channel_paused_events["b"].is_set()

    paused_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await paused_task


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", new=MagicMock())
async def test_resume_resets_done_channel_pause_wait_tasks() -> None:
    """Covers subscriber.py line 314: done per-channel pause waits are dropped."""
    client = AsyncMock()
    server = _server(client)
    subscriber = SqsSubscriber(server, {"a": AsyncMock()}, dispatcher=SubscriberDispatcher())

    stale_task = subscriber._get_channel_pause_wait_task("a")
    subscriber._channel_pause_requested_events["a"].set()
    await stale_task
    assert stale_task.done()
    subscriber._channel_pause_wait_tasks["a"] = stale_task

    await subscriber.resume()

    assert subscriber._channel_pause_wait_tasks["a"] is None


def _real_server(client: AsyncMock) -> SqsServer:
    server = SqsServer(receive_wait_time_seconds=0, batch_size=10, visibility_timeout=30)
    server._client = client
    return server


async def _wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    """Await ``predicate`` becoming true, driving the loop meanwhile."""

    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout)


async def test_finish_cancels_blocked_callback_while_settle_janitor_waits() -> None:
    """The settle janitor must not hold the close lock while draining callbacks."""
    client = AsyncMock()
    server = _real_server(client)
    server._queue_url_cache["ch"] = "https://queue"
    callback_started = asyncio.Event()
    callback_release = asyncio.Event()

    async def blocked_callback(_message: ReceivedMessageT) -> None:
        callback_started.set()
        await callback_release.wait()

    calls = 0

    async def receive_message(**_: object) -> dict[str, list[dict[str, str]]]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return {
                "Messages": [
                    {"MessageId": "1", "ReceiptHandle": "h1", "Body": "aGk="},
                ],
            }
        return {}

    client.receive_message = AsyncMock(side_effect=receive_message)
    subscriber = cast(
        SqsSubscriber,
        await server.subscribe(
            channels_to_callbacks={"ch": blocked_callback},
            dispatcher=SubscriberDispatcher(),
        ),
    )

    await _wait_until(callback_started.is_set)
    # End intake on its own, with the callback still blocked, so the janitor
    # settle() starts first and waits for the callback.
    subscriber._active = False
    await _wait_until(lambda: bool(server._subscriber_cleanup_tasks))
    for _ in range(50):
        await asyncio.sleep(0)

    (callback_task,) = subscriber._admitted_tasks.tasks
    assert not callback_task.done()

    # stop/finish must stay usable: finish cancels the callback even though
    # the janitor is still draining it.
    await asyncio.wait_for(subscriber.finish(), timeout=2.0)

    assert callback_task.cancelled()
    client.change_message_visibility.assert_awaited()
    assert not subscriber._admitted_tasks.tasks
    await _wait_until(lambda: not server._subscriber_cleanup_tasks)
    assert subscriber not in server._active_subscribers


async def test_settle_cancels_pending_pause_waiters_without_channel_leak() -> None:
    """Idle global and per-channel pause waits are cancelled, awaited and cleared."""
    client = AsyncMock()
    client.receive_message = AsyncMock(return_value={})
    server = _real_server(client)
    server._queue_url_cache["a"] = "https://queue"
    server._queue_url_cache["b"] = "https://queue"
    subscriber = cast(
        SqsSubscriber,
        await server.subscribe(
            channels_to_callbacks={"a": AsyncMock(), "b": AsyncMock()},
            dispatcher=SubscriberDispatcher(),
        ),
    )

    await _wait_until(
        lambda: (
            subscriber._channel_pause_wait_tasks.get("a") is not None
            and subscriber._channel_pause_wait_tasks.get("b") is not None
        ),
    )
    pause_wait_task = subscriber._pause_wait_task
    shutdown_wait_task = subscriber._shutdown_wait_task
    channel_wait_task_a = subscriber._channel_pause_wait_tasks["a"]
    channel_wait_task_b = subscriber._channel_pause_wait_tasks["b"]
    assert channel_wait_task_a is not None
    assert channel_wait_task_b is not None
    assert pause_wait_task is not None
    assert not pause_wait_task.done()
    assert shutdown_wait_task is not None
    assert not shutdown_wait_task.done()

    # End intake on its own so the janitor settle() releases the subscriber.
    subscriber._active = False
    await _wait_until(lambda: subscriber not in server._active_subscribers)
    for _ in range(20):
        await asyncio.sleep(0)

    captured_wait_tasks = (
        channel_wait_task_a,
        channel_wait_task_b,
        pause_wait_task,
        shutdown_wait_task,
    )
    assert all(task.cancelled() for task in captured_wait_tasks)
    assert subscriber._pause_wait_task is None
    assert subscriber._shutdown_wait_task is None
    assert all(task is None for task in subscriber._channel_pause_wait_tasks.values())
    assert subscriber not in server._active_subscribers


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", new=MagicMock())
async def test_drain_and_release_awaits_wait_tasks_rearmed_mid_drain() -> None:
    """Wait tasks re-armed while idle waits drain are awaited as owned tasks."""
    client = AsyncMock()
    server = _real_server(client)
    subscriber = SqsSubscriber(server, {"a": AsyncMock()}, dispatcher=SubscriberDispatcher())

    def rearm_pause(_: asyncio.Task[Any]) -> None:
        # Simulates a channel task re-arming its pause wait while the drain is
        # still gathering the previous generation of idle waiters. The re-armed
        # waiter ends on its own shortly after, like a real event wait would.
        subscriber._pause_wait_task = asyncio.create_task(
            asyncio.sleep(0.01, result=True),
        )

    def rearm_shutdown(_: asyncio.Task[Any]) -> None:
        subscriber._shutdown_wait_task = asyncio.create_task(
            asyncio.sleep(0.01, result=True),
        )

    pause_task = asyncio.create_task(asyncio.Event().wait())
    pause_task.add_done_callback(rearm_pause)
    shutdown_task = asyncio.create_task(asyncio.Event().wait())
    shutdown_task.add_done_callback(rearm_shutdown)
    subscriber._pause_wait_task = pause_task
    subscriber._shutdown_wait_task = shutdown_task

    await asyncio.wait_for(subscriber._drain_and_release(), timeout=1)

    # The re-armed waiters were awaited as owned tasks, then cleared on release.
    assert subscriber._pause_wait_task is None
    assert subscriber._shutdown_wait_task is None


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", new=MagicMock())
async def test_pause_and_resume_channel_unknown_channel_is_noop() -> None:
    """Covers subscriber.py lines 324 and 333: unknown channels are ignored."""
    client = AsyncMock()
    server = _server(client)
    subscriber = SqsSubscriber(server, {"a": AsyncMock()}, dispatcher=SubscriberDispatcher())

    await subscriber.pause_channel("missing")
    await subscriber.resume_channel("missing")

    assert subscriber._channel_paused_events["a"].is_set()
