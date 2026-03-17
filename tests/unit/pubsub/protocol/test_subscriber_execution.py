import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import grpc.aio
import pytest

from repid.connections.pubsub.protocol import (
    ChannelConfig,
    InsecureCredentials,
    ResilienceState,
    proto,
)
from repid.connections.pubsub.protocol import subscriber as sub_module
from repid.connections.pubsub.protocol._helpers import QueuedDelivery


def _make_subscriber(**overrides: Any) -> sub_module.PubsubSubscriber:
    """Helper to create a subscriber with sensible defaults."""
    defaults: dict[str, Any] = {
        "channel": MagicMock(spec=grpc.aio.Channel),
        "channel_configs": [],
        "credentials_provider": InsecureCredentials(),
        "resilience_state": MagicMock(spec=ResilienceState),
        "server": MagicMock(),
        "stream_ack_deadline_seconds": 10,
        "client_id": "client",
        "concurrency_limit": 10,
    }
    defaults.update(overrides)
    return sub_module.PubsubSubscriber(**defaults)


def _make_config(**overrides: Any) -> ChannelConfig:
    """Helper to create a ChannelConfig with sensible defaults."""
    defaults: dict[str, Any] = {
        "channel": "chan1",
        "subscription_path": "sub1",
        "callback": AsyncMock(),
    }
    defaults.update(overrides)
    return ChannelConfig(**defaults)


async def test_streaming_pull_loop_normal_execution() -> None:
    config = _make_config()
    subscriber = _make_subscriber(channel_configs=[config])

    async def run_once(*_args: Any) -> None:
        subscriber._shutdown_event.set()

    with patch.object(subscriber, "_run_streaming_pull", side_effect=run_once) as mock_run:
        await subscriber._streaming_pull_loop(config)

        mock_run.assert_called_once_with(config, ANY)
        assert "sub1" in subscriber._write_queues


async def test_streaming_pull_loop_retry_logic() -> None:
    config = _make_config()
    resilience_state = MagicMock(spec=ResilienceState)
    resilience_state.is_retryable.return_value = True
    resilience_state.should_retry.return_value = True
    resilience_state.calculate_delay.return_value = 0.01

    subscriber = _make_subscriber(resilience_state=resilience_state)

    call_count = 0

    async def run_mock(*_args: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise grpc.aio.AioRpcError(
                code=grpc.StatusCode.UNAVAILABLE,
                initial_metadata=MagicMock(),
                trailing_metadata=MagicMock(),
                details="error",
            )
        subscriber._shutdown_event.set()

    with patch.object(subscriber, "_run_streaming_pull", side_effect=run_mock) as mock_run:
        await subscriber._streaming_pull_loop(config)

        assert mock_run.call_count == 2
        resilience_state.record_failure.assert_called_once()


async def test_run_streaming_pull_logic() -> None:
    channel = MagicMock(spec=grpc.aio.Channel)
    stream_mock = MagicMock()
    channel.stream_stream.return_value = stream_mock

    config = _make_config()

    subscriber = _make_subscriber(
        channel=channel,
        channel_configs=[config],
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
    )

    write_queue: asyncio.Queue[proto.StreamingPullRequest] = asyncio.Queue()

    msg1 = proto.ReceivedMessage(
        message=proto.PubsubMessage(data=b"data"),
        ack_id="ack1",
        delivery_attempt=1,
    )

    continue_stream = asyncio.Event()

    async def stream_iterator() -> AsyncIterator[proto.StreamingPullResponse]:
        yield proto.StreamingPullResponse(received_messages=[msg1])
        await continue_stream.wait()
        subscriber._shutdown_event.set()

    captured_iterator: AsyncIterator[proto.StreamingPullRequest] | None = None

    def side_effect(
        iterator: AsyncIterator[proto.StreamingPullRequest],
    ) -> AsyncIterator[proto.StreamingPullResponse]:
        nonlocal captured_iterator
        captured_iterator = iterator
        return stream_iterator()

    stream_mock.side_effect = side_effect

    task = asyncio.create_task(subscriber._run_streaming_pull(config, write_queue))

    await asyncio.sleep(0.1)

    assert captured_iterator is not None
    gen = captured_iterator

    req1 = await anext(gen)
    assert req1.subscription == "sub1"

    await asyncio.sleep(0.1)

    assert not subscriber._delivery_queue.empty()
    item = subscriber._delivery_queue.get_nowait()
    assert item.message.payload == b"data"

    req_out = proto.StreamingPullRequest(ack_ids=["ack1"])
    write_queue.put_nowait(req_out)

    req2 = await anext(gen)
    assert req2.ack_ids == ["ack1"]

    continue_stream.set()
    await task

    if hasattr(gen, "aclose"):
        await gen.aclose()


async def test_dispatch_loop() -> None:
    subscriber = _make_subscriber()

    callback = AsyncMock()
    msg = MagicMock()

    delivery = QueuedDelivery(callback=callback, message=msg)
    await subscriber._delivery_queue.put(delivery)

    task = asyncio.create_task(subscriber._dispatch_loop())

    await asyncio.sleep(0.1)

    callback.assert_called_once_with(msg)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_lifecycle_control() -> None:
    subscriber = _make_subscriber()

    assert subscriber.is_active

    await subscriber.pause()
    assert not subscriber.is_active
    assert not subscriber._pause_event.is_set()

    await subscriber.resume()
    assert subscriber.is_active
    assert subscriber._pause_event.is_set()

    subscriber._task = asyncio.create_task(asyncio.sleep(0.1))

    await subscriber.close()
    assert not subscriber.is_active
    assert subscriber._is_closing
    assert subscriber._task.cancelled()


async def test_heartbeat_loop_sends_heartbeats() -> None:
    subscriber = _make_subscriber(
        heartbeat_interval=0.02,
        stream_ack_deadline_seconds=10,
    )

    queue: asyncio.Queue[proto.StreamingPullRequest] = asyncio.Queue()
    task = asyncio.create_task(subscriber._heartbeat_loop(queue))

    await asyncio.sleep(0.07)

    subscriber._shutdown_event.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert queue.qsize() >= 2
    hb = queue.get_nowait()
    assert hb.stream_ack_deadline_seconds == 10
    assert not hb.ack_ids


async def test_heartbeat_loop_immediate_shutdown() -> None:
    subscriber = _make_subscriber(heartbeat_interval=0.01)
    queue: asyncio.Queue[proto.StreamingPullRequest] = asyncio.Queue()

    subscriber._shutdown_event.set()
    await subscriber._heartbeat_loop(queue)
    assert queue.qsize() == 0


async def test_subscriber_close_logic_flushes_queues() -> None:
    subscriber = _make_subscriber()

    mock_q = MagicMock(spec=asyncio.Queue)
    mock_q.join = AsyncMock()

    subscriber._write_queues = {"sub1": mock_q}

    await subscriber.close()

    mock_q.join.assert_called_once()


async def test_process_response_directly() -> None:
    subscriber = _make_subscriber()
    config = _make_config()
    queue: asyncio.Queue[proto.StreamingPullRequest] = asyncio.Queue()

    msg = proto.PubsubMessage(data=b"test-data")
    response = proto.StreamingPullResponse(
        received_messages=[
            proto.ReceivedMessage(message=msg, ack_id="a1"),
            proto.ReceivedMessage(message=None),  # should be skipped
        ],
    )

    await subscriber._process_response(response, config, queue)

    assert subscriber._delivery_queue.qsize() == 1
    assert len(subscriber._in_flight_messages) == 1


async def test_drain_write_queues_timeout() -> None:
    subscriber = _make_subscriber()

    q: asyncio.Queue[Any] = asyncio.Queue()
    q.put_nowait("item")  # not marked done, join hangs
    subscriber._write_queues["s1"] = q

    await subscriber._drain_write_queues(timeout=0.01)
    # Should not hang


async def test_cancel_callback_tasks() -> None:
    subscriber = _make_subscriber()
    t1, t2 = MagicMock(), MagicMock()
    subscriber._callback_tasks.add(t1)
    subscriber._callback_tasks.add(t2)

    result = subscriber._cancel_callback_tasks()

    assert len(result) == 2
    t1.cancel.assert_called_once()
    t2.cancel.assert_called_once()
    assert len(subscriber._callback_tasks) == 0


async def test_cancel_callback_tasks_empty() -> None:
    subscriber = _make_subscriber()
    assert subscriber._cancel_callback_tasks() == []
