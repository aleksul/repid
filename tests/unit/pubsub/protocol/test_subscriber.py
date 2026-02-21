import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from repid.connections.pubsub.message_broker import PubsubServer
from repid.connections.pubsub.protocol._helpers import ChannelConfig, QueuedDelivery
from repid.connections.pubsub.protocol.credentials import InsecureCredentials
from repid.connections.pubsub.protocol.proto import (
    PubsubMessage,
    ReceivedMessage,
    StreamingPullRequest,
    StreamingPullResponse,
)
from repid.connections.pubsub.protocol.resilience import ResilienceState
from repid.connections.pubsub.protocol.subscriber import PubsubSubscriber


def _make_subscriber(**overrides: Any) -> PubsubSubscriber:
    """Helper to create a subscriber with sensible defaults."""
    defaults: dict[str, Any] = {
        "channel": MagicMock(spec=grpc.aio.Channel),
        "channel_configs": [],
        "credentials_provider": InsecureCredentials(),
        "resilience_state": MagicMock(spec=ResilienceState),
        "server": MagicMock(spec=PubsubServer),
        "stream_ack_deadline_seconds": 10,
        "client_id": "test-client",
        "concurrency_limit": 1,
    }
    defaults.update(overrides)
    return PubsubSubscriber(**defaults)


def _make_config(**overrides: Any) -> ChannelConfig:
    """Helper to create a ChannelConfig with sensible defaults."""
    defaults: dict[str, Any] = {
        "channel": "test-channel",
        "subscription_path": "projects/p/subscriptions/s",
        "callback": AsyncMock(),
    }
    defaults.update(overrides)
    return ChannelConfig(**defaults)


async def test_create_starts_background_tasks() -> None:
    config = _make_config()
    sub = await PubsubSubscriber.create(
        channel=MagicMock(spec=grpc.aio.Channel),
        channel_configs=[config],
        credentials_provider=InsecureCredentials(),
        resilience_state=MagicMock(spec=ResilienceState),
        server=MagicMock(spec=PubsubServer),
        stream_ack_deadline_seconds=10,
        client_id="id",
        concurrency_limit=1,
    )
    assert sub.is_active
    assert sub._task is not None
    sub._task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await sub._task


async def test_start_empty_config_sets_inactive() -> None:
    sub = _make_subscriber()
    sub._start_background_tasks()
    assert not sub.is_active
    assert sub._task is None


# --- _is_expected_stream_close ---


def test_is_expected_stream_close_true() -> None:
    error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="The StreamingPull stream closed for an expected reason and should be recreated",
    )
    assert PubsubSubscriber._is_expected_stream_close(error) is True


def test_is_expected_stream_close_wrong_code() -> None:
    error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.INTERNAL,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="The StreamingPull stream closed for an expected reason and should be recreated",
    )
    assert PubsubSubscriber._is_expected_stream_close(error) is False


def test_is_expected_stream_close_wrong_details() -> None:
    error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="Some other error",
    )
    assert PubsubSubscriber._is_expected_stream_close(error) is False


# --- _create_received_message ---


def test_create_received_message() -> None:
    server = MagicMock(spec=PubsubServer)
    sub = _make_subscriber(server=server)
    config = _make_config(channel="my-ch", subscription_path="sub/path")
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    raw_msg = PubsubMessage(data=b"hello", message_id="msg-1")
    received = ReceivedMessage(message=raw_msg, ack_id="ack-1", delivery_attempt=2)

    result = sub._create_received_message(received, config, queue)

    assert result._raw_message is raw_msg
    assert result._ack_id == "ack-1"
    assert result._delivery_attempt == 2
    assert result._subscription_path == "sub/path"
    assert result._channel_name == "my-ch"
    assert result._write_queue is queue
    assert result._server is server


# --- _process_response ---


async def test_process_response_filters_none_messages() -> None:
    sub = _make_subscriber()
    config = _make_config()
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    valid_msg = PubsubMessage(data=b"data")
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=None),
            ReceivedMessage(message=valid_msg, ack_id="a1"),
        ],
    )

    await sub._process_response(response, config, queue)

    assert sub._delivery_queue.qsize() == 1
    assert len(sub._in_flight_messages) == 1
    delivery = sub._delivery_queue.get_nowait()
    assert delivery.message.payload == b"data"


async def test_process_response_empty() -> None:
    sub = _make_subscriber()
    response = StreamingPullResponse(received_messages=[])
    await sub._process_response(response, _make_config(), asyncio.Queue())
    assert sub._delivery_queue.qsize() == 0


# --- _heartbeat_loop ---


async def test_heartbeat_loop_sends_heartbeats() -> None:
    sub = _make_subscriber(heartbeat_interval=0.02, stream_ack_deadline_seconds=15)
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    task = asyncio.create_task(sub._heartbeat_loop(queue))
    await asyncio.sleep(0.07)
    sub._shutdown_event.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert queue.qsize() >= 2
    hb = queue.get_nowait()
    assert hb.stream_ack_deadline_seconds == 15


async def test_heartbeat_loop_stops_on_shutdown() -> None:
    sub = _make_subscriber(heartbeat_interval=0.01)
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    sub._shutdown_event.set()
    await sub._heartbeat_loop(queue)
    assert queue.qsize() == 0


async def test_heartbeat_loop_breaks_when_shutdown_during_sleep() -> None:
    """Test that the loop breaks if shutdown is set during sleep."""
    sub = _make_subscriber(heartbeat_interval=0.05)
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    # Set shutdown halfway through the heartbeat interval
    async def set_shutdown_later() -> None:
        await asyncio.sleep(0.02)
        sub._shutdown_event.set()

    shutdown_task = asyncio.create_task(set_shutdown_later())
    await sub._heartbeat_loop(queue)  # should exit via break
    await shutdown_task
    assert queue.qsize() == 0  # no heartbeat sent


# --- _request_iterator ---


async def test_request_iterator_initial_request() -> None:
    sub = _make_subscriber(
        stream_ack_deadline_seconds=20,
        client_id="cid",
        concurrency_limit=5,
    )
    config = _make_config(subscription_path="sub/1")
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    it = sub._request_iterator(config, queue)
    initial = await anext(it)

    assert initial.subscription == "sub/1"
    assert initial.stream_ack_deadline_seconds == 20
    assert initial.client_id == "cid"
    assert initial.max_outstanding_messages == 5

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()


async def test_request_iterator_forwards_queue_items() -> None:
    sub = _make_subscriber()
    config = _make_config()
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    req = StreamingPullRequest(ack_ids=["ack-1"])
    queue.put_nowait(req)

    it = sub._request_iterator(config, queue)
    await anext(it)  # skip initial

    result = await anext(it)
    assert result.ack_ids == ["ack-1"]

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()


async def test_request_iterator_stops_on_shutdown() -> None:
    sub = _make_subscriber()
    config = _make_config()
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    it = sub._request_iterator(config, queue)
    await anext(it)  # initial request

    # Set shutdown so the iterator exits on next timeout
    sub._shutdown_event.set()

    items = []
    async for item in it:
        items.append(item)
    assert items == []


async def test_request_iterator_timeout_then_item() -> None:
    """Test that the iterator continues polling after a timeout."""
    sub = _make_subscriber(poll_interval=0.02)
    config = _make_config()
    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()

    it = sub._request_iterator(config, queue)
    await anext(it)  # initial request

    # Schedule an item to arrive after one timeout cycle
    async def put_later() -> None:
        await asyncio.sleep(0.05)
        queue.put_nowait(StreamingPullRequest(ack_ids=["delayed"]))

    put_task = asyncio.create_task(put_later())
    result = await anext(it)  # timeout -> continue -> gets item
    assert result.ack_ids == ["delayed"]

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()
    await put_task


# --- _execute_callback ---


async def test_execute_callback_success() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    sub._in_flight_messages.add(msg)
    callback = AsyncMock()
    delivery = QueuedDelivery(callback=callback, message=msg)

    await sub._execute_callback(delivery)

    callback.assert_called_once_with(msg)
    assert msg not in sub._in_flight_messages


async def test_execute_callback_exception_still_removes_in_flight() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    sub._in_flight_messages.add(msg)
    delivery = QueuedDelivery(
        callback=AsyncMock(side_effect=ValueError("boom")),
        message=msg,
    )

    await sub._execute_callback(delivery)
    assert msg not in sub._in_flight_messages


async def test_execute_callback_cancelled_propagates() -> None:
    sub = _make_subscriber()
    delivery = QueuedDelivery(
        callback=AsyncMock(side_effect=asyncio.CancelledError),
        message=MagicMock(),
    )
    with pytest.raises(asyncio.CancelledError):
        await sub._execute_callback(delivery)


# --- _dispatch_loop ---


async def test_dispatch_loop_dispatches_and_cancels() -> None:
    sub = _make_subscriber()
    callback = AsyncMock()
    msg = MagicMock()
    delivery = QueuedDelivery(callback=callback, message=msg)
    await sub._delivery_queue.put(delivery)

    task = asyncio.create_task(sub._dispatch_loop())
    await asyncio.sleep(0.05)

    callback.assert_called_once_with(msg)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --- Lifecycle: pause / resume / task ---


async def test_pause_resume_lifecycle() -> None:
    sub = _make_subscriber()
    assert sub.is_active

    await sub.pause()
    assert not sub.is_active
    assert not sub._pause_event.is_set()

    await sub.resume()
    assert sub.is_active
    assert sub._pause_event.is_set()


async def test_pause_noop_when_inactive() -> None:
    sub = _make_subscriber()
    sub._is_active = False
    with patch.object(sub._pause_event, "clear") as mock_clear:
        await sub.pause()
        mock_clear.assert_not_called()


async def test_resume_noop_when_shutdown() -> None:
    sub = _make_subscriber()
    sub._shutdown_event.set()
    with patch.object(sub._pause_event, "set") as mock_set:
        await sub.resume()
        mock_set.assert_not_called()


async def test_task_property_raises_when_not_started() -> None:
    sub = _make_subscriber()
    with pytest.raises(RuntimeError, match="Subscriber has not been started"):
        _ = sub.task


async def test_task_property_returns_task() -> None:
    config = _make_config()
    sub = _make_subscriber(channel_configs=[config])
    sub._start_background_tasks()
    assert sub.task is not None
    sub._task.cancel()  # type: ignore[union-attr]
    with contextlib.suppress(asyncio.CancelledError):
        await sub._task  # type: ignore[misc]


# --- _drain_write_queues ---


async def test_drain_write_queues_empty() -> None:
    sub = _make_subscriber()
    await sub._drain_write_queues()
    # No error, just returns


async def test_drain_write_queues_completes() -> None:
    sub = _make_subscriber()
    q: asyncio.Queue[Any] = asyncio.Queue()
    sub._write_queues["s1"] = q
    # Empty queue, join completes immediately
    await sub._drain_write_queues()


async def test_drain_write_queues_timeout() -> None:
    sub = _make_subscriber()

    q: asyncio.Queue[Any] = asyncio.Queue()
    q.put_nowait("item")  # item not marked done, so join will hang
    sub._write_queues["s1"] = q

    await sub._drain_write_queues(timeout=0.01)
    # Should not hang; the unfinished task gets cancelled


# --- _cancel_callback_tasks ---


def test_cancel_callback_tasks_empty() -> None:
    sub = _make_subscriber()
    result = sub._cancel_callback_tasks()
    assert result == []


def test_cancel_callback_tasks_cancels_all() -> None:
    sub = _make_subscriber()
    t1, t2 = MagicMock(), MagicMock()
    sub._callback_tasks.add(t1)
    sub._callback_tasks.add(t2)

    result = sub._cancel_callback_tasks()

    assert len(result) == 2
    t1.cancel.assert_called_once()
    t2.cancel.assert_called_once()
    assert len(sub._callback_tasks) == 0


# --- close ---


async def test_close_basic() -> None:
    sub = _make_subscriber()
    await sub.close()
    assert sub._is_closing
    assert not sub.is_active
    assert sub._shutdown_event.is_set()


async def test_close_idempotent() -> None:
    sub = _make_subscriber()
    await sub.close()
    # Second close is a no-op
    with patch.object(sub._pause_event, "clear") as mock_clear:
        await sub.close()
        mock_clear.assert_not_called()


async def test_close_cancels_main_task() -> None:
    sub = _make_subscriber()
    sub._task = asyncio.create_task(asyncio.sleep(10))
    await sub.close()
    assert sub._task.cancelled()


async def test_close_flushes_write_queues() -> None:
    sub = _make_subscriber()
    q: asyncio.Queue[Any] = asyncio.Queue()
    sub._write_queues["s1"] = q
    await sub.close()  # Empty queue, drains immediately


async def test_close_cancels_pending_callbacks() -> None:
    sub = _make_subscriber()
    t = MagicMock()
    sub._callback_tasks.add(t)

    with patch("asyncio.wait", return_value=(set(), set())):
        await sub.close()

    t.cancel.assert_called_once()
    assert len(sub._callback_tasks) == 0


# --- _streaming_pull_loop ---


async def test_streaming_pull_loop_normal_execution() -> None:
    config = _make_config(subscription_path="sub1")
    sub = _make_subscriber(channel_configs=[config])

    async def run_once(*_: Any) -> None:
        sub._shutdown_event.set()

    with patch.object(sub, "_run_streaming_pull", side_effect=run_once):
        await sub._streaming_pull_loop(config)

    assert "sub1" in sub._write_queues


async def test_streaming_pull_loop_expected_reconnect() -> None:
    config = _make_config(subscription_path="sub1")
    sub = _make_subscriber()

    rpc_error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="The StreamingPull stream closed for an expected reason and should be recreated",
    )

    call_count = 0

    async def side_effect(*_: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise rpc_error
        sub._shutdown_event.set()

    with patch.object(sub, "_run_streaming_pull", side_effect=side_effect):
        await sub._streaming_pull_loop(config)

    assert call_count == 2


async def test_streaming_pull_loop_non_retryable_error() -> None:
    config = _make_config()
    resilience = MagicMock(spec=ResilienceState)
    resilience.is_retryable.return_value = False
    sub = _make_subscriber(resilience_state=resilience)

    rpc_error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.INVALID_ARGUMENT,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="Invalid",
    )

    with (
        patch.object(sub, "_run_streaming_pull", side_effect=rpc_error),
        pytest.raises(grpc.aio.AioRpcError),
    ):
        await sub._streaming_pull_loop(config)

    resilience.record_failure.assert_called_once()


async def test_streaming_pull_loop_max_retries_exhausted() -> None:
    config = _make_config()
    resilience = MagicMock(spec=ResilienceState)
    resilience.is_retryable.return_value = True
    resilience.should_retry.return_value = False
    sub = _make_subscriber(resilience_state=resilience)

    rpc_error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="error",
    )

    with (
        patch.object(sub, "_run_streaming_pull", side_effect=rpc_error),
        pytest.raises(grpc.aio.AioRpcError),
    ):
        await sub._streaming_pull_loop(config)


async def test_streaming_pull_loop_retryable_error_with_backoff() -> None:
    config = _make_config()
    resilience = MagicMock(spec=ResilienceState)
    resilience.is_retryable.return_value = True
    resilience.should_retry.return_value = True
    resilience.calculate_delay.return_value = 0.01
    sub = _make_subscriber(resilience_state=resilience)

    rpc_error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="error",
    )

    call_count = 0

    async def side_effect(*_: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise rpc_error
        sub._shutdown_event.set()

    with patch.object(sub, "_run_streaming_pull", side_effect=side_effect):
        await sub._streaming_pull_loop(config)

    assert call_count == 2
    resilience.record_failure.assert_called_once()


async def test_streaming_pull_loop_cancelled() -> None:
    config = _make_config()
    sub = _make_subscriber()

    async def hang_forever(*_: Any) -> None:
        await asyncio.sleep(1000)

    with patch.object(sub, "_run_streaming_pull", side_effect=hang_forever):
        task = asyncio.create_task(sub._streaming_pull_loop(config))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_streaming_pull_loop_generic_exception() -> None:
    config = _make_config()
    sub = _make_subscriber(error_retry_delay=0.0)

    call_count = 0

    async def side_effect(*_: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("boom")
        sub._shutdown_event.set()

    with patch.object(sub, "_run_streaming_pull", side_effect=side_effect):
        await sub._streaming_pull_loop(config)

    assert call_count == 2


# --- _run_streaming_pull ---


async def test_run_streaming_pull_processes_messages() -> None:
    config = _make_config()
    sub = _make_subscriber(
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
    )

    msg = PubsubMessage(data=b"payload")
    response = StreamingPullResponse(
        received_messages=[ReceivedMessage(message=msg, ack_id="a1")],
    )

    async def stream_call(iterator: Any) -> AsyncIterator[StreamingPullResponse]:
        async for _ in iterator:
            yield response
            sub._shutdown_event.set()
            break

    stream_stream = cast(AsyncMock, sub._channel.stream_stream)
    stream_stream.return_value = stream_call

    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()
    await sub._run_streaming_pull(config, queue)

    assert sub._delivery_queue.qsize() == 1
    delivery = sub._delivery_queue.get_nowait()
    assert delivery.message.payload == b"payload"


async def test_run_streaming_pull_shutdown_during_response() -> None:
    sub = _make_subscriber(
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
    )

    async def stream_call(_iterator: Any) -> AsyncIterator[StreamingPullResponse]:
        yield MagicMock()
        sub._shutdown_event.set()
        yield MagicMock()  # This should be skipped

    stream_stream = cast(AsyncMock, sub._channel.stream_stream)
    stream_stream.return_value = stream_call

    await sub._run_streaming_pull(_make_config(), asyncio.Queue())


async def test_run_streaming_pull_cleans_up_heartbeat() -> None:
    config = _make_config()
    sub = _make_subscriber(
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
        heartbeat_interval=100,  # won't fire during test
    )

    async def stream_call(_iterator: Any) -> AsyncIterator[StreamingPullResponse]:
        sub._shutdown_event.set()
        return
        yield  # make it an async generator

    stream_stream = cast(AsyncMock, sub._channel.stream_stream)
    stream_stream.return_value = stream_call

    queue: asyncio.Queue[StreamingPullRequest] = asyncio.Queue()
    await sub._run_streaming_pull(config, queue)
    # Heartbeat task should have been cleaned up (no hanging tasks)


# --- _process_background ---


async def test_process_background_runs_all_tasks() -> None:
    config = _make_config()
    sub = _make_subscriber(channel_configs=[config])

    with (
        patch.object(sub, "_streaming_pull_loop", new_callable=AsyncMock) as mock_stream,
        patch.object(sub, "_dispatch_loop", new_callable=AsyncMock) as mock_dispatch,
    ):
        sub._start_background_tasks()
        assert sub._task is not None
        await sub._task

        mock_stream.assert_called_once_with(config)
        mock_dispatch.assert_called_once()
