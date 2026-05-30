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

    raw_msg = PubsubMessage(data=b"hello", message_id="msg-1")
    received = ReceivedMessage(message=raw_msg, ack_id="ack-1", delivery_attempt=2)

    result = sub._create_received_message(received, config)

    assert result._raw_message is raw_msg
    assert result._ack_id == "ack-1"
    assert result._delivery_attempt == 2
    assert result._subscription_path == "sub/path"
    assert result._channel_name == "my-ch"
    assert result._server is server


# --- _process_response ---


async def test_process_response_filters_none_messages() -> None:
    sub = _make_subscriber()
    config = _make_config()

    valid_msg = PubsubMessage(data=b"data")
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=None),
            ReceivedMessage(message=valid_msg, ack_id="a1"),
        ],
    )

    await sub._process_response(response, config)

    assert sub._delivery_queue.qsize() == 1
    assert len(sub._in_flight_messages) == 1
    delivery = sub._delivery_queue.get_nowait()
    assert delivery.message.payload == b"data"


async def test_process_response_empty() -> None:
    sub = _make_subscriber()
    response = StreamingPullResponse(received_messages=[])
    await sub._process_response(response, _make_config())
    assert sub._delivery_queue.qsize() == 0


# --- _request_iterator ---


async def test_request_iterator_initial_request() -> None:
    sub = _make_subscriber(
        stream_ack_deadline_seconds=20,
        client_id="cid",
        concurrency_limit=5,
    )
    config = _make_config(subscription_path="sub/1")

    it = sub._request_iterator(config)
    initial = await anext(it)

    assert initial.subscription == "sub/1"
    assert initial.stream_ack_deadline_seconds == 20
    assert initial.client_id == "cid"
    assert initial.max_outstanding_messages == 5

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()


async def test_request_iterator_stops_on_shutdown() -> None:
    sub = _make_subscriber(heartbeat_interval=0.01)
    config = _make_config()

    it = sub._request_iterator(config)
    await anext(it)  # initial request

    # Set shutdown so the iterator exits on next timeout
    sub._shutdown_event.set()

    items = []
    async for item in it:
        items.append(item)
    assert items == []


async def test_request_iterator_stays_open_until_shutdown() -> None:
    sub = _make_subscriber(heartbeat_interval=0.1)
    config = _make_config()

    it = sub._request_iterator(config)
    await anext(it)  # initial request

    async def get_next_item() -> StreamingPullRequest:
        return await anext(it)

    next_item = asyncio.create_task(get_next_item())
    await asyncio.sleep(0.05)
    assert not next_item.done()

    sub._shutdown_event.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(next_item, timeout=1.0)

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()


async def test_request_iterator_sends_heartbeats() -> None:
    sub = _make_subscriber(heartbeat_interval=0.01, stream_ack_deadline_seconds=15)
    config = _make_config()

    it = sub._request_iterator(config)
    await anext(it)  # initial request

    heartbeat = await anext(it)
    assert heartbeat.stream_ack_deadline_seconds == 15
    assert not heartbeat.ack_ids
    assert not heartbeat.modify_deadline_ack_ids

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()


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


async def test_execute_callback_exception_nacks_unacted_message() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = False
    sub._in_flight_messages.add(msg)
    delivery = QueuedDelivery(
        callback=AsyncMock(side_effect=ValueError("boom")),
        message=msg,
    )

    await sub._execute_callback(delivery)
    msg.nack.assert_called_once()
    assert msg not in sub._in_flight_messages


async def test_execute_callback_exception_skips_nack_when_acted_on() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = True
    sub._in_flight_messages.add(msg)
    delivery = QueuedDelivery(
        callback=AsyncMock(side_effect=ValueError("boom")),
        message=msg,
    )

    await sub._execute_callback(delivery)
    msg.nack.assert_not_called()
    assert msg not in sub._in_flight_messages


async def test_execute_callback_cancelled_rejects_unacted_message() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = False
    sub._in_flight_messages.add(msg)
    delivery = QueuedDelivery(
        callback=AsyncMock(side_effect=asyncio.CancelledError),
        message=msg,
    )
    with pytest.raises(asyncio.CancelledError):
        await sub._execute_callback(delivery)
    msg.reject.assert_called_once()
    assert msg not in sub._in_flight_messages


async def test_execute_callback_cancelled_skips_reject_when_acted_on() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = True
    sub._in_flight_messages.add(msg)
    delivery = QueuedDelivery(
        callback=AsyncMock(side_effect=asyncio.CancelledError),
        message=msg,
    )
    with pytest.raises(asyncio.CancelledError):
        await sub._execute_callback(delivery)
    msg.reject.assert_not_called()


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


async def test_close_cancels_pending_callbacks() -> None:
    sub = _make_subscriber()
    t = MagicMock()
    sub._callback_tasks.add(t)

    with patch("asyncio.wait", return_value=(set(), set())):
        await sub.close()

    t.cancel.assert_called_once()
    assert len(sub._callback_tasks) == 0


async def test_close_rejects_delivery_queue_messages() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = False
    delivery = QueuedDelivery(callback=AsyncMock(), message=msg)
    await sub._delivery_queue.put(delivery)

    await sub.close()

    msg.reject.assert_called_once()


async def test_close_rejects_in_flight_unacted_messages() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = False
    sub._in_flight_messages.add(msg)

    await sub.close()

    msg.reject.assert_called_once()


async def test_close_skips_reject_for_acted_in_flight() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = True
    sub._in_flight_messages.add(msg)

    await sub.close()

    msg.reject.assert_not_called()


async def test_close_waits_for_callbacks_before_rejecting_in_flight() -> None:
    sub = _make_subscriber()

    class FakeMessage:
        def __init__(self) -> None:
            self.acted = False
            self.reject = AsyncMock()

        @property
        def is_acted_on(self) -> bool:
            return self.acted

    msg = FakeMessage()
    sub._in_flight_messages.add(msg)  # type: ignore[arg-type]

    async def callback_task() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            msg.acted = True
            raise

    task = asyncio.create_task(callback_task())
    sub._callback_tasks.add(task)
    await asyncio.sleep(0)

    await sub.close()

    assert msg.acted
    msg.reject.assert_not_called()


# --- _streaming_pull_loop ---


async def test_streaming_pull_loop_normal_execution() -> None:
    config = _make_config(subscription_path="sub1")
    sub = _make_subscriber(channel_configs=[config])

    async def run_once(*_: Any) -> None:
        sub._shutdown_event.set()

    with patch.object(sub, "_run_streaming_pull", side_effect=run_once):
        await sub._streaming_pull_loop(config)


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

    await sub._run_streaming_pull(config)

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

    await sub._run_streaming_pull(_make_config())


async def test_run_streaming_pull_empty_stream() -> None:
    config = _make_config()
    sub = _make_subscriber(
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
    )

    async def stream_call(_iterator: Any) -> AsyncIterator[StreamingPullResponse]:
        sub._shutdown_event.set()
        return
        yield  # make it an async generator

    stream_stream = cast(AsyncMock, sub._channel.stream_stream)
    stream_stream.return_value = stream_call

    await sub._run_streaming_pull(config)


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
