import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

import repid.connections._subscriber as subscriber_helpers
from repid.admission import ExecutionAdmission, MessageLimits
from repid.connections import SubscriberDispatcher
from repid.connections._subscriber import run_supervised
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
from repid.limits import BackpressurePolicy


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
        "dispatcher": SubscriberDispatcher(),
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
        dispatcher=SubscriberDispatcher(),
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


async def test_channel_pause_holds_only_that_channels_deliveries() -> None:
    paused_config = _make_config(channel="paused", subscription_path="sub/paused")
    flowing_config = _make_config(channel="flowing", subscription_path="sub/flowing")
    sub = _make_subscriber(channel_configs=[paused_config, flowing_config])
    response = StreamingPullResponse(
        received_messages=[ReceivedMessage(message=PubsubMessage(data=b"data"), ack_id="a1")],
    )

    await sub.pause_channel("paused")
    processing_paused = asyncio.create_task(sub._process_response(response, paused_config))
    await asyncio.sleep(0)
    assert not processing_paused.done()
    assert sub._delivery_queue.qsize() == 0

    # A global resume must not un-pause the channel.
    await sub.resume()
    await asyncio.sleep(0)
    assert not processing_paused.done()

    await sub.resume_channel("paused")
    await asyncio.wait_for(processing_paused, timeout=1)
    assert sub._delivery_queue.qsize() == 1

    # The flowing channel was never held.
    await sub._process_response(response, flowing_config)
    assert sub._delivery_queue.qsize() == 2


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
        dispatcher=SubscriberDispatcher(
            MessageLimits(max_messages=5, max_payload_bytes=7),
        ),
    )
    config = _make_config(subscription_path="sub/1")

    it = sub._request_iterator(config)
    initial = await anext(it)

    assert initial.subscription == "sub/1"
    assert initial.stream_ack_deadline_seconds == 20
    assert initial.client_id == "cid"
    assert initial.max_outstanding_messages == 5
    assert initial.max_outstanding_bytes == 7

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()


async def test_pubsub_claims_per_channel_native_flow_control() -> None:
    # The StreamingPull server-side outstanding limits are real intake
    # windows: one per stream (per channel), counted until the message is
    # acked after admission.  There is no subscription-wide window.
    capabilities = PubsubServer(default_project="p").capabilities
    assert not capabilities["supports_native_message_flow_control"]
    assert capabilities["supports_native_message_flow_control_per_channel"]
    assert not capabilities["supports_native_payload_flow_control"]
    assert capabilities["supports_native_payload_flow_control_per_channel"]


async def test_early_ack_does_not_release_pubsub_intake_capacity() -> None:
    acknowledged = asyncio.Event()
    release_callback = asyncio.Event()
    messages = []
    server = MagicMock()
    server._control_batcher = MagicMock(add_ack=AsyncMock())

    async def callback(message: Any) -> None:
        messages.append(message)
        await message.ack()
        acknowledged.set()
        await release_callback.wait()

    config = _make_config(callback=callback)
    subscriber = _make_subscriber(
        channel_configs=[config],
        dispatcher=SubscriberDispatcher(MessageLimits(max_messages=1)),
        server=server,
    )
    first = StreamingPullResponse(
        received_messages=[ReceivedMessage(message=PubsubMessage(data=b"first"), ack_id="first")],
    )
    second = StreamingPullResponse(
        received_messages=[ReceivedMessage(message=PubsubMessage(data=b"second"), ack_id="second")],
    )
    dispatching = asyncio.create_task(subscriber._dispatch_loop())
    try:
        await subscriber._process_response(first, config)
        await asyncio.wait_for(acknowledged.wait(), timeout=1)

        processing_second = asyncio.create_task(subscriber._process_response(second, config))
        await asyncio.sleep(0)

        assert messages[0].is_acted_on
        assert len(subscriber._in_flight_messages) == 2
        assert not processing_second.done()

        release_callback.set()
        await asyncio.wait_for(processing_second, timeout=1)
        await subscriber._admitted_tasks.drain()
    finally:
        release_callback.set()
        dispatching.cancel()
        await asyncio.gather(dispatching, return_exceptions=True)


async def test_execution_admission_rejects_native_only_pubsub_limits() -> None:
    strict_native = BackpressurePolicy(strategies=("native",), on_unavailable="error")
    limits = MessageLimits(max_messages=1, backpressure=strict_native)
    admission = ExecutionAdmission(
        server=MagicMock(
            capabilities={
                "supports_pause_per_channel": False,
                "supports_pause": False,
            },
        ),
        limits=limits,
    )
    config = _make_config()
    prepared = admission.prepare({config.channel: []})
    subscriber = _make_subscriber(
        channel_configs=[config],
        dispatcher=SubscriberDispatcher(limits, prepared),
    )
    admission.server_subscriber = subscriber

    with pytest.raises(ValueError, match="no available strategy"):
        admission.validate_backpressure()


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
    delivery = QueuedDelivery(callback=callback, message=msg, lease=AsyncMock())

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
        lease=AsyncMock(),
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
        lease=AsyncMock(),
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
        lease=AsyncMock(),
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
        lease=AsyncMock(),
    )
    with pytest.raises(asyncio.CancelledError):
        await sub._execute_callback(delivery)
    msg.reject.assert_not_called()


# --- _dispatch_loop ---


async def test_dispatch_loop_dispatches_and_cancels() -> None:
    sub = _make_subscriber()
    callback = AsyncMock()
    msg = MagicMock()
    delivery = QueuedDelivery(callback=callback, message=msg, lease=AsyncMock())
    await sub._delivery_queue.put(delivery)

    task = asyncio.create_task(sub._dispatch_loop())
    await asyncio.sleep(0.05)

    callback.assert_called_once_with(msg)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_dispatch_loop_unstarted_delivery_is_not_rejected_twice_on_close() -> None:
    sub = _make_subscriber()
    callback = AsyncMock()
    allow_reject = asyncio.Event()
    reject_started = asyncio.Event()
    lease = AsyncMock()
    msg = MagicMock(is_acted_on=False)

    async def blocked_reject() -> None:
        reject_started.set()
        await allow_reject.wait()

    msg.reject = AsyncMock(side_effect=blocked_reject)
    delivery = QueuedDelivery(callback=callback, message=msg, lease=lease)
    sub._in_flight_messages.add(msg)
    original_start_task = sub._admitted_tasks.start_task

    def start_and_cancel(*args: Any, **kwargs: Any) -> asyncio.Task[None]:
        task = original_start_task(*args, **kwargs)
        task.cancel()
        return task

    with patch.object(sub._admitted_tasks, "start_task", side_effect=start_and_cancel):
        dispatching = asyncio.create_task(sub._dispatch_loop())
        try:
            await sub._delivery_queue.put(delivery)
            await asyncio.wait_for(reject_started.wait(), timeout=1)

            await sub.stop()

            callback.assert_not_awaited()
            msg.reject.assert_awaited_once()
            lease.release.assert_not_awaited()

            allow_reject.set()
            cleanup_tasks = tuple(sub._admitted_tasks._cleanup_tasks)
            await asyncio.gather(*cleanup_tasks)
        finally:
            dispatching.cancel()
            await asyncio.gather(dispatching, return_exceptions=True)

    msg.reject.assert_awaited_once()
    lease.release.assert_awaited_once()


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


async def test_pause_resume_lifecycle_clears_channel_pause_events() -> None:
    config = _make_config()
    sub = _make_subscriber(channel_configs=[config])
    channel_event = sub._channel_pause_events["test-channel"]
    assert channel_event.is_set()

    await sub.pause()
    assert not channel_event.is_set()
    assert not sub.is_active

    await sub.resume()
    assert channel_event.is_set()
    assert sub.is_active


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


# --- close ---


async def test_close_basic() -> None:
    sub = _make_subscriber()
    await sub.stop()
    await sub.finish()
    assert sub._is_closing
    assert not sub.is_active
    assert sub._shutdown_event.is_set()


async def test_close_idempotent() -> None:
    sub = _make_subscriber()
    await sub.stop()
    await sub.finish()
    # Second stop is a no-op
    with patch.object(sub._pause_event, "clear") as mock_clear:
        await sub.stop()
        await sub.finish()
        mock_clear.assert_not_called()


async def test_close_cancels_main_task() -> None:
    sub = _make_subscriber()
    sub._task = asyncio.create_task(asyncio.sleep(10))
    await sub.stop()
    await sub.finish()
    assert sub._task.cancelled()


async def test_close_cancels_pending_callbacks() -> None:
    sub = _make_subscriber()
    task = asyncio.create_task(asyncio.sleep(10))
    sub._admitted_tasks.tasks.add(task)
    await asyncio.sleep(0)

    await sub.stop()
    await sub.finish()

    assert task.cancelled()


async def test_close_rejects_delivery_queue_messages() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = False
    delivery = QueuedDelivery(callback=AsyncMock(), message=msg, lease=AsyncMock())
    await sub._delivery_queue.put(delivery)

    await sub.stop()
    await sub.finish()

    msg.reject.assert_called_once()


async def test_close_rejects_in_flight_unacted_messages() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = False
    sub._in_flight_messages.add(msg)

    await sub.stop()
    await sub.finish()

    msg.reject.assert_called_once()


async def test_close_skips_reject_for_acted_in_flight() -> None:
    sub = _make_subscriber()
    msg = MagicMock()
    msg.is_acted_on = True
    sub._in_flight_messages.add(msg)

    await sub.stop()
    await sub.finish()

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
    sub._admitted_tasks.tasks.add(task)
    await asyncio.sleep(0)

    await sub.stop()
    await sub.finish()

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


async def test_process_background_cancels_siblings_after_fatal_stream_error() -> None:
    fatal_config = _make_config(channel="fatal")
    sibling_config = _make_config(channel="sibling")
    sub = _make_subscriber(channel_configs=[fatal_config, sibling_config])
    sibling_started = asyncio.Event()
    dispatch_started = asyncio.Event()
    sibling_cancelled = asyncio.Event()
    dispatch_cancelled = asyncio.Event()
    sibling_finished = asyncio.Event()
    dispatch_finished = asyncio.Event()
    release = asyncio.Event()

    async def stream(config: ChannelConfig) -> None:
        if config is fatal_config:
            await asyncio.gather(sibling_started.wait(), dispatch_started.wait())
            raise RuntimeError("fatal")
        sibling_started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise
        finally:
            sibling_finished.set()

    async def dispatch() -> None:
        dispatch_started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            dispatch_cancelled.set()
            raise
        finally:
            dispatch_finished.set()

    try:
        with (
            patch.object(sub, "_streaming_pull_loop", side_effect=stream),
            patch.object(sub, "_dispatch_loop", side_effect=dispatch),
        ):
            with pytest.raises(RuntimeError, match="fatal"):
                await sub._process_background()
            assert sibling_cancelled.is_set()
            assert dispatch_cancelled.is_set()
    finally:
        release.set()
        await asyncio.wait_for(
            asyncio.gather(sibling_finished.wait(), dispatch_finished.wait()),
            timeout=1,
        )


async def test_paused_stream_does_not_admit_more_deliveries() -> None:
    sub = _make_subscriber()
    config = _make_config()
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=PubsubMessage(data=b"payload"), ack_id="ack"),
        ],
    )
    await sub.pause()
    with patch.object(sub._dispatcher, "reserve", wraps=sub._dispatcher.reserve) as reserve:
        receiving = asyncio.create_task(sub._process_response(response, config))
        try:
            await asyncio.sleep(0)
            reserve.assert_not_awaited()
            await sub.resume()
            await receiving
            reserve.assert_awaited_once()
        finally:
            receiving.cancel()
            await asyncio.gather(receiving, return_exceptions=True)
            await sub.stop()
            await sub.finish()


async def test_finish_waits_for_blocked_inflight_reject_after_stop() -> None:
    sub = _make_subscriber()
    allow_reject = asyncio.Event()
    reject_started = asyncio.Event()

    class BlockedMessage:
        def __init__(self) -> None:
            self.is_acted_on = False
            self.keep_alive_interval = None
            self.reject = AsyncMock(side_effect=self._blocked_reject)

        async def _blocked_reject(self) -> None:
            reject_started.set()
            if not allow_reject.is_set():
                await allow_reject.wait()

    msg = BlockedMessage()
    sub._in_flight_messages.add(msg)  # type: ignore[arg-type]

    await asyncio.wait_for(sub.stop(), timeout=1)
    finishing = asyncio.create_task(sub.finish())
    await asyncio.wait_for(reject_started.wait(), timeout=1)

    done, _ = await asyncio.wait({finishing}, timeout=0.05)
    assert finishing not in done

    allow_reject.set()
    await asyncio.wait_for(finishing, timeout=1)

    msg.reject.assert_awaited_once()
    await sub._drain_close_cleanups()
    assert msg not in sub._in_flight_messages


async def test_close_rejects_messages_blocked_at_intake_and_response_tail() -> None:
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    sub = _make_subscriber(dispatcher=dispatcher)
    config = _make_config()
    blocker = await dispatcher.reserve(
        MagicMock(channel="test-channel", payload=b"blocker", keep_alive_interval=None),
    )
    assert blocker is not None
    reservation_started = asyncio.Event()

    class Message:
        channel = "test-channel"
        payload = b"message"
        keep_alive_interval = None

        def __init__(self) -> None:
            self.is_acted_on = False

        async def reject(self) -> None:
            self.is_acted_on = True

    messages = [Message(), Message()]
    reserve = dispatcher.reserve

    async def wait_for_reservation(message: Any) -> Any:
        reservation_started.set()
        return await reserve(message)

    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=PubsubMessage(data=b"one"), ack_id="one"),
            ReceivedMessage(message=PubsubMessage(data=b"two"), ack_id="two"),
        ],
    )
    with (
        patch.object(sub, "_create_received_message", side_effect=messages),
        patch.object(dispatcher, "reserve", new=AsyncMock(side_effect=wait_for_reservation)),
    ):
        sub._task = asyncio.create_task(sub._process_response(response, config))
        await asyncio.wait_for(reservation_started.wait(), timeout=1)
        await sub.stop()
        await sub.finish()

    for message in messages:
        assert message.is_acted_on
        assert message not in sub._in_flight_messages
    await blocker.release()


async def test_close_rejects_response_messages_paused_before_admission() -> None:
    sub = _make_subscriber()
    config = _make_config()

    class Message:
        channel = "test-channel"
        payload = b"message"
        keep_alive_interval = None

        def __init__(self) -> None:
            self.is_acted_on = False
            self.reject = AsyncMock(side_effect=self._reject)

        async def _reject(self) -> None:
            self.is_acted_on = True

    messages = [Message(), Message()]
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=PubsubMessage(data=b"one"), ack_id="one"),
            ReceivedMessage(message=PubsubMessage(data=b"two"), ack_id="two"),
        ],
    )
    await sub.pause()
    with patch.object(sub, "_create_received_message", side_effect=messages):
        sub._task = asyncio.create_task(sub._process_response(response, config))
        await asyncio.sleep(0)
        await sub.stop()
        await sub.finish()

    for message in messages:
        message.reject.assert_awaited_once()


async def test_close_does_not_reject_disposed_response_message_twice() -> None:
    dispatcher = SubscriberDispatcher(
        MessageLimits(max_payload_bytes=1, on_oversized_payload="reject"),
    )
    sub = _make_subscriber(dispatcher=dispatcher)

    class Message:
        channel = "test-channel"
        payload = b"oversized"
        keep_alive_interval = None

        def __init__(self) -> None:
            self.is_acted_on = False
            self.reject = AsyncMock(side_effect=self._reject)

        async def _reject(self) -> None:
            self.is_acted_on = True

    message = Message()
    response = StreamingPullResponse(
        received_messages=[ReceivedMessage(message=PubsubMessage(data=b"oversized"), ack_id="one")],
    )
    with patch.object(sub, "_create_received_message", return_value=message):
        await sub._process_response(response, _make_config())
        await sub.stop()
        await sub.finish()

    message.reject.assert_awaited_once()


async def test_finish_drains_blocked_cleanup_once() -> None:
    sub = _make_subscriber()
    message = MagicMock(
        channel="test-channel",
        payload=b"message",
        is_acted_on=False,
        keep_alive_interval=None,
    )
    reject_started = asyncio.Event()
    allow_reject = asyncio.Event()

    async def reject() -> None:
        reject_started.set()
        await allow_reject.wait()

    message.reject = AsyncMock(side_effect=reject)
    response = StreamingPullResponse(
        received_messages=[ReceivedMessage(message=PubsubMessage(data=b"message"), ack_id="one")],
    )
    await sub.pause()
    with patch.object(sub, "_create_received_message", return_value=message):
        sub._task = asyncio.create_task(sub._process_response(response, _make_config()))
        await asyncio.sleep(0)
        await asyncio.wait_for(sub.stop(), timeout=1)

    draining_close = asyncio.create_task(sub.finish())
    await asyncio.wait_for(reject_started.wait(), timeout=1)
    done, _ = await asyncio.wait({draining_close}, timeout=0.05)
    assert draining_close not in done

    allow_reject.set()
    await asyncio.wait_for(draining_close, timeout=1)

    message.reject.assert_awaited_once()
    assert not sub._close_cleanup_tasks


async def test_concurrent_finish_waits_for_cleanup_scheduling() -> None:
    sub = _make_subscriber()
    setup_started = asyncio.Event()
    allow_setup = asyncio.Event()
    reject_started = asyncio.Event()
    allow_reject = asyncio.Event()
    message = MagicMock(is_acted_on=False, keep_alive_interval=None)

    async def reject() -> None:
        reject_started.set()
        await allow_reject.wait()

    async def pause_cancel_scheduling(*, drain: bool) -> None:
        if not drain:
            setup_started.set()
            await allow_setup.wait()

    message.reject = AsyncMock(side_effect=reject)
    sub._in_flight_messages.add(message)

    with patch.object(
        sub._admitted_tasks,
        "cancel_and_drain",
        side_effect=pause_cancel_scheduling,
    ):
        first_finish = asyncio.create_task(sub.finish())
        await asyncio.wait_for(setup_started.wait(), timeout=1)

        second_finish = asyncio.create_task(sub.finish())
        await asyncio.sleep(0)
        assert not second_finish.done()

        allow_setup.set()
        await asyncio.wait_for(reject_started.wait(), timeout=1)
        assert not second_finish.done()

        allow_reject.set()
        await asyncio.wait_for(first_finish, timeout=1)
        await asyncio.wait_for(second_finish, timeout=1)

    message.reject.assert_awaited_once()
    assert not sub._close_cleanup_tasks


async def test_retry_close_schedules_cleanup_after_initial_draining_close_is_cancelled() -> None:
    sub = _make_subscriber()
    drain_started = asyncio.Event()
    allow_drain = asyncio.Event()
    reject_started = asyncio.Event()
    allow_reject = asyncio.Event()
    message = MagicMock(is_acted_on=False, keep_alive_interval=None)

    async def reject() -> None:
        reject_started.set()
        await allow_reject.wait()

    async def block_drain(*, drain: bool) -> None:
        if drain:
            drain_started.set()
            await allow_drain.wait()

    message.reject = AsyncMock(side_effect=reject)
    sub._in_flight_messages.add(message)

    with patch.object(sub._admitted_tasks, "cancel_and_drain", side_effect=block_drain):

        async def full_close() -> None:
            await sub.stop()
            await sub.finish()

        initial_close = asyncio.create_task(full_close())
        await asyncio.wait_for(
            asyncio.gather(drain_started.wait(), reject_started.wait()),
            timeout=1,
        )

        initial_close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await initial_close

        allow_drain.set()
        retry_close = asyncio.create_task(sub.finish())
        await asyncio.sleep(0)
        assert not retry_close.done()

        allow_reject.set()
        await asyncio.wait_for(retry_close, timeout=1)

    message.reject.assert_awaited_once()
    assert not sub._close_cleanup_tasks


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ValueError("reservation failed"), id="error"),
        pytest.param(asyncio.CancelledError(), id="cancellation"),
    ],
)
async def test_process_response_cleans_unadmitted_messages_when_reservation_fails(
    error: BaseException,
) -> None:
    sub = _make_subscriber()
    lease = AsyncMock()

    class Message:
        channel = "test-channel"
        payload = b"message"
        keep_alive_interval = None

        def __init__(self) -> None:
            self.is_acted_on = False
            self.reject = AsyncMock(side_effect=self._reject)

        async def _reject(self) -> None:
            self.is_acted_on = True

    admitted = Message()
    unadmitted = Message()
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=PubsubMessage(data=b"admitted"), ack_id="one"),
            ReceivedMessage(message=PubsubMessage(data=b"unadmitted"), ack_id="two"),
        ],
    )

    with (
        patch.object(sub, "_create_received_message", side_effect=[admitted, unadmitted]),
        patch.object(sub._dispatcher, "reserve", side_effect=[lease, error]),
        pytest.raises(type(error), match=str(error) or None),
    ):
        await sub._process_response(response, _make_config())

    admitted.reject.assert_not_awaited()
    unadmitted.reject.assert_awaited_once()
    assert admitted in sub._in_flight_messages
    assert unadmitted not in sub._in_flight_messages
    assert sub._delivery_queue.qsize() == 1
    await sub.stop()
    await sub.finish()


@pytest.mark.parametrize(
    "drain",
    [pytest.param(True, id="drain"), pytest.param(False, id="no-drain-then-drain")],
)
async def test_close_releases_late_admission_lease(drain: bool) -> None:  # noqa: PLR0915, C901
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    batcher = MagicMock(add_modify_deadline=AsyncMock())
    sub = _make_subscriber(dispatcher=dispatcher, server=MagicMock(_control_batcher=batcher))
    callback = AsyncMock()
    config = _make_config(callback=callback)
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=PubsubMessage(data=b"payload"), ack_id="late"),
        ],
    )
    real_reserve = dispatcher.reserve
    admission_started = asyncio.Event()
    allow_admission = asyncio.Event()
    admission_returned = asyncio.Event()
    cancellation_started = asyncio.Event()
    allow_child_cancellation = asyncio.Event()

    async def gated_reserve(message: Any) -> Any:
        admission_started.set()
        await allow_admission.wait()
        lease = await real_reserve(message)
        admission_returned.set()
        return lease

    async def delayed_cancel_and_drain(
        tasks: Iterable[asyncio.Task[Any]],
        *,
        drain: bool = True,
    ) -> None:
        all_tasks = tuple(tasks)
        # Hold the response child's cancellation until its ready admission resumes,
        # reproducing the gap between cancelling the supervisor and its children.
        for task in all_tasks[1:]:
            task.cancel()
        cancellation_started.set()
        await allow_child_cancellation.wait()
        for task in all_tasks:
            if not task.done():
                task.cancel()
        if drain:
            await asyncio.gather(*all_tasks, return_exceptions=True)

    with (
        patch.object(dispatcher, "reserve", side_effect=gated_reserve),
        patch.object(subscriber_helpers, "cancel_and_drain", side_effect=delayed_cancel_and_drain),
    ):
        sub._task = asyncio.create_task(
            run_supervised(sub._process_response(response, config), sub._dispatch_loop()),
        )
        closing: asyncio.Task[None] | None = None
        try:
            await asyncio.wait_for(admission_started.wait(), timeout=1)
            closing = asyncio.create_task(sub.stop())
            finishing: asyncio.Task[None] | None = None
            if drain:
                finishing = asyncio.create_task(sub.finish())
            await asyncio.wait_for(cancellation_started.wait(), timeout=1)
            assert sub._delivery_queue.empty()
            await asyncio.wait_for(asyncio.shield(closing), timeout=1)

            allow_admission.set()
            await asyncio.wait_for(admission_returned.wait(), timeout=1)
            probe = await asyncio.wait_for(
                real_reserve(
                    MagicMock(channel="test-channel", payload=b"probe", keep_alive_interval=None),
                ),
                timeout=1,
            )
            assert probe is not None
            await probe.release()
            allow_child_cancellation.set()
            await asyncio.wait_for(asyncio.shield(closing), timeout=1)
            if finishing is None:
                finishing = asyncio.create_task(sub.finish())
            await asyncio.wait_for(asyncio.shield(finishing), timeout=1)

            assert sub._delivery_queue.empty()
            assert not sub._close_cleanup_tasks
            assert not dispatcher._pre_admission_keepalives
            assert not dispatcher._keepalive_tasks
            callback.assert_not_awaited()
            batcher.add_modify_deadline.assert_awaited_once_with(
                config.subscription_path,
                "late",
                1,
            )
        finally:
            allow_admission.set()
            allow_child_cancellation.set()
            await sub.stop()
            await sub.finish()
            if closing is not None:
                await asyncio.gather(closing, return_exceptions=True)
            if finishing is not None:
                await asyncio.gather(finishing, return_exceptions=True)


async def test_close_logs_failed_scheduled_cleanup(caplog: pytest.LogCaptureFixture) -> None:
    sub = _make_subscriber()
    lease = MagicMock(release=AsyncMock())
    message = MagicMock(is_acted_on=False)
    lease.release = AsyncMock(side_effect=RuntimeError("release failed"))
    sub._delivery_queue.put_nowait(
        QueuedDelivery(callback=AsyncMock(), message=message, lease=lease),
    )

    await sub.stop()
    await asyncio.wait_for(sub.finish(), timeout=1)

    error = next(
        record for record in caplog.records if record.message == "subscriber.close.cleanup.error"
    )
    assert error.exc_info is not None
    assert str(error.exc_info[1]) == "release failed"
    assert not sub._close_cleanup_tasks


async def test_finish_without_stop_terminates_background_loops() -> None:
    sub = _make_subscriber(channel_configs=[_make_config()])
    streaming_started = asyncio.Event()
    streaming_stopped = asyncio.Event()

    async def streaming_pull(_config: ChannelConfig) -> None:
        streaming_started.set()
        try:
            await asyncio.Future()
        finally:
            streaming_stopped.set()

    with patch.object(sub, "_run_streaming_pull", side_effect=streaming_pull):
        sub._start_background_tasks()
        try:
            await asyncio.wait_for(streaming_started.wait(), timeout=1)
            await asyncio.wait_for(sub.finish(), timeout=1)
            assert streaming_stopped.is_set()
            assert sub.task.done()
            assert sub._shutdown_event.is_set()
            assert not sub.is_active
            await asyncio.wait_for(sub.finish(), timeout=1)
        finally:
            await sub.stop()
            await sub.finish()


async def test_finish_without_stop_is_a_pure_drain() -> None:
    sub = _make_subscriber()
    sub._is_closing = True

    await sub.finish()
    assert not sub._close_cleanup_tasks


async def test_close_cleanup_done_tolerates_cancelled_cleanup() -> None:
    sub = _make_subscriber()
    cleanup_started = asyncio.Event()

    async def block() -> None:
        cleanup_started.set()
        await asyncio.sleep(3600)

    sub._schedule_close_cleanup(block())
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)

    cleanup_task = next(iter(sub._close_cleanup_tasks))
    cleanup_task.cancel()
    await asyncio.wait_for(sub._drain_close_cleanups(), timeout=1)

    assert cleanup_task.cancelled()
    assert not sub._close_cleanup_tasks
