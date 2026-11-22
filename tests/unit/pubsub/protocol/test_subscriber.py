import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from repid.connections.abc import ReceivedMessageT
from repid.connections.pubsub.message_broker import PubsubServer
from repid.connections.pubsub.protocol._helpers import ChannelConfig
from repid.connections.pubsub.protocol.control_batcher import PubsubControlBatcher
from repid.connections.pubsub.protocol.credentials import InsecureCredentials
from repid.connections.pubsub.protocol.proto import (
    PubsubMessage,
    ReceivedMessage,
    StreamingPullRequest,
    StreamingPullResponse,
)
from repid.connections.pubsub.protocol.received_message import PubsubReceivedMessage
from repid.connections.pubsub.protocol.resilience import ResilienceState
from repid.connections.pubsub.protocol.subscriber import PubsubSubscriber
from repid.data.concurrency import Concurrency
from tests.intake import CallbackIntake, MessageHandler, callback_intake


def _make_subscriber(**overrides: Any) -> PubsubSubscriber:
    """Helper to create a subscriber with sensible defaults."""
    defaults: dict[str, Any] = {
        "channel": MagicMock(spec=grpc.aio.Channel),
        "channel_configs": [],
        "credentials_provider": InsecureCredentials(),
        "resilience_state": MagicMock(spec=ResilienceState),
        "server": MagicMock(spec=PubsubServer),
        "stream_ack_deadline_seconds": 10,
    }
    defaults.update(overrides)
    return PubsubSubscriber(**defaults)


def _make_config(
    handler: MessageHandler | None = None,
    intake: CallbackIntake | None = None,
    **overrides: Any,
) -> ChannelConfig:
    """Helper to create a ChannelConfig with sensible defaults."""
    channel = overrides.pop("channel", "test-channel")
    intake = intake or callback_intake({channel: handler or AsyncMock()})
    defaults: dict[str, Any] = {
        "channel": channel,
        "subscription_path": "projects/p/subscriptions/s",
        "intake": intake.channels[channel],
    }
    defaults.update(overrides)
    return ChannelConfig(**defaults)


def _full_intake(channel: str = "test-channel") -> CallbackIntake:
    """An intake with no room left, so the subscriber holds what it receives."""
    intake = callback_intake({channel: AsyncMock()}, Concurrency(messages=1))
    intake.charge(channel, 0)
    return intake


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


def test_create_received_message() -> None:
    server = MagicMock(spec=PubsubServer)
    sub = _make_subscriber(server=server)
    config = _make_config(channel="my-ch", subscription_path="sub/path")

    raw_msg = PubsubMessage(data=b"hello", message_id="msg-1")
    received = ReceivedMessage(message=raw_msg, ack_id="ack-1", delivery_attempt=2)

    result = sub._create_received_message(received, config)

    assert result is not None
    assert result._raw_message is raw_msg
    assert result._ack_id == "ack-1"
    assert result._delivery_attempt == 2
    assert result._subscription_path == "sub/path"
    assert result._channel_name == "my-ch"
    assert result._server is server


async def test_process_response_filters_none_messages() -> None:
    intake = callback_intake({"test-channel": AsyncMock()})
    config = _make_config(intake=intake)
    sub = _make_subscriber(channel_configs=[config])

    valid_msg = PubsubMessage(data=b"data")
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=None),
            ReceivedMessage(message=valid_msg, ack_id="a1"),
        ],
    )

    await sub._process_response(response, config)

    (submitted,) = intake.submitted
    assert submitted.payload == b"data"
    await intake.join()


async def test_process_response_empty() -> None:
    intake = callback_intake({"test-channel": AsyncMock()})
    config = _make_config(intake=intake)
    sub = _make_subscriber(channel_configs=[config])
    await sub._process_response(StreamingPullResponse(received_messages=[]), config)
    assert not intake.submitted


async def test_process_response_holds_deliveries_until_the_worker_has_room() -> None:
    intake = _full_intake()
    config = _make_config(intake=intake)
    sub = _make_subscriber(channel_configs=[config])
    response = StreamingPullResponse(
        received_messages=[ReceivedMessage(message=PubsubMessage(data=b"held"), ack_id="a1")],
    )
    await sub._process_response(response, config)
    assert not intake.submitted

    intake.release("test-channel", 0)
    await asyncio.sleep(0)
    (submitted,) = intake.submitted
    assert submitted.payload == b"held"
    await intake.join()


async def test_request_iterator_initial_request() -> None:
    config = _make_config(subscription_path="sub/1", concurrency=Concurrency(messages=5))
    sub = _make_subscriber(
        stream_ack_deadline_seconds=20,
        client_id="cid",
    )

    it = sub._request_iterator(config)
    initial = await anext(it)

    assert initial.subscription == "sub/1"
    assert initial.stream_ack_deadline_seconds == 20
    assert initial.client_id == "cid"
    assert initial.max_outstanding_messages == 5

    await cast(AsyncGenerator[StreamingPullRequest, None], it).aclose()


async def test_request_iterator_keeps_client_id_per_logical_stream() -> None:
    first = _make_config()
    second = _make_config()
    sub = _make_subscriber(channel_configs=[first, second])

    async def initial_request(config: ChannelConfig) -> StreamingPullRequest:
        iterator = cast(
            AsyncGenerator[StreamingPullRequest, None],
            sub._request_iterator(config),
        )
        try:
            return await anext(iterator)
        finally:
            await iterator.aclose()

    first_request = await initial_request(first)
    assert first_request.client_id == (await initial_request(first)).client_id
    assert first_request.client_id != (await initial_request(second)).client_id


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
    sub._task.cancel()  # pyrefly: ignore[missing-attribute]
    with contextlib.suppress(asyncio.CancelledError):
        await sub._task  # pyrefly: ignore[not-async]


async def test_close_basic() -> None:
    sub = _make_subscriber()
    await sub.close()
    assert sub._close_task is not None
    assert sub._close_task.done()
    assert not sub.is_active
    assert sub._shutdown_event.is_set()


async def test_close_idempotent() -> None:
    sub = _make_subscriber()
    await sub.close()
    with patch.object(sub._pause_event, "clear") as mock_clear:
        await sub.close()
        mock_clear.assert_not_called()


async def test_close_cancels_main_task() -> None:
    sub = _make_subscriber()
    sub._task = asyncio.create_task(asyncio.sleep(10))
    await sub.close()
    assert sub._task.cancelled()


async def test_concurrent_close_waits_for_unsettled_receipts() -> None:
    sub = _make_subscriber()
    rejecting = asyncio.Event()
    rejected = asyncio.Event()

    async def reject() -> None:
        rejecting.set()
        await rejected.wait()

    msg = MagicMock(reject=AsyncMock(side_effect=reject), is_acted_on=False)
    sub._in_flight_messages.add(msg)
    closing = asyncio.create_task(sub.close())
    second_close: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(rejecting.wait(), 1)
        second_close = asyncio.create_task(sub.close())
        await asyncio.sleep(0)
        assert not second_close.done()
    finally:
        rejected.set()
        await closing
        if second_close is not None:
            await second_close


async def test_close_rejects_held_deliveries() -> None:
    config = _make_config(intake=_full_intake())
    sub = _make_subscriber(channel_configs=[config])
    msg = MagicMock(reject=AsyncMock(), is_acted_on=False, keep_alive_interval=None)
    sub._submissions["test-channel"].put(msg)

    await sub.close()

    msg.reject.assert_awaited_once()


async def test_close_batches_buffered_rejections() -> None:
    client = MagicMock(modify_ack_deadline=AsyncMock())
    batcher = PubsubControlBatcher(client, flush_interval=10, max_batch_ids=2)
    await batcher.start()
    config = _make_config(intake=_full_intake())
    sub = _make_subscriber(server=MagicMock(_control_batcher=batcher), channel_configs=[config])
    await sub._process_response(
        StreamingPullResponse(
            received_messages=[
                ReceivedMessage(message=PubsubMessage(), ack_id="first"),
                ReceivedMessage(message=PubsubMessage(), ack_id="second"),
            ],
        ),
        config,
    )
    try:
        await asyncio.wait_for(sub.close(), 1)
        client.modify_ack_deadline.assert_awaited_once()
        subscription, ids, seconds = client.modify_ack_deadline.await_args.args
        assert subscription == "projects/p/subscriptions/s"
        assert set(ids) == {"first", "second"}
        assert seconds == 1
    finally:
        await batcher.stop()


@pytest.mark.parametrize("close_from_handler", [True, False])
async def test_delivery_handler_can_close_its_own_subscriber(close_from_handler: bool) -> None:
    batcher = MagicMock(add_modify_deadline=AsyncMock())
    started = asyncio.Event()
    closed = asyncio.Event()

    async def handler(_message: ReceivedMessageT) -> None:
        started.set()
        try:
            if not close_from_handler:
                await asyncio.Future()
        finally:
            await sub.close()
            closed.set()

    intake = callback_intake({"test-channel": handler})
    config = _make_config(intake=intake)
    sub = _make_subscriber(server=MagicMock(_control_batcher=batcher), channel_configs=[config])
    await sub._process_response(
        StreamingPullResponse(
            received_messages=[ReceivedMessage(message=PubsubMessage(), ack_id="a")],
        ),
        config,
    )
    await asyncio.wait_for(started.wait(), 1)
    if not close_from_handler:
        await asyncio.wait_for(sub.close(), 1)
        for task in intake.tasks:
            task.cancel()
    await asyncio.wait_for(closed.wait(), 1)
    # The worker owns submitted deliveries, so closing does not return them.
    batcher.add_modify_deadline.assert_not_awaited()


async def test_close_rejects_in_flight_unacted_messages() -> None:
    sub = _make_subscriber()
    msg = MagicMock(reject=AsyncMock(), is_acted_on=False)
    sub._in_flight_messages.add(msg)

    await sub.close()

    msg.reject.assert_awaited_once()


async def test_close_skips_reject_for_acted_in_flight() -> None:
    sub = _make_subscriber()
    msg = MagicMock(is_acted_on=True)
    sub._in_flight_messages.add(msg)

    await sub.close()

    msg.reject.assert_not_called()


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


def _mock_stream_call(
    responses: Callable[
        [AsyncIterator[StreamingPullRequest]],
        AsyncIterator[StreamingPullResponse],
    ],
) -> Callable[[AsyncIterator[StreamingPullRequest]], MagicMock]:
    def make_call(requests: AsyncIterator[StreamingPullRequest]) -> MagicMock:
        call = MagicMock()
        call.__aiter__.side_effect = lambda: responses(requests)
        return call

    return make_call


async def test_run_streaming_pull_processes_messages() -> None:
    intake = callback_intake({"test-channel": AsyncMock()})
    config = _make_config(intake=intake)
    sub = _make_subscriber(
        channel_configs=[config],
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

    stream_stream = cast(MagicMock, sub._channel.stream_stream)
    stream_stream.return_value.side_effect = _mock_stream_call(stream_call)

    await sub._run_streaming_pull(config)

    (submitted,) = intake.submitted
    assert submitted.payload == b"payload"
    await intake.join()


async def test_run_streaming_pull_shutdown_during_response() -> None:
    config = _make_config()
    sub = _make_subscriber(
        channel_configs=[config],
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
    )

    async def stream_call(_iterator: Any) -> AsyncIterator[StreamingPullResponse]:
        yield MagicMock()
        sub._shutdown_event.set()
        yield MagicMock()  # This should be skipped

    stream_stream = cast(MagicMock, sub._channel.stream_stream)
    stream_stream.return_value.side_effect = _mock_stream_call(stream_call)

    await sub._run_streaming_pull(config)


async def test_run_streaming_pull_empty_stream() -> None:
    config = _make_config()
    sub = _make_subscriber(
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
    )

    async def stream_call(_iterator: Any) -> AsyncIterator[StreamingPullResponse]:
        sub._shutdown_event.set()
        return
        yield  # make it an async generator

    stream_stream = cast(MagicMock, sub._channel.stream_stream)
    stream_stream.return_value.side_effect = _mock_stream_call(stream_call)

    await sub._run_streaming_pull(config)


async def test_process_background_runs_all_tasks() -> None:
    config = _make_config()
    sub = _make_subscriber(channel_configs=[config])

    with patch.object(sub, "_streaming_pull_loop", new_callable=AsyncMock) as mock_stream:
        sub._start_background_tasks()
        assert sub._task is not None
        await sub._task

        mock_stream.assert_called_once_with(config)


async def test_fatal_stream_failure_stops_siblings_and_close_returns_held_deliveries() -> None:
    failing = _make_config(channel="failing", intake=_full_intake("failing"))
    sibling = _make_config(channel="sibling")
    sub = _make_subscriber(channel_configs=[failing, sibling])
    sibling_stopped = asyncio.Event()
    held = asyncio.Event()

    async def pull(config: ChannelConfig) -> None:
        if config is failing:
            await held.wait()
            raise RuntimeError("fatal stream failure")
        try:
            await asyncio.Future()
        finally:
            sibling_stopped.set()

    message = MagicMock(is_acted_on=False, reject=AsyncMock(), keep_alive_interval=None)
    with patch.object(sub, "_streaming_pull_loop", side_effect=pull):
        sub._start_background_tasks()
        sub._submissions["failing"].put(message)
        held.set()
        with pytest.raises(RuntimeError, match="fatal stream failure"):
            await asyncio.wait_for(sub.task, 1)

    assert sibling_stopped.is_set()
    await asyncio.wait_for(sub.close(), 1)
    message.reject.assert_awaited_once()


@pytest.mark.parametrize("buffered_response", [False, True])
async def test_pause_cancels_live_pull_and_resume_reopens_stream(buffered_response: bool) -> None:
    delivered: asyncio.Queue[PubsubReceivedMessage] = asyncio.Queue()
    reopened = asyncio.Event()

    async def callback(message: ReceivedMessageT) -> None:
        delivered.put_nowait(cast(PubsubReceivedMessage, message))

    class PullCall:
        def __init__(self, ack_id: str) -> None:
            self.ack_id = ack_id
            self.cancelled = asyncio.Event()

        def cancel(self) -> None:
            self.cancelled.set()

        async def __aiter__(self) -> AsyncIterator[StreamingPullResponse]:
            if self.ack_id == "second":
                reopened.set()
            yield StreamingPullResponse(
                received_messages=[ReceivedMessage(message=PubsubMessage(), ack_id=self.ack_id)],
            )
            await self.cancelled.wait()
            if buffered_response:
                yield StreamingPullResponse(
                    received_messages=[ReceivedMessage(message=PubsubMessage(), ack_id="buffered")],
                )
            else:
                raise asyncio.CancelledError

    first, second = PullCall("first"), PullCall("second")
    batcher = MagicMock(add_ack=AsyncMock(), add_modify_deadline=AsyncMock())
    sub = _make_subscriber(
        channel_configs=[_make_config(callback)],
        resilience_state=MagicMock(spec=ResilienceState, record_success=AsyncMock()),
        server=MagicMock(_control_batcher=batcher),
    )
    stream = cast(MagicMock, sub._channel.stream_stream)
    stream.return_value.side_effect = [first, second]
    sub._start_background_tasks()
    try:
        message = await asyncio.wait_for(delivered.get(), 1)
        await sub.pause()
        assert first.cancelled.is_set()
        await message.ack()
        batcher.add_ack.assert_awaited_once_with("projects/p/subscriptions/s", "first")
        await asyncio.sleep(0)
        assert not reopened.is_set()
        assert delivered.empty()

        await sub.resume()
        await asyncio.wait_for(reopened.wait(), 1)
        resumed = await asyncio.wait_for(delivered.get(), 1)
        assert resumed._ack_id == "second"
        if buffered_response:
            batcher.add_modify_deadline.assert_awaited_once_with(
                "projects/p/subscriptions/s",
                "buffered",
                1,
            )
    finally:
        await sub.close()


async def test_pause_during_credentials_refresh_skips_opening_pull() -> None:
    refreshing = asyncio.Event()
    release = asyncio.Event()

    async def refresh() -> None:
        refreshing.set()
        await release.wait()

    sub = _make_subscriber(
        credentials_provider=MagicMock(ensure_valid=AsyncMock(side_effect=refresh)),
    )
    starting = asyncio.create_task(sub._run_streaming_pull(_make_config()))
    try:
        await asyncio.wait_for(refreshing.wait(), 1)
        await sub.pause()
    finally:
        release.set()
        await asyncio.wait_for(starting, 1)
    cast(MagicMock, sub._channel.stream_stream).assert_not_called()
