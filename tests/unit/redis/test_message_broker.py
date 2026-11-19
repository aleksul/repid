import asyncio
import contextlib
import itertools
import json
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from repid import Concurrency, LeaseRenewal, Router
from repid._runner import _Runner
from repid.connections.abc import MessageAction, MessageCallbackT, ReceivedMessageT
from repid.connections.redis import message_broker
from repid.connections.redis.message_broker import (
    ChannelConfig,
    RedisReceivedMessage,
    RedisSentMessage,
    RedisServer,
    RedisSubscriber,
    _build_message_fields,
    _default_consumer_group_strategy,
    _default_dlq_stream_strategy,
    _default_stream_name_strategy,
    _parse_message_fields,
    _StreamReadResultT,
)
from repid.data import ActorExecutionContext
from repid.serializer import default_serializer
from tests.channel_limits import channel_limits

_Settle = Callable[[RedisReceivedMessage], Awaitable[None]]


class _ForwardingPipeline:
    """Queues pipelined XRANGE calls and runs them through the client's mock on execute."""

    def __init__(self, client: MagicMock) -> None:
        self._client = client
        self._calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def xrange(self, *args: object, **kwargs: object) -> None:
        self._calls.append((args, kwargs))

    async def execute(self) -> list[object]:
        return [await self._client.xrange(*args, **kwargs) for args, kwargs in self._calls]


def _redis_client_mock() -> AsyncMock:
    client = AsyncMock(
        xrevrange=AsyncMock(return_value=[]),
        xread=AsyncMock(return_value=[]),
        xpending_range=AsyncMock(return_value=[]),
        xrange=AsyncMock(return_value=[]),
    )

    def pipeline(*, transaction: bool = True) -> _ForwardingPipeline:  # noqa: ARG001
        return _ForwardingPipeline(client)

    client.pipeline = MagicMock(side_effect=pipeline)
    return client


@pytest.fixture
def pipeline_mock() -> tuple[MagicMock, MagicMock]:
    pipe = MagicMock(execute=AsyncMock())
    pipe.__aenter__.return_value = pipe
    pipe.__aexit__.return_value = None
    client = MagicMock(
        xack=AsyncMock(),
        xclaim=AsyncMock(),
        xreadgroup=AsyncMock(return_value=[]),
        xautoclaim=AsyncMock(),
        xpending_range=AsyncMock(return_value=[]),
        xrange=AsyncMock(return_value=[]),
    )

    # Settlement uses transactional pipelines; recovery batches reads without one.
    def pipeline(*, transaction: bool = True) -> MagicMock | _ForwardingPipeline:
        return pipe if transaction else _ForwardingPipeline(client)

    client.pipeline = MagicMock(side_effect=pipeline)
    return client, pipe


@pytest.fixture
def make_received_message(pipeline_mock: tuple[MagicMock, MagicMock]) -> Any:
    redis_client, _ = pipeline_mock

    def _factory(**overrides: Any) -> RedisReceivedMessage:
        defaults: dict[str, Any] = {
            "payload": b"p",
            "headers": {},
            "content_type": "c",
            "reply_to": None,
            "message_id": "1-0",
            "channel": "chan",
            "stream_name": "s",
            "consumer_group": "g",
            "redis_client": redis_client,
            "dlq_stream": "dlq",
            "server": MagicMock(),
        }
        defaults.update(overrides)
        return RedisReceivedMessage(**defaults)

    return _factory


def test_redis_default_strategies() -> None:
    assert _default_stream_name_strategy("chan") == "repid:chan"
    assert _default_consumer_group_strategy("chan") == "repid:chan:group"
    assert _default_dlq_stream_strategy("chan") == "repid:chan:dlq"


def test_redis_build_message_fields() -> None:
    payload = b"test"
    headers = {"a": "b"}
    content_type = "application/json"

    fields = _build_message_fields(
        payload,
        headers,
        content_type,
        reply_to="reply-chan",
    )

    assert fields[b"payload"] == payload
    assert fields[b"headers"] == json.dumps(headers)
    assert fields[b"content_type"] == content_type
    assert fields[b"reply_to"] == "reply-chan"

    fields_min = _build_message_fields(payload, None, None, None)
    assert fields_min[b"payload"] == payload
    assert b"headers" not in fields_min
    assert b"content_type" not in fields_min


def test_redis_parse_message_fields() -> None:
    payload = b"test"
    headers = {"a": "b"}
    content_type = "application/json"
    fields: dict[bytes | str, bytes | str] = {
        b"payload": payload,
        b"headers": json.dumps(headers).encode(),
        b"content_type": content_type.encode(),
        b"reply_to": b"reply-chan",
    }

    p, h, ct, reply_to = _parse_message_fields(fields)
    assert p == payload
    assert h == headers
    assert ct == content_type
    assert reply_to == "reply-chan"

    fields_str: dict[bytes | str, bytes | str] = {b"payload": "str_payload"}
    p, _, _, _ = _parse_message_fields(fields_str)
    assert p == b"str_payload"

    p, h, ct, reply_to = _parse_message_fields({})
    assert p == b""
    assert h is None
    assert ct is None
    assert reply_to is None


@patch.object(message_broker, "Redis")
async def test_redis_server_connect_disconnect(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost")

    await server.connect()
    assert server.is_connected
    mock_redis_cls.from_url.assert_called_once()
    mock_client.ping.assert_awaited_once()

    await server.connect()
    assert mock_redis_cls.from_url.call_count == 1

    sub_mock = AsyncMock()
    server._active_subscribers.append(sub_mock)

    await server.disconnect()
    assert not server.is_connected
    sub_mock.close.assert_awaited_once()
    mock_client.aclose.assert_awaited_once()


@patch.object(message_broker, "Redis")
async def test_redis_server_disconnect_subscriber_error(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost")
    await server.connect()

    sub_mock = AsyncMock()
    sub_mock.close.side_effect = ResponseError("Some redis error")
    server._active_subscribers.append(sub_mock)

    await server.disconnect()
    sub_mock.close.assert_awaited_once()
    mock_client.aclose.assert_awaited_once()


@patch.object(message_broker, "Redis")
async def test_redis_server_connection_context_manager(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost")

    async with server.connection() as s:
        assert s is server
        assert server.is_connected

    assert not server.is_connected


def _pending_future(*_args: Any, **_kwargs: Any) -> asyncio.Future[Any]:
    return asyncio.Future()


async def test_redis_publish_not_connected() -> None:
    server = RedisServer("redis://localhost")
    msg = RedisSentMessage(payload=b"test")
    with pytest.raises(ConnectionError, match="Not connected"):
        await server.publish(channel="test", message=msg)


@patch.object(message_broker, "Redis")
async def test_redis_publish(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost")
    await server.connect()

    msg = RedisSentMessage(payload=b"test", headers={"h": "v"}, content_type="t")

    await server.publish(channel="test", message=msg)

    mock_client.xadd.assert_awaited_once()
    args, kwargs = mock_client.xadd.call_args
    assert args[0] == "repid:test"
    assert args[1][b"payload"] == b"test"
    assert args[1][b"headers"] == json.dumps({"h": "v"})
    assert args[1][b"content_type"] == "t"
    assert kwargs["id"] == "*"

    mock_client.xadd.reset_mock()
    await server.publish(
        channel="test",
        message=msg,
        server_specific_parameters={
            "maxlen": 100,
            "approximate": False,
            "nomkstream": True,
            "stream_id": "1-0",
        },
    )
    _, kwargs = mock_client.xadd.call_args
    assert kwargs["maxlen"] == 100
    assert kwargs["approximate"] is False
    assert kwargs["nomkstream"] is True
    assert kwargs["id"] == "1-0"


async def test_redis_subscribe_not_connected() -> None:
    server = RedisServer("redis://localhost")
    with pytest.raises(ConnectionError, match="Not connected"):
        await server.subscribe(
            channel_concurrency=channel_limits(()),
            channels_to_callbacks={},
        )


@patch.object(message_broker, "Redis")
async def test_redis_subscribe_ensure_group(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = _pending_future

    server = RedisServer("redis://localhost")
    await server.connect()

    mock_callback = AsyncMock()
    typed_callback = cast(
        MessageCallbackT,
        mock_callback,
    )

    sub = await server.subscribe(
        channel_concurrency=channel_limits(("chan",)),
        channels_to_callbacks={"chan": typed_callback},
    )
    assert isinstance(sub, RedisSubscriber)
    assert sub in server._active_subscribers

    mock_client.xgroup_create.assert_awaited_once_with(
        "repid:chan",
        "repid:chan:group",
        id="0",
        mkstream=True,
    )

    mock_client.xgroup_create.reset_mock()
    mock_client.xgroup_create.side_effect = ResponseError("BUSYGROUP")
    sub2 = await server.subscribe(
        channel_concurrency=channel_limits(("chan2",)),
        channels_to_callbacks={"chan2": typed_callback},
    )
    mock_client.xgroup_create.assert_awaited_once()

    mock_client.xgroup_create.reset_mock()
    mock_client.xgroup_create.side_effect = ResponseError("OTHER")
    with pytest.raises(ResponseError, match="OTHER"):
        await server.subscribe(
            channel_concurrency=channel_limits(("chan3",)),
            channels_to_callbacks={"chan3": typed_callback},
        )

    await sub.close()
    await sub2.close()


@patch.object(message_broker, "Redis")
async def test_redis_subscriber_consume_loop(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost", retry_attempts=0)
    await server.connect()

    callback = AsyncMock()
    channels_to_callbacks = {
        "chan": cast(MessageCallbackT, callback),
    }

    fields = {b"payload": b"hello"}
    mock_client.xreadgroup.side_effect = [
        [[b"repid:chan", [(b"1-0", fields)]]],
        asyncio.CancelledError,
    ]

    sub = await server.subscribe(
        channel_concurrency=channel_limits(channels_to_callbacks),
        channels_to_callbacks=channels_to_callbacks,
    )

    with contextlib.suppress(asyncio.CancelledError):
        await sub.task

    callback.assert_awaited_once()
    msg = callback.call_args[0][0]
    assert isinstance(msg, RedisReceivedMessage)
    assert msg.payload == b"hello"
    assert msg.message_id == "1-0"

    mock_client.xreadgroup.assert_awaited()


@patch.object(message_broker, "Redis")
async def test_redis_subscriber_close(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    server = RedisServer("redis://localhost")
    await server.connect()

    async def wait_forever(*_: Any, **__: Any) -> None:
        await asyncio.Future()

    mock_client.xreadgroup.side_effect = wait_forever

    callback = cast(MessageCallbackT, AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channel_concurrency=channel_limits(("c",)),
            channels_to_callbacks={"c": callback},
        ),
    )

    assert sub.is_active

    async def dummy() -> None:
        pass

    task = asyncio.create_task(dummy())
    sub._callback_tasks.add(task)

    await sub.close()

    assert not sub.is_active
    assert sub._closed
    assert task.done()


@pytest.mark.parametrize(
    "block_ms",
    [pytest.param(0, id="nonblocking"), pytest.param(30000, id="blocking")],
)
@pytest.mark.parametrize(
    "batch_size",
    [
        pytest.param(0, id="empty_batch"),
        pytest.param(1, id="single_message"),
        pytest.param(3, id="multiple_messages"),
    ],
)
async def test_redis_close_drains_outstanding_read(
    pipeline_mock: tuple[MagicMock, MagicMock],
    block_ms: int,
    batch_size: int,
) -> None:
    client, pipe = pipeline_mock
    read_started = asyncio.Event()
    release_read = asyncio.Event()

    async def read_messages(
        **_: object,
    ) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
        read_started.set()
        await release_read.wait()
        if not batch_size:
            return []
        return [
            (b"stream", [(f"{i}-0".encode(), {b"payload": b"pending"}) for i in range(batch_size)]),
        ]

    callback = AsyncMock()

    client.xreadgroup.side_effect = read_messages
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        channel_concurrency={"chan": Concurrency(messages=1)},
        server=MagicMock(),
        block_ms=block_ms,
    )
    subscriber.start()
    await read_started.wait()
    await subscriber.pause()
    closing = asyncio.create_task(subscriber.close())
    try:
        await asyncio.sleep(0)
        assert not closing.done()
    finally:
        release_read.set()
        await closing

    callback.assert_not_awaited()
    assert pipe.execute.await_count == batch_size
    assert subscriber.in_flight_count == 0
    assert not subscriber._callback_tasks
    assert subscriber.task.done()
    assert "block" not in client.xreadgroup.call_args.kwargs


async def test_redis_rechecks_group_after_capturing_notification_cursor() -> None:
    client = _redis_client_mock()
    messages = [[b"stream", [(b"2-0", {b"payload": b"arrived"})]]]
    client.xreadgroup.side_effect = [cast(list[Any], []), messages]
    client.xrevrange.return_value = [(b"2-0", {b"payload": b"arrived"})]
    callback = AsyncMock()
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
        block_ms=0,
    )
    await subscriber._consume_batch("group", subscriber._channels)
    await asyncio.gather(*subscriber._callback_tasks)

    callback.assert_awaited_once()
    assert callback.call_args.args[0].message_id == "2-0"
    client.xread.assert_not_awaited()


@pytest.mark.parametrize("block_ms", [0, 30000])
async def test_redis_notification_does_not_deliver_or_claim_messages(block_ms: int) -> None:
    client = _redis_client_mock()
    messages = [[b"stream", [(b"2-0", {b"payload": b"arrived"})]]]
    client.xreadgroup.side_effect = [cast(list[Any], []), cast(list[Any], []), messages]
    client.xrevrange.return_value = [(b"1-0", {b"payload": b"old"})]
    client.xread.return_value = messages
    callback = AsyncMock()
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
        block_ms=block_ms,
    )
    await subscriber._consume_batch("group", subscriber._channels)
    callback.assert_not_awaited()
    client.xread.assert_awaited_once_with(streams={"stream": b"1-0"}, count=1, block=block_ms)
    await subscriber._consume_batch("group", subscriber._channels)
    await asyncio.gather(*subscriber._callback_tasks)
    callback.assert_awaited_once()


@pytest.mark.parametrize("close_during", ["snapshot", "probe"])
async def test_redis_close_during_notification_setup(close_during: str) -> None:
    client = _redis_client_mock()
    client.xreadgroup.return_value = cast(list[Any], [])
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": AsyncMock()},
        consumer_name="consumer",
        server=MagicMock(),
        block_ms=0,
    )

    async def close(*_: object, **__: object) -> list[object]:
        await subscriber.close()
        return []

    if close_during == "snapshot":
        client.xrevrange.side_effect = close
    else:
        client.xreadgroup.side_effect = close
    await subscriber._wait_for_messages("group", subscriber._channels)
    client.xread.assert_not_awaited()


@pytest.mark.parametrize("block_ms", [0, 30000])
@pytest.mark.parametrize(
    "operation",
    ["claim", "snapshot", "probe", "recovery_metadata", "recovery_payload"],
)
async def test_redis_close_bounds_stalled_read(operation: str, block_ms: int) -> None:
    client = _redis_client_mock()
    started = asyncio.Event()
    cancelled = asyncio.Event()
    attempts = 0

    async def stall(*_: object, **__: object) -> None:
        started.set()
        try:
            await asyncio.Future[None]()
        finally:
            cancelled.set()

    async def read_group(**_: object) -> list[object]:
        nonlocal attempts
        attempts += 1
        if operation.startswith("recovery_"):
            raise RedisConnectionError("Lost reply")
        if operation == "claim" or (operation == "probe" and attempts == 2):
            await stall()
        return []

    client.xreadgroup.side_effect = read_group
    if operation == "snapshot":
        client.xrevrange.side_effect = stall
    elif operation == "recovery_metadata":
        client.xpending_range.side_effect = stall
    elif operation == "recovery_payload":
        client.xpending_range.return_value = [{"message_id": b"1-0"}]
        client.xrange.side_effect = stall
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": AsyncMock()},
        consumer_name="consumer",
        server=MagicMock(),
        block_ms=block_ms,
        read_timeout=0.05,
        retry_delay=0.001,
    )
    subscriber.start()
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(subscriber.close(), timeout=1)
    assert cancelled.is_set()
    assert subscriber.task.done()
    assert not subscriber._notification_tasks
    client.xread.assert_not_awaited()


@pytest.mark.parametrize("read_timeout", [0, -1, float("inf"), float("nan")])
@pytest.mark.parametrize("target", ["server", "subscriber"])
def test_redis_rejects_unbounded_read_timeout(read_timeout: float, target: str) -> None:
    if target == "server":
        with pytest.raises(ValueError, match="read_timeout must be positive and finite"):
            RedisServer("redis://localhost", read_timeout=read_timeout)
    else:
        with pytest.raises(ValueError, match="read_timeout must be positive and finite"):
            RedisSubscriber(
                redis_client=AsyncMock(),
                channels={},
                callbacks={},
                consumer_name="consumer",
                server=MagicMock(),
                read_timeout=read_timeout,
            )


@pytest.mark.parametrize(
    "settlement",
    [
        pytest.param(RedisReceivedMessage.ack, id="ack"),
        pytest.param(RedisReceivedMessage.nack, id="nack"),
        pytest.param(RedisReceivedMessage.reject, id="reject"),
    ],
)
async def test_redis_recovery_skips_delivered_and_rescans_for_late_claims(
    pipeline_mock: tuple[MagicMock, MagicMock],
    settlement: _Settle,
) -> None:
    client, _ = pipeline_mock
    replies: list[_StreamReadResultT] = [
        [(b"stream", [(b"1-0", {b"payload": b"active"}), (b"2-0", {b"payload": b"lost"})])],
        [(b"stream", [(b"3-0", None)])],
        [(b"stream", [])],
        [("stream", [("4-0", {"payload": "late"})])],
        [],
    ]
    cursors: list[str] = []
    delivered: list[str | None] = []

    async def read_pending(
        _stream: str,
        _group: str,
        **kwargs: object,
    ) -> list[dict[str, bytes | str]]:
        cursors.append(cast("str", kwargs["min"]))
        return [
            {"message_id": message_id}
            for _, messages in replies.pop(0)
            for message_id, _ in messages
        ]

    async def callback(message: ReceivedMessageT) -> None:
        delivered.append(message.message_id)
        assert isinstance(message, RedisReceivedMessage)
        await settlement(message)

    client.xpending_range.side_effect = read_pending
    client.xrange.side_effect = [
        [(b"2-0", {b"payload": b"lost"})],
        cast(list[Any], []),
        [("4-0", {"payload": "late"})],
    ]
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
        claim_interval=0,
    )
    subscriber._uncertain_groups.add("group")
    subscriber._delivered_messages.add(("chan", "1-0"))
    for _ in range(5):
        await subscriber._recover_pending("group", subscriber._channels)
        await asyncio.gather(*subscriber._callback_tasks)

    assert delivered == ["2-0", "4-0"]
    assert cursors == [
        "(0-0",
        "(2-0",
        "(3-0",
        "(0-0",
        "(4-0",
    ]
    assert subscriber._delivered_messages == {("chan", "1-0")}
    assert not subscriber._pending_cursors
    assert subscriber._uncertain_groups == {"group"}
    client.xack.assert_any_await("stream", "group", "3-0")
    assert client.xrange.await_count == 3  # Known work was not fetched again.


async def test_redis_recovery_fetches_payloads_in_bounded_round_trips() -> None:
    client = _redis_client_mock()
    ids = [f"{index}-0" for index in range(1, message_broker._RECOVERY_FETCH_CHUNK + 2)]
    client.xpending_range.return_value = [{"message_id": message_id} for message_id in ids]

    async def read_entry(*_: object, **kwargs: object) -> list[tuple[object, dict[bytes, bytes]]]:
        return [(kwargs["min"], {b"payload": b"lost"})]

    client.xrange.side_effect = read_entry
    delivered: list[str | None] = []

    async def callback(message: ReceivedMessageT) -> None:
        delivered.append(message.message_id)
        await message.ack()

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
    )
    with patch.object(message_broker.asyncio, "wait_for", wraps=asyncio.wait_for) as wait_for:
        await subscriber._recover_pending("group", subscriber._channels)
    await asyncio.gather(*subscriber._callback_tasks)

    assert delivered == ids
    assert client.pipeline.call_count == 2
    # Each chunk gets one read timeout per entry it fetches.
    chunk_timeouts = [call.kwargs["timeout"] for call in wait_for.call_args_list][-2:]
    assert chunk_timeouts == [
        subscriber._read_timeout * message_broker._RECOVERY_FETCH_CHUNK,
        subscriber._read_timeout,
    ]


@pytest.mark.parametrize("operation", ["metadata", "payload"])
async def test_redis_recovery_timeout_keeps_scan_position(operation: str) -> None:
    client = _redis_client_mock()
    client.xpending_range.return_value = [{"message_id": b"1-0"}]
    client.xrange.return_value = [(b"1-0", {b"payload": b"lost"})]
    delivered: list[str | None] = []

    async def stall(*_: object, **__: object) -> None:
        await asyncio.Future[None]()

    async def callback(message: ReceivedMessageT) -> None:
        delivered.append(message.message_id)
        await message.ack()

    (client.xpending_range if operation == "metadata" else client.xrange).side_effect = stall
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
        read_timeout=0.01,
    )
    with pytest.raises(asyncio.TimeoutError):
        await subscriber._recover_pending("group", subscriber._channels)
    assert not subscriber._intake_requests
    assert subscriber._pending_cursors == {"group": {"stream": "0-0"}}
    client.xpending_range.side_effect = None
    client.xrange.side_effect = None
    await subscriber._recover_pending("group", subscriber._channels)
    await asyncio.gather(*subscriber._callback_tasks)
    assert delivered == ["1-0"]
    assert not subscriber._delivered_messages


@pytest.mark.parametrize(
    "first_result",
    [pytest.param("manual", id="manual"), pytest.param("ack_failure", id="ack_failure")],
)
async def test_redis_stale_reclaim_retries_completed_unsettled_delivery(first_result: str) -> None:
    client = _redis_client_mock()
    response: _StreamReadResultT = [(b"stream", [(b"1-0", {b"payload": b"job"})])]
    entries = response[0][1]
    client.xreadgroup.return_value = response
    client.xpending_range.return_value = [{"message_id": b"1-0"}]
    client.xautoclaim.return_value = [b"0-0", entries, cast(list[Any], [])]
    if first_result == "ack_failure":
        client.xack.side_effect = [RedisConnectionError("Ack failed"), None]
    calls = 0

    async def callback(message: ReceivedMessageT) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            if first_result == "ack_failure":
                # The runner catches actor/confirmation errors before returning.
                with pytest.raises(RedisConnectionError, match="Ack failed"):
                    await message.ack()
            return
        await message.ack()

    cfg = ChannelConfig(stream="stream", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": cfg},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
        claim_interval=1,
    )
    await subscriber._process_stream_messages(entries, "chan", "stream", callback)
    await asyncio.gather(*subscriber._callback_tasks)
    assert calls == 1
    assert subscriber._delivered_messages == {("chan", "1-0")}
    assert not subscriber._in_flight_messages

    # Uncertain-read recovery must still suppress this known delivery.
    await subscriber._recover_pending("group", subscriber._channels)
    await asyncio.gather(*subscriber._callback_tasks)
    assert calls == 1
    # A successful stale claim explicitly authorizes another attempt.
    await subscriber._reclaim_pending(cfg, "chan", callback)
    await asyncio.gather(*subscriber._callback_tasks)
    assert calls == 2
    assert not subscriber._delivered_messages


@pytest.mark.parametrize(
    ("idle_ms", "min_idle_ms", "redelivered"),
    [
        pytest.param(60_000, 60_000, True, id="stale"),
        pytest.param(59_999, 60_000, False, id="recently_renewed"),
        pytest.param(60_000, 0, False, id="staleness_disabled"),
    ],
)
async def test_redis_recovery_retries_known_delivery_once_stale(
    idle_ms: int,
    min_idle_ms: int,
    redelivered: bool,
) -> None:
    client = _redis_client_mock()
    client.xpending_range.return_value = [
        {"message_id": b"1-0", "time_since_delivered": idle_ms},
    ]
    client.xrange.return_value = [(b"1-0", {b"payload": b"job"})]
    delivered: list[str | None] = []

    async def callback(message: ReceivedMessageT) -> None:
        delivered.append(message.message_id)
        await message.ack()

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
        min_idle_ms=min_idle_ms,
    )
    # A delivery whose settlement failed stays known but unsettled.
    subscriber._delivered_messages.add(("chan", "1-0"))
    await subscriber._recover_pending("group", subscriber._channels)
    await asyncio.gather(*subscriber._callback_tasks)

    assert delivered == (["1-0"] if redelivered else [])
    assert subscriber._delivered_messages == (set() if redelivered else {("chan", "1-0")})


async def test_redis_recovery_skips_delivery_settled_during_pending_inspection() -> None:
    client = _redis_client_mock()
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": AsyncMock()},
        consumer_name="consumer",
        server=MagicMock(),
    )
    subscriber._delivered_messages.add(("chan", "1-0"))

    async def read_pending(
        _stream: str,
        _group: str,
        **_: object,
    ) -> list[dict[str, bytes | str]]:
        # Redis captured the pending reply before the actor's acknowledgement.
        subscriber._delivered_messages.clear()
        return [{"message_id": b"1-0"}]

    client.xpending_range.side_effect = read_pending
    await subscriber._recover_pending("group", subscriber._channels)
    assert not subscriber._callback_tasks
    client.xrange.assert_not_awaited()


@pytest.mark.parametrize(
    "inspection",
    [pytest.param("metadata", id="metadata"), pytest.param("payload", id="payload")],
)
@pytest.mark.parametrize(
    "settlement",
    [
        pytest.param(RedisReceivedMessage.ack, id="ack"),
        pytest.param(RedisReceivedMessage.nack, id="nack"),
        pytest.param(RedisReceivedMessage.reject, id="reject"),
    ],
)
async def test_redis_recovery_reserves_intake_during_inspection(
    pipeline_mock: tuple[MagicMock, MagicMock],
    inspection: str,
    settlement: _Settle,
) -> None:
    client, pipe = pipeline_mock
    cfg = ChannelConfig(stream="stream", group="group", dlq=None)
    response: _StreamReadResultT = [(b"stream", [(b"1-0", {b"payload": b"job"})])]
    entries = response[0][1]
    client.xpending_range.return_value = [{"message_id": b"1-0"}]
    client.xrange.return_value = entries
    client.xautoclaim.return_value = [b"0-0", entries, cast(list[Any], [])]
    calls = 0

    async def callback(message: ReceivedMessageT) -> None:
        nonlocal calls
        calls += 1
        assert isinstance(message, RedisReceivedMessage)
        await settlement(message)

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": cfg},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
    )

    async def try_concurrent_claim(*_: object, **__: object) -> object:
        # The recovery page already has the stream's available callback slots.
        await subscriber._reclaim_pending(cfg, "chan", callback)
        client.xautoclaim.assert_not_awaited()
        return [{"message_id": b"1-0"}] if inspection == "metadata" else entries

    if inspection == "metadata":
        client.xpending_range.side_effect = try_concurrent_claim
    else:
        client.xrange.side_effect = try_concurrent_claim
    await subscriber._recover_pending("group", subscriber._channels)
    await asyncio.gather(*subscriber._callback_tasks)
    assert calls == 1
    assert not subscriber._intake_requests
    if settlement is RedisReceivedMessage.ack:
        client.xack.assert_awaited_once()
    else:
        pipe.execute.assert_awaited_once()


@pytest.mark.parametrize(
    "closing",
    [
        pytest.param(False, id="recover_while_running"),
        pytest.param(True, id="recover_during_close"),
    ],
)
async def test_redis_recovery_after_empty_scan_or_during_close(
    pipeline_mock: tuple[MagicMock, MagicMock],
    closing: bool,
) -> None:
    client, pipe = pipeline_mock
    pending = [{"message_id": b"1-0"}]
    client.xreadgroup.return_value = cast(list[Any], [])
    client.xpending_range.side_effect = (
        [pending, cast(list[Any], [])] if closing else [[], pending, []]
    )
    client.xrange.return_value = [(b"1-0", {b"payload": b"lost"})]
    delivered: list[str | None] = []

    async def callback(message: ReceivedMessageT) -> None:
        delivered.append(message.message_id)
        await message.ack()

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        server=MagicMock(),
        retry_delay=0.001,
        claim_interval=0,
        block_ms=0,
    )
    subscriber._uncertain_groups.add("group")
    if not closing:
        await subscriber._consume_batch("group", subscriber._channels)
        assert not delivered
        # A server-side assignment can become visible only after the first scan.
        await subscriber._consume_batch("group", subscriber._channels)
        await asyncio.gather(*subscriber._callback_tasks)
        assert delivered == ["1-0"]
    await subscriber.close()
    assert not subscriber._delivered_messages
    assert not subscriber._pending_cursors
    if closing:
        assert not delivered
        pipe.xadd.assert_called_once()
        pipe.xack.assert_called_once()


@pytest.mark.parametrize("block_ms", [0, 30000])
@pytest.mark.parametrize("stop_method", ["close", "cancel"])
async def test_redis_interrupts_only_notification_reads(block_ms: int, stop_method: str) -> None:
    client = _redis_client_mock()
    client.xreadgroup.return_value = cast(list[Any], [])
    waiting = asyncio.Event()

    async def wait_for_activity(**_: object) -> None:
        waiting.set()
        await asyncio.Future()

    client.xread.side_effect = wait_for_activity
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={},
        consumer_name="consumer",
        server=MagicMock(),
        block_ms=block_ms,
        read_timeout=0.01,
    )
    subscriber.start()
    await asyncio.wait_for(waiting.wait(), timeout=1)
    # A finite RPC deadline must not shorten the blocking notification wait.
    await asyncio.sleep(0.02)
    assert not subscriber.task.done()
    if stop_method == "cancel":
        subscriber.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await subscriber.task
    await asyncio.wait_for(subscriber.close(), timeout=1)
    assert not subscriber._notification_tasks
    client.xread.assert_awaited_once_with(streams={"stream": "0-0"}, count=1, block=block_ms)


@pytest.mark.parametrize("pause_before_schedule", [False, True])
async def test_redis_paused_deliveries_wait_for_resume(pause_before_schedule: bool) -> None:
    callback = AsyncMock()
    subscriber = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        channel_concurrency={"chan": Concurrency(messages=1)},
        server=MagicMock(),
    )
    if pause_before_schedule:
        await subscriber.pause()
    await subscriber._process_stream_messages(
        [(b"1-0", {b"payload": b"pending"})],
        "chan",
        "stream",
        callback,
    )
    await subscriber.pause()
    try:
        await asyncio.sleep(0)
        callback.assert_not_awaited()
        assert subscriber.in_flight_count == 1
        await subscriber.resume()
        await asyncio.gather(*subscriber._callback_tasks)
        callback.assert_awaited_once()
        assert subscriber.in_flight_count == 0
    finally:
        await subscriber.close()


@pytest.mark.parametrize("concurrency", [1, 2])
async def test_redis_runner_drains_batch_with_local_concurrency(
    concurrency: int,
) -> None:
    client = _redis_client_mock()
    message_count = concurrency * 2 + 1
    client.xreadgroup.side_effect = [
        [
            (
                b"repid:default",
                [
                    (f"{i}-0".encode(), {b"payload": b"{}", b"headers": b'{"topic":"job"}'})
                    for i in range(message_count)
                ],
            ),
        ],
    ]
    server = RedisServer("redis://localhost")
    server._redis = client
    release = asyncio.Event()
    saturated = asyncio.Event()
    started = finished = 0
    router = Router()

    @router.actor
    async def job() -> None:
        nonlocal started, finished
        started += 1
        if started == concurrency:
            saturated.set()
        await release.wait()
        finished += 1

    runner = _Runner(
        actor_context=ActorExecutionContext(
            server=server,
            publish=AsyncMock(),
            default_serializer=default_serializer,
        ),
        max_tasks=message_count,
        concurrency=Concurrency(messages=concurrency),
    )
    running = asyncio.create_task(
        runner.run(
            channels_to_actors=router._actors_per_channel_address,
            graceful_termination_timeout=1,
        ),
    )
    try:
        await asyncio.wait_for(saturated.wait(), timeout=1)
        release.set()
        await asyncio.wait_for(asyncio.shield(running), timeout=1)
        assert finished == message_count
        assert runner.processed == message_count
    finally:
        release.set()
        runner.stop_consume_event.set()
        await asyncio.wait_for(running, timeout=1)
        await server.disconnect()


@pytest.mark.parametrize("error_type", [RedisConnectionError, ValueError])
async def test_redis_close_interrupts_retry_backoff(error_type: type[Exception]) -> None:
    client = _redis_client_mock()
    client.xreadgroup.side_effect = error_type("Read failed")
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={},
        consumer_name="consumer",
        server=MagicMock(),
        retry_delay=30,
    )
    backing_off = asyncio.Event()
    wait = subscriber._close_event.wait

    async def wait_for_close() -> bool:
        backing_off.set()
        return await wait()

    with patch.object(subscriber._close_event, "wait", side_effect=wait_for_close):
        subscriber.start()
        await asyncio.wait_for(backing_off.wait(), timeout=1)
        await asyncio.wait_for(subscriber.close(), timeout=1)

    client.xreadgroup.assert_awaited_once()
    assert subscriber.task.done()


async def test_redis_consumer_retries_after_backoff() -> None:
    subscriber = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="consumer",
        server=MagicMock(),
        retry_delay=0.001,
    )
    attempts = 0

    async def consume_batch(_group: str, _channels: dict[str, ChannelConfig]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RedisConnectionError("Read failed")
        await subscriber.close()

    with patch.object(subscriber, "_consume_batch", side_effect=consume_batch):
        await asyncio.wait_for(subscriber._consume_group_loop("group", {}), timeout=1)
    assert attempts == 2


@patch.object(message_broker, "Redis")
async def test_redis_subscriber_pause_resume(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    server = RedisServer("redis://localhost")
    await server.connect()

    mock_client.xreadgroup.side_effect = _pending_future

    callback = cast(MessageCallbackT, AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channel_concurrency=channel_limits(("c",)),
            channels_to_callbacks={"c": callback},
        ),
    )

    await sub.pause()
    assert not sub._paused_event.is_set()

    await sub.resume()
    assert sub._paused_event.is_set()

    await sub.close()


@patch.object(message_broker, "Redis")
async def test_redis_subscriber_consume_loop_exceptions(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    server = RedisServer("redis://localhost")
    await server.connect()

    callback = cast(MessageCallbackT, AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channel_concurrency=channel_limits(("c",)),
            channels_to_callbacks={"c": callback},
        ),
    )

    mock_client.xreadgroup.side_effect = [
        ResponseError("Redis error"),
        asyncio.CancelledError,
    ]
    mock_client.xpending_range.return_value = []  # pyrefly: ignore[implicit-any-empty-container]

    with patch.object(sub, "_wait_for_retry", new_callable=AsyncMock) as mock_sleep:
        with contextlib.suppress(asyncio.CancelledError):
            await sub.task

        mock_sleep.assert_awaited_once_with()

    await sub.close()


async def test_redis_received_message_ack(
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    redis_client, _ = pipeline_mock

    msg = make_received_message(
        message_id="1-0",
        channel="chan",
        stream_name="s",
        consumer_group="g",
    )

    assert not msg.is_acted_on
    assert msg.channel == "chan"
    assert msg.message_id == "1-0"

    await msg.ack()

    assert msg.is_acted_on
    assert msg.action == MessageAction.acked
    redis_client.xack.assert_awaited_once_with("s", "g", "1-0")

    redis_client.xack.reset_mock()
    await msg.ack()
    redis_client.xack.assert_not_awaited()


async def test_redis_received_message_nack_with_dlq(
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    _, pipe = pipeline_mock

    msg = make_received_message(
        payload=b"p",
        headers={"h": "v"},
        content_type="c",
        message_id="1-0",
        stream_name="s",
        consumer_group="g",
        dlq_stream="dlq",
    )

    await msg.nack()

    assert msg.is_acted_on
    pipe.xadd.assert_called_once()
    args, _ = pipe.xadd.call_args
    assert args[0] == "dlq"
    assert args[1][b"payload"] == b"p"
    assert args[1][b"original_stream"] == "s"
    assert args[1][b"original_id"] == "1-0"

    pipe.xack.assert_called_once_with("s", "g", "1-0")
    pipe.execute.assert_awaited_once()


async def test_redis_received_message_nack_no_dlq(
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    _, pipe = pipeline_mock

    msg = make_received_message(
        message_id="1-0",
        stream_name="s",
        consumer_group="g",
        dlq_stream=None,
    )

    await msg.nack()

    pipe.xadd.assert_not_called()
    pipe.xack.assert_called_once_with("s", "g", "1-0")
    pipe.execute.assert_awaited_once()


async def test_redis_received_message_reject(
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    _, pipe = pipeline_mock

    msg = make_received_message(
        message_id="1-0",
        stream_name="s",
        consumer_group="g",
        dlq_stream="dlq",
    )

    await msg.reject()

    assert msg.is_acted_on

    pipe.xadd.assert_called_once()
    args, _ = pipe.xadd.call_args
    assert args[0] == "s"

    pipe.xack.assert_called_once_with("s", "g", "1-0")
    pipe.execute.assert_awaited_once()


async def test_redis_received_message_reply(
    pipeline_mock: tuple[MagicMock, MagicMock],  # noqa: ARG001
    make_received_message: Any,
) -> None:
    msg = make_received_message()
    with pytest.raises(NotImplementedError, match="Redis does not support native replies"):
        await msg.reply(
            payload=b"resp",
            headers={"r": "h"},
            content_type="rc",
            channel="reply_chan",
        )


@patch.object(message_broker, "Redis")
async def test_redis_subscriber_process_stream_messages_callback_error(
    mock_redis_cls: MagicMock,
) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = _pending_future

    server = RedisServer("redis://localhost")
    await server.connect()

    callback = AsyncMock(side_effect=ResponseError("Callback error"))

    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channel_concurrency=channel_limits(("chan",)),
            channels_to_callbacks={
                "chan": cast(
                    MessageCallbackT,
                    callback,
                ),
            },
        ),
    )

    msg = MagicMock(spec=RedisReceivedMessage, is_acted_on=False, nack=AsyncMock())

    await sub._run_callback(
        cast(MessageCallbackT, callback),
        cast(RedisReceivedMessage, msg),
        "1-0",
    )

    callback.assert_awaited_once()
    msg.nack.assert_awaited_once()
    assert not sub._in_flight_messages

    await sub.close()


async def test_redis_received_message_properties_and_ack_acted_on(
    make_received_message: Any,
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    redis_client, _ = pipeline_mock
    msg = make_received_message(headers={"h": "v"}, content_type="c")
    assert msg.headers == {"h": "v"}
    assert msg.content_type == "c"

    msg_with_meta = make_received_message(reply_to="reply-1")
    assert msg_with_meta.reply_to == "reply-1"

    msg._action = MessageAction.acked
    await msg.ack()
    redis_client.xack.assert_not_called()


@pytest.mark.parametrize(
    "action",
    [
        pytest.param(RedisReceivedMessage.nack, id="nack"),
        pytest.param(RedisReceivedMessage.reject, id="reject"),
        pytest.param(partial(RedisReceivedMessage.reply, payload=b"r"), id="reply"),
    ],
)
async def test_redis_received_message_already_acted_guard(
    make_received_message: Any,
    pipeline_mock: tuple[MagicMock, MagicMock],
    action: _Settle,
) -> None:
    redis_client, _ = pipeline_mock
    msg = make_received_message()
    msg._action = MessageAction.acked
    await action(msg)
    redis_client.pipeline.assert_not_called()


async def test_redis_ack_allows_keep_alive_but_blocks_other_settlements(
    make_received_message: Any,
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    redis_client, pipe = pipeline_mock
    msg = make_received_message(message_id="1-0", stream_name="s", consumer_group="g")

    xack_started = asyncio.Event()
    release_xack = asyncio.Event()

    async def block_xack(*_: object) -> None:
        xack_started.set()
        await release_xack.wait()

    redis_client.xack.side_effect = block_xack

    ack_task = asyncio.create_task(msg.ack())
    await xack_started.wait()
    other_tasks = [
        asyncio.create_task(msg.nack()),
        asyncio.create_task(msg.reject()),
        asyncio.create_task(msg.keep_alive()),
    ]
    await asyncio.gather(*other_tasks)
    release_xack.set()
    await ack_task

    assert msg.action is MessageAction.acked
    redis_client.xack.assert_awaited_once_with("s", "g", "1-0")
    pipe.xack.assert_not_called()
    pipe.xadd.assert_not_called()
    redis_client.xclaim.assert_awaited_once()


async def test_redis_cancelled_settlement_stays_reserved_and_blocks_retry(
    make_received_message: Any,
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    redis_client, _ = pipeline_mock
    msg = make_received_message()

    xack_started = asyncio.Event()
    release_xack = asyncio.Event()

    async def block_xack(*_: object) -> None:
        xack_started.set()
        await release_xack.wait()

    redis_client.xack.side_effect = block_xack
    task = asyncio.create_task(msg.ack())
    await xack_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Cancellation may have left the RPC in flight, so the settlement
    # stays reserved and a retry must not issue a second operation.
    assert msg.action is MessageAction.acked
    redis_client.xack.side_effect = None
    await msg.ack()
    redis_client.xack.assert_awaited_once_with("s", "g", "1-0")
    assert msg.action is MessageAction.acked


async def test_redis_consume_batch_when_closed() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()
    sub = RedisSubscriber(
        redis_client=client,
        channels={},
        callbacks={},
        consumer_name="c",
        server=server,
    )
    sub._closed = True
    await sub._consume_batch("g", {})
    client.xreadgroup.assert_not_awaited()


async def test_redis_consume_batch_empty_result() -> None:
    server = RedisServer("redis://localhost")
    client = _redis_client_mock()
    client.xreadgroup.return_value = cast(list[Any], [])

    sub = RedisSubscriber(
        redis_client=client,
        channels={"c": ChannelConfig(stream="s", group="g", dlq=None)},
        callbacks={},
        consumer_name="c",
        server=server,
    )

    await sub._consume_batch("g", {"c": ChannelConfig(stream="s", group="g", dlq=None)})
    assert client.xreadgroup.await_count == 2
    client.xread.assert_awaited_once_with(streams={"s": "0-0"}, count=1, block=5000)


async def test_redis_consume_batch_unknown_stream_and_missing_callback() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()
    client.xreadgroup.return_value = [[b"unknown_stream", [(b"1", {b"payload": b"p"})]]]

    chan_cfg = ChannelConfig(stream="known_stream", group="g", dlq=None)
    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={"chan": AsyncMock()},
        consumer_name="c",
        server=server,
    )

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._consume_batch("g", {"chan": chan_cfg})
        mock_process.assert_not_awaited()

        client.xreadgroup.reset_mock()
        client.xreadgroup.return_value = [[b"known_stream", [(b"1", {b"payload": b"p"})]]]

        sub._callbacks = {}

        await sub._consume_batch("g", {"chan": chan_cfg})

        mock_process.assert_not_awaited()


async def test_redis_run_callback_exception() -> None:
    server = RedisServer("redis://localhost")

    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        server=server,
    )

    callback = AsyncMock(side_effect=ResponseError("Error"))
    msg = MagicMock(spec=RedisReceivedMessage, is_acted_on=False, nack=AsyncMock())

    await sub._run_callback(callback, msg, "1")

    callback.assert_awaited_once()
    msg.nack.assert_awaited_once()

    callback.reset_mock()
    msg.reset_mock()
    msg.is_acted_on = True

    await sub._run_callback(callback, msg, "1")
    msg.nack.assert_not_awaited()


@patch.object(message_broker, "Redis")
async def test_redis_subscribe_no_dlq_strategy(mock_redis_cls: MagicMock) -> None:
    server = RedisServer("redis://localhost", dlq_stream_strategy=None)
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = asyncio.CancelledError

    await server.connect()
    sub = await server.subscribe(
        channel_concurrency=channel_limits(("c",)),
        channels_to_callbacks={"c": AsyncMock()},
    )

    await sub.close()


async def test_redis_ensure_consumer_group_no_redis() -> None:
    server = RedisServer("redis://localhost")
    server._redis = None
    await server._ensure_consumer_group("s", "g")


@patch.object(message_broker, "Redis")
async def test_redis_subscriber_task_done_removes_from_active(mock_redis_cls: MagicMock) -> None:
    server = RedisServer("redis://localhost")
    mock_client = _redis_client_mock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = asyncio.CancelledError

    await server.connect()
    sub = await server.subscribe(
        channel_concurrency=channel_limits(("c",)),
        channels_to_callbacks={"c": AsyncMock()},
    )
    with contextlib.suppress(asyncio.CancelledError):
        await sub.task

    await asyncio.sleep(0)
    assert sub not in server._active_subscribers
    await sub.close()
    await sub.close()


async def test_redis_ensure_consumer_group_busy() -> None:
    server = RedisServer("redis://localhost")
    server._redis = AsyncMock()
    server._redis.xgroup_create.side_effect = ResponseError(
        "BUSYGROUP Consumer Group name already exists",
    )
    await server._ensure_consumer_group("s", "g")

    server._redis.xgroup_create.side_effect = ResponseError("OTHER ERROR")
    with pytest.raises(ResponseError, match="OTHER ERROR"):
        await server._ensure_consumer_group("s", "g")


async def test_redis_server_not_connected_publish_subscribe() -> None:
    server = RedisServer("redis://localhost")
    assert not server.is_connected
    with pytest.raises(ConnectionError, match="Not connected"):
        await server.publish(channel="c", message=MagicMock())
    with pytest.raises(ConnectionError, match="Not connected"):
        await server.subscribe(
            channel_concurrency=channel_limits(()),
            channels_to_callbacks={},
        )


async def test_redis_received_message_nack(
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    _, pipe = pipeline_mock

    msg = make_received_message(dlq_stream="dlq")
    await msg.nack()
    pipe.xadd.assert_called()
    pipe.execute.assert_awaited()

    pipe.reset_mock()
    msg_no_dlq = make_received_message(dlq_stream=None)
    await msg_no_dlq.nack()
    pipe.xadd.assert_not_called()
    pipe.execute.assert_awaited()


@patch.object(message_broker, "Redis")
async def test_redis_subscribe_multi_channel_groups(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    # Suspend the read loop so the test can inspect subscriber state synchronously.
    mock_client.xreadgroup.side_effect = _pending_future

    server = RedisServer("redis://localhost")
    await server.connect()

    cb_a = cast(MessageCallbackT, AsyncMock())
    cb_b = cast(MessageCallbackT, AsyncMock())

    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channel_concurrency=channel_limits(("alpha", "beta")),
            channels_to_callbacks={"alpha": cb_a, "beta": cb_b},
        ),
    )

    assert mock_client.xgroup_create.call_count == 2
    create_calls = {call.args[1] for call in mock_client.xgroup_create.call_args_list}
    assert "repid:alpha:group" in create_calls
    assert "repid:beta:group" in create_calls

    assert "alpha" in sub._channels
    assert "beta" in sub._channels
    assert sub._channels["alpha"].group == "repid:alpha:group"
    assert sub._channels["beta"].group == "repid:beta:group"
    assert sub._channels["alpha"].stream == "repid:alpha"
    assert sub._channels["beta"].stream == "repid:beta"

    await sub.close()


async def test_redis_received_message_nack_with_dlq_maxlen(
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    _, pipe = pipeline_mock

    msg = make_received_message(
        payload=b"p",
        message_id="1-0",
        stream_name="s",
        consumer_group="g",
        dlq_stream="dlq",
        dlq_maxlen=500,
    )

    await msg.nack()

    assert msg.is_acted_on
    pipe.xadd.assert_called_once()
    _, kwargs = pipe.xadd.call_args
    assert kwargs["maxlen"] == 500
    assert kwargs["approximate"] is True
    pipe.xack.assert_called_once_with("s", "g", "1-0")
    pipe.execute.assert_awaited_once()


def test_redis_server_stream_name_for() -> None:
    server = RedisServer("redis://localhost")
    assert server.stream_name_for("chan") == "repid:chan"

    custom = RedisServer("redis://localhost", stream_name_strategy=lambda c: f"custom:{c}")
    assert custom.stream_name_for("mychan") == "custom:mychan"


async def test_redis_subscriber_in_flight_count() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        server=RedisServer("redis://localhost"),
    )
    assert sub.in_flight_count == 0
    sub._in_flight_messages.add(("jobs", "1-0"))
    assert sub.in_flight_count == 1
    sub._in_flight_messages.discard(("jobs", "1-0"))
    assert sub.in_flight_count == 0


async def test_redis_subscriber_start_with_claim_interval() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        server=RedisServer("redis://localhost"),
        claim_interval=60.0,
    )
    assert sub._claim_task is None
    sub.start()
    assert sub._claim_task is not None
    await sub.close()
    assert sub._claim_task.done()


async def test_redis_subscriber_close_with_active_claim_task() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": AsyncMock()},
        consumer_name="c",
        server=RedisServer("redis://localhost"),
        claim_interval=60.0,
    )
    sub.start()

    # Let the claim loop begin its periodic wait.
    await asyncio.sleep(0)

    assert sub._claim_task is not None
    assert not sub._claim_task.done()  # still sleeping

    await sub.close()
    assert sub._claim_task.done()


async def test_redis_consume_group_loop_unexpected_exception() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        server=RedisServer("redis://localhost"),
    )

    call_count = 0

    async def batch_effect(*_: Any, **__: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("Unexpected")
        raise asyncio.CancelledError

    with (
        patch.object(sub, "_consume_batch", side_effect=batch_effect),
        patch.object(sub, "_wait_for_retry", new_callable=AsyncMock),
    ):
        await sub._consume_group_loop("g", {})

    assert call_count == 2


async def test_redis_claim_loop_single_iteration() -> None:
    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()

    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(MessageCallbackT, callback),
        },
        consumer_name="c",
        server=RedisServer("redis://localhost"),
        claim_interval=0.01,
    )

    reclaim_calls = 0

    async def fake_reclaim(cfg: Any, ch: Any, cb: Any) -> None:  # noqa: ARG001
        nonlocal reclaim_calls
        reclaim_calls += 1
        sub._closed = True  # stop after first call

    with (
        patch.object(sub, "_reclaim_pending", side_effect=fake_reclaim),
        patch.object(sub, "_wait_for_close", new_callable=AsyncMock),
    ):
        await sub._claim_loop()

    assert reclaim_calls == 1


async def test_redis_claim_loop_stops_on_closed_after_sleep() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        server=RedisServer("redis://localhost"),
        claim_interval=0.01,
    )

    async def set_closed(_: float) -> None:
        sub._closed = True

    with patch.object(sub, "_wait_for_close", side_effect=set_closed):
        await sub._claim_loop()


async def test_redis_claim_loop_callback_none() -> None:
    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)

    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"chan": chan_cfg},
        callbacks={},  # no callback registered for "chan"
        consumer_name="c",
        server=RedisServer("redis://localhost"),
        claim_interval=0.01,
    )

    sleep_count = 0

    async def mock_sleep(_: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:  # stop after second sleep (past first full iteration)
            sub._closed = True

    with (
        patch.object(sub, "_reclaim_pending", new_callable=AsyncMock) as mock_reclaim,
        patch.object(sub, "_wait_for_close", side_effect=mock_sleep),
    ):
        await sub._claim_loop()

    mock_reclaim.assert_not_awaited()


async def test_redis_claim_loop_redis_error() -> None:
    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()

    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(MessageCallbackT, callback),
        },
        consumer_name="c",
        server=RedisServer("redis://localhost"),
        claim_interval=0.01,
    )

    async def failing_reclaim(cfg: Any, ch: Any, cb: Any) -> None:  # noqa: ARG001
        sub._closed = True
        raise ConnectionError("Redis down")

    with (
        patch.object(sub, "_reclaim_pending", side_effect=failing_reclaim),
        patch.object(sub, "_wait_for_close", new_callable=AsyncMock),
    ):
        await sub._claim_loop()  # should not propagate the ConnectionError


async def test_redis_reclaim_pending_single_batch() -> None:
    client = AsyncMock()
    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()

    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(MessageCallbackT, callback),
        },
        consumer_name="consumer1",
        server=RedisServer("redis://localhost"),
        min_idle_ms=5000,
    )

    # "0-0" as next_id means scan complete; return one message
    client.xautoclaim.return_value = [b"0-0", [(b"1-0", {b"payload": b"p"})], cast(list[Any], [])]

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._reclaim_pending(chan_cfg, "chan", callback)

    client.xautoclaim.assert_awaited_once_with(
        "s",
        "g",
        "consumer1",
        min_idle_time=5000,
        start_id="0-0",
        count=10,
    )
    mock_process.assert_awaited_once()


async def test_redis_reclaim_pending_multi_batch() -> None:
    client = AsyncMock()
    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()

    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(MessageCallbackT, callback),
        },
        consumer_name="c",
        server=RedisServer("redis://localhost"),
    )

    # First batch: non-zero next_id (bytes) with messages
    # Second batch: "0-0" with no messages → stop
    client.xautoclaim.side_effect = [
        [b"5-0", [(b"1-0", {b"payload": b"a"})], cast(list[Any], [])],
        ["0-0", cast(list[Any], []), cast(list[Any], [])],
    ]

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._reclaim_pending(chan_cfg, "chan", callback)

    assert client.xautoclaim.await_count == 2
    # Second xautoclaim used "5-0" (decoded from bytes) as start_id
    second_call_kwargs = client.xautoclaim.call_args_list[1][1]
    assert second_call_kwargs["start_id"] == "5-0"
    # _process_stream_messages only called for first batch (had messages)
    mock_process.assert_awaited_once()


async def test_redis_reclaim_resumes_after_intake_fills() -> None:
    client = AsyncMock()
    client.xautoclaim.side_effect = [
        [b"2-0", [(b"1-0", {b"payload": b"first"})], []],  # pyrefly: ignore[implicit-any-empty-container]
        [b"0-0", [(b"2-0", {b"payload": b"second"})], []],  # pyrefly: ignore[implicit-any-empty-container]
        [b"0-0", [], []],  # pyrefly: ignore[implicit-any-empty-container]
    ]
    config = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()
    sub = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": callback},
        consumer_name="worker",
        channel_concurrency={"jobs": Concurrency(messages=1)},
        server=RedisServer("redis://localhost"),
    )
    for _ in range(3):
        await sub._reclaim_pending(config, "jobs", callback)
        await asyncio.gather(*sub._callback_tasks)

    assert [call.kwargs["start_id"] for call in client.xautoclaim.call_args_list] == [
        "0-0",
        "2-0",
        "0-0",
    ]
    assert [call.args[0].message_id for call in callback.call_args_list] == ["1-0", "2-0"]


@patch.object(message_broker, "Redis")
async def test_redis_subscribe_consumer_group_start_id(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = _pending_future

    server = RedisServer("redis://localhost", consumer_group_start_id="$")
    await server.connect()

    sub = await server.subscribe(
        channel_concurrency=channel_limits(("chan",)),
        channels_to_callbacks={"chan": AsyncMock()},
    )

    mock_client.xgroup_create.assert_awaited_once_with(
        "repid:chan",
        "repid:chan:group",
        id="$",
        mkstream=True,
    )
    await sub.close()


@patch.object(message_broker, "Redis")
async def test_redis_subscribe_dlq_maxlen(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = _pending_future

    server = RedisServer(
        "redis://localhost",
        dlq_maxlen=1000,
        claim_interval=30.0,
        min_idle_ms=2000,
        read_timeout=0.125,
    )
    await server.connect()

    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channel_concurrency=channel_limits(("chan",)),
            channels_to_callbacks={"chan": AsyncMock()},
        ),
    )

    assert sub._channels["chan"].dlq_maxlen == 1000
    assert sub._claim_interval == 30.0
    assert sub._min_idle_ms == 2000
    assert sub._read_timeout == 0.125
    await sub.close()


@pytest.mark.parametrize(
    ("dsn", "expected_host", "expected_pathname"),
    [
        pytest.param("redis://localhost:6379/1", "localhost:6379", "/1", id="with_port_and_path"),
        pytest.param("redis://localhost", "localhost", None, id="no_port_no_path"),
        pytest.param("redis://localhost/", "localhost", None, id="root_path"),
        pytest.param("redis://localhost/db", "localhost", "/db", id="no_port_with_path"),
    ],
)
def test_redis_server_init_dsn_parsing(
    dsn: str,
    expected_host: str,
    expected_pathname: str | None,
) -> None:
    server = RedisServer(dsn)
    assert server.host == expected_host
    assert server.pathname == expected_pathname


async def test_redis_server_disconnect_when_not_connected() -> None:
    server = RedisServer("redis://localhost")
    assert not server.is_connected
    await server.disconnect()
    assert not server.is_connected


async def test_redis_consume_batch_str_stream_name() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()
    # Stream name returned as str (not bytes) — exercises the non-decode branch.
    client.xreadgroup.return_value = [["repid:chan", [(b"1", {b"payload": b"p"})]]]

    callback = AsyncMock()
    chan_cfg = ChannelConfig(stream="repid:chan", group="g", dlq=None)
    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(MessageCallbackT, callback),
        },
        consumer_name="c",
        server=server,
    )

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._consume_batch("g", {"chan": chan_cfg})

    mock_process.assert_awaited_once()


async def test_redis_process_stream_messages_str_msg_id() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()

    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()

    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(MessageCallbackT, callback),
        },
        consumer_name="c",
        server=server,
    )

    # msg_id as str (not bytes) — exercises the non-decode branch.
    await sub._process_stream_messages(
        [("1-0", {b"payload": b"hello"})],
        "chan",
        "s",
        cast(MessageCallbackT, callback),
    )

    await asyncio.sleep(0)

    callback.assert_awaited_once()
    msg = callback.call_args[0][0]
    assert isinstance(msg, RedisReceivedMessage)
    assert msg.message_id == "1-0"


async def test_redis_reclaim_pending_closed_mid_iteration() -> None:
    client = AsyncMock()
    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()

    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(MessageCallbackT, callback),
        },
        consumer_name="c",
        server=RedisServer("redis://localhost"),
    )

    call_count = 0

    async def autoclaim_stop_after_first(*_: Any, **__: Any) -> list[Any]:
        nonlocal call_count
        call_count += 1
        sub._closed = True
        # Return non-zero next_id so the loop would continue if _closed weren't checked.
        return [b"5-0", [(b"1-0", {b"payload": b"p"})], []]

    client.xautoclaim.side_effect = autoclaim_stop_after_first

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._reclaim_pending(chan_cfg, "chan", callback)

    assert call_count == 1
    mock_process.assert_awaited_once()


async def test_redis_consume_loop_groups_channels_by_group() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={
            "chan_a": ChannelConfig(stream="repid:chan_a", group="shared:group", dlq=None),
            "chan_b": ChannelConfig(stream="repid:chan_b", group="shared:group", dlq=None),
        },
        callbacks={},
        consumer_name="c",
        server=RedisServer("redis://localhost"),
    )

    loop_calls: list[tuple[str, set[str]]] = []

    async def fake_group_loop(group_name: str, group_channels: dict[str, ChannelConfig]) -> None:
        loop_calls.append((group_name, set(group_channels.keys())))

    with patch.object(sub, "_consume_group_loop", side_effect=fake_group_loop):
        await sub._consume_loop()

    assert len(loop_calls) == 1
    assert loop_calls[0] == ("shared:group", {"chan_a", "chan_b"})


@patch.object(message_broker, "Redis")
async def test_redis_publish_maxlen_default_approximate(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost")
    await server.connect()

    msg = RedisSentMessage(payload=b"test")
    await server.publish(
        channel="test",
        message=msg,
        server_specific_parameters={"maxlen": 50},
    )

    _, kwargs = mock_client.xadd.call_args
    assert kwargs["maxlen"] == 50
    assert kwargs["approximate"] is True


@pytest.mark.parametrize(
    "error_type",
    [
        pytest.param(ConnectionError, id="builtin_connection_error"),
        pytest.param(TimeoutError, id="builtin_timeout_error"),
        pytest.param(RedisConnectionError, id="redis_connection_error"),
        pytest.param(RedisTimeoutError, id="redis_timeout_error"),
    ],
)
async def test_redis_consume_group_loop_retries_redis_transient_errors(
    error_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
) -> None:
    subscriber = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="consumer",
        server=MagicMock(),
    )
    calls = 0

    async def consume_batch(*_: Any, **__: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error_type("temporary")
        subscriber._closed = True

    with (
        patch.object(subscriber, "_consume_batch", side_effect=consume_batch),
        patch.object(subscriber, "_wait_for_retry", new_callable=AsyncMock) as sleep,
    ):
        await subscriber._consume_group_loop("group", {})

    assert calls == 2
    sleep.assert_awaited_once_with()
    assert any(record.getMessage() == "consumer.error.redis" for record in caplog.records)


@pytest.mark.parametrize(
    "error_type",
    [
        pytest.param(RedisConnectionError, id="redis_connection_error"),
        pytest.param(RedisTimeoutError, id="redis_timeout_error"),
    ],
)
async def test_redis_claim_loop_retries_redis_transient_errors(
    error_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = ChannelConfig(stream="jobs", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        server=MagicMock(),
        claim_interval=1.0,
    )
    calls = 0

    async def reclaim(*_: Any, **__: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error_type("temporary")
        subscriber._closed = True

    with (
        patch.object(subscriber, "_reclaim_pending", side_effect=reclaim),
        patch.object(subscriber, "_wait_for_close", new_callable=AsyncMock),
    ):
        await subscriber._claim_loop()

    assert calls == 2
    assert any(record.getMessage() == "consumer.reclaim.error" for record in caplog.records)


@pytest.mark.parametrize(
    ("error_type", "retry_attempts", "expected_attempts"),
    [
        pytest.param(RedisConnectionError, 3, 2, id="connection_error_retries"),
        pytest.param(RedisTimeoutError, 3, 2, id="timeout_error_retries"),
        pytest.param(RedisConnectionError, 0, 1, id="zero_retries"),
    ],
)
@patch.object(message_broker, "Redis")
async def test_redis_server_connect_uses_async_retry(
    mock_redis_cls: MagicMock,
    error_type: type[Exception],
    retry_attempts: int,
    expected_attempts: int,
) -> None:
    client = MagicMock(ping=AsyncMock())
    mock_redis_cls.from_url.return_value = client
    server = RedisServer("redis://localhost", retry_attempts=retry_attempts)

    await server.connect()

    retry = mock_redis_cls.from_url.call_args.kwargs["retry"]
    operation = AsyncMock(side_effect=[error_type("transient"), 42])
    on_failure = AsyncMock()
    if retry_attempts == 0:
        with pytest.raises(error_type, match="transient"):
            await retry.call_with_retry(operation, on_failure)
    else:
        assert await retry.call_with_retry(operation, on_failure) == 42

    assert operation.await_count == expected_attempts
    on_failure.assert_awaited_once()


@pytest.mark.parametrize(
    "action",
    [
        pytest.param(RedisReceivedMessage.ack, id="ack"),
        pytest.param(RedisReceivedMessage.nack, id="nack"),
        pytest.param(RedisReceivedMessage.reject, id="reject"),
    ],
)
async def test_redis_failed_confirmation_can_be_retried(
    action: _Settle,
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    client, pipe = pipeline_mock
    client.xack.side_effect = [ConnectionError("offline"), None]
    pipe.execute.side_effect = [ConnectionError("offline"), None]
    message = make_received_message()

    with pytest.raises(ConnectionError, match="offline"):
        await action(message)
    assert not message.is_acted_on
    await action(message)
    assert message.is_acted_on


async def test_redis_reclaim_deduplicates_only_within_the_same_channel() -> None:
    subscriber = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={
            channel: ChannelConfig(stream=channel, group="group", dlq=None)
            for channel in ("first", "second")
        },
        callbacks={},
        consumer_name="consumer",
        server=MagicMock(),
    )
    started: list[str] = []
    release = asyncio.Event()

    async def callback(message: ReceivedMessageT) -> None:
        started.append(message.channel)
        await message.ack()
        await release.wait()

    try:
        for channel in ("first", "second", "first"):
            await subscriber._process_stream_messages(
                [(b"1-0", {b"payload": b"x"})],
                channel,
                channel,
                callback,
            )
        await asyncio.sleep(0)
        assert started == ["first", "second"]
        assert subscriber.in_flight_count == 2
    finally:
        release.set()
        await subscriber.close()
    assert subscriber.in_flight_count == 0


@pytest.mark.parametrize(
    ("concurrency", "channel_concurrency", "expected"),
    [
        (Concurrency(messages=4), {}, 4),
        (Concurrency(), {}, 10),
        (Concurrency(messages=1000), {"a": Concurrency(messages=3)}, 3),
    ],
)
async def test_native_channel_count_hints(
    concurrency: Concurrency,
    channel_concurrency: dict[str, Concurrency],
    expected: int,
) -> None:
    client = _redis_client_mock()
    read_counts: asyncio.Queue[int] = asyncio.Queue()

    async def read_group(*, count: int, streams: dict[str, str], **_: object) -> _StreamReadResultT:
        if ">" in streams.values():
            read_counts.put_nowait(count)
        return []

    client.xreadgroup.side_effect = read_group
    server = RedisServer("redis://localhost", batch_size=10)
    server._redis = client
    subscriber = await server.subscribe(
        channels_to_callbacks={"a": AsyncMock()},
        channel_concurrency=channel_limits(("a",), concurrency, channel_concurrency),
    )
    try:
        assert await asyncio.wait_for(read_counts.get(), 1) == expected
    finally:
        await subscriber.close()


async def test_saturated_redis_stream_does_not_block_shared_group() -> None:
    client = _redis_client_mock()
    first_started = asyncio.Event()
    other_started = asyncio.Event()
    release = asyncio.Event()
    notification_started = asyncio.Event()
    calls: list[dict[str, str]] = []

    async def read_group(*, streams: dict[str, str], **_: object) -> _StreamReadResultT:
        calls.append(streams)
        if len(calls) == 1:
            return [(b"repid:a", [(b"1-0", {b"payload": b"a"})])]
        if len(calls) == 2 and "repid:a" not in streams:
            return [(b"repid:b", [(b"2-0", {b"payload": b"b"})])]
        return []

    async def read_activity(**_: Any) -> None:
        notification_started.set()
        await asyncio.Event().wait()

    async def blocked(_message: ReceivedMessageT) -> None:
        first_started.set()
        await release.wait()

    client.xreadgroup.side_effect = read_group
    client.xread.side_effect = read_activity
    server = RedisServer("redis://localhost", consumer_group_strategy=lambda _: "shared")
    server._redis = client
    subscriber = await server.subscribe(
        channels_to_callbacks={
            "a": blocked,
            "b": AsyncMock(side_effect=lambda _: other_started.set()),  # pyrefly: ignore[implicit-any-lambda]
        },
        channel_concurrency=channel_limits(("a", "b"), Concurrency(messages=1)),
    )
    try:
        await asyncio.wait_for(first_started.wait(), 1)
        await asyncio.wait_for(other_started.wait(), 1)
        await asyncio.wait_for(notification_started.wait(), 1)
        assert calls[1] == {"repid:b": ">"}
        release.set()
        async with asyncio.timeout(1):
            while not any("repid:a" in streams for streams in calls[2:]):
                await asyncio.sleep(0)
    finally:
        release.set()
        await subscriber.close()


async def test_read_and_reclaim_share_available_callback_slots() -> None:
    read_started = asyncio.Event()
    finish_read = asyncio.Event()
    finish_callback = asyncio.Event()
    started: asyncio.Queue[str | None] = asyncio.Queue()
    client = _redis_client_mock()
    client.xautoclaim.return_value = [b"0-0", [(b"2-0", {b"payload": b"stale"})], []]  # pyrefly: ignore[implicit-any-empty-container]

    async def read(**_: object) -> _StreamReadResultT:
        read_started.set()
        await finish_read.wait()
        return [(b"stream", [(b"1-0", {b"payload": b"fresh"})])]

    async def callback(message: ReceivedMessageT) -> None:
        started.put_nowait(message.message_id)
        await finish_callback.wait()
        await message.ack()

    client.xreadgroup.side_effect = read
    cfg = ChannelConfig(stream="stream", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": cfg},
        callbacks={"chan": callback},
        consumer_name="consumer",
        channel_concurrency={"chan": Concurrency(messages=2)},
        server=MagicMock(),
    )
    reading = asyncio.create_task(subscriber._consume_batch("group", subscriber._channels))
    try:
        await asyncio.wait_for(read_started.wait(), 1)
        await subscriber._reclaim_pending(cfg, "chan", callback)
        client.xautoclaim.assert_not_awaited()
        finish_read.set()
        await reading
        assert await asyncio.wait_for(started.get(), 1) == "1-0"
        await subscriber._reclaim_pending(cfg, "chan", callback)
        assert await asyncio.wait_for(started.get(), 1) == "2-0"
        assert client.xautoclaim.await_args.kwargs["count"] == 1
        assert subscriber.in_flight_count == 2
    finally:
        finish_read.set()
        finish_callback.set()
        await asyncio.gather(reading, return_exceptions=True)
        await subscriber.close()


async def test_pending_recovery_skips_saturated_stream_and_resumes_after_release() -> None:
    release = asyncio.Event()
    delivered: asyncio.Queue[bytes] = asyncio.Queue()
    client = _redis_client_mock()
    client.xpending_range.side_effect = [[], [{"message_id": b"2-0"}], []]  # pyrefly: ignore[implicit-any-empty-container]
    client.xrange.return_value = [(b"2-0", {b"payload": b"recovered"})]
    client.xreadgroup.side_effect = [
        [(b"b", [(b"1-0", {b"payload": b"other"})])],
        [],  # pyrefly: ignore[implicit-any-empty-container]
    ]

    async def callback(message: ReceivedMessageT) -> None:
        if message.payload == b"held":
            await release.wait()
        else:
            delivered.put_nowait(message.payload)
        await message.ack()

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={name: ChannelConfig(stream=name, group="group", dlq=None) for name in ("a", "b")},
        callbacks={"a": callback, "b": callback},
        consumer_name="consumer",
        channel_concurrency={"a": Concurrency(messages=1)},
        server=MagicMock(),
    )
    subscriber._uncertain_groups.add("group")
    await subscriber._process_stream_messages(
        [(b"1-0", {b"payload": b"held"})],
        "a",
        "a",
        callback,
    )
    try:
        await subscriber._consume_batch("group", subscriber._channels)
        assert await asyncio.wait_for(delivered.get(), 1) == b"other"
        assert client.xpending_range.await_args.args[:2] == ("b", "group")
        assert client.xreadgroup.await_args.kwargs["streams"] == {"b": ">"}
        release.set()
        await asyncio.gather(*subscriber._callback_tasks)
        await subscriber._consume_batch("group", subscriber._channels)
        assert await asyncio.wait_for(delivered.get(), 1) == b"recovered"
        assert client.xpending_range.await_args_list[1].kwargs["count"] == 1
    finally:
        release.set()
        client.xpending_range.side_effect = None
        await subscriber.close()


async def test_pending_recovery_rewinds_stream_independently_of_saturated_peer() -> None:
    recovered: list[str | None] = []
    client = _redis_client_mock()
    pending_pages: list[list[dict[str, bytes]]] = [
        [{"message_id": b"2-0"}],
        [],
        [{"message_id": b"1-0"}],
    ]
    client.xpending_range.side_effect = pending_pages
    client.xrange.side_effect = [
        [(b"2-0", {b"payload": b"first"})],
        [(b"1-0", {b"payload": b"late claim"})],
    ]

    async def callback(message: ReceivedMessageT) -> None:
        recovered.append(message.message_id)
        await message.ack()

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={name: ChannelConfig(stream=name, group="group", dlq=None) for name in ("a", "b")},
        callbacks={"a": callback, "b": callback},
        consumer_name="consumer",
        channel_concurrency={"a": Concurrency(messages=1)},
        server=MagicMock(),
    )
    subscriber._callback_counts["a"] = 1

    for _ in range(3):
        await subscriber._recover_pending("group", subscriber._channels)
        await asyncio.gather(*subscriber._callback_tasks)

    assert recovered == ["2-0", "1-0"]
    assert [
        (call.args[0], call.kwargs["min"]) for call in client.xpending_range.await_args_list
    ] == [("b", "(0-0"), ("b", "(2-0"), ("b", "(0-0")]


@pytest.mark.parametrize(
    "stop",
    [
        pytest.param(RedisSubscriber.pause, id="pause"),
        pytest.param(RedisSubscriber.close, id="close"),
    ],
)
async def test_stopping_during_pending_inspection_prevents_a_fresh_read(
    stop: Callable[[RedisSubscriber], Awaitable[None]],
) -> None:
    client = _redis_client_mock()
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": AsyncMock()},
        consumer_name="consumer",
        server=RedisServer("redis://localhost"),
    )
    subscriber._uncertain_groups.add("group")

    async def inspect(*_: object, **__: object) -> list[object]:
        await stop(subscriber)
        return []

    client.xpending_range.side_effect = inspect
    await subscriber._consume_batch("group", subscriber._channels)
    client.xreadgroup.assert_not_awaited()
    client.xread.assert_not_awaited()
    await subscriber.close()


async def test_paused_delivery_accepts_one_argument_callback_and_closes_renewal() -> None:
    renewed: asyncio.Queue[asyncio.Task[object] | None] = asyncio.Queue()
    seen: list[str | None] = []

    async def callback(message: ReceivedMessageT) -> None:
        seen.append(message.message_id)
        await message.ack()

    def record_renewal(*_: object, **__: object) -> list[bytes]:
        renewed.put_nowait(asyncio.current_task())
        return [b"1-0"]

    client = _redis_client_mock()
    client.xclaim.side_effect = record_renewal
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        min_idle_ms=30,
        server=RedisServer("redis://localhost"),
    )
    await subscriber.pause()
    try:
        await subscriber._process_stream_messages(
            [(b"1-0", {b"payload": b"held"})],
            "chan",
            "stream",
            callback,
        )
        renewal_task = await asyncio.wait_for(renewed.get(), 1)
        assert not seen
        await subscriber.resume()
        await asyncio.gather(*subscriber._callback_tasks)
        assert seen == ["1-0"]
        client.xack.assert_awaited_once()
        assert renewal_task is not None
        assert renewal_task.done()
    finally:
        await subscriber.close()


async def test_paused_delivery_hands_lease_to_worker_without_extra_renewal() -> None:
    renewals_at_callback: list[int] = []

    async def callback(message: ReceivedMessageT) -> None:
        renewals_at_callback.append(client.xclaim.await_count)
        await message.ack()

    client = _redis_client_mock()
    client.xclaim.return_value = [b"1-0"]
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        # Long enough that the paused renewal never fires during the test.
        min_idle_ms=60_000,
        server=RedisServer("redis://localhost"),
    )
    await subscriber.pause()
    try:
        await subscriber._process_stream_messages(
            [(b"1-0", {b"payload": b"held"})],
            "chan",
            "stream",
            callback,
        )
        await asyncio.sleep(0)  # The delivery starts and waits for resume.
        await subscriber.resume()
        await asyncio.gather(*subscriber._callback_tasks)
        # The paused renewal keeps the lease within one interval, so the
        # worker's own schedule takes over without an extra round trip.
        assert renewals_at_callback == [0]
    finally:
        await subscriber.close()


async def test_paused_delivery_waits_when_paused_again_before_waking() -> None:
    seen: list[str | None] = []

    async def callback(message: ReceivedMessageT) -> None:
        seen.append(message.message_id)
        await message.ack()

    client = _redis_client_mock()
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        min_idle_ms=60_000,
        server=RedisServer("redis://localhost"),
    )
    await subscriber.pause()
    try:
        await subscriber._process_stream_messages(
            [(b"1-0", {b"payload": b"held"})],
            "chan",
            "stream",
            callback,
        )
        await asyncio.sleep(0)  # The delivery starts and waits for resume.
        # Resume and pause again before the waiting delivery gets to run.
        await subscriber.resume()
        await subscriber.pause()
        for _ in range(5):
            await asyncio.sleep(0)
        assert not seen
        await subscriber.resume()
        await asyncio.gather(*subscriber._callback_tasks)
        assert seen == ["1-0"]
    finally:
        await subscriber.close()


async def test_redis_renews_while_paused_then_worker_takes_over() -> None:  # noqa: PLR0915
    read_started = asyncio.Event()
    deliver = asyncio.Event()
    actor_started = asyncio.Event()
    finish_actor = asyncio.Event()
    loop, min_idle = asyncio.get_running_loop(), 0.3
    renewals: list[float] = []
    leases: list[LeaseRenewal] = []
    paused_renewed = asyncio.Event()
    actor_renewed = asyncio.Event()
    lease_init = LeaseRenewal.__init__

    def track_lease(lease: LeaseRenewal, message: ReceivedMessageT) -> None:
        lease_init(lease, message)
        leases.append(lease)

    async def read_group(**_: Any) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
        if read_started.is_set():
            return []
        read_started.set()
        await deliver.wait()
        return [
            (b"repid:default", [(b"1-0", {b"payload": b"{}", b"headers": b'{"topic":"job"}'})]),
        ]

    def record_renewal(*_: object, **__: object) -> list[bytes]:
        renewals.append(loop.time())
        (actor_renewed if actor_started.is_set() else paused_renewed).set()
        return [b"1-0"]

    client = _redis_client_mock()
    client.xreadgroup.side_effect = read_group
    client.xclaim.side_effect = record_renewal
    server = RedisServer("redis://localhost", min_idle_ms=int(min_idle * 1000))
    server._redis = client
    router = Router()

    @router.actor
    async def job() -> None:
        actor_started.set()
        await finish_actor.wait()

    runner = _Runner(
        actor_context=ActorExecutionContext(
            server=server,
            publish=AsyncMock(),
            default_serializer=default_serializer,
        ),
        max_tasks=1,
    )
    with patch.object(LeaseRenewal, "__init__", track_lease):
        running = asyncio.create_task(runner.run(router._actors_per_channel_address, 1))
        try:
            async with asyncio.timeout(1):
                await read_started.wait()
                subscriber = runner._server_subscriber
                assert subscriber is not None
                await subscriber.pause()
                delivered_at = loop.time()
                deliver.set()
                await paused_renewed.wait()
                resumed_at = loop.time()
                await subscriber.resume()
                await actor_renewed.wait()
        finally:
            deliver.set()
            finish_actor.set()
            runner.stop_consume_event.set()
            await asyncio.wait_for(running, 2)
            await server.disconnect()

    # The worker adopts the subscriber's renewal, so one schedule renews throughout.
    (lease,) = leases
    assert any(at > resumed_at for at in renewals)
    # The lease is renewed before it can become idle long enough to be reclaimed.
    gaps = itertools.pairwise([delivered_at, *renewals])
    assert max(later - earlier for earlier, later in gaps) < min_idle
    assert lease._timer is None
    assert lease._task is None or lease._task.done()
    assert runner.processed == 1


@pytest.mark.parametrize(
    "unknown_pending",
    [pytest.param(False, id="no_unknown_pending"), pytest.param(True, id="unknown_pending")],
)
async def test_redis_callback_can_close_its_subscriber(
    pipeline_mock: tuple[MagicMock, MagicMock],
    unknown_pending: bool,
) -> None:
    finished = asyncio.Event()
    client, pipe = pipeline_mock
    client.xreadgroup.return_value = [(b"stream", [(b"1-0", {b"payload": b"job"})])]

    async def callback(message: ReceivedMessageT) -> None:
        if unknown_pending:
            subscriber._uncertain_groups.add("group")
            client.xpending_range.side_effect = [[{"message_id": b"2-0"}], []]  # pyrefly: ignore[implicit-any-empty-container]
            client.xrange.return_value = [(b"2-0", {b"payload": b"lost"})]
        await subscriber.close()
        await message.ack()
        finished.set()

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"chan": ChannelConfig(stream="stream", group="group", dlq=None)},
        callbacks={"chan": callback},
        consumer_name="consumer",
        channel_concurrency={"chan": Concurrency(messages=1)},
        server=RedisServer("redis://localhost"),
    )
    subscriber.start()
    await asyncio.wait_for(finished.wait(), 1)
    assert subscriber.task.done()
    client.xack.assert_awaited_once_with("stream", "group", "1-0")
    assert pipe.execute.await_count == int(unknown_pending)


@pytest.mark.parametrize("pause", [False, True])
async def test_due_reclaim_gets_next_slot_ahead_of_fresh_reads(pause: bool) -> None:
    client = _redis_client_mock()
    reads: list[_StreamReadResultT] = [
        [(b"stream", [(b"1-0", {b"payload": b"fresh"})])],
        [],
        [],
    ]
    client.xreadgroup.side_effect = reads
    client.xread.side_effect = _pending_future
    client.xautoclaim.return_value = [b"0-0", [(b"2-0", {b"payload": b"stale"})]]
    release = asyncio.Event()
    started: asyncio.Queue[str | None] = asyncio.Queue()

    async def callback(message: ReceivedMessageT) -> None:
        started.put_nowait(message.message_id)
        await release.wait()
        await message.ack()

    cfg = ChannelConfig(stream="stream", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": cfg},
        callbacks={"jobs": callback},
        consumer_name="consumer",
        channel_concurrency={"jobs": Concurrency(messages=1)},
        server=MagicMock(),
    )
    subscriber.start()
    try:
        async with asyncio.timeout(1):
            assert await started.get() == "1-0"
            if pause:
                await subscriber.pause()
            reclaim = asyncio.create_task(subscriber._reclaim_when_available(cfg, "jobs", callback))
            try:
                await asyncio.sleep(0)
                client.xautoclaim.assert_not_awaited()
                release.set()
                if pause:
                    await asyncio.gather(*subscriber._callback_tasks)
                    client.xautoclaim.assert_not_awaited()
                    await subscriber.resume()
                assert await started.get() == "2-0"
                await reclaim
            finally:
                reclaim.cancel()
                await asyncio.gather(reclaim, return_exceptions=True)
    finally:
        release.set()
        await subscriber.close()
