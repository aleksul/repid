import asyncio
import contextlib
import json
from collections.abc import Callable, Coroutine
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from repid.connections.abc import MessageAction, ReceivedMessageT
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
)
from repid.limits import NativeFlow, NativeWindow


@pytest.fixture
def pipeline_mock() -> tuple[MagicMock, MagicMock]:
    pipe = MagicMock(execute=AsyncMock())
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=None)
    client = MagicMock(pipeline=MagicMock(return_value=pipe), xack=AsyncMock())
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
    fields = {
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

    fields_str = {b"payload": "str_payload"}
    p, _, _, _ = _parse_message_fields(fields_str)
    assert p == b"str_payload"

    p, h, ct, reply_to = _parse_message_fields({})
    assert p == b""
    assert h is None
    assert ct is None
    assert reply_to is None


def test_redis_sent_message() -> None:
    msg = RedisSentMessage(
        payload=b"foo",
        headers={"x": "y"},
        content_type="text/plain",
        reply_to="reply-chan",
    )
    assert msg.payload == b"foo"
    assert msg.headers == {"x": "y"}
    assert msg.content_type == "text/plain"
    assert msg.reply_to == "reply-chan"


def test_redis_server_init() -> None:
    dsn = "redis://user:pass@localhost:6379/1"
    server = RedisServer(dsn)

    assert server.host == "localhost:6379"
    assert server.pathname == "/1"
    assert server.protocol == "redis"
    assert server.protocol_version == "6.2"
    assert server.is_connected is False

    assert server.title is None
    assert server.summary is None
    assert server.description is None
    assert server.variables is None
    assert server.security is None
    assert server.tags is None
    assert server.external_docs is None
    assert server.bindings is None

    caps = server.capabilities
    assert not caps["supports_native_reply"]
    assert caps["supports_worker_pause"]


@patch("repid.connections.redis.message_broker.Redis")
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
    sub_mock.finish.assert_awaited_once()
    mock_client.aclose.assert_awaited_once()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_server_disconnect_subscriber_error(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost")
    await server.connect()

    sub_mock = AsyncMock()
    sub_mock.finish.side_effect = ResponseError("Some redis error")
    server._active_subscribers.append(sub_mock)

    await server.disconnect()
    sub_mock.finish.assert_awaited_once()
    mock_client.aclose.assert_awaited_once()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_server_connection_context_manager(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost")

    async with server.connection() as s:
        assert s is server
        assert server.is_connected

    assert not server.is_connected


async def test_redis_publish_not_connected() -> None:
    server = RedisServer("redis://localhost")
    msg = RedisSentMessage(payload=b"test")
    with pytest.raises(ConnectionError, match="Not connected"):
        await server.publish(channel="test", message=msg)


@patch("repid.connections.redis.message_broker.Redis")
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
        await server.subscribe(channels_to_callbacks={})


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscribe_ensure_group(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    server = RedisServer("redis://localhost")
    await server.connect()

    mock_callback = AsyncMock()
    typed_callback = cast(
        Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        mock_callback,
    )

    sub = await server.subscribe(channels_to_callbacks={"chan": typed_callback})
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
    sub2 = await server.subscribe(channels_to_callbacks={"chan2": typed_callback})
    mock_client.xgroup_create.assert_awaited_once()

    mock_client.xgroup_create.reset_mock()
    mock_client.xgroup_create.side_effect = ResponseError("OTHER")
    with pytest.raises(ResponseError, match="OTHER"):
        await server.subscribe(channels_to_callbacks={"chan3": typed_callback})

    await sub.finish()
    await sub2.finish()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_consume_loop(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client

    server = RedisServer("redis://localhost", retry_attempts=0)
    await server.connect()

    callback = AsyncMock()
    channels_to_callbacks = {
        "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
    }

    fields = {b"payload": b"hello"}
    mock_client.xreadgroup.side_effect = [
        [[b"repid:chan", [(b"1-0", fields)]]],
        asyncio.CancelledError,
    ]

    sub = await server.subscribe(channels_to_callbacks=channels_to_callbacks)

    with contextlib.suppress(asyncio.CancelledError):
        await sub.task

    callback.assert_awaited_once()
    msg = callback.call_args[0][0]
    assert isinstance(msg, RedisReceivedMessage)
    assert msg.payload == b"hello"
    assert msg.message_id == "1-0"

    mock_client.xreadgroup.assert_awaited()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_rejects_an_explicit_native_outstanding_window(
    mock_redis_cls: MagicMock,
) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    server = RedisServer("redis://localhost")
    await server.connect()
    with pytest.raises(ValueError, match="Unsupported native"):
        await server.subscribe(
            channels_to_callbacks={"c": AsyncMock()},
            native_flow=NativeFlow(worker=NativeWindow(max_messages=1)),
        )
    assert not server.capabilities["supports_worker_native_messages"]
    await server.disconnect()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_close(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    server = RedisServer("redis://localhost")
    await server.connect()

    async def wait_forever(*_: Any, **__: Any) -> None:
        await asyncio.Future()

    mock_client.xreadgroup.side_effect = wait_forever

    callback = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(channels_to_callbacks={"c": callback}),
    )

    assert sub.is_active

    await sub.finish()

    assert not sub.is_active
    assert sub._closed


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_pause_resume(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    server = RedisServer("redis://localhost")
    await server.connect()

    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    callback = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(channels_to_callbacks={"c": callback}),
    )

    await sub.pause()
    assert not sub._paused_event.is_set()

    await sub.resume()
    assert sub._paused_event.is_set()

    await sub.finish()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_consume_loop_exceptions(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    server = RedisServer("redis://localhost")
    await server.connect()

    callback = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(channels_to_callbacks={"c": callback}),
    )

    mock_client.xreadgroup.side_effect = [
        ResponseError("Redis error"),
        asyncio.CancelledError,
    ]

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with contextlib.suppress(asyncio.CancelledError):
            await sub.task

        mock_sleep.assert_awaited_with(1)

    await sub.finish()


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


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_process_stream_messages_callback_error(
    mock_redis_cls: MagicMock,
) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    server = RedisServer("redis://localhost")
    await server.connect()

    callback = AsyncMock(side_effect=ResponseError("Callback error"))

    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channels_to_callbacks={
                "chan": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
        ),
    )

    msg = MagicMock(spec=RedisReceivedMessage, is_acted_on=False, nack=AsyncMock())

    with pytest.raises(ResponseError, match="Callback error"):
        await sub._run_callback(
            cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
            cast(RedisReceivedMessage, msg),
            "1-0",
        )

    callback.assert_awaited_once()
    msg.nack.assert_not_awaited()
    assert not sub._in_flight_messages

    await sub.finish()


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
    ("action", "action_kwargs"),
    [
        pytest.param("nack", {}, id="nack"),
        pytest.param("reject", {}, id="reject"),
        pytest.param("reply", {"payload": b"r"}, id="reply"),
    ],
)
async def test_redis_received_message_already_acted_guard(
    make_received_message: Any,
    pipeline_mock: tuple[MagicMock, MagicMock],
    action: str,
    action_kwargs: dict[str, Any],
) -> None:
    redis_client, _ = pipeline_mock
    msg = make_received_message()
    msg._action = MessageAction.acked
    await getattr(msg, action)(**action_kwargs)
    redis_client.pipeline.assert_not_called()


async def test_redis_concurrent_settlement_issues_only_one_operation(
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
    release_xack.set()

    await asyncio.gather(ack_task, *other_tasks)

    assert msg.action is MessageAction.acked
    redis_client.xack.assert_awaited_once_with("s", "g", "1-0")
    pipe.xack.assert_not_called()
    pipe.xadd.assert_not_called()
    redis_client.xclaim.assert_not_called()


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
        native_flow=NativeFlow(),
        server=server,
    )
    sub._closed = True
    await sub._consume_batch("g", {})
    client.xreadgroup.assert_not_awaited()


async def test_redis_consume_batch_empty_result() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()
    client.xreadgroup.return_value = []

    sub = RedisSubscriber(
        redis_client=client,
        channels={"c": ChannelConfig(stream="s", group="g", dlq=None)},
        callbacks={},
        consumer_name="c",
        native_flow=NativeFlow(),
        server=server,
    )

    await sub._consume_batch("g", {"c": ChannelConfig(stream="s", group="g", dlq=None)})
    client.xreadgroup.assert_awaited_once()


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
        native_flow=NativeFlow(),
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


async def test_redis_callback_failure_does_not_settle_runner_owned_message() -> None:
    server = RedisServer("redis://localhost")
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        native_flow=NativeFlow(),
        server=server,
    )
    callback = AsyncMock(side_effect=ResponseError("Error"))
    msg = MagicMock(
        spec=RedisReceivedMessage,
        is_acted_on=False,
        keep_alive_interval=None,
        nack=AsyncMock(),
    )
    with pytest.raises(ResponseError, match="Error"):
        await sub._run_callback(callback, msg, "1")
    callback.assert_awaited_once()
    msg.nack.assert_not_awaited()


async def test_redis_submission_returns_without_waiting_for_actor_settlement() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()
    client.xreadgroup.return_value = [[b"s", [(b"1", {b"payload": b"p"})]]]
    cfg = ChannelConfig(stream="s", group="g", dlq=None)
    received = []

    async def callback(message: ReceivedMessageT) -> None:
        received.append(message)

    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": cfg},
        callbacks={"chan": callback},
        consumer_name="c",
        native_flow=NativeFlow(),
        server=server,
    )
    await sub._consume_batch("g", {"chan": cfg})
    assert len(received) == 1
    assert not received[0].is_acted_on
    await received[0].ack()
    await sub.finish()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscribe_no_dlq_strategy(mock_redis_cls: MagicMock) -> None:
    server = RedisServer("redis://localhost", dlq_stream_strategy=None)
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = asyncio.CancelledError

    await server.connect()
    sub = await server.subscribe(channels_to_callbacks={"c": AsyncMock()})

    await sub.finish()


async def test_redis_ensure_consumer_group_no_redis() -> None:
    server = RedisServer("redis://localhost")
    server._redis = None
    await server._ensure_consumer_group("s", "g")


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_task_done_removes_from_active(mock_redis_cls: MagicMock) -> None:
    server = RedisServer("redis://localhost")
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = asyncio.CancelledError

    await server.connect()
    sub = await server.subscribe(channels_to_callbacks={"c": AsyncMock()})
    with contextlib.suppress(asyncio.CancelledError):
        await sub.task

    await asyncio.sleep(0)
    assert sub not in server._active_subscribers
    await sub.finish()
    await sub.finish()


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
        await server.subscribe(channels_to_callbacks={})


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


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscribe_multi_channel_groups(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    # Suspend the read loop so the test can inspect subscriber state synchronously.
    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    server = RedisServer("redis://localhost")
    await server.connect()

    cb_a = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    cb_b = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())

    sub = cast(
        RedisSubscriber,
        await server.subscribe(channels_to_callbacks={"alpha": cb_a, "beta": cb_b}),
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

    await sub.finish()


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
        native_flow=NativeFlow(),
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
        native_flow=NativeFlow(),
        server=RedisServer("redis://localhost"),
        claim_interval=60.0,
    )
    assert sub._claim_task is None
    sub.start()
    assert sub._claim_task is not None
    await sub.finish()
    assert sub._claim_task.done()


async def test_redis_subscriber_close_with_active_claim_task() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        native_flow=NativeFlow(),
        server=RedisServer("redis://localhost"),
        claim_interval=60.0,  # long interval — claim task blocks at asyncio.sleep
    )
    sub.start()

    # Let the event loop run both tasks so _claim_loop reaches asyncio.sleep(60) and suspends.
    await asyncio.sleep(0)

    assert sub._claim_task is not None
    assert not sub._claim_task.done()  # still sleeping

    await sub.finish()
    assert sub._claim_task.done()


async def test_redis_consume_group_loop_unexpected_exception() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        native_flow=NativeFlow(),
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
        patch("asyncio.sleep", new_callable=AsyncMock),
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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        native_flow=NativeFlow(),
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
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        await sub._claim_loop()

    assert reclaim_calls == 1


async def test_redis_claim_loop_stops_on_closed_after_sleep() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        native_flow=NativeFlow(),
        server=RedisServer("redis://localhost"),
        claim_interval=0.01,
    )

    async def set_closed(_: float) -> None:
        sub._closed = True

    with patch("asyncio.sleep", side_effect=set_closed):
        await sub._claim_loop()


async def test_redis_claim_loop_callback_none() -> None:
    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)

    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"chan": chan_cfg},
        callbacks={},  # no callback registered for "chan"
        consumer_name="c",
        native_flow=NativeFlow(),
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
        patch("asyncio.sleep", side_effect=mock_sleep),
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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        native_flow=NativeFlow(),
        server=RedisServer("redis://localhost"),
        claim_interval=0.01,
    )

    async def failing_reclaim(cfg: Any, ch: Any, cb: Any) -> None:  # noqa: ARG001
        sub._closed = True
        raise ConnectionError("Redis down")

    with (
        patch.object(sub, "_reclaim_pending", side_effect=failing_reclaim),
        patch("asyncio.sleep", new_callable=AsyncMock),
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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="consumer1",
        native_flow=NativeFlow(),
        server=RedisServer("redis://localhost"),
        min_idle_ms=5000,
    )

    # "0-0" as next_id means scan complete; return one message
    client.xautoclaim.return_value = [b"0-0", [(b"1-0", {b"payload": b"p"})], []]

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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        native_flow=NativeFlow(),
        server=RedisServer("redis://localhost"),
    )

    # First batch: non-zero next_id (bytes) with messages
    # Second batch: "0-0" with no messages → stop
    client.xautoclaim.side_effect = [
        [b"5-0", [(b"1-0", {b"payload": b"a"})], []],
        ["0-0", [], []],
    ]

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._reclaim_pending(chan_cfg, "chan", callback)

    assert client.xautoclaim.await_count == 2
    # Second xautoclaim used "5-0" (decoded from bytes) as start_id
    second_call_kwargs = client.xautoclaim.call_args_list[1][1]
    assert second_call_kwargs["start_id"] == "5-0"
    # _process_stream_messages only called for first batch (had messages)
    mock_process.assert_awaited_once()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscribe_consumer_group_start_id(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    server = RedisServer("redis://localhost", consumer_group_start_id="$")
    await server.connect()

    sub = await server.subscribe(channels_to_callbacks={"chan": AsyncMock()})

    mock_client.xgroup_create.assert_awaited_once_with(
        "repid:chan",
        "repid:chan:group",
        id="$",
        mkstream=True,
    )
    await sub.finish()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscribe_dlq_maxlen(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    server = RedisServer(
        "redis://localhost",
        dlq_maxlen=1000,
        claim_interval=30.0,
        min_idle_ms=2000,
    )
    await server.connect()

    sub = cast(
        RedisSubscriber,
        await server.subscribe(channels_to_callbacks={"chan": AsyncMock()}),
    )

    assert sub._channels["chan"].dlq_maxlen == 1000
    assert sub._claim_interval == 30.0
    assert sub._min_idle_ms == 2000
    await sub.finish()


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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        native_flow=NativeFlow(),
        server=server,
    )

    await sub._consume_batch("g", {"chan": chan_cfg})

    callback.assert_awaited_once()
    assert callback.await_args is not None
    assert callback.await_args.args[0].channel == "chan"
    await sub.finish()


async def test_redis_process_stream_messages_str_msg_id() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()

    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    callback = AsyncMock()

    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        native_flow=NativeFlow(),
        server=server,
    )

    # msg_id as str (not bytes) — exercises the non-decode branch.
    await sub._process_stream_messages(
        [("1-0", {b"payload": b"hello"})],
        "chan",
        "s",
        cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        native_flow=NativeFlow(),
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
        native_flow=NativeFlow(),
        server=RedisServer("redis://localhost"),
    )

    loop_calls: list[tuple[str, set[str]]] = []

    async def fake_group_loop(group_name: str, group_channels: dict[str, ChannelConfig]) -> None:
        loop_calls.append((group_name, set(group_channels.keys())))

    with patch.object(sub, "_consume_group_loop", side_effect=fake_group_loop):
        await sub._consume_loop()

    assert len(loop_calls) == 1
    assert loop_calls[0] == ("shared:group", {"chan_a", "chan_b"})


@patch("repid.connections.redis.message_broker.Redis")
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
        native_flow=NativeFlow(),
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
        patch.object(asyncio, "sleep", new_callable=AsyncMock) as sleep,
    ):
        await subscriber._consume_group_loop("group", {})

    assert calls == 2
    sleep.assert_awaited_once_with(1.0)
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
        native_flow=NativeFlow(),
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
        patch.object(asyncio, "sleep", new_callable=AsyncMock),
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
        pytest.param("ack", id="ack"),
        pytest.param("nack", id="nack"),
        pytest.param("reject", id="reject"),
    ],
)
async def test_redis_failed_confirmation_can_be_retried(
    action: str,
    pipeline_mock: tuple[MagicMock, MagicMock],
    make_received_message: Any,
) -> None:
    client, pipe = pipeline_mock
    client.xack.side_effect = [ConnectionError("offline"), None]
    pipe.execute.side_effect = [ConnectionError("offline"), None]
    message = make_received_message()

    with pytest.raises(ConnectionError, match="offline"):
        await getattr(message, action)()
    assert not message.is_acted_on
    await getattr(message, action)()
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
        native_flow=NativeFlow(),
        server=MagicMock(),
    )
    started = []
    ready, release = asyncio.Event(), asyncio.Event()

    async def callback(message: ReceivedMessageT) -> None:
        started.append(message.channel)
        if len(started) == 2:
            ready.set()
        await release.wait()
        await message.ack()

    tasks = [
        asyncio.create_task(
            subscriber._process_stream_messages(
                [(b"1-0", {b"payload": b"x"})],
                channel,
                channel,
                callback,
            ),
        )
        for channel in ("first", "second")
    ]
    await asyncio.wait_for(ready.wait(), 1)
    await subscriber._process_stream_messages(
        [(b"1-0", {b"payload": b"x"})],
        "first",
        "first",
        callback,
    )
    assert started == ["first", "second"]
    release.set()
    await asyncio.gather(*tasks)
    await subscriber.finish()
    assert subscriber.in_flight_count == 0


@pytest.mark.parametrize("paused_after_fetch", [False, True])
async def test_redis_multistream_fetch_renews_and_disposes_later_streams(
    paused_after_fetch: bool,
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    first_started, second_renewed = asyncio.Event(), asyncio.Event()
    channels = {
        channel: ChannelConfig(stream=channel, group="shared", dlq=None)
        for channel in ("first", "second")
    }

    async def callback(message: ReceivedMessageT) -> None:
        assert message.channel == "first"
        assert {owned.channel for owned, _ in subscriber._buffer.owned.values()} == {"second"}
        first_started.set()
        await asyncio.Event().wait()

    async def fetch(**_kwargs: Any) -> list[Any]:
        if paused_after_fetch:
            await subscriber.pause()
        return [[channel.encode(), [(b"1-0", {b"payload": b"body"})]] for channel in channels]

    async def renew(stream: str, *_args: Any, **_kwargs: Any) -> None:
        if stream == "second":
            second_renewed.set()

    client.xreadgroup = AsyncMock(side_effect=fetch)
    client.xclaim = AsyncMock(side_effect=renew)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels=channels,
        callbacks=dict.fromkeys(channels, callback),
        consumer_name="consumer",
        server=RedisServer("redis://localhost"),
        min_idle_ms=3000,
    )
    subscriber.start()
    try:
        await asyncio.wait_for(second_renewed.wait(), 2)
        assert first_started.is_set() is not paused_after_fetch
        owned = list(subscriber._buffer.owned.values())
        expected = {"first", "second"} if paused_after_fetch else {"second"}
        assert {message.channel for message, _ in owned} == expected
        await asyncio.wait_for(subscriber.stop(), 2)
        assert not subscriber._buffer.owned
        assert all(renewal is not None and renewal.done() for _, renewal in owned)
        assert {call.args[0] for call in pipe.xack.call_args_list} == expected
        assert pipe.xack.call_count == len(expected)
    finally:
        await subscriber.finish()
