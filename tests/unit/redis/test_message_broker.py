import asyncio
import contextlib
import json
from collections.abc import Callable, Coroutine
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ResponseError

from repid.connections.abc import MessageAction, ReceivedMessageT
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
    assert caps["supports_acknowledgments"]
    assert caps["supports_persistence"]
    assert not caps["supports_reply"]
    assert caps["supports_lightweight_pause"]


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
    sub_mock.close.assert_awaited_once()
    mock_client.aclose.assert_awaited_once()


@patch("repid.connections.redis.message_broker.Redis")
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

    await sub.close()
    await sub2.close()


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
async def test_redis_subscriber_concurrency_limit(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    server = RedisServer("redis://localhost")
    await server.connect()

    callback = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(channels_to_callbacks={"c": callback}, concurrency_limit=1),
    )
    assert sub._semaphore is not None
    assert sub._semaphore._value == 1
    await sub.close()

    callback2 = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    sub2 = cast(
        RedisSubscriber,
        await server.subscribe(channels_to_callbacks={"c": callback2}, concurrency_limit=0),
    )
    assert sub2._semaphore is None
    await sub2.close()


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

    async def dummy() -> None:
        pass

    task = asyncio.create_task(dummy())
    sub._callback_tasks.add(task)

    await sub.close()

    assert not sub.is_active
    assert sub._closed
    assert task.done()


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

    await sub.close()


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

    await sub._run_callback(
        cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
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


async def test_redis_consume_batch_when_closed() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()
    sub = RedisSubscriber(
        redis_client=client,
        channels={},
        callbacks={},
        consumer_name="c",
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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


async def test_redis_semaphore_usage() -> None:
    server = RedisServer("redis://localhost")
    client = AsyncMock()
    client.xreadgroup.return_value = [[b"s", [(b"1", {b"payload": b"p"})]]]

    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={"chan": AsyncMock()},
        consumer_name="c",
        concurrency_limit=1,
        server=server,
    )

    actual_semaphore = sub._semaphore
    assert actual_semaphore is not None
    sub._semaphore = MagicMock(
        wraps=actual_semaphore,
        acquire=AsyncMock(wraps=actual_semaphore.acquire),
    )

    callback_future: asyncio.Future[None] = asyncio.Future()

    async def cb(_: RedisReceivedMessage) -> None:
        callback_future.set_result(None)

    sub._callbacks["chan"] = cast(
        Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        cb,
    )

    await sub._consume_batch("g", {"chan": chan_cfg})

    await callback_future
    await asyncio.sleep(0)

    sub._semaphore.acquire.assert_awaited()
    sub._semaphore.release.assert_called()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscribe_no_dlq_strategy(mock_redis_cls: MagicMock) -> None:
    server = RedisServer("redis://localhost", dlq_stream_strategy=None)
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = asyncio.CancelledError

    await server.connect()
    sub = await server.subscribe(channels_to_callbacks={"c": AsyncMock()})

    await sub.close()


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
        concurrency_limit=None,
        server=RedisServer("redis://localhost"),
    )
    assert sub.in_flight_count == 0
    sub._in_flight_messages.add("1-0")
    assert sub.in_flight_count == 1
    sub._in_flight_messages.discard("1-0")
    assert sub.in_flight_count == 0


async def test_redis_subscriber_start_with_claim_interval() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        concurrency_limit=None,
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
        channels={},
        callbacks={},
        consumer_name="c",
        concurrency_limit=None,
        server=RedisServer("redis://localhost"),
        claim_interval=60.0,  # long interval — claim task blocks at asyncio.sleep
    )
    sub.start()

    # Let the event loop run both tasks so _claim_loop reaches asyncio.sleep(60) and suspends.
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
    await sub.close()


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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        concurrency_limit=None,
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
            "chan": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        consumer_name="c",
        concurrency_limit=None,
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
        concurrency_limit=None,
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
        concurrency_limit=None,
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
