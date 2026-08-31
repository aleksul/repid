import asyncio
import contextlib
import json
from collections.abc import Callable, Coroutine
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from repid.admission import MessageLimits
from repid.connections import SubscriberDispatcher
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
from repid.limits import OversizedPayloadAction


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
    assert caps["supports_pause"]


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
    sub_mock.stop.assert_awaited_once()
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
    sub_mock.stop.assert_awaited_once()
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
        await server.subscribe(channels_to_callbacks={}, dispatcher=SubscriberDispatcher())


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

    sub = await server.subscribe(
        channels_to_callbacks={"chan": typed_callback},
        dispatcher=SubscriberDispatcher(),
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
        channels_to_callbacks={"chan2": typed_callback},
        dispatcher=SubscriberDispatcher(),
    )

    mock_client.xgroup_create.assert_awaited_once()

    mock_client.xgroup_create.reset_mock()
    mock_client.xgroup_create.side_effect = ResponseError("OTHER")
    with pytest.raises(ResponseError, match="OTHER"):
        await server.subscribe(
            channels_to_callbacks={"chan3": typed_callback},
            dispatcher=SubscriberDispatcher(),
        )

    await sub.stop()
    await sub.finish()
    await sub2.stop()
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

    sub = await server.subscribe(
        channels_to_callbacks=channels_to_callbacks,
        dispatcher=SubscriberDispatcher(),
    )

    with contextlib.suppress(asyncio.CancelledError):
        await sub.task

    callback.assert_awaited_once()
    msg = callback.call_args[0][0]
    assert isinstance(msg, RedisReceivedMessage)
    assert msg.payload == b"hello"
    assert msg.message_id == "1-0"

    mock_client.xreadgroup.assert_awaited()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_message_limit(mock_redis_cls: MagicMock) -> None:
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = lambda *_, **__: asyncio.Future()

    server = RedisServer("redis://localhost")
    await server.connect()

    callback = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    sub = cast(
        RedisSubscriber,
        await server.subscribe(
            channels_to_callbacks={"c": callback},
            dispatcher=SubscriberDispatcher(MessageLimits(max_messages=1)),
        ),
    )
    assert sub._dispatcher.native_message_limit("c") == 1
    await sub.stop()
    await sub.finish()

    callback2 = cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], AsyncMock())
    sub2 = cast(
        RedisSubscriber,
        await server.subscribe(
            channels_to_callbacks={"c": callback2},
            dispatcher=SubscriberDispatcher(),
        ),
    )
    assert sub2._dispatcher.native_message_limit("c") is None
    await sub2.stop()
    await sub2.finish()


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
        await server.subscribe(
            channels_to_callbacks={"c": callback},
            dispatcher=SubscriberDispatcher(),
        ),
    )

    assert sub.is_active

    async def dummy() -> None:
        pass

    task = asyncio.create_task(dummy())
    sub._admitted_tasks.tasks.add(task)

    await sub.stop()
    await sub.finish()

    assert not sub.is_active
    assert sub._closed
    assert task.done()


async def test_redis_subscriber_rejects_cancelled_callback_message() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
    )
    callback_started = asyncio.Event()
    message = Mock(
        is_acted_on=False,
        reject=AsyncMock(side_effect=RuntimeError("reject failed")),
    )

    async def callback(_: ReceivedMessageT) -> None:
        callback_started.set()
        await asyncio.Future()

    task = asyncio.create_task(sub._run_callback(callback, message, "1-0"))
    sub._admitted_tasks.tasks.add(task)
    await asyncio.wait_for(callback_started.wait(), timeout=1)

    await sub.stop()
    await sub.finish()

    message.reject.assert_awaited_once()


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
        await server.subscribe(
            channels_to_callbacks={"c": callback},
            dispatcher=SubscriberDispatcher(),
        ),
    )

    await sub.pause()
    assert not sub._paused_event.is_set()

    await sub.resume()
    assert sub._paused_event.is_set()

    await sub.stop()
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
        await server.subscribe(
            channels_to_callbacks={"c": callback},
            dispatcher=SubscriberDispatcher(),
        ),
    )

    mock_client.xreadgroup.side_effect = [
        ResponseError("Redis error"),
        asyncio.CancelledError,
    ]

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with contextlib.suppress(asyncio.CancelledError):
            await sub.task

        mock_sleep.assert_awaited_with(1)

    await sub.stop()
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
            dispatcher=SubscriberDispatcher(),
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

    await sub.stop()
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
    client = AsyncMock()
    sub = RedisSubscriber(
        redis_client=client,
        channels={},
        callbacks={},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
    )
    sub._closed = True
    await sub._consume_batch("g", {})
    client.xreadgroup.assert_not_awaited()


async def test_redis_consume_batch_empty_result() -> None:
    client = AsyncMock()
    client.xreadgroup.return_value = []

    sub = RedisSubscriber(
        redis_client=client,
        channels={"c": ChannelConfig(stream="s", group="g", dlq=None)},
        callbacks={},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
    )

    await sub._consume_batch("g", {"c": ChannelConfig(stream="s", group="g", dlq=None)})
    client.xreadgroup.assert_awaited_once()


async def test_redis_channel_pause_excludes_stream_from_batch_reads() -> None:
    client = AsyncMock()
    sub = RedisSubscriber(
        redis_client=client,
        channels={"c": ChannelConfig(stream="s", group="g", dlq=None)},
        callbacks={},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
    )

    await sub.pause_channel("c")
    # Every channel in the group is paused: no XREADGROUP is issued at all.
    await sub._consume_batch("g", {"c": ChannelConfig(stream="s", group="g", dlq=None)})
    client.xreadgroup.assert_not_awaited()

    await sub.resume_channel("c")
    client.xreadgroup.return_value = []
    await sub._consume_batch("g", {"c": ChannelConfig(stream="s", group="g", dlq=None)})
    client.xreadgroup.assert_awaited_once_with(
        groupname="g",
        consumername="c",
        streams={"s": ">"},
        count=sub._batch_size,
        block=sub._block_ms,
    )

    # A global pause and resume re-arms every channel.
    await sub.pause()
    assert not sub._channel_paused_events["c"].is_set()
    await sub.resume()
    assert sub._channel_paused_events["c"].is_set()


async def test_redis_consume_batch_unknown_stream_and_missing_callback() -> None:
    client = AsyncMock()
    client.xreadgroup.return_value = [[b"unknown_stream", [(b"1", {b"payload": b"p"})]]]

    chan_cfg = ChannelConfig(stream="known_stream", group="g", dlq=None)
    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={"chan": AsyncMock()},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
    )

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._consume_batch("g", {"chan": chan_cfg})
        mock_process.assert_not_awaited()

        client.xreadgroup.reset_mock()
        client.xreadgroup.return_value = [[b"known_stream", [(b"1", {b"payload": b"p"})]]]

        sub._callbacks = {}

        await sub._consume_batch("g", {"chan": chan_cfg})

        mock_process.assert_not_awaited()


async def test_redis_prefetches_valid_messages_before_settling_malformed_entries(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    order: list[str] = []
    settlement_started = asyncio.Event()
    allow_settlement = asyncio.Event()

    async def blocked_execute() -> None:
        order.append("settle")
        settlement_started.set()
        await allow_settlement.wait()

    pipe.execute.side_effect = blocked_execute
    config = ChannelConfig(stream="jobs", group="group", dlq="jobs:dlq")
    dispatcher = MagicMock(spec=SubscriberDispatcher)
    dispatcher.start_keep_alive.side_effect = lambda message: order.append(
        f"keepalive:{message.message_id}",
    )
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )

    prefetch = asyncio.create_task(
        subscriber._prefetch_messages(
            [
                (
                    b"jobs",
                    [
                        (b"bad", {b"payload": b"message", b"headers": b"{"}),
                        (b"good", {b"payload": b"message"}),
                    ],
                ),
            ],
            {"jobs": "jobs"},
        ),
    )
    await asyncio.wait_for(settlement_started.wait(), timeout=1)
    assert order == ["keepalive:good", "settle"]

    allow_settlement.set()
    prefetched = await prefetch
    assert list(prefetched) == [("jobs", "good")]


async def test_redis_prefetch_settlement_failure_requeues_valid_messages(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    pipe.execute.side_effect = ResponseError("settlement failed")
    dispatcher = MagicMock(spec=SubscriberDispatcher)
    dispatcher.stop_keep_alive = AsyncMock()
    config = ChannelConfig(stream="jobs", group="group", dlq="jobs:dlq")
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )
    client.xreadgroup = AsyncMock(
        return_value=[
            [
                b"jobs",
                [
                    (b"good-1", {b"payload": b"one"}),
                    (b"bad", {b"payload": b"message", b"headers": b"{"}),
                    (b"good-2", {b"payload": b"two"}),
                ],
            ],
        ],
    )

    with pytest.raises(ResponseError, match="settlement failed"):
        await subscriber._consume_batch("group", {"jobs": config})

    assert subscriber.in_flight_count == 0
    assert dispatcher.start_keep_alive.call_count == 2
    assert dispatcher.stop_keep_alive.await_count == 2
    assert pipe.execute.await_count == 3


async def test_redis_cancelled_prefetch_settlement_requeues_valid_messages(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    settlement_started = asyncio.Event()
    release_settlement = asyncio.Event()
    execute_calls = 0

    async def execute() -> None:
        nonlocal execute_calls
        execute_calls += 1
        if execute_calls == 1:
            settlement_started.set()
            await release_settlement.wait()

    pipe.execute.side_effect = execute
    dispatcher = MagicMock(spec=SubscriberDispatcher)
    dispatcher.stop_keep_alive = AsyncMock()
    config = ChannelConfig(stream="jobs", group="group", dlq="jobs:dlq")
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )
    client.xreadgroup = AsyncMock(
        return_value=[
            [
                b"jobs",
                [
                    (b"good-1", {b"payload": b"one"}),
                    (b"bad", {b"payload": b"message", b"headers": b"{"}),
                    (b"good-2", {b"payload": b"two"}),
                ],
            ],
        ],
    )

    consuming = asyncio.create_task(subscriber._consume_batch("group", {"jobs": config}))
    await asyncio.wait_for(settlement_started.wait(), timeout=1)
    consuming.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consuming

    assert subscriber.in_flight_count == 0
    assert dispatcher.start_keep_alive.call_count == 2
    assert dispatcher.stop_keep_alive.await_count == 2
    assert pipe.execute.await_count == 3


@pytest.mark.parametrize(
    ("dlq", "expected_xadd_calls"),
    [
        pytest.param("jobs:dlq", 1, id="dead_letters"),
        pytest.param(None, 0, id="discards_without_dlq"),
    ],
)
async def test_redis_malformed_fetched_message_is_terminally_settled(
    pipeline_mock: tuple[MagicMock, MagicMock],
    dlq: str | None,
    expected_xadd_calls: int,
) -> None:
    client, pipe = pipeline_mock
    callback = AsyncMock()
    config = ChannelConfig(stream="jobs", group="group", dlq=dlq)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": callback},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(),
    )
    malformed_fields = {b"payload": b"message", b"headers": b"{"}
    client.xreadgroup = AsyncMock(
        return_value=[[b"jobs", [(b"1-0", malformed_fields)]]],
    )

    await subscriber._consume_batch("group", {"jobs": config})

    callback.assert_not_awaited()
    assert subscriber.in_flight_count == 0
    assert pipe.xadd.call_count == expected_xadd_calls
    pipe.xack.assert_called_once_with("jobs", "group", "1-0")
    pipe.execute.assert_awaited_once()
    if dlq is not None:
        pipe.xadd.assert_called_once_with(
            dlq,
            {
                b"payload": b"message",
                b"headers": b"{",
                b"original_stream": "jobs",
                b"original_id": "1-0",
            },
        )


async def test_redis_run_callback_exception() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
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


async def test_redis_intake_gate_usage() -> None:
    client = AsyncMock()
    client.xreadgroup.return_value = [[b"s", [(b"1", {b"payload": b"p"})]]]

    chan_cfg = ChannelConfig(stream="s", group="g", dlq=None)
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    reserve = AsyncMock(wraps=dispatcher.reserve)
    sub = RedisSubscriber(
        redis_client=client,
        channels={"chan": chan_cfg},
        callbacks={"chan": AsyncMock()},
        consumer_name="c",
        dispatcher=dispatcher,
    )

    callback_future: asyncio.Future[None] = asyncio.Future()

    async def cb(_: RedisReceivedMessage) -> None:
        callback_future.set_result(None)

    sub._callbacks["chan"] = cast(
        Callable[[ReceivedMessageT], Coroutine[None, None, None]],
        cb,
    )

    with patch.object(dispatcher, "reserve", new=reserve):
        await sub._consume_batch("g", {"chan": chan_cfg})

    await callback_future
    await asyncio.sleep(0)

    reserve.assert_awaited()


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscribe_no_dlq_strategy(mock_redis_cls: MagicMock) -> None:
    server = RedisServer("redis://localhost", dlq_stream_strategy=None)
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = asyncio.CancelledError

    await server.connect()
    sub = await server.subscribe(
        channels_to_callbacks={"c": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    await sub.stop()
    await sub.finish()


async def test_redis_ensure_consumer_group_no_redis() -> None:
    server = RedisServer("redis://localhost")
    server._redis = None
    await server._ensure_consumer_group("s", "g")


@patch("repid.connections.redis.message_broker.Redis")
async def test_redis_subscriber_close_removes_from_active(mock_redis_cls: MagicMock) -> None:
    server = RedisServer("redis://localhost")
    mock_client = AsyncMock()
    mock_redis_cls.from_url.return_value = mock_client
    mock_client.xreadgroup.side_effect = asyncio.CancelledError

    await server.connect()
    sub = await server.subscribe(
        channels_to_callbacks={"c": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    with contextlib.suppress(asyncio.CancelledError):
        await sub.task

    assert sub in server._active_subscribers
    await sub.stop()
    await sub.finish()
    assert sub not in server._active_subscribers
    await sub.stop()
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
        await server.subscribe(channels_to_callbacks={}, dispatcher=SubscriberDispatcher())


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
        await server.subscribe(
            channels_to_callbacks={"alpha": cb_a, "beta": cb_b},
            dispatcher=SubscriberDispatcher(),
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

    await sub.stop()
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
        dispatcher=SubscriberDispatcher(),
    )
    assert sub.in_flight_count == 0
    sub._in_flight_messages.add(("jobs", "1-0"))
    assert sub.in_flight_count == 1
    sub._in_flight_messages.discard(("jobs", "1-0"))
    assert sub.in_flight_count == 0


async def test_redis_subscriber_claim_failure_ends_supervisor_and_cancels_consumer() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        claim_interval=1,
        dispatcher=SubscriberDispatcher(),
    )
    consumer_cancelled = asyncio.Event()
    claim_started = asyncio.Event()

    async def consume() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            consumer_cancelled.set()
            raise

    async def claim() -> None:
        claim_started.set()
        raise RuntimeError("claim failed")

    with (
        patch.object(sub, "_consume_loop", side_effect=consume),
        patch.object(sub, "_claim_loop", side_effect=claim),
    ):
        sub.start()
        await asyncio.wait_for(claim_started.wait(), timeout=1)
        with pytest.raises(RuntimeError, match="claim failed"):
            await sub.task

    assert consumer_cancelled.is_set()


async def test_redis_subscriber_consume_completion_cancels_claim_loop() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        claim_interval=1,
        dispatcher=SubscriberDispatcher(),
    )
    claim_cancelled = asyncio.Event()

    async def claim() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            claim_cancelled.set()
            raise

    with (
        patch.object(sub, "_consume_loop", new_callable=AsyncMock),
        patch.object(sub, "_claim_loop", side_effect=claim),
    ):
        sub.start()
        await sub.task

    assert claim_cancelled.is_set()


async def test_redis_subscriber_close_cancels_supervised_claim_loop() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        claim_interval=60.0,
        dispatcher=SubscriberDispatcher(),
    )
    sub.start()
    await asyncio.sleep(0)

    await sub.stop()
    await sub.finish()
    assert sub.task.done()


async def test_redis_subscriber_disabled_claims_do_not_start_claim_loop() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
    )

    with patch.object(sub, "_claim_loop", new_callable=AsyncMock) as claim_loop:
        sub.start()
        await asyncio.sleep(0)
        await sub.stop()
        await sub.finish()

    claim_loop.assert_not_called()


async def test_redis_consume_group_loop_unexpected_exception() -> None:
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={},
        callbacks={},
        consumer_name="c",
        dispatcher=SubscriberDispatcher(),
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
        claim_interval=0.01,
        dispatcher=SubscriberDispatcher(),
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
        claim_interval=0.01,
        dispatcher=SubscriberDispatcher(),
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
        claim_interval=0.01,
        dispatcher=SubscriberDispatcher(),
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
        claim_interval=0.01,
        dispatcher=SubscriberDispatcher(),
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
        min_idle_ms=5000,
        dispatcher=SubscriberDispatcher(),
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
        dispatcher=SubscriberDispatcher(),
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

    sub = await server.subscribe(
        channels_to_callbacks={"chan": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    mock_client.xgroup_create.assert_awaited_once_with(
        "repid:chan",
        "repid:chan:group",
        id="$",
        mkstream=True,
    )
    await sub.stop()
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
        await server.subscribe(
            channels_to_callbacks={"chan": AsyncMock()},
            dispatcher=SubscriberDispatcher(),
        ),
    )

    assert sub._channels["chan"].dlq_maxlen == 1000
    assert sub._claim_interval == 30.0
    assert sub._min_idle_ms == 2000
    await sub.stop()
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
        dispatcher=SubscriberDispatcher(),
    )

    with patch.object(sub, "_process_stream_messages", new_callable=AsyncMock) as mock_process:
        await sub._consume_batch("g", {"chan": chan_cfg})

    mock_process.assert_awaited_once()


async def test_redis_process_stream_messages_str_msg_id() -> None:
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
        dispatcher=SubscriberDispatcher(),
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
        dispatcher=SubscriberDispatcher(),
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
        dispatcher=SubscriberDispatcher(),
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
        dispatcher=SubscriberDispatcher(),
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
        dispatcher=SubscriberDispatcher(),
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
        dispatcher=SubscriberDispatcher(),
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
        await subscriber.stop()
        await subscriber.finish()
    assert subscriber.in_flight_count == 0


async def test_redis_intake_drop_skips_callback() -> None:
    callback = AsyncMock()
    sub = RedisSubscriber(
        redis_client=AsyncMock(),
        channels={"jobs": ChannelConfig(stream="jobs", group="group", dlq=None)},
        callbacks={"jobs": callback},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(),
    )
    sub._dispatcher = MagicMock(
        reserve=AsyncMock(return_value=None),
    )

    await sub._process_stream_messages(
        [(b"1-0", {b"payload": b"{}"})],
        "jobs",
        "jobs",
        cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
    )
    callback.assert_not_awaited()


async def test_redis_pause_stops_pending_reclaims() -> None:
    client = AsyncMock(xautoclaim=AsyncMock(return_value=[b"0-0", [], []]))
    config = ChannelConfig(stream="jobs", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(),
    )
    await subscriber.pause()
    reclaiming = asyncio.create_task(
        subscriber._reclaim_pending(config, "jobs", AsyncMock()),
    )
    try:
        await asyncio.sleep(0)
        client.xautoclaim.assert_not_awaited()
        await subscriber.resume()
        await reclaiming
        client.xautoclaim.assert_awaited_once()
    finally:
        reclaiming.cancel()
        await asyncio.gather(reclaiming, return_exceptions=True)
        await subscriber.stop()
        await subscriber.finish()


async def test_redis_channel_pause_stops_pending_reclaims() -> None:
    client = AsyncMock(xautoclaim=AsyncMock(return_value=[b"0-0", [], []]))
    config = ChannelConfig(stream="jobs", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(),
    )
    await subscriber.pause_channel("jobs")

    await asyncio.wait_for(
        subscriber._reclaim_pending(config, "jobs", AsyncMock()),
        timeout=0.5,
    )

    client.xautoclaim.assert_not_awaited()


async def test_redis_channel_pause_reclaims_other_channels() -> None:
    client = AsyncMock(xautoclaim=AsyncMock(return_value=[b"0-0", [], []]))
    paused_cfg = ChannelConfig(stream="paused", group="group", dlq=None)
    active_cfg = ChannelConfig(stream="active", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"paused": paused_cfg, "active": active_cfg},
        callbacks={"paused": AsyncMock(), "active": AsyncMock()},
        consumer_name="consumer",
        claim_interval=0.01,
        dispatcher=SubscriberDispatcher(),
    )
    await subscriber.pause_channel("paused")

    sleep_count = 0

    async def mock_sleep(_: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:  # stop after second sleep (past first full iteration)
            subscriber._closed = True

    with patch("asyncio.sleep", side_effect=mock_sleep):
        await subscriber._claim_loop()

    streams = [call.args[0] for call in client.xautoclaim.await_args_list]
    assert streams == ["active"]


async def test_redis_channel_resume_allows_pending_reclaims() -> None:
    client = AsyncMock(xautoclaim=AsyncMock(return_value=[b"0-0", [], []]))
    config = ChannelConfig(stream="jobs", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(),
    )
    await subscriber.pause_channel("jobs")
    await subscriber._reclaim_pending(config, "jobs", AsyncMock())
    client.xautoclaim.assert_not_awaited()

    await subscriber.resume_channel("jobs")
    await subscriber._reclaim_pending(config, "jobs", AsyncMock())

    client.xautoclaim.assert_awaited_once()


async def test_redis_channel_pause_between_batches_stops_next_fetch() -> None:
    client = AsyncMock()
    client.xautoclaim.side_effect = [[b"5-0", [(b"1-0", {b"payload": b"a"})], []]]
    config = ChannelConfig(stream="jobs", group="group", dlq=None)
    callback = AsyncMock()
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": callback},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(),
    )

    async def pause_channel_during_processing(*args: Any, **kwargs: Any) -> None:  # noqa: ARG001
        await subscriber.pause_channel("jobs")

    with patch.object(
        subscriber,
        "_process_stream_messages",
        new_callable=AsyncMock,
        side_effect=pause_channel_during_processing,
    ) as mock_process:
        await subscriber._reclaim_pending(config, "jobs", callback)

    client.xautoclaim.assert_awaited_once()
    mock_process.assert_awaited_once()


async def test_redis_close_requeues_unstarted_admitted_delivery(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    callback = AsyncMock()
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": ChannelConfig(stream="jobs", group="group", dlq=None)},
        callbacks={"jobs": callback},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )

    await subscriber._process_stream_messages(
        [(b"1-0", {b"payload": b"x"})],
        "jobs",
        "jobs",
        callback,
    )
    await subscriber.stop()
    await subscriber.finish()

    callback.assert_not_awaited()
    assert subscriber.in_flight_count == 0
    pipe.xadd.assert_called_once()
    pipe.xack.assert_called_once_with("jobs", "group", "1-0")

    lease = await dispatcher.reserve(
        MagicMock(channel="jobs", payload=b"next", keep_alive_interval=None),
    )
    assert lease is not None
    await lease.release()


async def test_redis_cancelled_intake_requeues_remaining_read_batch_entries(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    first = ChannelConfig(stream="first-stream", group="group", dlq=None)
    second = ChannelConfig(stream="second-stream", group="group", dlq=None)
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"first": first, "second": second},
        callbacks={"first": AsyncMock(), "second": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )
    blocker = await dispatcher.reserve(
        MagicMock(channel="first", payload=b"blocker", keep_alive_interval=None),
    )
    assert blocker is not None
    reservation_started = asyncio.Event()
    reserve = dispatcher.reserve

    async def wait_for_reservation(message: ReceivedMessageT) -> Any:
        reservation_started.set()
        return await reserve(message)

    client.xreadgroup = AsyncMock(
        return_value=[
            [b"first-stream", [(b"1-0", {b"payload": b"one"}), (b"2-0", {b"payload": b"two"})]],
            [b"second-stream", [(b"1-0", {b"payload": b"three"})]],
        ],
    )

    group_channels = {"first": first, "second": second}
    with patch.object(
        dispatcher,
        "reserve",
        new=AsyncMock(side_effect=wait_for_reservation),
    ):
        consuming = asyncio.create_task(subscriber._consume_batch("group", group_channels))
        await reservation_started.wait()
        consuming.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consuming

    assert subscriber.in_flight_count == 0
    assert pipe.xadd.call_count == 3
    assert pipe.xack.call_count == 3
    await blocker.release()


async def test_redis_cancelled_reclaim_requeues_unvisited_entries(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    config = ChannelConfig(stream="jobs", group="group", dlq=None)
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )
    blocker = await dispatcher.reserve(
        MagicMock(channel="jobs", payload=b"blocker", keep_alive_interval=None),
    )
    assert blocker is not None
    reservation_started = asyncio.Event()
    reserve = dispatcher.reserve

    async def wait_for_reservation(message: ReceivedMessageT) -> Any:
        reservation_started.set()
        return await reserve(message)

    client.xautoclaim = AsyncMock(
        return_value=[b"0-0", [(b"1-0", {b"payload": b"one"}), (b"2-0", {b"payload": b"two"})], []],
    )

    with patch.object(
        dispatcher,
        "reserve",
        new=AsyncMock(side_effect=wait_for_reservation),
    ):
        reclaiming = asyncio.create_task(subscriber._reclaim_pending(config, "jobs", AsyncMock()))
        await reservation_started.wait()
        reclaiming.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reclaiming

    assert subscriber.in_flight_count == 0
    assert pipe.xadd.call_count == 2
    assert pipe.xack.call_count == 2
    await blocker.release()


async def test_redis_finish_cancels_running_callback_and_waits_for_cleanup(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, _pipe = pipeline_mock
    callback_entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    block = asyncio.Event()

    async def callback(_message: Any) -> None:
        callback_entered.set()
        try:
            await block.wait()
        except asyncio.CancelledError:
            # Started tasks own their cancellation cleanup.
            cleanup_started.set()
            await allow_cleanup.wait()
            raise

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": ChannelConfig(stream="jobs", group="group", dlq=None)},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(MessageLimits(max_messages=1)),
    )
    received = subscriber._create_received_message("jobs", "jobs", "1-0", {b"payload": b"x"})
    key = ("jobs", "1-0")
    subscriber._in_flight_messages.add(key)
    subscriber._admitted_tasks.start(
        subscriber._dispatcher,
        AsyncMock(),
        received,
        callback,
    )

    await subscriber.stop()

    finishing = asyncio.create_task(subscriber.finish())
    await asyncio.wait_for(callback_entered.wait(), timeout=1)
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not finishing.done()

    allow_cleanup.set()
    await asyncio.wait_for(finishing, timeout=1)


async def test_redis_finish_waits_for_blocked_cleanup_before_releasing(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, _pipe = pipeline_mock
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    unregistered: list[None] = []
    block = asyncio.Event()

    async def callback(_message: Any) -> None:
        try:
            await block.wait()
        except asyncio.CancelledError:
            # Started tasks own their cancellation cleanup.
            cleanup_started.set()
            await allow_cleanup.wait()
            raise

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": ChannelConfig(stream="jobs", group="group", dlq=None)},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(),
    )
    received = subscriber._create_received_message("jobs", "jobs", "1-0", {b"payload": b"x"})
    key = ("jobs", "1-0")
    subscriber._in_flight_messages.add(key)
    subscriber._admitted_tasks.start(
        subscriber._dispatcher,
        AsyncMock(),
        received,
        callback,
    )
    subscriber._on_close = lambda: unregistered.append(None)

    await subscriber.stop()
    assert not unregistered

    draining_close = asyncio.create_task(subscriber.finish())
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not draining_close.done()
    assert not unregistered

    allow_cleanup.set()
    await draining_close

    assert unregistered == [None]


@patch.object(message_broker, "Redis")
async def test_redis_server_disconnect_closes_snapshot_when_subscriber_close_fails(
    mock_redis_cls: MagicMock,
) -> None:
    started = {f"c{index}": asyncio.Event() for index in range(3)}
    cancelled = {channel: asyncio.Event() for channel in started}
    delivered: set[str] = set()

    async def read_messages(*_: Any, streams: dict[str, str], **__: Any) -> list[Any]:
        stream = next(iter(streams))
        channel = stream.removeprefix("repid:")
        if channel not in delivered:
            delivered.add(channel)
            return [[stream, [(b"1-0", {b"payload": b"x"})]]]
        return await asyncio.Future[list[Any]]()

    async def callback(message: ReceivedMessageT) -> None:
        started[message.channel].set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled[message.channel].set()
            raise

    async def assert_subscribers_closed() -> None:
        assert [subscriber._closed for subscriber in subscribers] == [True, True, True]
        assert all(event.is_set() for event in cancelled.values())
        assert all(subscriber.task.done() for subscriber in subscribers)

    pipe = MagicMock(execute=AsyncMock())
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=None)
    client = MagicMock(
        aclose=AsyncMock(side_effect=assert_subscribers_closed),
        ping=AsyncMock(),
        pipeline=MagicMock(return_value=pipe),
        xgroup_create=AsyncMock(),
        xreadgroup=AsyncMock(side_effect=read_messages),
    )
    mock_redis_cls.from_url.return_value = client
    server = RedisServer("redis://localhost")
    await server.connect()
    subscribers = [
        cast(
            RedisSubscriber,
            await server.subscribe(
                channels_to_callbacks={channel: callback},
                dispatcher=SubscriberDispatcher(),
            ),
        )
        for channel in started
    ]
    second = subscribers[1]
    original_stop = second.stop
    original_finish = second.finish

    async def close_then_fail() -> None:
        await original_stop()
        await original_finish()
        raise ResponseError("close failed")

    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in started.values())),
        timeout=1,
    )

    with patch.object(second, "finish", side_effect=close_then_fail):
        await server.disconnect()

    assert server._active_subscribers == []
    client.aclose.assert_awaited_once()


async def test_redis_failed_admission_requeues_only_unadmitted_messages(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    first = ChannelConfig(stream="first-stream", group="group", dlq=None)
    second = ChannelConfig(stream="second-stream", group="group", dlq=None)

    def oversized_policy(message: ReceivedMessageT) -> OversizedPayloadAction:
        if message.payload == b"drop":
            return "nack"
        if message.payload == b"boom":
            raise ValueError("policy failed")
        return "run_alone"

    async def acknowledge(message: ReceivedMessageT) -> None:
        await message.ack()

    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"first": first, "second": second},
        callbacks={"first": acknowledge, "second": AsyncMock()},
        consumer_name="consumer",
        dispatcher=SubscriberDispatcher(
            MessageLimits(max_payload_bytes=1, on_oversized_payload=oversized_policy),
        ),
    )
    with (
        patch.object(
            client,
            "xreadgroup",
            new=AsyncMock(
                return_value=[
                    [
                        b"first-stream",
                        [
                            (b"1-0", {b"payload": b"x"}),
                            (b"2-0", {b"payload": b"drop"}),
                            (b"3-0", {b"payload": b"boom"}),
                        ],
                    ],
                    [b"second-stream", [(b"4-0", {b"payload": b"tail"})]],
                ],
            ),
        ),
        pytest.raises(ValueError, match="policy failed"),
    ):
        await subscriber._consume_batch("group", {"first": first, "second": second})
    await asyncio.sleep(0)

    assert pipe.xadd.call_count == 2
    assert pipe.xack.call_count == 3
    client.xack.assert_awaited_once_with("first-stream", "group", "1-0")
    assert subscriber.in_flight_count == 0
    await subscriber.stop()
    await subscriber.finish()


@patch.object(message_broker, "Redis")
async def test_redis_disconnect_closes_callback_after_supervisor_failure(
    mock_redis_cls: MagicMock,
) -> None:
    pipe = MagicMock(execute=AsyncMock())
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=None)
    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()
    read_count = 0

    async def read_messages(*_: Any, **__: Any) -> list[Any]:
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            return [[b"repid:jobs", [(b"1-0", {b"payload": b"x"})]]]
        return await asyncio.Future[list[Any]]()

    async def callback(_: ReceivedMessageT) -> None:
        callback_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    async def fail_claim() -> None:
        await callback_started.wait()
        raise ValueError("claim failed")

    client = MagicMock(
        aclose=AsyncMock(),
        ping=AsyncMock(),
        pipeline=MagicMock(return_value=pipe),
        xgroup_create=AsyncMock(),
        xreadgroup=AsyncMock(side_effect=read_messages),
    )
    mock_redis_cls.from_url.return_value = client
    server = RedisServer("redis://localhost", claim_interval=1.0)
    await server.connect()

    with patch.object(RedisSubscriber, "_claim_loop", side_effect=fail_claim):
        subscriber = cast(
            RedisSubscriber,
            await server.subscribe(
                channels_to_callbacks={"jobs": callback},
                dispatcher=SubscriberDispatcher(),
            ),
        )
        with pytest.raises(ValueError, match="claim failed"):
            await subscriber.task

        assert subscriber in server._active_subscribers
        await server.disconnect()

    assert callback_cancelled.is_set()
    assert server._active_subscribers == []


async def test_redis_prefetch_messages_skips_streams_without_channel(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    dispatcher = MagicMock(spec=SubscriberDispatcher)
    config = ChannelConfig(stream="jobs", group="group", dlq="jobs:dlq")
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )

    prefetched = await subscriber._prefetch_messages(
        [
            (b"unknown_stream", [(b"1", {b"payload": b"one"})]),
            (b"jobs", [(b"2", {b"payload": b"two"})]),
        ],
        {"jobs": "jobs"},
    )

    assert list(prefetched) == [("jobs", "2")]
    dispatcher.start_keep_alive.assert_called_once()
    pipe.execute.assert_not_awaited()


async def test_redis_nack_unparseable_message_uses_dlq_maxlen(
    pipeline_mock: tuple[MagicMock, MagicMock],
) -> None:
    client, pipe = pipeline_mock
    config = ChannelConfig(
        stream="jobs",
        group="group",
        dlq="jobs:dlq",
        dlq_maxlen=100,
    )
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=MagicMock(spec=SubscriberDispatcher),
    )

    await subscriber._nack_unparseable_message(
        "jobs",
        "jobs",
        "1-0",
        {b"payload": b"bad", b"headers": b"{"},
    )

    pipe.xadd.assert_called_once_with(
        "jobs:dlq",
        {
            b"payload": b"bad",
            b"headers": b"{",
            b"original_stream": "jobs",
            b"original_id": "1-0",
        },
        maxlen=100,
        approximate=True,
    )
    pipe.xack.assert_called_once_with("jobs", "group", "1-0")
    pipe.execute.assert_awaited_once()
