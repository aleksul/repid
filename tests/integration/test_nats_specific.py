import asyncio
from contextlib import suppress
from typing import Any
from unittest.mock import AsyncMock, Mock, PropertyMock, patch
from urllib.parse import urlparse

import nats
import pytest
from pytest_docker_tools import wrappers

from repid.connections.nats import NatsServer
from repid.connections.nats.message_broker import NatsReceivedMessage


class MockSentMsgNoHeaders:
    payload = b"test2"
    headers = None
    content_type = None


class MockSentMsg:
    payload = b"test"
    content_type = "text/plain"

    def __init__(self) -> None:
        self.headers = {"x": "y"}


async def _get_nats_dsn(nats_container: wrappers.Container) -> str:
    for _ in range(100):
        try:
            nats_container._container.reload()
            port = nats_container.ports["4222/tcp"][0]
            return f"nats://127.0.0.1:{port}"
        except (KeyError, AttributeError):
            await asyncio.sleep(0.1)
    raise RuntimeError("Timed out waiting for NATS container port mapping")


async def test_nats_basic_attributes(nats_connection: NatsServer) -> None:
    parsed = urlparse(nats_connection.dsn)
    expected_host = f"{parsed.hostname}:{parsed.port}"
    assert nats_connection.host == expected_host
    assert nats_connection.protocol == "nats"
    assert nats_connection.pathname is None
    assert nats_connection.title is None
    assert nats_connection.summary is None
    assert nats_connection.description is None
    assert nats_connection.protocol_version is None
    assert nats_connection.variables is None
    assert nats_connection.security is None
    assert nats_connection.tags is None
    assert nats_connection.external_docs is None
    assert nats_connection.bindings is None
    assert nats_connection.capabilities["supports_acknowledgments"] is True


async def test_nats_publish_subscribe(nats_connection: NatsServer) -> None:
    async with nats_connection.connection():
        await nats_connection.publish(channel="another", message=MockSentMsg())

        # Subscriber
        hit = []
        event = asyncio.Event()

        async def cb(msg: Any) -> None:
            hit.append(msg)
            assert msg.headers == {"x": "y", "content-type": "text/plain"}
            assert msg.content_type == "text/plain"
            assert msg.channel == "another"
            assert msg.action is None
            assert not msg.is_acted_on
            assert msg.message_id is not None
            await msg.ack()
            await msg.ack()  # Double ack
            event.set()

        sub = await nats_connection.subscribe(channels_to_callbacks={"another": cb})
        await asyncio.wait_for(event.wait(), timeout=5.0)
        assert len(hit) == 1

        # Pause/Resume
        await sub.pause()
        await sub.resume()
        assert sub.is_active is True

        # Test resume when already active
        await sub.resume()

        await sub.close()


async def test_nats_reject(nats_connection: NatsServer) -> None:
    async with nats_connection.connection():
        hit_reject = []
        event_reject = asyncio.Event()

        async def cb_reject(msg: Any) -> None:
            hit_reject.append(True)
            if len(hit_reject) == 1:
                await msg.reject()
                await msg.reject()
                event_reject.set()
            else:
                await msg.ack()

        await nats_connection.publish(channel="test_reject_channel", message=MockSentMsg())
        sub_reject = await nats_connection.subscribe(
            channels_to_callbacks={"test_reject_channel": cb_reject},
        )
        await asyncio.wait_for(event_reject.wait(), timeout=5.0)
        await sub_reject.close()


async def test_nats_nack(nats_connection: NatsServer) -> None:
    async with nats_connection.connection():
        hit_nack = []
        event_nack = asyncio.Event()

        async def cb_nack(msg: Any) -> None:
            hit_nack.append(True)
            if len(hit_nack) == 1:
                await msg.nack()
                await msg.nack()
                event_nack.set()
            else:
                await msg.ack()

        await nats_connection.publish(channel="test_nack_channel", message=MockSentMsg())
        sub_nack = await nats_connection.subscribe(
            channels_to_callbacks={"test_nack_channel": cb_nack},
        )
        await asyncio.wait_for(event_nack.wait(), timeout=5.0)
        await sub_nack.close()


async def test_nats_reply(nats_connection: NatsServer) -> None:
    async with nats_connection.connection():
        hit_reply = []
        event_reply = asyncio.Event()

        async def cb_reply(msg: Any) -> None:
            hit_reply.append(True)
            await msg.reply(payload=b"resp", channel="test_reply_channel")
            await msg.reply(payload=b"resp", channel="test_reply_channel")
            event_reply.set()

        await nats_connection.publish(channel="test_reply_channel", message=MockSentMsg())
        sub_reply = await nats_connection.subscribe(
            channels_to_callbacks={"test_reply_channel": cb_reply},
        )
        await asyncio.wait_for(event_reply.wait(), timeout=5.0)
        assert hit_reply
        await sub_reply.close()


async def test_nats_exception(nats_connection: NatsServer) -> None:
    async with nats_connection.connection():
        hit_exc = []
        event_exc = asyncio.Event()

        async def cb_exc(msg: Any) -> None:
            hit_exc.append(True)
            if len(hit_exc) == 1:
                event_exc.set()
                raise ValueError("test")
            await msg.ack()

        await nats_connection.publish(channel="test_exc_channel", message=MockSentMsg())
        sub_exc = await nats_connection.subscribe(
            channels_to_callbacks={"test_exc_channel": cb_exc},
            concurrency_limit=1,
        )
        await asyncio.wait_for(event_exc.wait(), timeout=5.0)
        assert hit_exc
        await sub_exc.close()


async def test_nats_connect_already_connected(nats_container: wrappers.Container) -> None:
    dsn = await _get_nats_dsn(nats_container)
    server = NatsServer(dsn, dlq_topic_strategy=None)

    await server.connect()
    await server.connect()
    await server.disconnect()


async def test_nats_subscribe_already_connected(nats_container: wrappers.Container) -> None:
    dsn = await _get_nats_dsn(nats_container)
    server = NatsServer(dsn, dlq_topic_strategy=None)

    await server.connect()

    async def cb(msg: Any) -> None:
        pass

    sub_dummy = await server.subscribe(channels_to_callbacks={"another_edge_case": cb})
    await sub_dummy.close()
    await server.disconnect()


async def test_nats_connection_context_manager(nats_container: wrappers.Container) -> None:
    dsn = await _get_nats_dsn(nats_container)
    server = NatsServer(dsn, dlq_topic_strategy=None)

    async with server.connection() as srv:
        assert srv.is_connected
    assert not server.is_connected


async def test_nats_message_without_headers_and_content_type(
    nats_container: wrappers.Container,
) -> None:
    dsn = await _get_nats_dsn(nats_container)
    server = NatsServer(dsn, dlq_topic_strategy=None)
    await server.connect()

    nc = await nats.connect(dsn)
    js = nc.jetstream()
    try:
        await js.add_stream(name="test_nack_channel_stream_2", subjects=["test_nack_channel_2"])
    except Exception as e:
        if "already" not in str(e).lower():
            raise
    await nc.close()

    hit = []
    event = asyncio.Event()

    async def cb(msg: Any) -> None:
        hit.append(msg)
        assert msg.headers is None
        assert msg.content_type is None
        await msg.nack()  # This will hit `dlq is None` -> `await self._msg.term()`
        event.set()

    # Call subscribe BEFORE publish to cover `if not self.is_connected` inside `subscribe`
    sub = await server.subscribe(channels_to_callbacks={"test_nack_channel_2": cb})

    await server.publish(channel="test_nack_channel_2", message=MockSentMsgNoHeaders())
    await asyncio.wait_for(event.wait(), timeout=5.0)
    assert hit
    await sub.close()
    await server.disconnect()


async def test_nats_received_message_properties_none() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)

    # Test message_id=None by creating a fake msg
    mock_msg = Mock(ack=AsyncMock(), headers=None, data=b"abc", metadata=None)

    wrapped = NatsReceivedMessage(mock_msg, server, "test")
    assert wrapped.message_id is None
    assert wrapped.headers is None
    assert wrapped.content_type is None


async def test_nats_received_message_reply_fallback_to_core_nats() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), reply="mock_reply")
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    # Test reply when _js is None
    server._js = None
    mock_nc = Mock(publish=AsyncMock())
    server._nc = mock_nc

    await wrapped.reply(payload=b"resp", content_type="text/plain")
    mock_nc.publish.assert_called_once_with(
        "test",
        b"resp",
        headers={"content-type": "text/plain"},
    )


async def test_nats_received_message_nack_fallback_to_core_nats_dlq() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=lambda ch: f"{ch}_dlq")
    mock_msg = Mock(ack=AsyncMock(), data=b"abc", headers=None)
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    mock_nc = Mock(publish=AsyncMock())
    server._nc = mock_nc

    # Test nack when _js is None and DLQ is present
    wrapped._is_acted_on = False
    await wrapped.nack()
    mock_nc.publish.assert_called_with(
        "test_dlq",
        b"abc",
        headers={"x-repid-original-channel": "test"},
    )


async def test_nats_received_message_reply_connection_error_calls_nak() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), nak=AsyncMock())
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = None
    wrapped._is_acted_on = False
    with suppress(ConnectionError):
        await wrapped.reply(payload=b"resp")
    mock_msg.nak.assert_called_once()


async def test_nats_received_message_nack_connection_error_calls_nak() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=lambda ch: f"{ch}_dlq")
    mock_msg = Mock(ack=AsyncMock(), nak=AsyncMock(), headers=None)
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = None

    wrapped._is_acted_on = False
    with suppress(ConnectionError):
        await wrapped.nack()
    mock_msg.nak.assert_called_once()


async def test_nats_received_message_nack_no_dlq_calls_term() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), term=AsyncMock())
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = None

    wrapped._is_acted_on = False
    await wrapped.nack()
    mock_msg.term.assert_called_once()


async def test_nats_received_message_nack_no_dlq_calls_nak_if_no_term() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), nak=AsyncMock(), spec=["ack", "nak"])
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = None

    wrapped._is_acted_on = False
    await wrapped.nack()
    mock_msg.nak.assert_called_once()


async def test_nats_publish_fallback_to_core_nats() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)

    server._js = None
    mock_nc = Mock(publish=AsyncMock(), is_connected=True)
    server._nc = mock_nc
    await server.publish(channel="test_pub", message=MockSentMsgNoHeaders())
    mock_nc.publish.assert_called_with("test_pub", b"test2", headers={})


async def test_nats_publish_connection_error_when_no_clients() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)

    server._js = None
    server._nc = None
    with (
        patch.object(NatsServer, "is_connected", new_callable=PropertyMock, return_value=True),
        suppress(ConnectionError),
    ):
        await server.publish(channel="test_pub", message=MockSentMsgNoHeaders())


async def test_nats_publish_when_not_connected() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    with suppress(ConnectionError):
        await server.publish(channel="test_pub", message=MockSentMsgNoHeaders())


async def test_nats_subscribe_when_not_connected() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    with suppress(ConnectionError):
        await server.subscribe(channels_to_callbacks={"test": AsyncMock()})


async def test_nats_subscribe_connection_error_when_no_js() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)

    server._js = None
    with (
        patch.object(NatsServer, "is_connected", new_callable=PropertyMock, return_value=True),
        pytest.raises(
            ConnectionError,
            match=r"NATS connection is not initialized\. Call connect\(\) first\.",
        ),
    ):
        await server.subscribe(channels_to_callbacks={"test": AsyncMock()})


async def test_nats_subscribe_callback_exception_calls_term() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    server._js = Mock(subscribe=AsyncMock(), publish=AsyncMock())
    server._dlq_topic_strategy = None

    with patch.object(NatsServer, "is_connected", new_callable=PropertyMock, return_value=True):
        sub_test = await server.subscribe(
            channels_to_callbacks={"test": AsyncMock(side_effect=ValueError)},
            concurrency_limit=None,
        )
        await asyncio.sleep(0.1)

    message_handler = server._js.subscribe.call_args[1]["cb"]

    fake_nats_msg = Mock(
        nak=AsyncMock(),
        term=AsyncMock(),
        ack=AsyncMock(),
        headers=None,
        data=b"",
        metadata=None,
    )

    await message_handler(fake_nats_msg)
    fake_nats_msg.term.assert_called_once()
    await sub_test.close()
