import asyncio
from contextlib import suppress
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, patch
from urllib.parse import urlparse

import nats
import pytest
from nats.js.api import ConsumerConfig
from pytest_docker_tools import wrappers

from repid.connections.abc import MessageAction
from repid.connections.nats import NatsServer
from repid.connections.nats.message_broker import NatsReceivedMessage, NatsSubscriber
from repid.limits import NativeFlow, NativeWindow


class MockSentMsgNoHeaders:
    payload = b"test2"
    headers = None
    content_type = None
    reply_to = None


class MockSentMsg:
    payload = b"test"
    content_type = "text/plain"
    reply_to = None

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
    assert nats_connection.capabilities["supports_native_reply"] is True


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

        await sub.finish()


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
        await sub_reject.finish()


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
        await sub_nack.finish()


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
        await sub_reply.finish()


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
            native_flow=NativeFlow(),
        )
        await asyncio.wait_for(event_exc.wait(), timeout=5.0)
        assert hit_exc
        await sub_exc.finish()


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
    await sub_dummy.finish()
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
    await sub.finish()
    await server.disconnect()


async def test_nats_received_message_properties_none() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)

    # Test message_id=None by creating a fake msg
    mock_msg = Mock(ack=AsyncMock(), headers=None, data=b"abc", metadata=None)

    wrapped = NatsReceivedMessage(mock_msg, server, "test")
    assert wrapped.message_id is None
    assert wrapped.headers is None
    assert wrapped.content_type is None
    assert wrapped.reply_to is None


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
        "mock_reply",
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
    wrapped._action = None
    await wrapped.nack()
    mock_nc.publish.assert_called_with(
        "test_dlq",
        b"abc",
        headers={"x-repid-original-channel": "test"},
    )


async def test_nats_received_message_reply_connection_error_calls_nak() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), nak=AsyncMock(), reply="test.reply")
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = None
    wrapped._action = None
    with pytest.raises(ConnectionError, match="NATS connection is not initialized"):
        await wrapped.reply(payload=b"resp")
    mock_msg.nak.assert_awaited_once()


async def test_nats_received_message_reply_requires_channel_or_reply_to() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), nak=AsyncMock())
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = Mock(publish=AsyncMock())
    wrapped._action = None

    with pytest.raises(ValueError, match="Reply channel is not set"):
        await wrapped.reply(payload=b"resp")


async def test_nats_received_message_reply_uses_js_client() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), reply="reply.target")
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    mock_js = Mock(publish=AsyncMock())
    server._js = mock_js
    server._nc = None

    await wrapped.reply(
        payload=b"resp",
        headers={"content-type": "application/json"},
    )

    mock_js.publish.assert_awaited_once_with(
        "reply.target",
        b"resp",
        headers={"content-type": "application/json"},
    )


async def test_nats_publish_with_correlation_and_reply_to() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)

    class SentWithMeta:
        def __init__(self) -> None:
            self.payload = b"test"
            self.headers = {"x": "1"}
            self.content_type = "text/plain"
            self.reply_to = "reply.topic"

    server._js = None
    mock_nc = Mock(publish=AsyncMock(), is_connected=True)
    server._nc = mock_nc

    await server.publish(channel="test_pub", message=SentWithMeta())
    mock_nc.publish.assert_called_with(
        "test_pub",
        b"test",
        reply="reply.topic",
        headers={"x": "1", "content-type": "text/plain"},
    )


async def test_nats_received_message_nack_connection_error_calls_nak() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=lambda ch: f"{ch}_dlq")
    mock_msg = Mock(ack=AsyncMock(), nak=AsyncMock(), headers=None)
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = None

    wrapped._action = None
    with suppress(ConnectionError):
        await wrapped.nack()
    mock_msg.nak.assert_called_once()


async def test_nats_received_message_nack_no_dlq_calls_term() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(ack=AsyncMock(), term=AsyncMock())
    wrapped = NatsReceivedMessage(mock_msg, server, "test")

    server._js = None
    server._nc = None

    wrapped._action = None
    await wrapped.nack()
    mock_msg.term.assert_called_once()


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


async def test_nats_submission_failure_preserves_transferred_ownership() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    message = Mock(
        nak=AsyncMock(),
        term=AsyncMock(),
        ack=AsyncMock(),
        headers=None,
        data=b"",
        metadata=None,
    )
    info = Mock(
        config=ConsumerConfig(
            ack_wait=30,
            deliver_subject="push",
            deliver_group="test_group",
            max_ack_pending=1000,
        ),
    )
    subscription = Mock(
        _sub=Mock(drain=AsyncMock()),
        unsubscribe=AsyncMock(),
        consumer_info=AsyncMock(return_value=info),
    )
    server._js = Mock(
        find_stream_name_by_subject=AsyncMock(return_value="test_stream"),
        subscribe=AsyncMock(return_value=subscription),
        consumer_info=AsyncMock(return_value=info),
    )
    subscriber = NatsSubscriber(server, {"test": AsyncMock(side_effect=ValueError("submit"))})
    await asyncio.wait_for(subscriber._ready.wait(), 2)
    await server._js.subscribe.await_args.kwargs["cb"](message)
    with pytest.raises(ValueError, match="submit"):
        await asyncio.wait_for(subscriber.task, 2)
    message.term.assert_not_awaited()
    message.nak.assert_not_awaited()
    with pytest.raises(ValueError, match="submit"):
        await subscriber.finish()


async def test_nats_received_message_reply_to_ignores_js_ack() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    mock_msg = Mock(headers=None, reply="$JS.ACK.stream.consumer.1.2.3")
    wrapped = NatsReceivedMessage(mock_msg, server, "test")
    assert wrapped.reply_to is None


async def test_nats_received_message_keep_alive() -> None:
    mock_msg = AsyncMock()
    mock_server = MagicMock()
    msg = NatsReceivedMessage(mock_msg, mock_server, "test_channel")

    # Test keep_alive when not acted on
    msg._action = None
    await msg.keep_alive()
    mock_msg.in_progress.assert_awaited_once()

    # Test keep_alive when acted on
    mock_msg.in_progress.reset_mock()
    msg._action = MessageAction.acked
    await msg.keep_alive()
    mock_msg.in_progress.assert_not_called()


async def test_nats_keep_alive_interval() -> None:
    mock_msg = AsyncMock()
    mock_server = MagicMock()
    msg = NatsReceivedMessage(mock_msg, mock_server, "test_channel", ack_wait=3.0)
    assert msg.keep_alive_interval == 1


async def test_nats_push_native_window_is_shared_and_released_by_settlement(
    nats_connection: NatsServer,
) -> None:
    async with nats_connection.connection():
        assert nats_connection._js is not None
        js = nats_connection._js
        channel = "push_capacity_contract"
        await js.add_stream(name=channel, subjects=[channel])
        first_window, third_delivery = asyncio.Event(), asyncio.Event()
        messages: list[Any] = []

        async def callback(message: Any) -> None:
            messages.append(message)
            if len(messages) == 2:
                first_window.set()
            if len(messages) == 3:
                third_delivery.set()

        flow = NativeFlow(channels={channel: NativeWindow(max_messages=2)})
        one = cast(
            NatsSubscriber,
            await nats_connection.subscribe(
                channels_to_callbacks={channel: callback},
                native_flow=flow,
            ),
        )
        await asyncio.wait_for(one._ready.wait(), 5)
        two = cast(
            NatsSubscriber,
            await nats_connection.subscribe(
                channels_to_callbacks={channel: callback},
                native_flow=flow,
            ),
        )
        # Ensure both bindings exist before publishing into the shared group.
        await asyncio.wait_for(one._ready.wait(), 5)
        await asyncio.wait_for(two._ready.wait(), 5)
        for _ in range(3):
            await nats_connection.publish(channel=channel, message=MockSentMsg())
        await asyncio.wait_for(first_window.wait(), 5)
        info = await js.consumer_info(channel, f"{channel}_group")
        assert info.config.deliver_subject
        assert info.config.max_ack_pending == 2
        assert info.num_ack_pending == 2
        assert info.num_pending == 1
        assert not third_delivery.is_set()
        await messages[0].ack()
        await asyncio.wait_for(third_delivery.wait(), 5)
        await one.stop()
        await two.stop()
        # Stopping subscriptions retains the shared connection for outstanding work.
        assert nats_connection.is_connected
        for message in messages[1:]:
            await message.keep_alive()
            # The server reply is a barrier for the following consumer-state assertion.
            await message._msg.ack_sync()
        assert (await js.consumer_info(channel, f"{channel}_group")).num_ack_pending == 0
        await one.finish()
        await two.finish()


async def test_nats_existing_push_durable_survives_pause_and_resume(
    nats_connection: NatsServer,
) -> None:
    async with nats_connection.connection():
        assert nats_connection._js is not None
        assert nats_connection._nc is not None
        js = nats_connection._js
        channel = "existing_push_contract"
        group = f"{channel}_group"
        await js.add_stream(name=channel, subjects=[channel])
        created = await js.add_consumer(
            channel,
            ConsumerConfig(
                durable_name=group,
                deliver_group=group,
                deliver_subject=nats_connection._nc.new_inbox(),
                filter_subject=channel,
                max_ack_pending=1,
            ),
        )
        delivered = asyncio.Event()

        async def callback(message: Any) -> None:
            await message.ack()
            delivered.set()

        subscriber = await nats_connection.subscribe(
            channels_to_callbacks={channel: callback},
            native_flow=NativeFlow(
                channels={channel: NativeWindow(max_messages=3, messages_automatic=True)},
            ),
        )
        await asyncio.wait_for(cast(NatsSubscriber, subscriber)._ready.wait(), 5)
        await subscriber.pause()
        await nats_connection.publish(channel=channel, message=MockSentMsg())
        assert (await js.consumer_info(channel, group)).num_ack_pending == 0
        await subscriber.resume()
        await asyncio.wait_for(delivered.wait(), 5)
        after = await js.consumer_info(channel, group)
        assert after.config.deliver_subject == created.config.deliver_subject
        assert after.config.max_ack_pending == 1
        await subscriber.finish()
