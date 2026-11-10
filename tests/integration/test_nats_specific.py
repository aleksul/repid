import asyncio
from contextlib import suppress
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, patch
from urllib.parse import urlparse

import pytest
from nats.js.api import ConsumerConfig, StreamConsumerLimits

from repid import Concurrency, Repid, Router
from repid.connections.abc import MessageAction, ReceivedMessageT, ServerT
from repid.connections.nats import NatsServer
from repid.connections.nats.message_broker import NatsReceivedMessage

pytestmark = pytest.mark.integration_target(name="nats")


class MockSentMsgNoHeaders:
    payload = b"test2"
    headers: None = None
    content_type: None = None
    reply_to: None = None


class MockSentMsg:
    payload = b"test"
    content_type = "text/plain"
    reply_to: None = None

    def __init__(self) -> None:
        self.headers = {"x": "y"}


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

        sub = await nats_connection.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"another": cb},
        )
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
            concurrency=Concurrency(),
            channel_concurrency={},
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
            concurrency=Concurrency(),
            channel_concurrency={},
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
            concurrency=Concurrency(),
            channel_concurrency={},
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
            channel_concurrency={},
            channels_to_callbacks={"test_exc_channel": cb_exc},
            concurrency=Concurrency(messages=1),
        )
        await asyncio.wait_for(event_exc.wait(), timeout=5.0)
        assert hit_exc
        await sub_exc.close()


async def test_nats_connect_already_connected(nats_connection: NatsServer) -> None:
    server = NatsServer(nats_connection.dsn, dlq_topic_strategy=None)

    await server.connect()
    await server.connect()
    await server.disconnect()


async def test_nats_subscribe_already_connected(nats_connection: NatsServer) -> None:
    server = NatsServer(nats_connection.dsn, dlq_topic_strategy=None)

    await server.connect()

    async def cb(msg: Any) -> None:
        pass

    sub_dummy = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"another_edge_case": cb},
    )
    await sub_dummy.close()
    await server.disconnect()


async def test_nats_connection_context_manager(nats_connection: NatsServer) -> None:
    server = NatsServer(nats_connection.dsn, dlq_topic_strategy=None)

    async with server.connection() as srv:
        assert srv.is_connected
    assert not server.is_connected


async def test_nats_message_without_headers_and_content_type(
    nats_connection: NatsServer,
) -> None:
    server = NatsServer(nats_connection.dsn, dlq_topic_strategy=None)
    await server.connect()

    hit = []
    event = asyncio.Event()

    async def cb(msg: Any) -> None:
        hit.append(msg)
        assert msg.headers is None
        assert msg.content_type is None
        await msg.nack()  # This will hit `dlq is None` -> `await self._msg.term()`
        event.set()

    # Call subscribe BEFORE publish to cover `if not self.is_connected` inside `subscribe`
    sub = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"test_nack_channel_2": cb},
    )

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
        await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"test": AsyncMock()},
        )


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
        await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"test": AsyncMock()},
        )


async def test_nats_subscribe_callback_exception_calls_term() -> None:
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    info = Mock(
        config=ConsumerConfig(ack_wait=30, deliver_group="test_group", deliver_subject="inbox"),
    )
    subscription = Mock(unsubscribe=AsyncMock())
    server._nc = MagicMock(is_connected=True)
    server._js = Mock(
        find_stream_name_by_subject=AsyncMock(return_value="stream"),
        consumer_info=AsyncMock(return_value=info),
        subscribe_bind=AsyncMock(return_value=subscription),
        publish=AsyncMock(),
    )
    server._dlq_topic_strategy = None

    with patch.object(NatsServer, "is_connected", new_callable=PropertyMock, return_value=True):
        sub_test = await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"test": AsyncMock(side_effect=ValueError)},
        )
        await asyncio.sleep(0.1)

    message_handler = server._js.subscribe_bind.call_args[1]["cb"]

    terminated = asyncio.Event()
    fake_nats_msg = Mock(
        nak=AsyncMock(),
        term=AsyncMock(side_effect=terminated.set),
        ack=AsyncMock(),
        headers=None,
        data=b"",
        metadata=None,
    )

    await message_handler(fake_nats_msg)
    await asyncio.wait_for(terminated.wait(), 1)
    fake_nats_msg.term.assert_awaited_once()
    await sub_test.close()


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


@pytest.mark.parametrize("hint", [None, 20])
async def test_nats_new_durable_inherits_stream_ack_limit(
    nats_connection: ServerT,
    hint: int | None,
) -> None:
    assert isinstance(nats_connection, NatsServer)
    channel = "limited_ack_pending"
    stream = "limited_ack_pending_stream"
    delivered = asyncio.Event()

    async def callback(message: ReceivedMessageT) -> None:
        await message.ack()
        delivered.set()

    async with nats_connection.connection():
        js = nats_connection._js
        assert js is not None
        await js.add_stream(
            name=stream,
            subjects=[channel],
            consumer_limits=StreamConsumerLimits(max_ack_pending=3),
        )
        try:
            subscriber = await nats_connection.subscribe(
                channel_concurrency={},
                channels_to_callbacks={channel: callback},
                concurrency=Concurrency(messages=hint),
            )
            try:
                await js.publish(channel, b"{}")
                await asyncio.wait_for(delivered.wait(), 2)
                info = await js.consumer_info(stream, f"{channel}_group")
                assert info.config.max_ack_pending == 3
            finally:
                await subscriber.close()
        finally:
            await js.delete_stream(stream)


async def test_nats_waiting_backlog_retains_short_broker_leases(
    nats_connection: NatsServer,
) -> None:
    channel = "admission_leases"
    stream = "admission_leases_stream"
    durable = f"{channel}_group"
    ack_wait = 0.25
    actor_started = asyncio.Event()
    release_actor = asyncio.Event()

    app = Repid()
    app.servers.register_server("nats", nats_connection, is_default=True)
    router = Router(channel=channel)

    @router.actor
    async def job() -> None:
        actor_started.set()
        await release_actor.wait()

    app.include_router(router)
    async with nats_connection.connection():
        js = nats_connection._js
        nc = nats_connection._nc
        assert js is not None
        assert nc is not None
        await js.add_stream(name=stream, subjects=[channel])
        try:
            await js.add_consumer(
                stream,
                config=ConsumerConfig(
                    durable_name=durable,
                    deliver_group=durable,
                    deliver_subject=nc.new_inbox(),
                    filter_subject=channel,
                    ack_wait=ack_wait,
                    max_deliver=1,
                    max_ack_pending=3,
                ),
            )
            for _ in range(3):
                await app.send_message_json(channel=channel, payload={}, headers={"topic": "job"})
            worker = asyncio.create_task(
                app.run_worker(
                    messages_limit=3,
                    concurrency=Concurrency(messages=1),
                    register_signals=[],
                ),
            )
            try:
                async with asyncio.timeout(5):
                    await actor_started.wait()
                    while (await js.consumer_info(stream, durable)).delivered.consumer_seq < 3:
                        await asyncio.sleep(0.01)
                    await asyncio.sleep(ack_wait * 3)
                    assert (await js.consumer_info(stream, durable)).num_ack_pending == 3
                    release_actor.set()
                    assert (await worker).processed == 3
            finally:
                release_actor.set()
                if not worker.done():
                    worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)
        finally:
            await js.delete_stream(stream)
