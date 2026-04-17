from __future__ import annotations

import asyncio
import contextlib
from typing import Any, ClassVar, cast

import pytest

from repid.connections.abc import MessageAction, SentMessageT
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.message_broker import AmqpServer
from repid.connections.amqp.protocol import ManagedSession
from repid.connections.amqp.protocol.connection import (
    AmqpConnection,
    ConnectionConfig,
)
from repid.connections.amqp.protocol.managed import ReceiverPool, SenderPool
from repid.connections.amqp.subscriber import AmqpSubscriber
from repid.data import MessageData

from .utils import (
    FakeConnection,
    FakeManagedConnection,
    FakeReceiverLink,
    FakeSession,
    FakeSessionForPools,
)


async def test_message_broker_not_connected_publish() -> None:
    broker = AmqpServer("amqp://localhost:5672")
    broker._managed_session = None
    with pytest.raises(ConnectionError, match="Not connected"):
        await broker.publish(
            channel="test",
            message=MessageData(payload=b"x", headers=None, content_type=None),
        )


async def test_message_broker_not_connected_subscribe() -> None:
    broker = AmqpServer("amqp://localhost:5672")
    broker._managed_session = None
    with pytest.raises(ConnectionError, match="Not connected"):
        await broker.subscribe(channels_to_callbacks={"test": lambda _x: asyncio.sleep(0)})


async def test_amqp_server_publish_subscribe_and_disconnect(monkeypatch: Any) -> None:
    server = AmqpServer("amqp://user:pass@localhost:5672/vhost")

    class DummyConnection:
        def __init__(self, _config: ConnectionConfig):
            self.is_connected = False

        async def connect(self) -> None:
            self.is_connected = True

        async def close(self) -> None:
            self.is_connected = False

    class DummyManagedSession:
        def __init__(self, _connection: Any):
            self.sender_pool = cast(SenderPool, object())
            self.receiver_pool = cast(ReceiverPool, object())
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    async def fake_create(*_args: Any, **_kwargs: Any) -> AmqpSubscriber:
        return cast(AmqpSubscriber, object())

    monkeypatch.setattr(
        "repid.connections.amqp.message_broker.AmqpConnection",
        DummyConnection,
    )
    monkeypatch.setattr(
        "repid.connections.amqp.message_broker.ManagedSession",
        DummyManagedSession,
    )
    monkeypatch.setattr(
        "repid.connections.amqp.message_broker.AmqpSubscriber.create",
        fake_create,
    )

    await server.connect()
    assert server.is_connected is True

    server._managed_session = DummyManagedSession(object())  # type: ignore[assignment]
    sender_calls: list[dict[str, Any]] = []

    class DummySenderPool:
        async def send(self, address: str, payload: bytes, **_kwargs: Any) -> None:
            sender_calls.append({"address": address, "payload": payload})

    managed_session = cast(ManagedSession, server._managed_session)
    cast(Any, managed_session).sender_pool = DummySenderPool()

    await server.publish(
        channel="queue",
        message=MessageData(payload=b"data", headers=None, content_type=None),
    )
    await server.publish(
        channel="queue",
        message=MessageData(payload=b"data", headers=None, content_type=None),
        server_specific_parameters={"to": "/direct"},
    )

    assert sender_calls[0]["address"].endswith("/queues/queue")
    assert sender_calls[1]["address"] == "/direct"

    await server.subscribe(channels_to_callbacks={"queue": lambda _msg: asyncio.sleep(0)})
    await server.disconnect()
    assert server.is_connected is False


async def test_amqp_subscriber_create_pause_resume_and_close() -> None:
    received: list[bytes] = []
    receiver_links: list[FakeReceiverLink] = [FakeReceiverLink(handle=5)]

    class DummyReceiverPool:
        def __init__(self) -> None:
            self.subscriptions: list[tuple[str, Any, str]] = []
            self.unsubscribed: list[str] = []

        async def subscribe(
            self,
            address: str,
            callback: Any,
            name: str,
            prefetch: int = 100,  # noqa: ARG002
        ) -> FakeReceiverLink:
            self.subscriptions.append((address, callback, name))
            return receiver_links[0]

        async def unsubscribe(self, address: str) -> None:
            self.unsubscribed.append(address)

    class DummyManagedSession:
        def __init__(self) -> None:
            self.receiver_pool = DummyReceiverPool()
            self.connection = FakeConnection()

        async def get_session(self) -> FakeSession:
            return FakeSession(connection=self.connection, channel=4)

    async def callback(msg: Any) -> None:
        received.append(msg.payload)

    managed = DummyManagedSession()

    subscriber = await AmqpSubscriber.create(
        managed_session=cast(ManagedSession, managed),
        queues_to_callbacks={"queue": callback},
        naming_strategy=lambda q: f"/queues/{q}",
        publish_fn=lambda **_kwargs: asyncio.sleep(0),
    )

    address, wrapped_callback, _name = managed.receiver_pool.subscriptions[0]
    await subscriber.pause()

    task = asyncio.create_task(
        wrapped_callback(b"data", None, 1, b"tag", receiver_links[0]),
    )
    await asyncio.sleep(0)
    assert received == []

    await subscriber.resume()
    await task

    assert received == [b"data"]
    assert address == "/queues/queue"

    await subscriber.close()
    assert managed.receiver_pool.unsubscribed == ["/queues/queue"]


async def test_amqp_received_message_headers_and_ack_nack_reply() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()

    published: list[tuple[str, MessageData]] = []

    async def publish_fn(*, channel: str, message: MessageData, **_kwargs: Any) -> None:
        published.append((channel, message))

    msg = AmqpReceivedMessage(
        payload=b"data",
        headers=cast(dict[str, Any], {b"a": b"1", "b": "2"}),
        link=cast(Any, link),
        delivery_id=1,
        delivery_tag=b"tag",
        channel_name="queue",
        managed_session=cast(ManagedSession, object()),
        publish_fn=publish_fn,
    )

    assert msg.headers == {"a": "1", "b": "2"}

    await msg.ack()
    await msg.nack()
    await msg.reply(payload=b"reply")

    assert msg.is_acted_on is True
    assert msg.action == MessageAction.acked
    assert len(connection.sent) == 1
    assert published == []  # reply is no-op after ack


async def test_amqp_received_message_nack() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers={},
        link=cast(Any, link),
        delivery_id=123,
        delivery_tag=b"tag",
        channel_name="test",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
    )
    await msg.nack()
    # Second nack should be no-op
    await msg.nack()
    assert msg._action == MessageAction.nacked


async def test_amqp_received_message_reject() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers={},
        link=cast(Any, link),
        delivery_id=123,
        delivery_tag=b"tag",
        channel_name="test",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
    )
    await msg.reject()
    # Second reject should be no-op
    await msg.reject()
    assert msg._action == MessageAction.rejected


async def test_amqp_received_message_properties() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers={"key": "value"},
        link=cast(Any, link),
        delivery_id=123,
        delivery_tag=b"tag",
        channel_name="test-channel",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
    )

    assert msg.content_type is None
    assert msg.reply_to is None
    assert msg.channel == "test-channel"
    assert msg.message_id is None
    assert msg.is_acted_on is False

    await msg.ack()
    assert msg.is_acted_on is True


async def test_amqp_received_message_bytes_message_id() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()

    def make_msg(props: Properties) -> AmqpReceivedMessage:
        return AmqpReceivedMessage(
            payload=b"test",
            headers=None,
            link=cast(Any, link),
            delivery_id=1,
            delivery_tag=b"tag",
            channel_name="q",
            managed_session=cast(ManagedSession, object()),
            publish_fn=lambda: asyncio.sleep(0),
            properties=props,
        )

    assert make_msg(Properties(message_id=b"my-bytes-id")).message_id == "my-bytes-id"
    assert make_msg(Properties(message_id=42)).message_id == "42"


async def test_amqp_received_message_reply_to() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers=None,
        link=cast(Any, link),
        delivery_id=1,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
        properties=Properties(reply_to="reply-to"),
    )

    assert msg.reply_to == "reply-to"


async def test_amqp_received_message_bytes_properties() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers=None,
        link=cast(Any, link),
        delivery_id=1,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
        properties=Properties(
            content_type=cast(Any, b"application/json"),
            reply_to=cast(Any, b"reply-bytes"),
        ),
    )

    assert msg.content_type == "application/json"
    assert msg.reply_to == "reply-bytes"


async def test_amqp_received_message_non_bytes_content_type_casts_to_str() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers=None,
        link=cast(Any, link),
        delivery_id=1,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
        properties=Properties(content_type=cast(Any, 123)),
    )

    assert msg.content_type == "123"


async def test_amqp_publish_fills_missing_message_id_on_existing_properties(
    monkeypatch: Any,
) -> None:
    sent_properties: list[Properties] = []

    class FakeSenderPool:
        async def send(
            self,
            address: str,  # noqa: ARG002
            body: bytes,  # noqa: ARG002
            *,
            headers: Any = None,  # noqa: ARG002
            message_properties: Properties | None = None,
            **kwargs: Any,  # noqa: ARG002
        ) -> None:
            if message_properties is not None:
                sent_properties.append(message_properties)

    class FakeManagedSessionWithSender:
        sender_pool = FakeSenderPool()
        is_connected = True

        async def get_session(self) -> None:
            pass

    broker = AmqpServer("amqp://guest:guest@localhost:5672/")
    monkeypatch.setattr(broker, "_managed_session", FakeManagedSessionWithSender())

    class FakeMessage:
        payload = b"hello"
        headers: ClassVar[dict] = {}

    # Provide Properties with message_id=None — should be auto-filled
    await broker.publish(
        channel="queue",
        message=cast(SentMessageT, FakeMessage()),
        server_specific_parameters={"properties": Properties(message_id=None)},
    )

    assert len(sent_properties) == 1
    assert sent_properties[0].message_id is not None


async def test_amqp_publish_fills_missing_reply_to_on_existing_properties(
    monkeypatch: Any,
) -> None:
    sent_properties: list[Properties] = []

    class FakeSenderPool:
        async def send(
            self,
            address: str,  # noqa: ARG002
            body: bytes,  # noqa: ARG002
            *,
            headers: Any = None,  # noqa: ARG002
            message_properties: Properties | None = None,
            **kwargs: Any,  # noqa: ARG002
        ) -> None:
            if message_properties is not None:
                sent_properties.append(message_properties)

    class FakeManagedSessionWithSender:
        sender_pool = FakeSenderPool()
        is_connected = True

        async def get_session(self) -> None:
            pass

    broker = AmqpServer("amqp://guest:guest@localhost:5672/")
    monkeypatch.setattr(broker, "_managed_session", FakeManagedSessionWithSender())

    class FakeMessage:
        payload = b"hello"
        headers: ClassVar[dict] = {}
        reply_to = "reply-1"

    await broker.publish(
        channel="queue",
        message=cast(SentMessageT, FakeMessage()),
        server_specific_parameters={"properties": Properties(message_id="id-1")},
    )

    assert len(sent_properties) == 1
    assert sent_properties[0].reply_to == "reply-1"


async def test_message_broker_properties() -> None:
    broker = AmqpServer(
        "amqp://user:pass@example.com:5672/vhost",
        title="Test Server",
        summary="Summary text",
        description="Description text",
    )

    assert broker.protocol == "amqp"
    assert broker.host == "example.com:5672"
    assert broker.pathname == "/vhost"
    assert broker.title == "Test Server"
    assert broker.summary == "Summary text"
    assert broker.description == "Description text"
    assert broker.protocol_version == "1.0.0"
    assert broker.variables is None
    assert broker.security is None
    assert broker.tags is None
    assert broker.external_docs is None
    assert broker.bindings is None

    caps = broker.capabilities
    assert caps["supports_acknowledgments"] is True
    assert caps["supports_persistence"] is True
    assert caps["supports_reply"] is True
    assert caps["supports_lightweight_pause"] is False

    assert broker.managed_session is None


async def test_subscriber_pause_resume() -> None:
    receiver_link = FakeReceiverLink(handle=1)

    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[receiver_link],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    paused_event = asyncio.Event()
    paused_event.set()

    subscriber = AmqpSubscriber(
        managed_session=managed,
        links=[cast(Any, receiver_link)],
        queues_to_callbacks={"test": lambda _x: asyncio.sleep(0)},
        concurrency_limit=None,
        paused_event=paused_event,
        naming_strategy=lambda x: x,
    )

    assert subscriber.is_active is True

    await subscriber.pause()
    assert subscriber.is_active is False
    assert not paused_event.is_set()

    await subscriber.resume()
    assert subscriber.is_active is True
    assert paused_event.is_set()

    await subscriber.close()


async def test_amqp_received_message_no_headers() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers=None,
        link=cast(Any, link),
        delivery_id=123,
        delivery_tag=b"tag",
        channel_name="test-channel",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
    )

    assert msg.headers is None


async def test_message_broker_connection_context(monkeypatch: Any) -> None:
    broker = AmqpServer("amqp://localhost:5672")

    class DummyConnection:
        def __init__(self, _config: ConnectionConfig):
            self.is_connected = False

        async def connect(self) -> None:
            self.is_connected = True

        async def close(self) -> None:
            self.is_connected = False

    class DummyManagedSession:
        def __init__(self, _connection: Any):
            pass

        async def close(self) -> None:
            pass

    monkeypatch.setattr("repid.connections.amqp.message_broker.AmqpConnection", DummyConnection)
    monkeypatch.setattr("repid.connections.amqp.message_broker.ManagedSession", DummyManagedSession)

    async with broker.connection():
        assert broker.is_connected is True
    assert broker.is_connected is False


async def test_message_broker_default_subscribe_naming() -> None:
    strategy = AmqpServer._default_subscribe_naming_strategy
    result = strategy("test-queue")
    assert result == "/queues/test-queue"


async def test_subscriber_task_property() -> None:
    def session_factory() -> FakeSessionForPools:
        return FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        )

    connection = FakeManagedConnection(is_connected=True, session_factory=session_factory)
    managed = ManagedSession(cast(AmqpConnection, connection))

    paused_event = asyncio.Event()
    paused_event.set()

    subscriber = AmqpSubscriber(
        managed_session=managed,
        links=[],
        queues_to_callbacks={},
        concurrency_limit=None,
        paused_event=paused_event,
        naming_strategy=lambda x: x,
    )

    # Test task property
    assert subscriber.task is not None
    assert isinstance(subscriber.task, asyncio.Task)

    # Cleanup
    subscriber.task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await subscriber.task


async def test_amqp_received_message_ack_double_call_is_noop() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"test",
        headers=None,
        link=cast(Any, link),
        delivery_id=5,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
    )

    await msg.ack()
    assert len(connection.sent) == 1

    await msg.ack()
    assert len(connection.sent) == 1  # Second ack is a no-op


async def test_amqp_received_message_reply_first() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    published: list[tuple[str, MessageData]] = []

    async def publish_fn(*, channel: str, message: MessageData, **_kwargs: Any) -> None:
        published.append((channel, message))

    msg = AmqpReceivedMessage(
        payload=b"original",
        headers=None,
        link=cast(Any, link),
        delivery_id=6,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=publish_fn,
        properties=Properties(reply_to="reply-to-channel"),
    )

    await msg.reply(payload=b"response", headers={"x": "1"})

    assert msg.action == MessageAction.replied
    assert len(connection.sent) == 1  # Ack was sent
    assert len(published) == 1
    assert published[0] == (
        "reply-to-channel",
        MessageData(payload=b"response", headers={"x": "1"}, content_type=None),
    )

    # Second reply is a no-op
    await msg.reply(payload=b"ignored")
    assert len(published) == 1


async def test_amqp_received_message_reply_uses_reply_to() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    published: list[tuple[str, MessageData, dict[str, Any]]] = []

    async def publish_fn(
        *,
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        published.append((channel, message, server_specific_parameters or {}))

    msg = AmqpReceivedMessage(
        payload=b"original",
        headers=None,
        link=cast(Any, link),
        delivery_id=8,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=publish_fn,
        properties=Properties(reply_to="reply-target"),
    )

    await msg.reply(payload=b"response")

    assert published[0][0] == "reply-target"
    assert published[0][1] == MessageData(
        payload=b"response",
        headers=None,
        content_type=None,
    )
    assert "properties" not in published[0][2]


async def test_amqp_received_message_reply_updates_existing_properties() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    published: list[dict[str, Any]] = []

    async def publish_fn(
        *,
        channel: str,
        message: MessageData,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        _ = channel
        _ = message
        published.append(server_specific_parameters or {})

    msg = AmqpReceivedMessage(
        payload=b"original",
        headers=None,
        link=cast(Any, link),
        delivery_id=10,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=publish_fn,
        properties=Properties(reply_to="reply-target"),
    )

    params = {"properties": Properties(message_id="id-1")}
    await msg.reply(payload=b"response", server_specific_parameters=params)

    assert isinstance(published[0]["properties"], Properties)


async def test_amqp_received_message_reply_requires_channel_or_reply_to() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession:
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()
    msg = AmqpReceivedMessage(
        payload=b"original",
        headers=None,
        link=cast(Any, link),
        delivery_id=9,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda **_kwargs: asyncio.sleep(0),
    )

    with pytest.raises(ValueError, match="Reply channel is not set"):
        await msg.reply(payload=b"response")
