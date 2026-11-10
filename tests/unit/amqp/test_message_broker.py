from __future__ import annotations

import asyncio
import contextlib
from typing import Any, ClassVar, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from repid import Channel, Concurrency, Repid, Router
from repid._utils._typing import override
from repid.connections.abc import MessageAction, SentMessageT
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp._uamqp.outcomes import Accepted, Rejected, Released
from repid.connections.amqp._uamqp.performatives import DispositionFrame
from repid.connections.amqp.helpers import AmqpReceivedMessage
from repid.connections.amqp.message_broker import AmqpServer
from repid.connections.amqp.protocol import ManagedSession, ReceiverLink
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

ReceiverSettlementState = Accepted | Rejected | Released


class FakeReceiverLinkCreditMixin:
    session: FakeSession
    released_delivery_id: int

    def is_delivery_settled(self, _delivery_id: int) -> bool:
        return False

    async def _send_disposition(self, delivery_id: int, state: ReceiverSettlementState) -> None:
        disp = DispositionFrame(
            role=True,
            first=delivery_id,
            last=delivery_id,
            settled=True,
            state=state,
        )
        await self.session.connection.send_performative(self.session.channel, disp)

    async def release_delivery_credit(self, delivery_id: int) -> None:
        self.released_delivery_id = delivery_id

    async def settle_delivery(self, delivery_id: int, state: ReceiverSettlementState) -> None:
        if not self.is_delivery_settled(delivery_id):
            await self._send_disposition(delivery_id, state)
        await self.release_delivery_credit(delivery_id)


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
        await broker.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"test": lambda _x: asyncio.sleep(0)},
        )


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

    server._managed_session = DummyManagedSession(object())  # pyrefly: ignore[bad-assignment]
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

    await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"queue": lambda _msg: asyncio.sleep(0)},
    )
    await server.disconnect()
    assert server.is_connected is False


async def test_amqp_subscriber_create_pause_resume_and_close() -> None:
    received: list[bytes] = []
    receiver_links: list[FakeReceiverLink] = [FakeReceiverLink(handle=5)]

    class DummyReceiverPool:
        def __init__(self) -> None:
            self.subscriptions: list[tuple[str, Any, str, str]] = []
            self.unsubscribed: list[str] = []

        async def subscribe(
            self,
            address: str,
            callback: Any,
            name: str,
            prefetch: int = 100,  # noqa: ARG002
            *,
            subscription_key: str,
        ) -> FakeReceiverLink:
            self.subscriptions.append((address, callback, name, subscription_key))
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
        prefetch_by_queue={"queue": 100},
        naming_strategy=lambda q: f"/queues/{q}",
        publish_fn=lambda **_kwargs: asyncio.sleep(0),  # pyrefly: ignore[implicit-any-lambda]
    )

    address, wrapped_callback, _name, key = managed.receiver_pool.subscriptions[0]
    await subscriber.pause()

    await wrapped_callback(b"data", None, 1, b"tag", receiver_links[0])
    await asyncio.sleep(0)
    assert received == []

    await subscriber.resume()
    await subscriber.pause()
    await asyncio.sleep(0)
    assert received == []

    await subscriber.resume()
    await asyncio.gather(*subscriber._submission_tasks)

    assert received == [b"data"]
    assert address == "/queues/queue"

    await subscriber.close()
    await subscriber.resume()
    assert not subscriber.is_active
    assert managed.receiver_pool.unsubscribed == [key]


async def test_amqp_received_message_headers_and_ack_nack_reply() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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


async def test_amqp_received_message_concurrent_settlement_sends_one_disposition() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    await asyncio.gather(msg.ack(), msg.nack())

    assert msg.action == MessageAction.acked
    assert len(connection.sent) == 1
    assert isinstance(connection.sent[0][1], DispositionFrame)


async def test_amqp_received_message_presettled_ack_sends_no_disposition() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
        handle: int = 7
        session: FakeSession = fake_session

        @override
        def is_delivery_settled(self, _delivery_id: int) -> bool:
            return True

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

    await msg.ack()

    assert msg.action == MessageAction.acked
    assert connection.sent == []
    assert link.released_delivery_id == 123


async def test_amqp_received_message_reject() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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
    assert msg.keep_alive_interval is None

    await msg.ack()
    assert msg.is_acted_on is True


async def test_amqp_received_message_uses_link_send_disposition() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeLinkWithDisposition(FakeReceiverLinkCreditMixin):
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeLinkWithDisposition()
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

    await msg.ack()

    assert isinstance(connection.sent[0][1], DispositionFrame)
    assert link.released_delivery_id == 123


async def test_amqp_received_message_bytes_message_id() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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


async def test_amqp_received_message_properties_content_type_none() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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
        properties=Properties(content_type=None),
    )

    assert msg.content_type is None


async def test_amqp_received_message_bytes_properties() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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
        headers: ClassVar[dict[str, Any]] = {}
        reply_to: None = None
        content_type: None = None

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
        headers: ClassVar[dict[str, Any]] = {}
        reply_to = "reply-1"
        content_type: None = None

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
    assert caps["supports_native_reply"] is True
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

    subscriber = await AmqpSubscriber.create(
        managed_session=managed,
        queues_to_callbacks={"test": lambda _x: asyncio.sleep(0)},
        naming_strategy=lambda x: x,
        publish_fn=AsyncMock(),
        prefetch_by_queue={"test": 1},
    )

    assert subscriber.is_active is True

    await subscriber.pause()
    assert subscriber.is_active is False

    await subscriber.resume()
    assert subscriber.is_active is True

    await subscriber.close()


async def test_amqp_received_message_no_headers() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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


async def test_message_broker_passes_session_window_to_connection(monkeypatch: Any) -> None:
    configs: list[ConnectionConfig] = []

    class DummyConnection:
        def __init__(self, config: ConnectionConfig):
            configs.append(config)
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

    broker = AmqpServer("amqp://localhost:5672", session_window=12345)
    await broker.connect()

    assert configs[0].session_window == 12345


def test_message_broker_rejects_invalid_session_window() -> None:
    with pytest.raises(ValueError, match="session_window must be greater than 0"):
        AmqpServer("amqp://localhost:5672", session_window=0)


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

    subscriber = AmqpSubscriber(managed)

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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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


async def test_amqp_received_message_reply_publish_failure_does_not_ack() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
        handle: int = 7
        session: FakeSession = fake_session

    link = FakeReceiverLinkWithSession()

    async def publish_fn(**_kwargs: Any) -> None:
        raise RuntimeError("publish failed")

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

    with pytest.raises(RuntimeError, match="publish failed"):
        await msg.reply(payload=b"response")

    assert msg.action is None
    assert connection.sent == []


async def test_amqp_received_message_reply_uses_reply_to() -> None:
    connection = FakeConnection()
    fake_session = FakeSession(connection=connection, channel=2)

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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

    class FakeReceiverLinkWithSession(FakeReceiverLinkCreditMixin):
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
        publish_fn=lambda **_kwargs: asyncio.sleep(0),  # pyrefly: ignore[implicit-any-lambda]
    )

    with pytest.raises(ValueError, match="Reply channel is not set"):
        await msg.reply(payload=b"response")


async def test_amqp_connect_failure_closes_connection_and_can_retry(monkeypatch: Any) -> None:
    server = AmqpServer("amqp://localhost:5672")

    class DummyConnection:
        def __init__(self, _config: ConnectionConfig, succeed: bool) -> None:
            self.is_connected = False
            self.closed = False
            self._succeed = succeed
            self.events = MagicMock()

        async def connect(self) -> None:
            if not self._succeed:
                raise ConnectionError("offline")
            self.is_connected = True

        async def close(self) -> None:
            self.closed = True

    instances: list[DummyConnection] = []

    def factory(config: ConnectionConfig) -> DummyConnection:
        instance = DummyConnection(config, succeed=len(instances) > 0)
        instances.append(instance)
        return instance

    monkeypatch.setattr(
        "repid.connections.amqp.message_broker.AmqpConnection",
        factory,
    )

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    assert server._connection is None
    assert server.is_connected is False
    assert instances[0].closed is True

    await server.connect()

    assert server.is_connected is True
    assert len(instances) == 2
    assert instances[1].closed is False


async def test_amqp_connect_preserves_error_when_cleanup_fails(monkeypatch: Any) -> None:
    server = AmqpServer("amqp://localhost:5672")

    class DummyConnection:
        def __init__(self, _config: ConnectionConfig) -> None:
            self.is_connected = False
            self.events = MagicMock()

        async def connect(self) -> None:
            raise ConnectionError("startup failed")

        async def close(self) -> None:
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(
        "repid.connections.amqp.message_broker.AmqpConnection",
        DummyConnection,
    )

    with pytest.raises(ConnectionError, match="startup failed"):
        await server.connect()

    assert server._connection is None
    assert server.is_connected is False


async def test_amqp_connect_closes_stale_connection_before_reconnect(monkeypatch: Any) -> None:
    server = AmqpServer("amqp://localhost:5672")

    class DummyConnection:
        def __init__(self, _config: ConnectionConfig) -> None:
            self.is_connected = True
            self.closed = False
            self.events = MagicMock()

        async def connect(self) -> None:
            self.is_connected = True

        async def close(self) -> None:
            self.closed = True

    stale_connection = DummyConnection(cast(ConnectionConfig, None))
    stale_connection.is_connected = False
    server._connection = cast(Any, stale_connection)
    server._managed_session = cast(Any, object())

    monkeypatch.setattr(
        "repid.connections.amqp.message_broker.AmqpConnection",
        DummyConnection,
    )

    await server.connect()

    assert stale_connection.closed is True
    assert server._connection is not stale_connection
    assert server.is_connected is True


class FailingSettleLink:
    async def settle_delivery(
        self,
        _delivery_id: int,
        _state: ReceiverSettlementState,
    ) -> None:
        raise RuntimeError("settle failed")


def _make_failing_settle_message() -> AmqpReceivedMessage:
    return AmqpReceivedMessage(
        payload=b"test",
        headers=None,
        link=cast(Any, FailingSettleLink()),
        delivery_id=1,
        delivery_tag=b"tag",
        channel_name="q",
        managed_session=cast(ManagedSession, object()),
        publish_fn=lambda: asyncio.sleep(0),
    )


async def test_amqp_received_message_ack_settle_failure_resets_action() -> None:
    msg = _make_failing_settle_message()

    with pytest.raises(RuntimeError, match="settle failed"):
        await msg.ack()

    assert msg.action is None
    assert not msg.is_acted_on


async def test_amqp_received_message_nack_settle_failure_resets_action() -> None:
    msg = _make_failing_settle_message()

    with pytest.raises(RuntimeError, match="settle failed"):
        await msg.nack()

    assert msg.action is None
    assert not msg.is_acted_on


async def test_amqp_received_message_reject_settle_failure_resets_action() -> None:
    msg = _make_failing_settle_message()

    with pytest.raises(RuntimeError, match="settle failed"):
        await msg.reject()

    assert msg.action is None
    assert not msg.is_acted_on


async def test_amqp_received_message_canceled_ack_waits_for_disposition_and_credit() -> None:
    settling = asyncio.Event()
    finish_settlement = asyncio.Event()

    async def settle(_delivery_id: int, _state: ReceiverSettlementState) -> None:
        settling.set()
        await finish_settlement.wait()

    link = MagicMock(spec=ReceiverLink)
    link.settle_delivery = AsyncMock(side_effect=settle)
    msg = AmqpReceivedMessage(
        payload=b"data",
        headers=None,
        link=link,
        delivery_id=1,
        delivery_tag=b"one",
        channel_name="a",
        managed_session=MagicMock(spec=ManagedSession),
        publish_fn=AsyncMock(),
    )
    ack = asyncio.create_task(msg.ack())
    await asyncio.wait_for(settling.wait(), 1)
    ack.cancel()
    await asyncio.sleep(0)
    assert not ack.done()
    await msg.reject()

    finish_settlement.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(ack, 1)
    link.settle_delivery.assert_awaited_once_with(1, Accepted())


async def test_amqp_cancelled_settlement_consumes_a_later_rpc_failure() -> None:
    started = asyncio.Event()
    finish = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    errors: list[dict[str, Any]] = []

    async def fail_settlement(_delivery_id: int, _state: ReceiverSettlementState) -> None:
        started.set()
        await finish.wait()
        raise ConnectionError("disposition failed")

    link = MagicMock(spec=ReceiverLink)
    link.settle_delivery = AsyncMock(side_effect=fail_settlement)
    message = AmqpReceivedMessage(
        payload=b"data",
        headers=None,
        link=link,
        delivery_id=1,
        delivery_tag=b"one",
        channel_name="a",
        managed_session=MagicMock(spec=ManagedSession),
        publish_fn=AsyncMock(),
    )
    loop.set_exception_handler(lambda _, context: errors.append(context))
    try:
        settlement = asyncio.create_task(message.ack())
        await started.wait()
        settlement.cancel()
        await asyncio.sleep(0)
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await settlement
        await asyncio.sleep(0)
        assert message.action is None
        assert not errors
    finally:
        finish.set()
        loop.set_exception_handler(previous_handler)


async def test_admission_does_not_block_protocol_and_close_drains_submissions() -> None:
    link = MagicMock(spec=ReceiverLink)
    link.settle_delivery = AsyncMock()
    managed = MagicMock(spec=ManagedSession)
    managed.receiver_pool = MagicMock(
        subscribe=AsyncMock(return_value=link),
        unsubscribe=AsyncMock(),
    )
    started = asyncio.Event()

    async def blocked(_message: Any) -> None:
        started.set()
        await asyncio.Event().wait()

    subscriber = await AmqpSubscriber.create(
        managed_session=managed,
        queues_to_callbacks={"a": blocked},
        prefetch_by_queue={"a": 2},
        naming_strategy=lambda channel: channel,
        publish_fn=AsyncMock(),
    )
    receive = managed.receiver_pool.subscribe.call_args.args[1]
    await receive(b"one", None, 1, b"one", link)
    await asyncio.wait_for(started.wait(), 1)
    await receive(b"two", None, 2, b"two", link)
    assert len(subscriber._submission_tasks) == 2
    await subscriber.close()
    assert not subscriber._submission_tasks
    link.settle_delivery.assert_awaited_once_with(1, Released())
    managed.receiver_pool.unsubscribe.assert_awaited_once_with(
        managed.receiver_pool.subscribe.call_args.kwargs["subscription_key"],
    )


@pytest.mark.parametrize("settlement_error", [None, RuntimeError("settlement failed")])
async def test_amqp_failed_callback_requeues_delivery_before_returning_credit(
    settlement_error: RuntimeError | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    link = MagicMock(spec=ReceiverLink)
    link.settle_delivery = AsyncMock(side_effect=settlement_error)
    managed = MagicMock(spec=ManagedSession)
    managed.receiver_pool = MagicMock(
        subscribe=AsyncMock(return_value=link),
        unsubscribe=AsyncMock(),
    )
    subscriber = await AmqpSubscriber.create(
        managed_session=managed,
        queues_to_callbacks={"a": AsyncMock(side_effect=RuntimeError("callback failed"))},
        prefetch_by_queue={"a": 1},
        naming_strategy=lambda channel: channel,
        publish_fn=AsyncMock(),
    )
    try:
        receive = managed.receiver_pool.subscribe.call_args.args[1]
        await receive(b"one", None, 1, b"one", link)
        await asyncio.gather(*subscriber._submission_tasks, return_exceptions=True)
        link.settle_delivery.assert_awaited_once_with(1, Released())
        link.release_delivery_credit.assert_not_awaited()
        assert ("subscriber.callback.rejection_error" in caplog.messages) == (
            settlement_error is not None
        )
    finally:
        await subscriber.close()


async def test_amqp_subscriber_startup_failure_detaches_earlier_links() -> None:
    managed = MagicMock(spec=ManagedSession)
    link = MagicMock(spec=ReceiverLink)
    callback = AsyncMock()
    subscribed_keys: list[str] = []

    async def subscribe(
        address: str,
        receive: Any,
        _name: str,
        *,
        prefetch: int,
        subscription_key: str,
    ) -> ReceiverLink:
        assert prefetch == 1
        if address == "b":
            await asyncio.sleep(0)
            raise RuntimeError("failed to subscribe")
        subscribed_keys.append(subscription_key)
        await receive(b"one", None, 1, b"one", link)
        return link

    managed.receiver_pool = MagicMock(
        subscribe=AsyncMock(side_effect=subscribe),
        unsubscribe=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="failed to subscribe"):
        await AmqpSubscriber.create(
            managed_session=managed,
            queues_to_callbacks={"a": callback, "b": callback},
            prefetch_by_queue={"a": 1, "b": 1},
            naming_strategy=lambda channel: channel,
            publish_fn=AsyncMock(),
        )

    callback.assert_not_awaited()
    managed.receiver_pool.unsubscribe.assert_awaited_once_with(subscribed_keys[0])


async def test_amqp_subscriber_can_close_from_its_own_callback() -> None:
    session = FakeSession(connection=FakeConnection(), channel=1)
    link = ReceiverLink(cast(Any, session), "receiver", "a", 0, lambda *_: None)
    managed = MagicMock(spec=ManagedSession)

    async def unsubscribe(_key: str) -> None:
        await link.detach()

    managed.receiver_pool = MagicMock(
        subscribe=AsyncMock(return_value=link),
        unsubscribe=AsyncMock(side_effect=unsubscribe),
    )
    closed = asyncio.Event()

    async def callback(_msg: Any) -> None:
        await subscriber.close()
        closed.set()

    subscriber = await AmqpSubscriber.create(
        managed_session=managed,
        queues_to_callbacks={"a": callback},
        prefetch_by_queue={"a": 1},
        naming_strategy=lambda channel: channel,
        publish_fn=AsyncMock(),
    )
    receive = managed.receiver_pool.subscribe.call_args.args[1]
    await receive(b"one", None, 1, b"one", link)
    await asyncio.wait_for(closed.wait(), 1)
    assert subscriber.task.done()


async def test_close_stops_intake_before_cancellation_returns_credit() -> None:
    link = MagicMock(spec=ReceiverLink)
    managed = MagicMock(spec=ManagedSession)
    subscribed = True
    returned_credit = asyncio.Event()
    replacement_delivered = asyncio.Event()

    async def unsubscribe(_key: str) -> None:
        nonlocal subscribed
        subscribed = False

    managed.receiver_pool = MagicMock(
        subscribe=AsyncMock(return_value=link),
        unsubscribe=AsyncMock(side_effect=unsubscribe),
    )
    subscriber = await AmqpSubscriber.create(
        managed_session=managed,
        queues_to_callbacks={"a": AsyncMock()},
        prefetch_by_queue={"a": 100},
        naming_strategy=lambda channel: channel,
        publish_fn=AsyncMock(),
    )
    receive = managed.receiver_pool.subscribe.call_args.args[1]

    async def settle(_delivery_id: int, _state: ReceiverSettlementState) -> None:
        returned_credit.set()
        if subscribed:
            replacement_delivered.set()
            await receive(b"replacement", None, 2, b"two", link)

    link.settle_delivery = AsyncMock(side_effect=settle)
    await subscriber.pause()
    await receive(b"original", None, 1, b"one", link)
    await asyncio.sleep(0)
    closing = asyncio.create_task(subscriber.close())
    try:
        await asyncio.wait_for(asyncio.shield(closing), timeout=0.2)
        assert returned_credit.is_set()
        assert not replacement_delivered.is_set()
        assert not subscriber._submission_tasks
        managed.receiver_pool.unsubscribe.assert_awaited_once_with(
            managed.receiver_pool.subscribe.call_args.kwargs["subscription_key"],
        )
    finally:
        if not closing.done():
            await subscriber.resume()
            await asyncio.wait_for(closing, timeout=1)


@pytest.mark.parametrize(
    ("settings", "expected"),
    [({"concurrency": Concurrency(messages=4)}, [4, 4]), ({}, [100, 100])],
)
async def test_subscribe_sets_amqp_prefetch(settings: dict[str, Any], expected: list[int]) -> None:
    server = AmqpServer("amqp://localhost")
    server._connection = MagicMock(spec=AmqpConnection, is_connected=True)
    managed = MagicMock(spec=ManagedSession)
    managed.receiver_pool = MagicMock(subscribe=AsyncMock(), unsubscribe=AsyncMock())
    server._managed_session = managed

    subscriber = await server.subscribe(
        channels_to_callbacks={"a": AsyncMock(), "b": AsyncMock()},
        **{"concurrency": Concurrency(), "channel_concurrency": {}, **settings},  # pyrefly: ignore[bad-argument-type]
    )
    try:
        assert [
            call.kwargs["prefetch"] for call in managed.receiver_pool.subscribe.await_args_list
        ] == expected
    finally:
        await subscriber.close()


async def test_worker_propagates_channel_prefetch_to_amqp() -> None:
    server = AmqpServer("amqp://localhost")
    server._connection = MagicMock(spec=AmqpConnection, is_connected=True)
    managed = MagicMock(spec=ManagedSession)
    managed.receiver_pool = MagicMock(subscribe=AsyncMock(), unsubscribe=AsyncMock())
    server._managed_session = managed
    app = Repid()
    app.servers.register_server("default", server, is_default=True)
    router = Router()

    @router.actor(channel=Channel(address="images", concurrency=Concurrency(messages=3)))
    async def image() -> None:
        pass

    @router.actor(channel=Channel(address="emails", concurrency=Concurrency(messages=2000)))
    async def email() -> None:
        pass

    app.include_router(router)
    worker = asyncio.create_task(
        app.run_worker(concurrency=Concurrency(messages=1000), register_signals=[]),
    )
    try:
        async with asyncio.timeout(1):
            while managed.receiver_pool.subscribe.await_count < 2:
                await asyncio.sleep(0)
        assert {
            call.args[2]: call.kwargs["prefetch"]
            for call in managed.receiver_pool.subscribe.await_args_list
        } == {"receiver-images": 3, "receiver-emails": 1000}
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
