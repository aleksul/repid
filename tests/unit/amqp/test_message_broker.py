from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, ClassVar, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from repid.connections import SubscriberDispatcher
from repid.connections.abc import MessageAction, SentMessageT
from repid.connections.amqp._uamqp.message import Properties
from repid.connections.amqp._uamqp.outcomes import Accepted, Rejected, Released
from repid.connections.amqp._uamqp.performatives import DispositionFrame
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
from repid.limits import MessageLimits

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
            channels_to_callbacks={"test": lambda _x: asyncio.sleep(0)},
            dispatcher=SubscriberDispatcher(),
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

    await server.subscribe(
        channels_to_callbacks={"queue": lambda _msg: asyncio.sleep(0)},
        dispatcher=SubscriberDispatcher(),
    )

    await server.disconnect()
    assert server.is_connected is False


def _make_amqp_session(
    receiver_links: list[Any],
) -> Any:
    """A managed session whose receiver pool hands out the given links in order."""

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
            return cast(FakeReceiverLink, receiver_links[len(self.subscriptions) - 1])

        async def unsubscribe(self, address: str) -> None:
            self.unsubscribed.append(address)

    class DummyManagedSession:
        def __init__(self) -> None:
            self.receiver_pool = DummyReceiverPool()
            self.connection = FakeConnection()

        async def get_session(self) -> FakeSession:
            return FakeSession(connection=self.connection, channel=4)

    return DummyManagedSession()


async def test_amqp_subscriber_defers_paused_delivery_until_resume_and_close() -> None:
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
        dispatcher=SubscriberDispatcher(),
        naming_strategy=lambda q: f"/queues/{q}",
        publish_fn=lambda **_kwargs: asyncio.sleep(0),
    )

    address, wrapped_callback, _name = managed.receiver_pool.subscriptions[0]
    await subscriber.pause()
    returned = asyncio.Event()

    async def protocol_callback(delivery_id: int) -> None:
        await wrapped_callback(b"data", None, delivery_id, b"tag", receiver_links[0])
        returned.set()

    protocol_task = asyncio.create_task(protocol_callback(1))
    await asyncio.wait_for(returned.wait(), timeout=1)
    await protocol_task

    assert received == []
    assert receiver_links[0].deferred_delivery_ids == {1}
    assert receiver_links[0].released_delivery_ids == []

    pending_delivery = next(iter(subscriber._admitted_tasks.tasks))
    await subscriber.resume()
    await pending_delivery

    assert received == [b"data"]
    assert address == "/queues/queue"
    assert receiver_links[0].released_delivery_ids == [1]

    await subscriber.pause()
    await wrapped_callback(b"data", None, 2, b"tag", receiver_links[0])
    pending_delivery = next(iter(subscriber._admitted_tasks.tasks))
    await subscriber.stop()
    await subscriber.finish()

    assert pending_delivery.cancelled()
    assert received == [b"data"]
    assert receiver_links[0].released_delivery_ids == [1, 2]
    assert managed.receiver_pool.unsubscribed == ["/queues/queue"]


async def test_amqp_subscriber_channel_pause_defers_only_that_queue() -> None:
    received: list[bytes] = []
    links: list[FakeReceiverLink] = [FakeReceiverLink(handle=1), FakeReceiverLink(handle=2)]
    managed = _make_amqp_session(links)

    async def callback(msg: Any) -> None:
        received.append(msg.payload)

    subscriber = await AmqpSubscriber.create(
        managed_session=cast(ManagedSession, managed),
        queues_to_callbacks={"jobs": callback, "reports": callback},
        dispatcher=SubscriberDispatcher(),
        naming_strategy=lambda q: f"/queues/{q}",
        publish_fn=lambda **_kwargs: asyncio.sleep(0),
    )

    await subscriber.pause_channel("jobs")
    jobs_callback = managed.receiver_pool.subscriptions[0][1]
    reports_callback = managed.receiver_pool.subscriptions[1][1]

    delivered = asyncio.Event()

    async def deliver_jobs() -> None:
        await jobs_callback(b"jobs-data", None, 1, b"tag", links[0])

    async def deliver_reports() -> None:
        await reports_callback(b"reports-data", None, 1, b"tag", links[1])
        delivered.set()

    await deliver_jobs()
    await deliver_reports()
    await asyncio.wait_for(delivered.wait(), timeout=1)
    for _ in range(50):
        if links[1].released_delivery_ids:
            break
        await asyncio.sleep(0.01)

    # The paused queue holds its delivery; the resumed queue flows through.
    assert received == [b"reports-data"]
    assert links[0].deferred_delivery_ids == {1}
    assert links[1].released_delivery_ids == [1]

    await subscriber.resume_channel("jobs")
    for _ in range(50):
        if len(received) == 2:
            break
        await asyncio.sleep(0.01)
    assert received == [b"reports-data", b"jobs-data"]

    await subscriber.stop()
    await subscriber.finish()


@pytest.mark.parametrize(
    "drain",
    [pytest.param(True, id="drain"), pytest.param(False, id="no_drain")],
)
async def test_amqp_subscriber_close_honors_callback_drain_mode(drain: bool) -> None:
    class DummyReceiverPool:
        async def unsubscribe(self, address: str) -> None:
            pass

    managed = MagicMock(receiver_pool=DummyReceiverPool())
    subscriber = AmqpSubscriber(
        managed_session=cast(ManagedSession, managed),
        queues_to_callbacks={},
        naming_strategy=lambda queue: queue,
        dispatcher=SubscriberDispatcher(),
    )
    started = asyncio.Event()
    finish = asyncio.Event()

    async def stubborn_callback() -> None:
        started.set()
        while not finish.is_set():
            with contextlib.suppress(asyncio.CancelledError):
                await finish.wait()

    callback_task = asyncio.create_task(stubborn_callback())
    subscriber._admitted_tasks.tasks.add(callback_task)
    await started.wait()
    await subscriber.stop()
    finish_task = asyncio.create_task(subscriber.finish()) if drain else None

    if finish_task is not None:
        done, _ = await asyncio.wait({finish_task}, timeout=0.05)
        assert done == set()
    finish.set()
    await callback_task
    if finish_task is not None:
        await finish_task

    assert not subscriber.is_active


async def test_amqp_subscriber_close_continues_after_unsubscribe_error() -> None:
    link = FakeReceiverLink()

    class DummyReceiverPool:
        def __init__(self) -> None:
            self.unsubscribed: list[str] = []

        async def unsubscribe(self, address: str) -> None:
            self.unsubscribed.append(address)
            if address == "first":
                raise RuntimeError("first unsubscribe failed")

    managed = MagicMock(receiver_pool=DummyReceiverPool())
    subscriber = AmqpSubscriber(
        managed_session=cast(ManagedSession, managed),
        queues_to_callbacks={"first": AsyncMock(), "second": AsyncMock()},
        naming_strategy=lambda queue: queue,
        dispatcher=SubscriberDispatcher(),
    )
    await subscriber.pause()
    await subscriber._process_message(
        "first",
        AsyncMock(),
        lambda **_kwargs: asyncio.sleep(0),
        b"data",
        None,
        1,
        b"tag",
        cast(Any, link),
    )
    pending_delivery = next(iter(subscriber._admitted_tasks.tasks))

    async def full_close() -> None:
        await subscriber.stop()
        await subscriber.finish()

    with pytest.raises(RuntimeError, match="first unsubscribe failed"):
        await full_close()

    assert managed.receiver_pool.unsubscribed == ["first", "second"]
    assert pending_delivery.cancelled()
    assert link.released_delivery_ids == [1]


async def test_amqp_subscriber_create_cleans_up_after_partial_subscription_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyReceiverPool:
        def __init__(self) -> None:
            self.unsubscribed: list[str] = []
            self.calls = 0

        async def subscribe(self, _address: str, *_args: Any, **_kwargs: Any) -> MagicMock:
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("second subscription failed")
            return MagicMock()

        async def unsubscribe(self, address: str) -> None:
            self.unsubscribed.append(address)

    class DummyManagedSession:
        def __init__(self) -> None:
            self.receiver_pool = DummyReceiverPool()

    tasks: list[asyncio.Task[Any]] = []
    create_task = asyncio.create_task

    def track_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        task = create_task(coro, **kwargs)
        tasks.append(task)
        return task

    monkeypatch.setattr(asyncio, "create_task", track_task)
    managed = DummyManagedSession()

    with pytest.raises(RuntimeError, match="second subscription failed"):
        await AmqpSubscriber.create(
            managed_session=cast(ManagedSession, managed),
            queues_to_callbacks={"first": AsyncMock(), "second": AsyncMock()},
            dispatcher=SubscriberDispatcher(),
            naming_strategy=lambda queue: f"/queues/{queue}",
            publish_fn=lambda **_kwargs: asyncio.sleep(0),
        )

    assert len(tasks) == 1
    assert tasks[0].cancelled()
    assert managed.receiver_pool.unsubscribed == ["/queues/first", "/queues/second"]


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
        headers: ClassVar[dict] = {}
        reply_to = None
        content_type = None

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
        content_type = None

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
    assert caps["supports_pause"] is True

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
        queues_to_callbacks={"test": lambda _x: asyncio.sleep(0)},
        dispatcher=SubscriberDispatcher(MessageLimits(max_messages=1)),
        paused_event=paused_event,
        naming_strategy=lambda x: x,
    )

    assert subscriber.is_active is True
    # Native flow control is claimed through server capabilities; whether the
    # configured limits map onto an independent per-link window is decided by
    # the dispatcher.
    assert subscriber._dispatcher.native_limit_is_independent("test", ("test",), "messages")
    assert not subscriber._dispatcher.native_limit_is_independent(
        "test",
        ("test",),
        "payload_bytes",
    )

    await subscriber.pause()
    assert subscriber.is_active is False
    assert not paused_event.is_set()

    await subscriber.resume()
    assert subscriber.is_active is True
    assert paused_event.is_set()

    await subscriber.stop()
    await subscriber.finish()


async def test_subscriber_rejects_unacted_message_when_admission_fails() -> None:
    subscriber = AmqpSubscriber(
        managed_session=Mock(receiver_pool=Mock(unsubscribe=AsyncMock())),
        queues_to_callbacks={},
        dispatcher=Mock(reserve=AsyncMock(side_effect=RuntimeError("admission failed"))),
        naming_strategy=str,
    )
    message = Mock(is_acted_on=False, reject=AsyncMock(), nack=AsyncMock())
    link = Mock(release_delivery_credit=AsyncMock())
    try:
        await subscriber._dispatch_message(AsyncMock(), message, link, 1)

        message.reject.assert_awaited_once()
        message.nack.assert_not_awaited()
        link.release_delivery_credit.assert_awaited_once_with(1)
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_subscriber_nacks_unacted_message_when_callback_fails() -> None:
    subscriber = AmqpSubscriber(
        managed_session=Mock(receiver_pool=Mock(unsubscribe=AsyncMock())),
        queues_to_callbacks={},
        dispatcher=SubscriberDispatcher(),
        naming_strategy=str,
    )
    message = Mock(
        is_acted_on=False,
        keep_alive_interval=None,
        reject=AsyncMock(),
        nack=AsyncMock(),
    )
    link = Mock(release_delivery_credit=AsyncMock())

    async def callback(_: Any) -> None:
        raise RuntimeError("callback failed")

    try:
        await subscriber._dispatch_message(callback, message, link, 1)

        message.nack.assert_awaited_once()
        message.reject.assert_not_awaited()
        link.release_delivery_credit.assert_awaited_once_with(1)
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_subscriber_cancellation_rejects_unacted_message() -> None:
    subscriber = AmqpSubscriber(
        managed_session=Mock(receiver_pool=Mock(unsubscribe=AsyncMock())),
        queues_to_callbacks={},
        naming_strategy=str,
        dispatcher=SubscriberDispatcher(),
    )
    message = Mock(is_acted_on=False, reject=AsyncMock())
    link = Mock(release_delivery_credit=AsyncMock())
    await subscriber.pause()
    dispatch = asyncio.create_task(subscriber._dispatch_message(AsyncMock(), message, link, 1))
    try:
        await asyncio.sleep(0)
        dispatch.cancel()
        with pytest.raises(asyncio.CancelledError):
            await dispatch

        message.reject.assert_awaited_once()
        link.release_delivery_credit.assert_awaited_once_with(1)
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_subscriber_native_flow_requires_independent_channel_limits() -> None:
    connection = FakeManagedConnection(
        is_connected=True,
        session_factory=lambda: FakeSessionForPools(
            connection=None,
            sender_links=[],
            receiver_links=[],
        ),
    )
    managed = ManagedSession(cast(AmqpConnection, connection))
    callbacks = {"first": lambda _x: asyncio.sleep(0), "second": lambda _x: asyncio.sleep(0)}
    shared = MessageLimits(max_messages=1)
    shared_subscriber = AmqpSubscriber(
        managed_session=managed,
        queues_to_callbacks=callbacks,
        dispatcher=SubscriberDispatcher(channel_limits={"first": (shared,), "second": (shared,)}),
        naming_strategy=lambda value: value,
    )
    looser_shared_subscriber = AmqpSubscriber(
        managed_session=managed,
        queues_to_callbacks=callbacks,
        dispatcher=SubscriberDispatcher(
            MessageLimits(max_messages=2),
            {"first": (MessageLimits(max_messages=1),), "second": (MessageLimits(max_messages=1),)},
        ),
        naming_strategy=lambda value: value,
    )
    distinct_subscriber = AmqpSubscriber(
        managed_session=managed,
        queues_to_callbacks=callbacks,
        dispatcher=SubscriberDispatcher(
            channel_limits={
                "first": (MessageLimits(max_messages=1),),
                "second": (MessageLimits(max_messages=1),),
            },
        ),
        naming_strategy=lambda value: value,
    )
    try:
        assert not shared_subscriber._dispatcher.native_limit_is_independent(
            "first",
            ("first", "second"),
            "messages",
        )
        assert not looser_shared_subscriber._dispatcher.native_limit_is_independent(
            "first",
            ("first", "second"),
            "messages",
        )
        assert distinct_subscriber._dispatcher.native_limit_is_independent(
            "first",
            ("first", "second"),
            "messages",
        )
        assert distinct_subscriber._dispatcher.native_limit_is_independent(
            "second",
            ("first", "second"),
            "messages",
        )
    finally:
        await shared_subscriber.stop()
        await shared_subscriber.finish()
        await looser_shared_subscriber.stop()
        await looser_shared_subscriber.finish()
        await distinct_subscriber.stop()
        await distinct_subscriber.finish()


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

    paused_event = asyncio.Event()
    paused_event.set()

    subscriber = AmqpSubscriber(
        managed_session=managed,
        queues_to_callbacks={},
        paused_event=paused_event,
        naming_strategy=lambda x: x,
        dispatcher=SubscriberDispatcher(),
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
        publish_fn=lambda **_kwargs: asyncio.sleep(0),
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


async def test_subscriber_callback_error_disposition_failure_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    subscriber = AmqpSubscriber(
        managed_session=Mock(receiver_pool=Mock(unsubscribe=AsyncMock())),
        queues_to_callbacks={},
        dispatcher=SubscriberDispatcher(),
        naming_strategy=str,
    )
    message = Mock(
        is_acted_on=False,
        keep_alive_interval=None,
        nack=AsyncMock(side_effect=RuntimeError("nack failed")),
    )
    link = Mock(release_delivery_credit=AsyncMock())

    async def callback(_: Any) -> None:
        raise RuntimeError("callback failed")

    with caplog.at_level(logging.ERROR, logger="repid.connections.amqp"):
        await subscriber._dispatch_message(callback, message, link, 1)

    assert ("message.callback.error", logging.ERROR) in [
        (record.message, record.levelno) for record in caplog.records
    ]
    assert ("message.disposition.error", logging.ERROR) in [
        (record.message, record.levelno) for record in caplog.records
    ]
    await subscriber.stop()
    await subscriber.finish()


async def test_subscriber_credit_release_failure_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    subscriber = AmqpSubscriber(
        managed_session=Mock(receiver_pool=Mock(unsubscribe=AsyncMock())),
        queues_to_callbacks={},
        dispatcher=SubscriberDispatcher(),
        naming_strategy=str,
    )
    message = Mock(is_acted_on=False)
    link = Mock(
        release_delivery_credit=AsyncMock(side_effect=RuntimeError("credit release failed")),
    )

    with caplog.at_level(logging.ERROR, logger="repid.connections.amqp"):
        await subscriber._dispatch_message(AsyncMock(), message, link, 1)

    assert ("message.credit.release.error", logging.ERROR) in [
        (record.message, record.levelno) for record in caplog.records
    ]
    await subscriber.stop()
    await subscriber.finish()


async def test_subscriber_close_logs_secondary_unsubscribe_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class DummyReceiverPool:
        async def unsubscribe(self, address: str) -> None:
            raise RuntimeError(f"unsubscribe failed: {address}")

    managed = MagicMock(receiver_pool=DummyReceiverPool())
    subscriber = AmqpSubscriber(
        managed_session=cast(ManagedSession, managed),
        queues_to_callbacks={"first": AsyncMock(), "second": AsyncMock()},
        naming_strategy=lambda queue: queue,
        dispatcher=SubscriberDispatcher(),
    )

    async def full_close() -> None:
        await subscriber.stop()
        await subscriber.finish()

    with (
        caplog.at_level(logging.ERROR, logger="repid.connections.amqp"),
        pytest.raises(RuntimeError, match="unsubscribe failed: first"),
    ):
        await full_close()

    assert ("subscriber.close.unsubscribe.error", logging.ERROR) in [
        (record.message, record.levelno) for record in caplog.records
    ]


async def test_subscriber_cancelled_delivery_logs_credit_release_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingLink(FakeReceiverLink):
        async def release_delivery_credit(self, delivery_id: int) -> None:
            await super().release_delivery_credit(delivery_id)
            raise RuntimeError("credit release failed")

    failing_link = FailingLink()
    failing_link.deferred_delivery_ids = set()
    subscriber = AmqpSubscriber(
        managed_session=Mock(receiver_pool=Mock(unsubscribe=AsyncMock())),
        queues_to_callbacks={},
        naming_strategy=str,
        dispatcher=SubscriberDispatcher(),
    )
    await subscriber.pause()
    await subscriber._process_message(
        "queue",
        AsyncMock(),
        lambda **_kwargs: asyncio.sleep(0),
        b"data",
        None,
        1,
        b"tag",
        cast(Any, failing_link),
    )
    pending_delivery = next(iter(subscriber._admitted_tasks.tasks))

    with caplog.at_level(logging.ERROR, logger="repid.connections.amqp"):
        await subscriber.stop()
        await subscriber.finish()

    assert pending_delivery.cancelled()
    assert ("message.credit.release.error", logging.ERROR) in [
        (record.message, record.levelno) for record in caplog.records
    ]
