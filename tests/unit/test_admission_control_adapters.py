from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import pytest

from repid.admission import MessageLimits
from repid.connections import SubscriberDispatcher
from repid.connections.in_memory import InMemoryServer
from repid.connections.kafka.subscriber import KafkaSubscriber
from repid.connections.nats.message_broker import NatsSubscriber
from repid.connections.sqs.subscriber import SqsSubscriber


async def test_in_memory_subscriber_skips_callback_when_intake_rejects_message() -> None:
    server = InMemoryServer()
    called = False

    async def callback(message: Any) -> None:
        nonlocal called
        called = True
        await message.ack()

    dropped = asyncio.Event()

    async def drop(*args: Any, **kwargs: Any) -> None:  # noqa: ARG001
        dropped.set()

    async with server.connection():
        subscriber = cast(
            Any,
            await server.subscribe(
                channels_to_callbacks={"jobs": callback},
                dispatcher=SubscriberDispatcher(
                    MessageLimits(max_payload_bytes=1, oversized_payload_action="nack"),
                ),
            ),
        )
        subscriber._dispatcher = Mock(reserve=AsyncMock(side_effect=drop))
        await server.publish(
            channel="jobs",
            message=Mock(payload=b"{}", headers=None, reply_to=None, content_type=None),
        )
        await asyncio.wait_for(dropped.wait(), timeout=1)
        await subscriber.pause_channel("jobs")
        await subscriber.resume_channel("jobs")
        await subscriber.close()

    assert not called


async def test_in_memory_oversized_reject_can_be_closed() -> None:
    server = InMemoryServer()

    async with server.connection():
        subscriber = await server.subscribe(
            channels_to_callbacks={"jobs": AsyncMock()},
            dispatcher=SubscriberDispatcher(
                MessageLimits(max_payload_bytes=1, oversized_payload_action="reject"),
            ),
        )
        await server.publish(
            channel="jobs",
            message=Mock(payload=b"{}", headers=None, reply_to=None, content_type=None),
        )
        await asyncio.sleep(0)
        await asyncio.wait_for(subscriber.close(), timeout=1)


async def test_sqs_subscriber_skips_callback_when_intake_drops_message() -> None:
    server = Mock()
    server._client = AsyncMock()
    server._get_queue_url = AsyncMock(return_value="queue")
    server._batch_size = 1
    server._receive_wait_time_seconds = 1
    server._visibility_timeout = 30
    server._active_subscribers = set()
    dropped = asyncio.Event()

    async def callback(message: Any) -> None:  # noqa: ARG001
        raise AssertionError("dropped messages never reach callbacks")

    subscriber = SqsSubscriber(server, {"jobs": callback})

    async def drop(*_: Any, **__: Any) -> None:
        dropped.set()
        subscriber._shutdown_event.set()

    subscriber._dispatcher = Mock(
        native_message_limit=Mock(return_value=None),
        reserve=AsyncMock(side_effect=drop),
    )
    server._client.receive_message.return_value = {
        "Messages": [{"MessageId": "id", "ReceiptHandle": "receipt", "Body": "e30="}],
    }

    await asyncio.wait_for(dropped.wait(), timeout=1)
    await subscriber.task
    await subscriber.close()


async def test_nats_serializes_unsubscribe_and_resubscribe() -> None:
    unsubscribe_started = asyncio.Event()
    allow_unsubscribe = asyncio.Event()

    class Subscription:
        def __init__(self, *, blocks: bool) -> None:
            self.blocks = blocks

        async def unsubscribe(self) -> None:
            if self.blocks:
                unsubscribe_started.set()
                await allow_unsubscribe.wait()

    class JetStream:
        def __init__(self) -> None:
            self.subscriptions: list[Subscription] = []

        async def consumer_info(self, channel: str, group: str) -> Mock:  # noqa: ARG002
            return Mock(config=Mock(ack_wait=None))

        async def subscribe(self, channel: str, **kwargs: Any) -> Subscription:  # noqa: ARG002
            subscription = Subscription(blocks=not self.subscriptions)
            self.subscriptions.append(subscription)
            return subscription

    server = Mock(_js=JetStream())
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()})
    while not subscriber.is_active:
        await asyncio.sleep(0)

    pausing = asyncio.create_task(subscriber.pause())
    await unsubscribe_started.wait()
    resuming = asyncio.create_task(subscriber.resume())
    await asyncio.sleep(0)
    assert not resuming.done()

    allow_unsubscribe.set()
    await pausing
    await resuming

    assert subscriber.is_active
    assert len(subscriber._subs) == 1
    assert len(server._js.subscriptions) == 2
    await subscriber.close()


async def test_nats_recovers_from_partial_unsubscribe_failure() -> None:
    class Subscription:
        def __init__(self, *, fail_once: bool = False) -> None:
            self.fail_once = fail_once

        async def unsubscribe(self) -> None:
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("unsubscribe failed")

    class JetStream:
        def __init__(self) -> None:
            self.calls = 0

        async def consumer_info(self, channel: str, group: str) -> Mock:  # noqa: ARG002
            return Mock(config=Mock(ack_wait=None))

        async def subscribe(self, channel: str, **kwargs: Any) -> Subscription:  # noqa: ARG002
            self.calls += 1
            return Subscription(fail_once=self.calls == 2)

    server = Mock(_js=JetStream())
    subscriber = NatsSubscriber(server, {"first": AsyncMock(), "second": AsyncMock()})
    while not subscriber.is_active:
        await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="unsubscribe failed"):
        await subscriber.pause()

    assert not subscriber.is_active
    await subscriber.resume()
    assert subscriber.is_active
    assert set(subscriber._subs) == {"first", "second"}
    await subscriber.close()


async def test_kafka_global_pause_and_resume() -> None:
    consumer = Mock()
    consumer.assignment.return_value = ()

    async def getmany(**kwargs: Any) -> dict[object, object]:  # noqa: ARG001
        await asyncio.Future()
        return {}

    consumer.getmany = AsyncMock(side_effect=getmany)
    consumer.stop = AsyncMock()
    subscriber = KafkaSubscriber(Mock(), consumer, {})

    await subscriber.pause()
    await subscriber.resume()
    subscriber._dispatcher = Mock(reserve=AsyncMock(return_value=None))
    await subscriber._process_message(
        Mock(topic="unhandled", partition=0, offset=0, value=b"{}", headers=[]),
        Mock(),
    )
    await subscriber._run_callback(None, Mock())

    consumer.pause.assert_called_once_with()
    consumer.resume.assert_called_once_with()
    await subscriber.close()


async def test_kafka_subscriber_waits_for_intake_capacity_before_polling_again() -> None:
    consumer = Mock(assignment=Mock(return_value=()), stop=AsyncMock(), commit=AsyncMock())
    first_callback_started = asyncio.Event()
    second_reservation_waiting = asyncio.Event()
    release_first_callback = asyncio.Event()
    first_lease = AsyncMock()
    reservations = 0
    records = [
        Mock(topic="jobs", partition=0, offset=0, value=b"one", headers=[]),
        Mock(topic="jobs", partition=0, offset=1, value=b"two", headers=[]),
    ]

    async def reserve(*_: Any) -> AsyncMock:
        nonlocal reservations
        reservations += 1
        if reservations == 1:
            return first_lease
        second_reservation_waiting.set()
        await asyncio.Future()
        return AsyncMock()

    async def callback(message: Any) -> None:  # noqa: ARG001
        first_callback_started.set()
        await release_first_callback.wait()

    consumer.getmany = AsyncMock(return_value={Mock(): records})
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    dispatcher.reserve = AsyncMock(side_effect=reserve)  # type: ignore[method-assign]
    subscriber = KafkaSubscriber(
        Mock(),
        consumer,
        {"jobs": callback},
        dispatcher=dispatcher,
    )

    try:
        await asyncio.wait_for(first_callback_started.wait(), timeout=1)
        await asyncio.wait_for(second_reservation_waiting.wait(), timeout=1)
        assert consumer.getmany.await_count == 1
    finally:
        release_first_callback.set()
        await subscriber.close()


async def test_nats_subscriber_closes_before_start_and_preserves_pause_error() -> None:
    server = Mock(_js=Mock())
    closed = NatsSubscriber(server, {})
    closed._closed = True
    await closed.task

    failing = NatsSubscriber(server, {})
    await asyncio.sleep(0)
    with (
        patch.object(failing, "pause", side_effect=RuntimeError("pause failed")),
        pytest.raises(RuntimeError, match="pause failed"),
    ):
        await failing.close()


async def test_nats_subscriber_skips_callback_when_intake_drops_message() -> None:
    class JetStream:
        def __init__(self) -> None:
            self.callback: Callable[[Any], Awaitable[None]] | None = None

        async def consumer_info(self, channel: str, group: str) -> Mock:  # noqa: ARG002
            return Mock(config=Mock(ack_wait=None))

        async def subscribe(self, channel: str, **kwargs: Any) -> Mock:  # noqa: ARG002
            self.callback = kwargs["cb"]
            return Mock(unsubscribe=AsyncMock())

    server = Mock()
    server._js = JetStream()
    callback = AsyncMock()
    subscriber = NatsSubscriber(server, {"jobs": callback})
    subscriber._dispatcher = Mock(reserve=AsyncMock(return_value=None))
    await asyncio.sleep(0)

    assert server._js.callback is not None
    await server._js.callback(Mock(data=b"{}", headers=None))

    callback.assert_not_awaited()
    await subscriber.close()
