from __future__ import annotations

import asyncio
import contextlib
import unittest.mock
from typing import TYPE_CHECKING, cast

import pytest

from repid import Repid
from repid.connections.abc import ReceivedMessageT, SentMessageT
from repid.connections.kafka.subscriber import KafkaSubscriber

if TYPE_CHECKING:
    from repid.connections.abc import ServerT


@pytest.fixture
def kafka_repid(kafka_connection: ServerT) -> Repid:
    app = Repid()
    app.servers.register_server("default", kafka_connection, is_default=True)
    return app


class DummySentMessage(SentMessageT):
    def __init__(
        self,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        reply_to: str | None = None,
    ) -> None:
        self._payload = payload
        self._headers = headers
        self._content_type = content_type
        self._reply_to = reply_to

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def headers(self) -> dict[str, str] | None:
        return self._headers

    @property
    def content_type(self) -> str | None:
        return self._content_type

    @property
    def reply_to(self) -> str | None:
        return self._reply_to


async def test_kafka_reject(kafka_repid: Repid, kafka_connection: ServerT) -> None:
    channel_name = "test_reject_channel"

    async with kafka_repid.servers.default.connection():
        await kafka_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"reject_payload",
                headers={"topic": "test_reject"},
                content_type="text/plain",
            ),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=10.0)

        assert received_msg is not None
        assert received_msg.payload == b"reject_payload"
        assert received_msg.content_type == "text/plain"

        await received_msg.reject()

        await subscriber.close()

        requeued_msg: ReceivedMessageT | None = None
        requeued_event = asyncio.Event()

        async def on_requeued_message(msg: ReceivedMessageT) -> None:
            nonlocal requeued_msg
            requeued_msg = msg
            requeued_event.set()

        requeued_subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={channel_name: on_requeued_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(requeued_event.wait(), timeout=10.0)
        await requeued_subscriber.close()

        assert requeued_msg is not None
        assert requeued_msg.payload == b"reject_payload"


async def test_kafka_nack_to_dlq(kafka_repid: Repid, kafka_connection: ServerT) -> None:
    channel_name = "test_nack_channel"
    dlq_channel_name = f"repid_{channel_name}_dlq"

    async with kafka_repid.servers.default.connection():
        await kafka_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"bad_payload",
                headers={"topic": "test_nack"},
                content_type="application/json",
            ),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=10.0)

        assert received_msg is not None

        assert received_msg.content_type == "application/json"
        await received_msg.nack()

        await subscriber.close()

        dlq_received_msg: ReceivedMessageT | None = None
        dlq_event = asyncio.Event()

        async def on_dlq_message(msg: ReceivedMessageT) -> None:
            nonlocal dlq_received_msg
            dlq_received_msg = msg
            dlq_event.set()

        dlq_subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={dlq_channel_name: on_dlq_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(dlq_event.wait(), timeout=10.0)
        await dlq_subscriber.close()

        assert dlq_received_msg is not None
        assert dlq_received_msg.payload == b"bad_payload"


async def test_kafka_reply(kafka_repid: Repid, kafka_connection: ServerT) -> None:
    channel_name = "test_reply_channel"
    reply_channel_name = "test_reply_dest_channel"

    async with kafka_repid.servers.default.connection():
        await kafka_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"reply_payload",
                headers={"topic": "test_reply"},
                content_type="text/plain",
            ),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=10.0)

        assert received_msg is not None

        with pytest.raises(NotImplementedError, match=r"Kafka does not support native replies\."):
            await received_msg.reply(
                payload=b"reply_response",
                headers={"is_reply": "true"},
                content_type="application/json",
                channel=reply_channel_name,
            )

        await subscriber.close()


async def test_kafka_message_properties_and_double_actions(  # noqa: PLR0915
    kafka_repid: Repid,
    kafka_connection: ServerT,
) -> None:
    channel_name = "test_reject_channel"

    async with kafka_repid.servers.default.connection():
        await kafka_connection.disconnect()
        with pytest.raises(RuntimeError, match=r"Kafka producer is not connected\."):
            await kafka_connection.publish(
                channel=channel_name,
                message=DummySentMessage(payload=b"fail"),
            )
        await kafka_connection.connect()

        assert kafka_connection.host.startswith("127.0.0.1:")
        assert kafka_connection.protocol == "kafka"
        assert kafka_connection.pathname is None
        assert kafka_connection.title is None
        assert kafka_connection.summary is None
        assert kafka_connection.description is None
        assert kafka_connection.protocol_version is None
        assert kafka_connection.variables is None
        assert kafka_connection.security is None
        assert kafka_connection.tags is None
        assert kafka_connection.external_docs is None
        assert kafka_connection.bindings is None
        assert kafka_connection.is_connected
        assert kafka_connection.capabilities == {
            "supports_acknowledgments": True,
            "supports_persistence": True,
            "supports_reply": False,
            "supports_lightweight_pause": False,
        }

        await kafka_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"ack_payload",
            ),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
        )

        assert subscriber.is_active
        assert subscriber.task is not None
        await subscriber.pause()
        await subscriber.pause()  # early return
        await subscriber.resume()
        await subscriber.resume()  # early return

        await asyncio.wait_for(event.wait(), timeout=10.0)

        assert received_msg is not None
        assert received_msg.payload == b"ack_payload"
        assert received_msg.channel == channel_name
        assert received_msg.content_type is None
        assert received_msg.headers == {}
        assert received_msg.reply_to is None
        assert received_msg.message_id is not None
        assert received_msg.action is None
        assert not received_msg.is_acted_on

        await received_msg.ack()
        assert received_msg.is_acted_on

        await received_msg.ack()
        await received_msg.nack()
        await received_msg.reject()
        await received_msg.reply(payload=b"")

        await subscriber.close()


async def test_kafka_subscriber_callback_exception(
    kafka_repid: Repid,
    kafka_connection: ServerT,
) -> None:
    channel_name = "test_nack_channel"

    async with kafka_repid.servers.default.connection():
        await kafka_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"exception_payload",
            ),
        )

        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:  # noqa: ARG001
            event.set()
            raise Exception("Test exception in callback")

        subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=10.0)
        # Give it a moment to process the exception and release semaphore
        await asyncio.sleep(0.5)
        await subscriber.close()


async def test_kafka_subscriber_close_exception_and_cancellation(
    kafka_repid: Repid,
    kafka_connection: ServerT,
) -> None:
    channel_name = "test_close_channel"

    async with kafka_repid.servers.default.connection():
        await kafka_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"slow_payload",
            ),
        )

        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:  # noqa: ARG001
            event.set()
            # Sleep a long time so that the task is still in background_tasks
            # when subscriber.close() is called.
            await asyncio.sleep(60.0)

        subscriber = await kafka_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=10.0)

        kafka_subscriber = cast(KafkaSubscriber, subscriber)

        assert len(kafka_subscriber._background_tasks) > 0

        original_stop = kafka_subscriber._consumer.stop

        with unittest.mock.patch.object(
            kafka_subscriber._consumer,
            "stop",
            side_effect=Exception("Consumer stop failed"),
        ):
            await subscriber.close()

        with contextlib.suppress(Exception):
            await original_stop()
