from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import botocore.exceptions
import pytest

from repid import Repid
from repid.connections.abc import MessageAction, ReceivedMessageT, SentMessageT
from repid.connections.sqs import SqsServer
from repid.connections.sqs.message import SqsReceivedMessage
from repid.connections.sqs.subscriber import SqsSubscriber

if TYPE_CHECKING:
    from repid.connections.abc import ServerT


@pytest.fixture
def sqs_repid(sqs_connection: ServerT) -> Repid:
    app = Repid()
    app.servers.register_server("default", sqs_connection, is_default=True)
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
        self._headers = headers or {}
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


class MockSubscriberWithProcessException(SqsSubscriber):
    def _process_message(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ARG002
        self._active = False
        raise ValueError("mock create_task exception")


class MockSubscriberWithCloseException(SqsSubscriber):
    async def close(self) -> None:
        raise ValueError("mock close exception")


class MockSubscriberWithMessageException(SqsSubscriber):
    async def _process_message(
        self,
        channel: str,
        queue_url: str,
        msg: Any,
        callback: Any,  # noqa: ARG002
    ) -> None:
        try:
            SqsReceivedMessage(self._server, channel, queue_url, msg)
        except Exception:
            logger = logging.getLogger("repid.connections.sqs")
            logger.exception("Error creating message", extra={"channel": channel})
            return


class MockClientFailingReceive:
    async def receive_message(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ARG002
        raise ValueError("mock error")

    async def close(self) -> None:
        pass


def _consume_mock(*_args: Any) -> None:
    return


def make_mock_server(
    *,
    client: Any | None = None,
    queue_url: str | None = None,
    receive_wait_time_seconds: int = 0,
    batch_size: int = 10,
    dlq_queue_strategy: Any = None,
) -> MagicMock:
    server = MagicMock(
        spec=SqsServer,
        _client=client,
        _receive_wait_time_seconds=receive_wait_time_seconds,
        _batch_size=batch_size,
        _dlq_queue_strategy=dlq_queue_strategy,
    )
    if queue_url is not None:
        server._get_queue_url = AsyncMock(return_value=queue_url)
    return server


async def test_sqs_nack_routes_message_to_dlq(sqs_repid: Repid, sqs_connection: ServerT) -> None:
    channel_name = "test_nack_channel"
    dlq_channel_name = f"repid_{channel_name}_dlq"

    async with sqs_repid.servers.default.connection():
        await sqs_connection.publish(
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

        subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=15.0)
        await subscriber.close()

        assert received_msg is not None
        assert received_msg.content_type == "application/json"

        await received_msg.nack()

        dlq_received_msg: ReceivedMessageT | None = None
        dlq_event = asyncio.Event()

        async def on_dlq_message(msg: ReceivedMessageT) -> None:
            nonlocal dlq_received_msg
            dlq_received_msg = msg
            dlq_event.set()

        dlq_subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={dlq_channel_name: on_dlq_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(dlq_event.wait(), timeout=15.0)
        await dlq_subscriber.close()

        assert dlq_received_msg is not None
        assert dlq_received_msg.payload == b"bad_payload"
        assert dlq_received_msg.content_type == "application/json"
        assert dlq_received_msg.headers == {"topic": "test_nack"}


async def test_sqs_reply_sends_message_to_reply_channel(
    sqs_repid: Repid,
    sqs_connection: ServerT,
) -> None:
    channel_name = "test_reply_channel"
    reply_channel_name = "test_reply_dest_channel"

    async with sqs_repid.servers.default.connection():
        await sqs_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"reply_payload",
            ),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=15.0)
        await subscriber.close()

        assert received_msg is not None

        with pytest.raises(NotImplementedError, match=r"SQS does not support native replies\."):
            await received_msg.reply(
                payload=b"replied_payload",
                channel=reply_channel_name,
            )


async def test_sqs_publish_preserves_empty_payload(
    sqs_repid: Repid,
    sqs_connection: ServerT,
) -> None:
    channel_name = "test_empty_payload_channel"

    async with sqs_repid.servers.default.connection():
        server = cast(SqsServer, sqs_connection)
        assert server._client is not None
        await server._client.create_queue(QueueName=channel_name)

        await sqs_connection.publish(
            channel=channel_name,
            message=DummySentMessage(payload=b""),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=15.0)
        await subscriber.close()

        assert received_msg is not None
        assert received_msg.payload == b""


async def test_sqs_reply_preserves_empty_payload(sqs_repid: Repid, sqs_connection: ServerT) -> None:
    channel_name = "test_reply_empty_source"
    reply_channel_name = "test_reply_empty_dest"

    async with sqs_repid.servers.default.connection():
        server = cast(SqsServer, sqs_connection)
        assert server._client is not None
        await server._client.create_queue(QueueName=channel_name)
        await server._client.create_queue(QueueName=reply_channel_name)

        await sqs_connection.publish(
            channel=channel_name,
            message=DummySentMessage(payload=b"source"),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=15.0)
        await subscriber.close()

        assert received_msg is not None
        with pytest.raises(NotImplementedError, match=r"SQS does not support native replies\."):
            await received_msg.reply(payload=b"", channel=reply_channel_name)


async def test_sqs_multiple_actions_are_ignored(sqs_repid: Repid, sqs_connection: ServerT) -> None:
    channel_name = "test_double_actions_channel"

    async with sqs_repid.servers.default.connection():
        await sqs_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"double_action_payload",
            ),
        )

        received_msg: ReceivedMessageT | None = None
        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:
            nonlocal received_msg
            received_msg = msg
            event.set()

        subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=15.0)

        assert received_msg is not None

        await received_msg.ack()
        await received_msg.ack()
        await received_msg.nack()
        await received_msg.reject()
        await received_msg.reply(payload=b"")

        await subscriber.close()


async def test_sqs_subscriber_handles_callback_exception(
    sqs_repid: Repid,
    sqs_connection: ServerT,
) -> None:
    channel_name = "test_exception_channel"

    async with sqs_repid.servers.default.connection():
        await sqs_connection.publish(
            channel=channel_name,
            message=DummySentMessage(
                payload=b"exception_payload",
            ),
        )

        event = asyncio.Event()

        async def on_message(msg: ReceivedMessageT) -> None:  # noqa: ARG001
            event.set()
            raise Exception("Test exception in callback")

        subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={channel_name: on_message},
            concurrency_limit=1,
        )

        await asyncio.wait_for(event.wait(), timeout=15.0)
        await asyncio.sleep(0.5)
        await subscriber.close()


async def test_sqs_subscriber_closes_gracefully(
    sqs_repid: Repid,
    sqs_connection: ServerT,
) -> None:
    channel_name = "test_close_channel"

    async with sqs_repid.servers.default.connection():
        subscriber = await sqs_connection.subscribe(
            channels_to_callbacks={channel_name: lambda msg: asyncio.sleep(0)},  # noqa: ARG005
            concurrency_limit=1,
        )

        await asyncio.sleep(0.1)
        await subscriber.close()
        assert not subscriber.is_active


async def test_sqs_server_properties_are_correct(sqs_connection: ServerT) -> None:
    server = SqsServer(
        endpoint_url=sqs_connection.host,
        region_name="elasticmq",
        title="MySQS",
        summary="summary",
        description="description",
        variables={"var": "val"},  # type: ignore[dict-item]
        security=[{"sec": "val"}],
        tags=[{"name": "tag1"}],  # type: ignore[list-item]
        external_docs={"url": "http://docs"},  # type: ignore[arg-type]
        bindings={"sqs": "binding"},
        aws_access_key_id="x",
        aws_secret_access_key="x",
    )

    assert server.host == sqs_connection.host
    assert server.protocol == "sqs"
    assert server.pathname is None
    assert server.title == "MySQS"
    assert server.summary == "summary"
    assert server.description == "description"
    assert server.protocol_version is None
    assert server.variables == {"var": "val"}
    assert server.security == [{"sec": "val"}]
    assert server.tags == [{"name": "tag1"}]
    assert server.external_docs == {"url": "http://docs"}
    assert server.bindings == {"sqs": "binding"}
    assert server.capabilities["supports_native_reply"] is False


async def test_sqs_server_connection_state(sqs_connection: ServerT) -> None:
    server = SqsServer(
        endpoint_url=sqs_connection.host,
        region_name="elasticmq",
        aws_access_key_id="x",
        aws_secret_access_key="x",
    )

    assert server.is_connected is False
    async with server.connection():
        assert server.is_connected is True

        await server.publish(
            channel="default",
            message=DummySentMessage(b"cov"),
            server_specific_parameters={"DelaySeconds": 1},
        )


async def test_sqs_server_raises_when_not_connected_for_get_queue_url() -> None:
    server = SqsServer()
    with pytest.raises(RuntimeError, match=r"SQS client is not connected\."):
        await server._get_queue_url("any")


async def test_sqs_server_raises_when_not_connected_for_publish() -> None:
    server = SqsServer()
    with pytest.raises(RuntimeError, match=r"SQS client is not connected\."):
        await server.publish(channel="default", message=None)  # type: ignore[arg-type]


async def test_sqs_server_raises_when_not_connected_for_subscribe() -> None:
    server = SqsServer()
    with pytest.raises(RuntimeError, match=r"SQS client is not connected\."):
        await server.subscribe(channels_to_callbacks={})


async def test_sqs_message_properties_parsing(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)

    msg_dict = {
        "MessageId": "mid",
        "ReceiptHandle": "rhandle",
        "Body": "test_payload",
        "MessageAttributes": {
            "content-type": {"StringValue": "text/plain"},
            "custom-header": {"StringValue": "header_val"},
        },
    }

    msg = SqsReceivedMessage(
        server=server,
        channel="my_channel",
        queue_url="http://queue",
        msg=msg_dict,
    )

    assert msg.payload == b"test_payload"
    assert msg.headers == {"custom-header": "header_val"}
    assert msg.content_type == "text/plain"
    assert msg.reply_to is None
    assert msg.channel == "my_channel"
    assert msg.action is None
    assert msg.is_acted_on is False
    assert msg.message_id == "mid"


async def test_sqs_message_ack_action(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)

    msg_dict = {
        "MessageId": "mid",
        "ReceiptHandle": "rhandle",
        "Body": "dGVzdF9wYXlsb2Fk",
    }

    msg = SqsReceivedMessage(
        server=server,
        channel="my_channel",
        queue_url="http://queue",
        msg=msg_dict,
    )

    await msg.ack()
    assert msg.action == "acked"
    assert msg.is_acted_on is True


async def test_sqs_message_reject_suppresses_exceptions(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        q1_url = await server._get_queue_url("default")
        msg_dict1 = {"MessageId": "1", "ReceiptHandle": "r1", "Body": "eQ=="}
        msg1 = SqsReceivedMessage(server, "default", q1_url, msg_dict1)
        await msg1.reject()
        assert msg1.action is not None
        assert msg1.is_acted_on is True


async def test_sqs_message_reply_with_server_params(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        q1_url = await server._get_queue_url("default")
        msg_dict2 = {"MessageId": "2", "ReceiptHandle": "r2", "Body": "eQ=="}
        msg2 = SqsReceivedMessage(server, "default", q1_url, msg_dict2)
        with pytest.raises(NotImplementedError, match=r"SQS does not support native replies\."):
            await msg2.reply(
                payload=b"hi",
                headers={"foo": "bar"},
                content_type="text/plain",
                server_specific_parameters={"DelaySeconds": 1},
            )


async def test_sqs_message_nack_without_dlq(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        q1_url = await server._get_queue_url("default")
        server._dlq_queue_strategy = None
        msg_dict3 = {"MessageId": "3", "ReceiptHandle": "r3", "Body": "eQ=="}
        msg3 = SqsReceivedMessage(server, "default", q1_url, msg_dict3)
        await msg3.nack()


async def test_sqs_subscriber_active_state(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        assert sub.is_active is True
        await sub.close()


async def test_sqs_subscriber_task_property(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        task = sub.task
        assert isinstance(task, asyncio.Task)
        await sub.close()


async def test_sqs_subscriber_start_consuming_when_already_active(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        sub._start_consuming()
        await sub.close()


async def test_sqs_subscriber_resume_when_active(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        await sub.resume()
        await sub.close()


async def test_sqs_subscriber_pause_and_resume(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        await sub.pause()
        await sub.resume()
        await sub.close()


async def test_sqs_subscriber_process_message_handles_callback_exception(
    sqs_connection: ServerT,
) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )

        async def err_cb(_: ReceivedMessageT) -> None:
            raise ValueError("err")

        await sub._process_message(
            "default",
            "http://default",
            {"MessageId": "4", "Body": "eQ=="},
            err_cb,
        )
        await sub.close()


async def test_sqs_subscriber_task_raises_when_not_active(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        await sub.close()
        sub._main_task = None
        with pytest.raises(RuntimeError):
            _ = sub.task


async def test_sqs_subscriber_handles_receive_exception(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )

        original_client = server._client
        server._client = cast(Any, MockClientFailingReceive())

        task = asyncio.create_task(sub._consume_channel("default"))
        await asyncio.sleep(0.1)
        task.cancel()
        await asyncio.sleep(0.1)

        server._client = original_client
        await sub.close()


async def test_sqs_subscriber_handles_consume_cancellation(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        consume_task = asyncio.create_task(sub._consume())
        await asyncio.sleep(0.1)
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        await sub.close()


async def test_sqs_subscriber_handles_pause_cancellation(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = cast(
            SqsSubscriber,
            await server.subscribe(
                channels_to_callbacks={
                    "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
                },
            ),
        )
        await asyncio.sleep(0.1)
        if sub._main_task:
            sub._main_task.cancel()
        await sub.pause()
        await sub.close()


async def test_sqs_subscriber_raises_if_client_is_none(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)
    async with server.connection():
        sub = await server.subscribe(
            channels_to_callbacks={
                "default": lambda m: asyncio.sleep(0),  # noqa: ARG005
            },
        )

        original_client = server._client
        server._client = None
        try:
            with pytest.raises(RuntimeError, match=r"SQS client is not connected\."):
                await sub._consume_channel("default")  # type: ignore[attr-defined]
        finally:
            server._client = original_client
            await sub.close()


async def test_sqs_message_handles_invalid_base64(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)

    msg_dict = {"Body": "!!invalid_base64!!"}
    msg = SqsReceivedMessage(server, "default", "http://queue", msg_dict)
    assert msg.payload == b"!!invalid_base64!!"


async def test_sqs_subscriber_handles_process_message_exception(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)

    async with server.connection():
        await server.publish(channel="default", message=DummySentMessage(payload=b"trigger"))

        sub = MockSubscriberWithProcessException(
            server,
            {"default": lambda _: asyncio.sleep(0)},
            concurrency_limit=1,
        )
        try:
            await sub._consume_channel("default")
        finally:
            with contextlib.suppress(ValueError):
                await sub.close()


async def test_sqs_subscriber_handles_message_instantiation_error(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)

    async with server.connection():
        sub = MockSubscriberWithMessageException(
            server,
            {"default": lambda _: asyncio.sleep(0)},
            concurrency_limit=1,
        )
        await sub._process_message("default", "http://queue", {}, lambda _: asyncio.sleep(0))
        await sub.close()


async def test_sqs_server_handles_subscriber_close_error(sqs_connection: ServerT) -> None:
    server = cast(SqsServer, sqs_connection)

    async with server.connection():
        sub = MockSubscriberWithCloseException(server, {"default": lambda _: asyncio.sleep(0)})
        server._active_subscribers.add(sub)
        await server.disconnect()


async def test_sqs_message_re_raises_other_client_errors() -> None:
    error_response: Any = {"Error": {"Code": "AccessDenied"}}
    client_error = botocore.exceptions.ClientError(error_response, "operation")
    client = AsyncMock(
        delete_message=AsyncMock(side_effect=client_error),
        change_message_visibility=AsyncMock(side_effect=client_error),
    )
    server = make_mock_server(client=client, dlq_queue_strategy=None)

    msg = SqsReceivedMessage(server, "channel", "url", {"ReceiptHandle": "123"})

    with pytest.raises(botocore.exceptions.ClientError):
        await msg.ack()

    with pytest.raises(botocore.exceptions.ClientError):
        await msg.nack()

    with pytest.raises(botocore.exceptions.ClientError):
        await msg.reject()


async def test_sqs_message_skips_actions_if_already_acted() -> None:
    server = make_mock_server(client=AsyncMock())

    msg = SqsReceivedMessage(server, "channel", "url", {"ReceiptHandle": "123"})
    msg._action = MessageAction.acked

    await msg.ack()
    await msg.nack()
    await msg.reject()

    server._client.delete_message.assert_not_called()
    server._client.change_message_visibility.assert_not_called()


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
@patch("repid.connections.sqs.subscriber.SqsReceivedMessage")
async def test_subscriber_process_message_reject_exception(
    mock_received_message: MagicMock,
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server(client=AsyncMock(), queue_url="url")

    subscriber = SqsSubscriber(server, {})
    subscriber._active = True

    async def failing_callback(msg: Any) -> None:  # noqa: ARG001
        raise asyncio.CancelledError()

    class BadMessage:
        @property
        def is_acted_on(self) -> bool:
            return False

        async def reject(self) -> None:
            raise Exception("test exception")

    mock_received_message.return_value = BadMessage()

    with contextlib.suppress(asyncio.CancelledError):
        await subscriber._process_message("channel", "url", {}, failing_callback)


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
@patch("repid.connections.sqs.subscriber.SqsReceivedMessage")
async def test_subscriber_process_message_nack_exception_sync(
    mock_received_message: MagicMock,
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server(client=AsyncMock(), queue_url="url")

    subscriber = SqsSubscriber(server, {})
    subscriber._active = True

    async def failing_callback(msg: Any) -> None:
        pass

    class BadMessage:
        @property
        def is_acted_on(self) -> bool:
            return False

        async def nack(self) -> None:
            raise Exception("test exception")

    mock_received_message.return_value = BadMessage()
    await subscriber._process_message("channel", "url", {}, failing_callback)


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
@patch(
    "repid.connections.sqs.subscriber.SqsReceivedMessage",
    side_effect=Exception("creation error"),
)
async def test_subscriber_process_message_creation_exception(
    received_message_mock: MagicMock,
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(received_message_mock, start_consuming_mock)
    server = make_mock_server()

    subscriber = SqsSubscriber(server, {})
    subscriber._active = True

    async def failing_callback(msg: Any) -> None:
        pass

    await subscriber._process_message("channel", "url", {}, failing_callback)


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
@patch("repid.connections.sqs.subscriber.SqsReceivedMessage")
async def test_subscriber_consume_channel_cancelled_unprocessed_exception(
    mock_received_message: MagicMock,
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    client = AsyncMock(receive_message=AsyncMock(return_value={"Messages": [{"Body": "hi"}]}))
    server = make_mock_server(client=client, queue_url="url", receive_wait_time_seconds=0)

    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, concurrency_limit=1)
    subscriber._active = True

    class RejectingMessage:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def reject(self) -> None:
            raise Exception("Reject failed")

    mock_received_message.side_effect = RejectingMessage
    subscriber._semaphore = AsyncMock()
    subscriber._semaphore.acquire = AsyncMock(side_effect=asyncio.CancelledError())

    with contextlib.suppress(asyncio.CancelledError):
        await subscriber._consume_channel("test")


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
@patch(
    "repid.connections.sqs.subscriber.SqsReceivedMessage",
    side_effect=Exception("Creation failed"),
)
async def test_subscriber_consume_channel_cancelled_unprocessed_creation_exception(
    received_message_mock: MagicMock,
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(received_message_mock, start_consuming_mock)
    client = AsyncMock(receive_message=AsyncMock(return_value={"Messages": [{"Body": "hi"}]}))
    server = make_mock_server(client=client, queue_url="url", receive_wait_time_seconds=0)

    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, concurrency_limit=1)
    subscriber._active = True
    subscriber._semaphore = AsyncMock()
    subscriber._semaphore.acquire = AsyncMock(side_effect=asyncio.CancelledError())

    with contextlib.suppress(asyncio.CancelledError):
        await subscriber._consume_channel("test")


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
@patch("repid.connections.sqs.subscriber.SqsReceivedMessage")
async def test_subscriber_process_message_nack_cancelled_sync(
    mock_received_message: MagicMock,
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server(client=AsyncMock(), queue_url="url")

    subscriber = SqsSubscriber(server, {})
    subscriber._active = True

    async def failing_callback(msg: Any) -> None:  # noqa: ARG001
        raise Exception("callback exception")

    class BadMessage:
        @property
        def is_acted_on(self) -> bool:
            return False

        async def nack(self) -> None:
            raise asyncio.CancelledError()

    mock_received_message.return_value = BadMessage()
    await subscriber._process_message("channel", "url", {}, failing_callback)


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
@patch("repid.connections.sqs.subscriber.SqsReceivedMessage")
async def test_subscriber_process_message_nack_error_sync(
    mock_received_message: MagicMock,
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server(client=AsyncMock(), queue_url="url")

    subscriber = SqsSubscriber(server, {})
    subscriber._active = True

    async def failing_callback(msg: Any) -> None:  # noqa: ARG001
        raise Exception("callback exception")

    class BadMessage:
        @property
        def is_acted_on(self) -> bool:
            return False

        async def nack(self) -> None:
            raise Exception("nack exception")

    mock_received_message.return_value = BadMessage()
    await subscriber._process_message("channel", "url", {}, failing_callback)


async def test_subscriber_pause_sets_pause_signals() -> None:
    server = MagicMock(spec=SqsServer)
    subscriber = SqsSubscriber(server, {})

    await subscriber.pause()

    assert subscriber._paused_event.is_set() is False
    assert subscriber._pause_requested_event.is_set() is True


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_consume_channel_breaks_if_shutdown_after_pause_wait(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server(client=AsyncMock(), queue_url="url", receive_wait_time_seconds=0)

    subscriber = SqsSubscriber(server, {"test": AsyncMock()})

    subscriber._active = True
    subscriber._paused_event.clear()

    async def release_wait() -> None:
        await asyncio.sleep(0)
        subscriber._shutdown_event.set()
        subscriber._paused_event.set()

    releaser = asyncio.create_task(release_wait())
    await subscriber._consume_channel("test")
    await releaser


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_consume_channel_receive_preempted_by_pause(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server_client = AsyncMock()
    server = make_mock_server(client=server_client, queue_url="url", receive_wait_time_seconds=0)

    async def receive_message(*args: Any, **kwargs: Any) -> dict[str, list[dict[str, str]]]:  # noqa: ARG001
        await asyncio.sleep(0.5)
        return {"Messages": []}

    server_client.receive_message = AsyncMock(side_effect=receive_message)
    subscriber = SqsSubscriber(server, {"test": AsyncMock()})

    subscriber._active = True
    subscriber._paused_event.set()

    async def request_pause() -> None:
        await asyncio.sleep(0.01)
        subscriber._active = False
        subscriber._paused_event.clear()
        subscriber._pause_requested_event.set()

    pauser = asyncio.create_task(request_pause())
    await subscriber._consume_channel("test")
    await pauser


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_consume_channel_semaphore_preempted_by_pause(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    client = AsyncMock(receive_message=AsyncMock(return_value={"Messages": [{"Body": "hi"}]}))
    server = make_mock_server(client=client, queue_url="url", receive_wait_time_seconds=0)

    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, concurrency_limit=1)

    subscriber._active = True
    subscriber._semaphore = asyncio.Semaphore(0)
    subscriber._paused_event.set()

    async def request_pause() -> None:
        await asyncio.sleep(0.01)
        subscriber._active = False
        subscriber._paused_event.clear()
        subscriber._pause_requested_event.set()

    pauser = asyncio.create_task(request_pause())
    await subscriber._consume_channel("test")
    await pauser


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_consume_channel_breaks_on_semaphore_wait_when_paused(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server_client = AsyncMock()
    server = make_mock_server(client=server_client, queue_url="url", receive_wait_time_seconds=0)

    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, concurrency_limit=1)

    async def receive_message(*args: Any, **kwargs: Any) -> dict[str, list[dict[str, str]]]:  # noqa: ARG001
        return {"Messages": [{"Body": "hi"}]}

    server_client.receive_message = AsyncMock(side_effect=receive_message)

    subscriber._active = True

    class FlipSemaphore:
        def __bool__(self) -> bool:
            subscriber._paused_event.clear()
            subscriber._active = False
            return True

    subscriber._semaphore = FlipSemaphore()  # type: ignore[assignment]
    await subscriber._consume_channel("test")


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_resume_returns_if_shutdown(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server()

    subscriber = SqsSubscriber(server, {})

    subscriber._shutdown_event.set()
    subscriber._paused_event.clear()
    await subscriber.resume()
    assert subscriber._paused_event.is_set() is False


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_resume_clears_done_pause_wait_task(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server()

    subscriber = SqsSubscriber(server, {})

    done_task: asyncio.Task[bool] = asyncio.create_task(asyncio.sleep(0, result=True))
    await done_task
    subscriber._pause_wait_task = done_task
    subscriber._active = True

    await subscriber.resume()

    assert subscriber._pause_wait_task is None


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_resume_sets_active_and_restarts(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server()

    subscriber = SqsSubscriber(server, {})

    called = {"started": False}

    def fake_start() -> None:
        called["started"] = True

    with patch.object(subscriber, "_start_consuming", side_effect=fake_start):
        subscriber._active = False
        subscriber._paused_event.clear()
        subscriber._main_task = None

        await subscriber.resume()

    assert subscriber._active is True
    assert subscriber._paused_event.is_set() is True
    assert called["started"] is True


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_consume_channel_breaks_message_loop_on_shutdown(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server_client = AsyncMock()
    server = make_mock_server(client=server_client, queue_url="url", receive_wait_time_seconds=0)

    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, concurrency_limit=1)

    async def receive_message(*args: Any, **kwargs: Any) -> dict[str, list[dict[str, str]]]:  # noqa: ARG001
        subscriber._shutdown_event.set()
        return {"Messages": [{"Body": "hi"}]}

    server_client.receive_message = AsyncMock(side_effect=receive_message)

    subscriber._active = True
    subscriber._paused_event.set()

    await subscriber._consume_channel("test")


async def test_sqs_server_batch_size_validation() -> None:
    with pytest.raises(ValueError, match="batch_size must be between 1 and 10 for SQS"):
        SqsServer(batch_size=0)

    with pytest.raises(ValueError, match="batch_size must be between 1 and 10 for SQS"):
        SqsServer(batch_size=11)


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_consume_channel_uses_server_batch_size(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    client = AsyncMock()
    server = make_mock_server(
        client=client,
        queue_url="url",
        receive_wait_time_seconds=0,
        batch_size=3,
    )
    subscriber = SqsSubscriber(server, {"test": AsyncMock()}, concurrency_limit=1)
    subscriber._active = True

    async def receive_message(**_kwargs: Any) -> dict[str, list[dict[str, str]]]:
        subscriber._shutdown_event.set()
        return {"Messages": []}

    client.receive_message = AsyncMock(side_effect=receive_message)

    await subscriber._consume_channel("test")

    client.receive_message.assert_awaited_once()
    assert client.receive_message.await_args.kwargs["MaxNumberOfMessages"] == 3


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_close_skips_removal_if_not_active_subscriber(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server()
    server._active_subscribers = set()

    subscriber = SqsSubscriber(server, {})

    await subscriber.close()


@patch("repid.connections.sqs.subscriber.SqsSubscriber._start_consuming", return_value=None)
async def test_subscriber_close_removes_active_subscriber(
    start_consuming_mock: MagicMock,
) -> None:
    _consume_mock(start_consuming_mock)
    server = make_mock_server()
    server._active_subscribers = set()

    subscriber = SqsSubscriber(server, {})
    server._active_subscribers.add(subscriber)

    await subscriber.close()

    assert subscriber not in server._active_subscribers
