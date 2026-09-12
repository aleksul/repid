from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from repid.connections.abc import MessageAction
from repid.connections.sqs.constants import (
    EMPTY_PAYLOAD_ATTRIBUTE,
    EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
    EMPTY_PAYLOAD_BODY_PLACEHOLDER,
)
from repid.connections.sqs.message import SqsReceivedMessage
from repid.connections.sqs.message_broker import SqsServer


def _server() -> MagicMock:
    server = MagicMock(spec=SqsServer)
    server._client = AsyncMock()
    server._dlq_queue_strategy = lambda channel: f"{channel}-dlq"
    server._get_queue_url = AsyncMock(return_value="dlq-url")
    return server


async def test_concurrent_settlement_and_keep_alive_issue_only_one_broker_operation() -> None:
    server = _server()
    message = SqsReceivedMessage(
        server=server,
        channel="orders",
        queue_url="orders-url",
        msg={"ReceiptHandle": "receipt"},
    )
    delete_started = asyncio.Event()
    release_delete = asyncio.Event()

    async def block_delete(**_: object) -> None:
        delete_started.set()
        await release_delete.wait()

    server._client.delete_message.side_effect = block_delete
    ack_task = asyncio.create_task(message.ack())
    await delete_started.wait()
    other_tasks = [
        asyncio.create_task(message.nack()),
        asyncio.create_task(message.reject()),
        asyncio.create_task(message.keep_alive()),
    ]
    release_delete.set()

    await asyncio.gather(ack_task, *other_tasks)

    assert message.action is MessageAction.acked
    server._client.delete_message.assert_awaited_once()
    server._client.send_message.assert_not_awaited()
    server._client.change_message_visibility.assert_not_awaited()


async def test_cancelled_settlement_stays_reserved_and_blocks_retry() -> None:
    server = _server()
    message = SqsReceivedMessage(
        server=server,
        channel="orders",
        queue_url="orders-url",
        msg={"ReceiptHandle": "receipt"},
    )
    delete_started = asyncio.Event()
    release_delete = asyncio.Event()

    async def block_delete(**_: object) -> None:
        delete_started.set()
        await release_delete.wait()

    server._client.delete_message.side_effect = block_delete
    task = asyncio.create_task(message.ack())
    await delete_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Cancellation may have left the RPC in flight (delete_message can be
    # non-cancellable in botocore), so the settlement stays reserved and a
    # retry must not issue a second broker operation.
    assert message.action is MessageAction.acked
    server._client.delete_message.side_effect = None
    await message.ack()
    server._client.delete_message.assert_awaited_once()
    assert message.action is MessageAction.acked


async def test_nack_round_trip_preserves_empty_payload_marker() -> None:
    server = _server()
    message = SqsReceivedMessage(
        server=server,
        channel="orders",
        queue_url="orders-url",
        msg={
            "ReceiptHandle": "receipt",
            "Body": EMPTY_PAYLOAD_BODY_PLACEHOLDER,
            "MessageAttributes": {
                EMPTY_PAYLOAD_ATTRIBUTE: {
                    "DataType": "String",
                    "StringValue": EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
                },
                "trace-id": {"DataType": "String", "StringValue": "trace"},
            },
        },
    )

    assert message.payload == b""
    assert message.headers == {"trace-id": "trace"}
    await message.nack()

    sent: dict[str, Any] = server._client.send_message.await_args.kwargs
    assert sent["MessageAttributes"][EMPTY_PAYLOAD_ATTRIBUTE] == {
        "DataType": "String",
        "StringValue": EMPTY_PAYLOAD_ATTRIBUTE_VALUE,
    }
    dlq_message = SqsReceivedMessage(
        server=server,
        channel="orders-dlq",
        queue_url="dlq-url",
        msg={
            "Body": sent["MessageBody"],
            "MessageAttributes": sent["MessageAttributes"],
        },
    )
    assert dlq_message.payload == b""
    assert dlq_message.headers == {"trace-id": "trace"}


async def test_nack_round_trip_preserves_ordinary_payload_metadata() -> None:
    server = _server()
    message = SqsReceivedMessage(
        server=server,
        channel="orders",
        queue_url="orders-url",
        msg={
            "ReceiptHandle": "receipt",
            "Body": "cGF5bG9hZA==",
            "MessageAttributes": {
                "content-type": {"DataType": "String", "StringValue": "application/json"},
                "trace-id": {"DataType": "String", "StringValue": "trace"},
            },
        },
    )

    await message.nack()

    sent: dict[str, Any] = server._client.send_message.await_args.kwargs
    assert EMPTY_PAYLOAD_ATTRIBUTE not in sent["MessageAttributes"]
    dlq_message = SqsReceivedMessage(
        server=server,
        channel="orders-dlq",
        queue_url="dlq-url",
        msg={
            "Body": sent["MessageBody"],
            "MessageAttributes": sent["MessageAttributes"],
        },
    )
    assert dlq_message.payload == b"payload"
    assert dlq_message.content_type == "application/json"
    assert dlq_message.headers == {"trace-id": "trace"}
