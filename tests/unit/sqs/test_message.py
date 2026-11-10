from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError

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
    server._dlq_queue_strategy = lambda channel: f"{channel}-dlq"  # pyrefly: ignore[implicit-any-lambda]
    server._get_queue_url = AsyncMock(return_value="dlq-url")
    return server


@pytest.mark.parametrize(("visibility_timeout", "interval"), [(0, None), (2, 2 / 3), (30, 10)])
def test_sqs_keep_alive_interval(visibility_timeout: int, interval: float | None) -> None:
    message = SqsReceivedMessage(
        server=_server(),
        channel="orders",
        queue_url="orders-url",
        msg={"ReceiptHandle": "receipt"},
        visibility_timeout=visibility_timeout,
    )
    assert message.keep_alive_interval == interval


async def test_ack_allows_keep_alive_but_blocks_other_settlements() -> None:
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
    await asyncio.gather(*other_tasks)
    release_delete.set()
    await ack_task

    assert message.action is MessageAction.acked
    server._client.delete_message.assert_awaited_once()
    server._client.send_message.assert_not_awaited()
    server._client.change_message_visibility.assert_awaited_once()


@pytest.mark.parametrize("action", ["ack", "nack", "reject"])
async def test_cancelled_settlement_finishes_before_propagating_cancellation(action: str) -> None:
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

    operation = (
        server._client.change_message_visibility
        if action == "reject"
        else server._client.delete_message
    )
    operation.side_effect = block_delete
    task = asyncio.create_task(getattr(message, action)())
    await delete_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    await message.ack()
    operation.assert_awaited_once()
    release_delete.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await message.ack()
    operation.assert_awaited_once()
    assert (
        message.action
        is {
            "ack": MessageAction.acked,
            "nack": MessageAction.nacked,
            "reject": MessageAction.rejected,
        }[action]
    )


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


@pytest.mark.parametrize("race", ["none", "cancel", "ack"])
async def test_reject_waits_for_in_flight_visibility_extension(race: str) -> None:
    server = _server()
    message = SqsReceivedMessage(
        server=server,
        channel="orders",
        queue_url="orders-url",
        msg={"ReceiptHandle": "receipt"},
    )
    extending = asyncio.Event()
    release = asyncio.Event()

    async def change_visibility(**kwargs: Any) -> None:
        if kwargs["VisibilityTimeout"] > 0:
            extending.set()
            await release.wait()

    server._client.change_message_visibility.side_effect = change_visibility
    keep_alive_task = asyncio.create_task(message.keep_alive())
    await asyncio.wait_for(extending.wait(), 1)
    rejection = asyncio.create_task(message.reject())
    try:
        await asyncio.sleep(0)
        server._client.change_message_visibility.assert_awaited_once()
        if race == "cancel":
            rejection.cancel()
            with pytest.raises(asyncio.CancelledError):
                await rejection
            assert not message.is_acted_on
            rejection = asyncio.create_task(message.reject())
        elif race == "ack":
            await message.ack()
    finally:
        release.set()
        await asyncio.gather(keep_alive_task, rejection)
    if race == "ack":
        assert message.action is MessageAction.acked
        server._client.change_message_visibility.assert_awaited_once()
        return
    server._client.change_message_visibility.assert_awaited_with(
        QueueUrl="orders-url",
        ReceiptHandle="receipt",
        VisibilityTimeout=0,
    )


async def test_cancelled_renewal_finishes_before_rejection_changes_visibility() -> None:
    server = _server()
    message = SqsReceivedMessage(server, "orders", "orders-url", {"ReceiptHandle": "receipt"})
    extending = asyncio.Event()
    release = asyncio.Event()

    async def extend(**kwargs: Any) -> None:
        if kwargs["VisibilityTimeout"] > 0:
            extending.set()
            await release.wait()

    server._client.change_message_visibility.side_effect = extend
    renewal = asyncio.create_task(message.keep_alive())
    await extending.wait()
    renewal.cancel()
    rejection = asyncio.create_task(message.reject())
    try:
        await asyncio.sleep(0)
        server._client.change_message_visibility.assert_awaited_once()
        assert not renewal.done()
        assert not rejection.done()
    finally:
        release.set()
        await asyncio.gather(renewal, rejection, return_exceptions=True)
    assert renewal.cancelled()
    server._client.change_message_visibility.assert_awaited_with(
        QueueUrl="orders-url",
        ReceiptHandle="receipt",
        VisibilityTimeout=0,
    )


@pytest.mark.parametrize("code", ["ReceiptHandleIsInvalid", "MessageNotInflight"])
async def test_stale_delivery_stops_renewals_and_settlement(code: str) -> None:
    server = _server()
    message = SqsReceivedMessage(server, "orders", "orders-url", {"ReceiptHandle": "receipt"})
    server._client.change_message_visibility.side_effect = ClientError(
        {"Error": {"Code": code}},
        "ChangeMessageVisibility",
    )
    assert await message.keep_alive() is False
    assert await message.keep_alive() is False
    await message.nack()
    await message.ack()
    await message.reject()
    server._client.change_message_visibility.assert_awaited_once()
    server._client.delete_message.assert_not_awaited()
    server._client.send_message.assert_not_awaited()


async def test_zero_visibility_timeout_disables_explicit_renewal() -> None:
    server = _server()
    message = SqsReceivedMessage(
        server,
        "orders",
        "orders-url",
        {"ReceiptHandle": "receipt"},
        visibility_timeout=0,
    )
    assert await message.keep_alive() is False
    server._client.change_message_visibility.assert_not_awaited()


async def test_cancelled_settlement_consumes_a_later_rpc_failure() -> None:
    server = _server()
    message = SqsReceivedMessage(server, "orders", "orders-url", {"ReceiptHandle": "receipt"})
    started = asyncio.Event()
    finish = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    errors: list[dict[str, Any]] = []

    async def fail_delete(**_: object) -> None:
        started.set()
        await finish.wait()
        raise ConnectionError("delete failed")

    server._client.delete_message.side_effect = fail_delete
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
