import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from repid import Concurrency
from repid.connections.sqs import SqsServer
from repid.connections.sqs.subscriber import SqsSubscriber


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        ({"concurrency": Concurrency(messages=4)}, 4),
        ({}, 10),
        ({"concurrency": Concurrency(messages=30)}, 10),
        (
            {
                "concurrency": Concurrency(messages=1000),
                "channel_concurrency": {"a": Concurrency(messages=3)},
            },
            3,
        ),
    ],
)
async def test_native_channel_receive_count(settings: dict[str, Any], expected: int) -> None:
    received = asyncio.Event()
    client = MagicMock()

    async def receive(**kwargs: object) -> dict[str, object]:
        assert kwargs["MaxNumberOfMessages"] == expected
        received.set()
        await asyncio.Event().wait()
        return {}

    client.receive_message = AsyncMock(side_effect=receive)
    server = SqsServer(receive_wait_time_seconds=1)
    server._client = client
    with patch.object(server, "_get_queue_url", return_value="queue-url"):
        subscriber = await server.subscribe(
            channels_to_callbacks={"a": AsyncMock()},
            **{"concurrency": Concurrency(), "channel_concurrency": {}, **settings},  # pyrefly: ignore[bad-argument-type]
        )
        try:
            await asyncio.wait_for(received.wait(), 1)
        finally:
            await subscriber.close()


@pytest.mark.parametrize("visibility_timeout", [0, 3])
async def test_sqs_receive_lease_matches_the_renewal_interval(visibility_timeout: int) -> None:
    requested: asyncio.Queue[dict[str, object]] = asyncio.Queue()

    async def receive(**kwargs: object) -> dict[str, object]:
        requested.put_nowait(kwargs)
        await asyncio.Event().wait()
        return {}

    server = SqsServer(visibility_timeout=visibility_timeout)
    server._client = MagicMock(receive_message=AsyncMock(side_effect=receive))
    with patch.object(server, "_get_queue_url", return_value="queue-url"):
        subscriber = await server.subscribe(
            channels_to_callbacks={"a": AsyncMock()},
            concurrency=Concurrency(messages=1),
            channel_concurrency={},
        )
        try:
            request = await asyncio.wait_for(requested.get(), 1)
            assert request.get("VisibilityTimeout") == (visibility_timeout or None)
        finally:
            await subscriber.close()


async def test_saturated_sqs_queue_does_not_block_other_queues() -> None:
    first_started = asyncio.Event()
    other_started = asyncio.Event()
    release = asyncio.Event()
    delivered_other = False
    next_receipt = 0

    async def receive(*, QueueUrl: str, **_: Any) -> dict[str, Any]:  # noqa: N803
        nonlocal delivered_other, next_receipt
        if QueueUrl == "b":
            await first_started.wait()
            if delivered_other:
                await asyncio.Event().wait()
            delivered_other = True
        next_receipt += 1
        return {"Messages": [{"ReceiptHandle": str(next_receipt)}]}

    async def blocked(_message: Any) -> None:
        first_started.set()
        await release.wait()

    server = SqsServer(receive_wait_time_seconds=1)
    server._client = MagicMock(
        receive_message=AsyncMock(side_effect=receive),
        change_message_visibility=AsyncMock(),
    )
    with patch.object(server, "_get_queue_url", side_effect=lambda channel: channel):  # pyrefly: ignore[implicit-any-lambda]
        subscriber = await server.subscribe(
            channel_concurrency={},
            channels_to_callbacks={
                "a": blocked,
                "b": AsyncMock(side_effect=lambda _: other_started.set()),  # pyrefly: ignore[implicit-any-lambda]
            },
            concurrency=Concurrency(messages=1),
        )
        try:
            await asyncio.wait_for(other_started.wait(), 1)
        finally:
            release.set()
            await subscriber.close()


@pytest.mark.parametrize("settings", [{"concurrency": Concurrency(messages=2)}, {}])
async def test_sqs_receives_only_as_many_messages_as_it_can_submit(
    settings: dict[str, Any],
) -> None:
    requested: asyncio.Queue[int] = asyncio.Queue()
    started: asyncio.Queue[str | None] = asyncio.Queue()
    release = asyncio.Event()
    receipts = iter(("1", "2"))

    async def receive(**kwargs: Any) -> dict[str, Any]:
        requested.put_nowait(kwargs["MaxNumberOfMessages"])
        if (receipt := next(receipts, None)) is not None:
            return {"Messages": [{"MessageId": receipt, "ReceiptHandle": receipt}]}
        await asyncio.Event().wait()
        return {}

    async def callback(message: Any) -> None:
        started.put_nowait(message.message_id)
        await release.wait()

    server = SqsServer(receive_wait_time_seconds=1, batch_size=2)
    server._client = MagicMock(
        receive_message=AsyncMock(side_effect=receive),
        change_message_visibility=AsyncMock(),
    )
    with patch.object(server, "_get_queue_url", return_value="queue-url"):
        subscriber = await server.subscribe(
            channels_to_callbacks={"a": callback},
            **{"concurrency": Concurrency(), "channel_concurrency": {}, **settings},  # pyrefly: ignore[bad-argument-type]
        )
        try:
            assert await asyncio.wait_for(requested.get(), 1) == 2
            assert await asyncio.wait_for(requested.get(), 1) == 1
            assert await asyncio.wait_for(started.get(), 1) == "1"
            assert await asyncio.wait_for(started.get(), 1) == "2"
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(requested.get(), 0.02)
            release.set()
            assert await asyncio.wait_for(requested.get(), 1) == 2
        finally:
            release.set()
            await subscriber.close()


async def test_sqs_pause_returns_receipts_from_a_racing_receive() -> None:
    receiving = asyncio.Event()
    returned = asyncio.Event()
    resumed = asyncio.Event()
    callback = AsyncMock()

    async def receive(**_: Any) -> dict[str, Any]:
        if receiving.is_set():
            resumed.set()
            await asyncio.Event().wait()
            return {}
        receiving.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return {"Messages": [{"ReceiptHandle": "racing-receipt"}]}
        return {}

    server = SqsServer(receive_wait_time_seconds=1)
    client = MagicMock(
        receive_message=AsyncMock(side_effect=receive),
        change_message_visibility=AsyncMock(side_effect=lambda **_: returned.set()),  # pyrefly: ignore[implicit-any-lambda]
    )
    server._client = client
    with patch.object(server, "_get_queue_url", return_value="queue-url"):
        subscriber = await server.subscribe(
            channel_concurrency={},
            channels_to_callbacks={"a": callback},
            concurrency=Concurrency(messages=1),
        )
        try:
            await asyncio.wait_for(receiving.wait(), 1)
            await subscriber.pause()
            await asyncio.wait_for(returned.wait(), 1)
            callback.assert_not_awaited()
            client.change_message_visibility.assert_awaited_once_with(
                QueueUrl="queue-url",
                ReceiptHandle="racing-receipt",
                VisibilityTimeout=0,
            )
            await subscriber.resume()
            await asyncio.wait_for(resumed.wait(), 1)
        finally:
            await subscriber.close()


async def test_sqs_busy_queue_can_receive_during_an_empty_queue_poll() -> None:
    cold_polling = asyncio.Event()
    hot_received = asyncio.Event()
    hot_delivered = False

    async def receive(*, QueueUrl: str, **_: Any) -> dict[str, Any]:  # noqa: N803
        nonlocal hot_delivered
        if QueueUrl == "cold":
            cold_polling.set()
        elif not hot_delivered:
            await cold_polling.wait()
            hot_delivered = True
            return {"Messages": [{"ReceiptHandle": "hot-receipt"}]}
        await asyncio.Event().wait()
        return {}

    server = SqsServer()
    server._client = MagicMock(receive_message=AsyncMock(side_effect=receive))
    with patch.object(server, "_get_queue_url", side_effect=lambda channel: channel):  # pyrefly: ignore[implicit-any-lambda]
        subscriber = await server.subscribe(
            channel_concurrency={},
            channels_to_callbacks={
                "cold": AsyncMock(),
                "hot": AsyncMock(side_effect=lambda _: hot_received.set()),  # pyrefly: ignore[implicit-any-lambda]
            },
            concurrency=Concurrency(messages=1),
        )
        try:
            await asyncio.wait_for(hot_received.wait(), 1)
        finally:
            await subscriber.close()


async def test_sqs_does_not_poll_when_paused_during_queue_lookup() -> None:
    looking_up = asyncio.Event()
    lookup_complete = asyncio.Event()
    release_lookup = asyncio.Event()
    polling = asyncio.Event()

    async def lookup(_channel: str) -> str:
        looking_up.set()
        await release_lookup.wait()
        lookup_complete.set()
        return "queue-url"

    async def receive(**_: Any) -> None:
        polling.set()
        await asyncio.Event().wait()

    server = SqsServer()
    server._client = MagicMock(receive_message=AsyncMock(side_effect=receive))
    with patch.object(server, "_get_queue_url", side_effect=lookup):
        subscriber = await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"a": AsyncMock()},
        )
        try:
            await asyncio.wait_for(looking_up.wait(), 1)
            await subscriber.pause()
            release_lookup.set()
            await asyncio.wait_for(lookup_complete.wait(), 1)
            server._client.receive_message.assert_not_awaited()

            await subscriber.resume()
            await asyncio.wait_for(polling.wait(), 1)
        finally:
            release_lookup.set()
            await subscriber.close()


async def test_disconnect_waits_for_a_receipt_returned_by_a_cancelled_receive() -> None:
    receiving = asyncio.Event()
    returning = asyncio.Event()
    finish_return = asyncio.Event()

    async def receive(**_: Any) -> dict[str, Any]:
        receiving.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            return {"Messages": [{"ReceiptHandle": "racing-receipt"}]}
        return {}

    async def change_visibility(**_: Any) -> None:
        returning.set()
        await finish_return.wait()

    client = MagicMock(
        receive_message=AsyncMock(side_effect=receive),
        change_message_visibility=AsyncMock(side_effect=change_visibility),
    )
    server = SqsServer()
    server._client = client
    server._client_cm = AsyncMock()
    with patch.object(server, "_get_queue_url", return_value="queue-url"):
        await server.subscribe(
            channels_to_callbacks={"a": AsyncMock()},
            concurrency=Concurrency(messages=1),
            channel_concurrency={},
        )
        await receiving.wait()
        disconnect = asyncio.create_task(server.disconnect())
        try:
            await returning.wait()
            assert not disconnect.done()
            assert server._client is client
            server._client_cm.__aexit__.assert_not_awaited()
        finally:
            finish_return.set()
            await asyncio.wait_for(disconnect, 1)
    assert server._client is None
    client.change_message_visibility.assert_awaited_once_with(
        QueueUrl="queue-url",
        ReceiptHandle="racing-receipt",
        VisibilityTimeout=0,
    )


async def test_close_returns_a_callback_cancelled_before_start() -> None:
    returned = asyncio.Event()
    callback = AsyncMock()
    fetched = False

    async def receive(**_: Any) -> dict[str, Any]:
        nonlocal fetched
        if not fetched:
            fetched = True
            return {"Messages": [{"ReceiptHandle": "receipt"}]}
        await asyncio.Future()
        return {}

    server = SqsServer()
    server._client = MagicMock(
        receive_message=AsyncMock(side_effect=receive),
        change_message_visibility=AsyncMock(side_effect=lambda **_: returned.set()),  # pyrefly: ignore[implicit-any-lambda]
    )
    create_task = asyncio.create_task

    def cancel_submission(coroutine: Any) -> asyncio.Task[Any]:
        task = create_task(coroutine)
        if coroutine.__qualname__.endswith("SqsSubscriber._process_message"):
            task.cancel()
        return task

    with (
        patch.object(server, "_get_queue_url", return_value="queue-url"),
        patch(
            "repid.connections.sqs.subscriber.asyncio.create_task",
            side_effect=cancel_submission,
        ),
    ):
        subscriber = await server.subscribe(
            channels_to_callbacks={"a": callback},
            concurrency=Concurrency(messages=1),
            channel_concurrency={},
        )
        try:
            await asyncio.wait_for(returned.wait(), 1)
        finally:
            await subscriber.close()
    callback.assert_not_awaited()


async def test_pause_resume_returns_receipts_from_the_previous_receive_generation() -> None:
    receiving = asyncio.Event()
    returned = asyncio.Event()
    fetched = False

    async def receive(**_: Any) -> dict[str, Any]:
        nonlocal fetched
        if fetched:
            await asyncio.Future()
        fetched = True
        receiving.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            return {"Messages": [{"ReceiptHandle": "old-receipt"}]}
        return {}

    server = SqsServer()
    server._client = MagicMock(
        receive_message=AsyncMock(side_effect=receive),
        change_message_visibility=AsyncMock(side_effect=lambda **_: returned.set()),  # pyrefly: ignore[implicit-any-lambda]
    )
    callback = AsyncMock()
    with patch.object(server, "_get_queue_url", return_value="queue-url"):
        subscriber = await server.subscribe(
            channels_to_callbacks={"a": callback},
            concurrency=Concurrency(messages=1),
            channel_concurrency={},
        )
        try:
            await receiving.wait()
            await subscriber.pause()
            await subscriber.resume()
            await asyncio.wait_for(returned.wait(), 1)
            callback.assert_not_awaited()
        finally:
            await subscriber.close()


async def test_callback_can_close_its_subscriber() -> None:
    closed = asyncio.Event()
    fetched = False

    async def receive(**_: Any) -> dict[str, Any]:
        nonlocal fetched
        if not fetched:
            fetched = True
            return {"Messages": [{"ReceiptHandle": "receipt"}]}
        await asyncio.Future()
        return {}

    async def callback(message: Any) -> None:
        await subscriber.close()
        closed.set()
        await message.ack()

    server = SqsServer()
    server._client = MagicMock(
        receive_message=AsyncMock(side_effect=receive),
        delete_message=AsyncMock(),
    )
    with patch.object(server, "_get_queue_url", return_value="queue-url"):
        subscriber = await server.subscribe(
            channels_to_callbacks={"a": callback},
            concurrency=Concurrency(messages=1),
            channel_concurrency={},
        )
        try:
            await asyncio.wait_for(closed.wait(), 1)
            await asyncio.wait_for(asyncio.gather(subscriber.task, return_exceptions=True), 1)
        finally:
            await subscriber.close()
    server._client.delete_message.assert_awaited_once()


async def test_failed_channel_stops_other_receive_loops() -> None:
    receiving = asyncio.Event()
    cancelled = asyncio.Event()

    async def consume_channel(channel: str) -> None:
        if channel == "bad":
            await receiving.wait()
            raise RuntimeError("failed channel")
        receiving.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    server = SqsServer()
    with patch.object(SqsSubscriber, "_consume_channel", side_effect=consume_channel):
        subscriber = SqsSubscriber(server, {"bad": AsyncMock(), "other": AsyncMock()})
        try:
            with pytest.raises(RuntimeError, match="failed channel"):
                await asyncio.wait_for(asyncio.shield(subscriber.task), 1)
            assert cancelled.is_set()
        finally:
            await subscriber.close()


@pytest.mark.parametrize("unexpected_error", [False, True])
async def test_close_joins_pending_callback_receipt_returns(unexpected_error: bool) -> None:
    started = asyncio.Event()
    returning = asyncio.Event()
    release_return = asyncio.Event()

    async def process(*_: Any) -> None:
        started.set()
        if unexpected_error:
            raise RuntimeError("unexpected submission failure")
        await asyncio.Future()

    async def return_receipt(**_: Any) -> None:
        returning.set()
        await release_return.wait()

    server = SqsServer()
    server._client = MagicMock(
        receive_message=AsyncMock(return_value={"Messages": [{"ReceiptHandle": "receipt"}]}),
        change_message_visibility=AsyncMock(side_effect=return_receipt),
    )
    with (
        patch.object(server, "_get_queue_url", return_value="queue-url"),
        patch.object(SqsSubscriber, "_process_message", side_effect=process),
    ):
        subscriber = await server.subscribe(
            channels_to_callbacks={"a": AsyncMock()},
            concurrency=Concurrency(messages=1),
            channel_concurrency={},
        )
        await started.wait()
        closing = asyncio.create_task(subscriber.close())
        try:
            await asyncio.wait_for(returning.wait(), 1)
            assert not closing.done()
        finally:
            release_return.set()
            await asyncio.wait_for(closing, 1)
    assert not subscriber.is_active
