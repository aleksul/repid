import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import cast

import pytest

from repid import Concurrency
from repid.connections.abc import MessageAction, ReceivedMessageT
from repid.connections.in_memory.message_broker import (
    _SUBMISSION_BATCH_SIZE,
    InMemoryReceivedMessage,
    InMemorySentMessage,
    InMemoryServer,
    InMemorySubscriber,
)
from repid.connections.in_memory.utils import DummyQueue


def test_sent_message_properties() -> None:
    msg = InMemorySentMessage(
        payload=b"test",
        headers={"key": "val"},
        reply_to="reply_chan",
        content_type="text",
        message_id="msg_id",
    )
    assert msg.payload == b"test"
    assert msg.headers == {"key": "val"}
    assert msg.reply_to == "reply_chan"
    assert msg.content_type == "text"
    assert msg.message_id == "msg_id"


async def test_in_memory_subscriber_acks_duplicate_ids_after_header_mutation() -> None:
    server = InMemoryServer()
    received = 0
    completed = asyncio.Event()

    async def callback(message: ReceivedMessageT) -> None:
        nonlocal received
        received += 1
        assert message.headers is not None
        message.headers["mutated"] = "yes"
        await message.ack()
        if received == 2:
            completed.set()

    async with server.connection():
        subscriber = await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"jobs": callback},
        )
        for _ in range(2):
            await server.publish(
                channel="jobs",
                message=InMemorySentMessage(payload=b"same", headers={"key": "value"}),
                server_specific_parameters={"message_id": "same-id"},
            )
        await asyncio.wait_for(completed.wait(), timeout=1)
        await subscriber.close()

    assert received == 2
    assert not server.queues["jobs"].processing


async def test_received_message_ack() -> None:
    queue = DummyQueue()
    d_msg = DummyQueue.Message(payload=b"abc")
    queue.processing.add(d_msg)

    msg = InMemoryReceivedMessage(d_msg, queue, "chan")
    assert not msg.is_acted_on
    assert msg.payload == b"abc"
    assert msg.headers is None
    assert msg.content_type is None
    assert msg.reply_to is None
    assert msg.message_id is None
    assert msg.channel == "chan"

    await msg.ack()
    assert msg.is_acted_on
    assert msg.action == MessageAction.acked
    assert d_msg not in queue.processing

    # Second call does nothing
    await msg.ack()


async def test_received_message_nack() -> None:
    queue = DummyQueue()
    d_msg = DummyQueue.Message(payload=b"abc")
    queue.processing.add(d_msg)

    msg = InMemoryReceivedMessage(d_msg, queue, "chan")
    await msg.nack()
    assert msg.is_acted_on
    assert d_msg not in queue.processing

    # Second call does nothing
    await msg.nack()


async def test_received_message_reject() -> None:
    queue = DummyQueue()
    d_msg = DummyQueue.Message(payload=b"abc")
    queue.processing.add(d_msg)

    msg = InMemoryReceivedMessage(d_msg, queue, "chan")
    await msg.reject()
    assert msg.is_acted_on
    assert d_msg not in queue.processing
    assert await queue.queue.get() == d_msg

    # Second call does nothing
    await msg.reject()


async def test_received_message_reply() -> None:
    queue = DummyQueue()
    queues = {"chan": queue}
    d_msg_no_reply = DummyQueue.Message(payload=b"abc")
    queue.processing.add(d_msg_no_reply)

    msg = InMemoryReceivedMessage(d_msg_no_reply, queue, "chan", queues)
    with pytest.raises(ValueError, match="Reply channel is not set"):
        await msg.reply(payload=b"reply", headers={"h": "v"})

    # Remove the no-reply-to message and create a fresh one with reply_to set
    queue.processing.remove(d_msg_no_reply)
    d_msg = DummyQueue.Message(payload=b"abc", reply_to="chan")
    queue.processing.add(d_msg)
    msg = InMemoryReceivedMessage(d_msg, queue, "chan", queues)
    await msg.reply(payload=b"reply", headers={"h": "v"})

    assert msg.is_acted_on
    assert d_msg not in queue.processing

    assert not queue.queue.empty()
    reply_msg = queue.queue.get_nowait()
    assert reply_msg.payload == b"reply"
    assert reply_msg.headers == {"h": "v"}
    assert reply_msg.message_id is not None

    # Second reply ignored
    await msg.reply(payload=b"ignored")
    assert queue.queue.empty()

    # Test reply to different channel with NEW message
    d_msg2 = DummyQueue.Message(payload=b"xyz", reply_to="chan")
    queue.processing.add(d_msg2)
    msg2 = InMemoryReceivedMessage(d_msg2, queue, "chan", queues)

    await msg2.reply(payload=b"reply2", channel="other")

    assert d_msg2 not in queue.processing
    assert not queues["other"].queue.empty()
    reply_msg2 = queues["other"].queue.get_nowait()
    assert reply_msg2.payload == b"reply2"


async def test_received_message_reply_already_acted() -> None:
    queue = DummyQueue()
    queues = {"chan": queue}
    d_msg = DummyQueue.Message(payload=b"abc")
    queue.processing.add(d_msg)
    msg = InMemoryReceivedMessage(d_msg, queue, "chan", queues)
    await msg.ack()
    await msg.reply(payload=b"abc")  # Should return immediately and not enqueue anything
    assert queue.queue.empty()


def test_server_properties() -> None:
    server = InMemoryServer()
    assert server.host == "localhost"
    assert server.protocol == "in-memory"
    assert server.pathname is None
    assert server.title == "In-Memory Server"
    assert server.summary is not None
    assert server.description is not None
    assert server.protocol_version == "1.0.0"
    assert server.variables is None
    assert server.security is None
    assert server.tags is None
    assert server.external_docs is None
    assert server.bindings is None
    assert server.capabilities["supports_native_reply"]
    assert not server.is_connected


async def test_server_connection() -> None:
    server = InMemoryServer()
    assert not server.is_connected
    await server.connect()
    assert server.is_connected
    await server.disconnect()
    assert not server.is_connected

    async with server.connection() as s:
        assert s.is_connected
        assert s is server
    assert not server.is_connected


async def test_server_publish_not_connected() -> None:
    server = InMemoryServer()
    msg = InMemorySentMessage(payload=b"abc")
    with pytest.raises(RuntimeError):
        await server.publish(channel="c", message=msg)


async def test_server_publish() -> None:
    server = InMemoryServer()
    await server.connect()
    msg = InMemorySentMessage(
        payload=b"abc",
        headers={"h": "1"},
        reply_to="reply_chan",
    )

    await server.publish(channel="test_chan", message=msg)

    queue = server.queues["test_chan"]
    received = await queue.queue.get()
    assert received.payload == b"abc"
    assert received.headers == {"h": "1"}
    assert received.reply_to == "reply_chan"
    assert received.message_id is not None  # Generated UUID

    # Test with provided message_id
    await server.publish(
        channel="test_chan",
        message=msg,
        server_specific_parameters={"message_id": "custom-id"},
    )
    received2 = await queue.queue.get()
    assert received2.message_id == "custom-id"


async def test_server_subscribe_not_connected() -> None:
    server = InMemoryServer()
    with pytest.raises(RuntimeError):
        await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={},
        )


async def test_server_subscribe_and_consume() -> None:
    server = InMemoryServer()
    await server.connect()

    received_msgs = []

    async def callback(msg: InMemoryReceivedMessage) -> None:
        received_msgs.append(msg)
        await msg.ack()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
        ),
    )
    await asyncio.sleep(0)
    assert subscriber.is_active

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))

    # allow tasks to run
    for _ in range(10):
        await asyncio.sleep(0.01)

    assert len(received_msgs) == 1
    assert received_msgs[0].payload == b"1"

    await subscriber.close()
    assert not subscriber.is_active


@pytest.mark.parametrize("ack_first", [False, True])
async def test_server_subscribe_callback_exception_releases_semaphore(ack_first: bool) -> None:
    server = InMemoryServer()
    await server.connect()

    error_event = asyncio.Event()

    async def failing_callback(msg: InMemoryReceivedMessage) -> None:
        if ack_first:
            await msg.ack()
        error_event.set()
        raise RuntimeError("boom")

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channel_concurrency={},
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    failing_callback,
                ),
            },
            concurrency=Concurrency(messages=1),
        ),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))
    await asyncio.wait_for(error_event.wait(), timeout=1.0)
    await asyncio.sleep(0.05)  # Let the finally block complete

    error_event.clear()
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"2"))
    await asyncio.wait_for(error_event.wait(), timeout=1.0)

    await subscriber.close()
    assert not server.queues["chan1"].processing


async def test_server_subscribe_concurrency_limit() -> None:
    server = InMemoryServer()
    await server.connect()

    ack_event = asyncio.Event()

    async def callback(msg: InMemoryReceivedMessage) -> None:
        await msg.ack()
        ack_event.set()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channel_concurrency={},
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
            concurrency=Concurrency(messages=2),
        ),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))

    await asyncio.wait_for(ack_event.wait(), timeout=1.0)

    await subscriber.close()


async def test_server_subscribe_no_concurrency_limit() -> None:
    server = InMemoryServer()
    await server.connect()

    async def callback(msg: InMemoryReceivedMessage) -> None:
        await msg.ack()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
        ),
    )

    await subscriber.close()


async def test_subscriber_pause_resume() -> None:
    server = InMemoryServer()
    await server.connect()

    received: asyncio.Queue[bytes] = asyncio.Queue()

    async def callback(msg: ReceivedMessageT) -> None:
        await msg.ack()
        received.put_nowait(msg.payload)

    subscriber = await server.subscribe(
        concurrency=Concurrency(),
        channel_concurrency={},
        channels_to_callbacks={"chan1": callback},
    )
    try:
        # Let the supervisor start and the channel reader wait for a message.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await subscriber.pause()
        for payload in (b"first", b"second"):
            await server.publish(channel="chan1", message=InMemorySentMessage(payload=payload))
        await asyncio.sleep(0)
        assert received.empty()
        assert server.queues["chan1"].queue.qsize() == 2

        await subscriber.resume()
        assert await asyncio.wait_for(received.get(), 1) == b"first"
        assert await asyncio.wait_for(received.get(), 1) == b"second"
    finally:
        await subscriber.close()


async def test_subscriber_close_twice() -> None:
    server = InMemoryServer()
    await server.connect()
    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={},
        ),
    )
    await subscriber.close()
    await subscriber.close()  # Should be fine


async def test_closing_saturated_consumer_leaves_pending_deliveries_in_order() -> None:
    queue = DummyQueue()
    first = DummyQueue.Message(payload=b"first")
    pending = DummyQueue.Message(payload=b"pending")
    later = DummyQueue.Message(payload=b"later")
    queue.queue.put_nowait(first)
    queue.queue.put_nowait(pending)
    queue.queue.put_nowait(later)
    started = asyncio.Event()
    release = asyncio.Event()

    async def callback(msg: ReceivedMessageT) -> None:
        assert msg.payload == first.payload
        started.set()
        await release.wait()
        await msg.ack()

    subscriber = InMemorySubscriber(
        channels_to_callbacks={"chan": callback},
        queues={"chan": queue},
        channel_concurrency={"chan": Concurrency(messages=1)},
    )
    try:
        await asyncio.wait_for(started.wait(), 1)
        await subscriber.close()
        assert queue.queue.get_nowait() is pending
        assert queue.queue.get_nowait() is later
        assert queue.processing == {first}
    finally:
        release.set()
        await asyncio.gather(*subscriber._callback_tasks)
        await subscriber.close()


@pytest.mark.parametrize("cancel_before_start", [True, False])
async def test_cancelled_callback_requeues_message_and_releases_slot(
    cancel_before_start: bool,
) -> None:
    server = InMemoryServer()
    received: asyncio.Queue[ReceivedMessageT] = asyncio.Queue()
    cancelled: ReceivedMessageT | None = None

    async def callback(message: ReceivedMessageT) -> None:
        nonlocal cancelled
        if not cancel_before_start and cancelled is None:
            cancelled = message
            raise asyncio.CancelledError
        await message.ack()
        received.put_nowait(message)

    async with server.connection():
        subscriber = cast(
            InMemorySubscriber,
            await server.subscribe(
                channels_to_callbacks={"jobs": callback},
                concurrency=Concurrency(messages=1),
                channel_concurrency={},
            ),
        )
        try:
            await server.publish(
                channel="jobs",
                message=InMemorySentMessage(payload=b"pending"),
                server_specific_parameters={"message_id": "retry"},
            )
            if cancel_before_start:
                # The supervisor starts the reader, which then schedules the callback.
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                (scheduled,) = subscriber._callback_tasks
                scheduled.cancel()
            redelivery = await asyncio.wait_for(received.get(), 1)
            assert redelivery.action == MessageAction.acked
            assert redelivery.message_id == "retry"
            if not cancel_before_start:
                assert cancelled is not None
                assert cancelled.action == MessageAction.rejected
        finally:
            await subscriber.close()
    assert not server.queues["jobs"].processing


async def test_saturated_consumer_leaves_work_for_a_competing_subscriber() -> None:
    server = InMemoryServer()
    started = asyncio.Event()
    release = asyncio.Event()
    other_received: asyncio.Queue[bytes] = asyncio.Queue()

    async def slow_callback(message: ReceivedMessageT) -> None:
        started.set()
        await release.wait()
        await message.ack()

    async def other_callback(message: ReceivedMessageT) -> None:
        await message.ack()
        other_received.put_nowait(message.payload)

    async with server.connection():
        for payload in (b"first", b"pending"):
            await server.publish(channel="jobs", message=InMemorySentMessage(payload=payload))
        subscriber = cast(
            InMemorySubscriber,
            await server.subscribe(
                channels_to_callbacks={"jobs": slow_callback},
                concurrency=Concurrency(messages=1),
                channel_concurrency={},
            ),
        )
        try:
            await asyncio.wait_for(started.wait(), 1)
            other = await server.subscribe(
                channels_to_callbacks={"jobs": other_callback},
                concurrency=Concurrency(messages=1),
                channel_concurrency={},
            )
            try:
                assert await asyncio.wait_for(other_received.get(), 1) == b"pending"
            finally:
                await other.close()
        finally:
            await subscriber.close()
            release.set()
            await asyncio.gather(*subscriber._callback_tasks)
    assert not server.queues["jobs"].processing


async def test_supervisor_cancellation() -> None:
    # This test tries to hit the except asyncio.CancelledError block in _supervisor
    server = InMemoryServer()
    await server.connect()

    async def callback(msg: InMemoryReceivedMessage) -> None:
        await msg.ack()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
                "chan2": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
        ),
    )

    # We want to cancel the supervisor task manually and see if it cancels children
    task = subscriber.task
    await asyncio.sleep(0)  # Ensure tasks started
    task.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Verify children are cancelled
    for t in subscriber._channel_tasks.values():
        assert t.cancelled()

    # cleanup for clean teardown of subscribers registry in server
    await subscriber.close()
    await asyncio.sleep(0)  # Let done callback run


async def test_batching_yields_to_other_channels_with_a_backlog() -> None:
    server = InMemoryServer()
    first_channel_count = 0
    other_channel_seen: asyncio.Queue[int] = asyncio.Queue()
    message_count = 2 * _SUBMISSION_BATCH_SIZE

    async def first(message: ReceivedMessageT) -> None:
        nonlocal first_channel_count
        first_channel_count += 1
        await message.ack()

    async def other(message: ReceivedMessageT) -> None:
        other_channel_seen.put_nowait(first_channel_count)
        await message.ack()

    async with server.connection():
        message = InMemorySentMessage(payload=b"x")
        for _ in range(message_count):
            await server.publish(channel="first", message=message)
        await server.publish(channel="other", message=message)
        subscriber = await server.subscribe(
            concurrency=Concurrency(),
            channel_concurrency={},
            channels_to_callbacks={"first": first, "other": other},
        )
        try:
            assert await asyncio.wait_for(other_channel_seen.get(), 1) < message_count
        finally:
            await subscriber.close()


async def test_channel_intake_limits_pending_callbacks() -> None:
    server = InMemoryServer()
    callbacks = 0
    saturated = asyncio.Event()
    release = asyncio.Event()

    async def callback(message: ReceivedMessageT) -> None:
        nonlocal callbacks
        callbacks += 1
        if callbacks == 3:
            saturated.set()
        await release.wait()
        await message.ack()

    async with server.connection():
        for _ in range(6):
            await server.publish(channel="images", message=InMemorySentMessage(payload=b"x"))
        subscriber = cast(
            InMemorySubscriber,
            await server.subscribe(
                channels_to_callbacks={"images": callback},
                concurrency=Concurrency(messages=1000),
                channel_concurrency={"images": Concurrency(messages=3)},
            ),
        )
        try:
            await asyncio.wait_for(saturated.wait(), 1)
            assert callbacks == 3
        finally:
            await subscriber.close()
            release.set()
            await asyncio.gather(*subscriber._callback_tasks)
