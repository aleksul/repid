import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import cast

import pytest

from repid.connections.abc import MessageAction, ReceivedMessageT
from repid.connections.in_memory.message_broker import (
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
        await server.subscribe(channels_to_callbacks={})


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


async def test_server_subscribe_callback_exception_releases_semaphore() -> None:
    server = InMemoryServer()
    await server.connect()

    error_event = asyncio.Event()

    async def failing_callback(msg: InMemoryReceivedMessage) -> None:
        await msg.ack()
        error_event.set()
        raise RuntimeError("boom")

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    failing_callback,
                ),
            },
            concurrency_limit=2,
        ),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))
    await asyncio.wait_for(error_event.wait(), timeout=1.0)
    await asyncio.sleep(0.05)  # Let the finally block complete

    # Semaphore should have been released despite the exception
    assert subscriber._semaphore is not None
    assert subscriber._semaphore._value == 2  # Back to full capacity

    await subscriber.close()


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
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
            concurrency_limit=2,
        ),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))

    await asyncio.wait_for(ack_event.wait(), timeout=1.0)

    assert subscriber._semaphore is not None  # Implementation detail check

    await subscriber.close()


async def test_server_subscribe_no_concurrency_limit() -> None:
    server = InMemoryServer()
    await server.connect()

    async def callback(msg: InMemoryReceivedMessage) -> None:
        await msg.ack()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
            concurrency_limit=0,
        ),
    )

    assert subscriber._semaphore is None

    await subscriber.close()


async def test_subscriber_pause_resume() -> None:
    server = InMemoryServer()
    await server.connect()

    received_count = 0

    async def callback(msg: InMemoryReceivedMessage) -> None:
        nonlocal received_count
        received_count += 1
        await msg.ack()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
        ),
    )

    await subscriber.pause()
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))

    await asyncio.sleep(0.05)
    assert received_count == 0

    await subscriber.resume()
    for _ in range(10):
        await asyncio.sleep(0.01)
    assert received_count == 1

    await subscriber.close()


async def test_subscriber_close_twice() -> None:
    server = InMemoryServer()
    await server.connect()
    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(channels_to_callbacks={}),
    )
    await subscriber.close()
    await subscriber.close()  # Should be fine


async def test_supervisor_cancellation() -> None:
    # This test tries to hit the except asyncio.CancelledError block in _supervisor
    server = InMemoryServer()
    await server.connect()

    async def callback(msg: InMemoryReceivedMessage) -> None:
        await msg.ack()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
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
