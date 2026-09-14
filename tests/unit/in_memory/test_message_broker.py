import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from repid.connections import SubscriberDispatcher
from repid.connections.abc import MessageAction, ReceivedMessageT
from repid.connections.in_memory.message_broker import (
    InMemoryReceivedMessage,
    InMemorySentMessage,
    InMemoryServer,
    InMemorySubscriber,
)
from repid.connections.in_memory.utils import DummyQueue
from repid.limits import MessageLimits


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
            channels_to_callbacks={"jobs": callback},
            dispatcher=SubscriberDispatcher(),
        )

        for _ in range(2):
            await server.publish(
                channel="jobs",
                message=InMemorySentMessage(payload=b"same", headers={"key": "value"}),
                server_specific_parameters={"message_id": "same-id"},
            )
        await asyncio.wait_for(completed.wait(), timeout=1)
        await subscriber.stop()
        await subscriber.finish()

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
    assert server.capabilities["supports_pause_per_channel"]
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
        await server.subscribe(channels_to_callbacks={}, dispatcher=SubscriberDispatcher())


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
            dispatcher=SubscriberDispatcher(),
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

    await subscriber.stop()
    await subscriber.finish()
    assert not subscriber.is_active


async def test_server_subscribe_callback_exception_releases_intake() -> None:
    server = InMemoryServer()
    await server.connect()

    error_event = asyncio.Event()
    second_ack_event = asyncio.Event()
    calls = 0

    async def callback(msg: InMemoryReceivedMessage) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            await msg.ack()
            error_event.set()
            raise RuntimeError("boom")
        await msg.ack()
        second_ack_event.set()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    callback,
                ),
            },
            dispatcher=SubscriberDispatcher(MessageLimits(max_messages=1)),
        ),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))
    await asyncio.wait_for(error_event.wait(), timeout=1.0)

    # The first lease must be released despite the exception, so a second
    # message is admitted under the max_messages=1 cap.
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"2"))
    await asyncio.wait_for(second_ack_event.wait(), timeout=1.0)

    await subscriber.stop()
    await subscriber.finish()


async def test_server_subscribe_callback_exception_nacks_and_releases_intake() -> None:
    server = InMemoryServer()
    await server.connect()
    failed = asyncio.Event()
    second_acked = asyncio.Event()
    received: list[InMemoryReceivedMessage] = []

    async def callback(message: InMemoryReceivedMessage) -> None:
        received.append(message)
        if len(received) == 1:
            failed.set()
            raise RuntimeError("boom")
        await message.ack()
        second_acked.set()

    subscriber = await server.subscribe(
        channels_to_callbacks={
            "chan1": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        dispatcher=SubscriberDispatcher(MessageLimits(max_messages=1)),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))
    await asyncio.wait_for(failed.wait(), timeout=1)
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"2"))
    await asyncio.wait_for(second_acked.wait(), timeout=1)

    assert received[0].action == MessageAction.nacked
    assert not server.queues["chan1"].processing
    await subscriber.stop()
    await subscriber.finish()


async def test_server_subscribe_callback_exception_after_ack_does_not_resettle() -> None:
    server = InMemoryServer()
    await server.connect()
    callback_finished = asyncio.Event()
    received: list[InMemoryReceivedMessage] = []

    async def callback(message: InMemoryReceivedMessage) -> None:
        received.append(message)
        await message.ack()
        callback_finished.set()
        raise RuntimeError("boom")

    subscriber = await server.subscribe(
        channels_to_callbacks={
            "chan1": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        dispatcher=SubscriberDispatcher(),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))
    await asyncio.wait_for(callback_finished.wait(), timeout=1)
    await asyncio.sleep(0)

    assert received[0].action == MessageAction.acked
    assert not server.queues["chan1"].processing
    await subscriber.stop()
    await subscriber.finish()


async def test_subscriber_close_requeues_message_waiting_for_intake() -> None:
    server = InMemoryServer()
    await server.connect()
    entered = asyncio.Event()
    finished = asyncio.Event()

    async def callback(msg: InMemoryReceivedMessage) -> None:  # noqa: ARG001
        entered.set()
        try:
            await asyncio.Future()
        finally:
            finished.set()

    subscriber = await server.subscribe(
        channels_to_callbacks={
            "chan1": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        dispatcher=SubscriberDispatcher(MessageLimits(max_messages=1)),
    )
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"first"))
    await asyncio.wait_for(entered.wait(), timeout=1)
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"second"))

    queue = server.queues["chan1"]
    for _ in range(10):
        if len(queue.processing) == 2:
            break
        await asyncio.sleep(0)
    assert len(queue.processing) == 2

    await subscriber.stop()
    await subscriber.finish()
    assert queue.queue.get_nowait().payload == b"second"

    assert finished.is_set()
    assert queue.queue.get_nowait().payload == b"first"
    assert not queue.processing


async def test_server_subscribe_message_limit() -> None:
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
            dispatcher=SubscriberDispatcher(MessageLimits(max_messages=2)),
        ),
    )

    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))

    await asyncio.wait_for(ack_event.wait(), timeout=1.0)

    assert subscriber._dispatcher.native_message_limit("chan1") == 2

    await subscriber.stop()
    await subscriber.finish()


async def test_server_subscribe_no_limits() -> None:
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
            dispatcher=SubscriberDispatcher(),
        ),
    )

    assert subscriber._dispatcher.native_message_limit("chan1") is None

    await subscriber.stop()
    await subscriber.finish()


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
            dispatcher=SubscriberDispatcher(),
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

    await subscriber.stop()
    await subscriber.finish()


async def test_subscriber_close_twice() -> None:
    server = InMemoryServer()
    await server.connect()
    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(channels_to_callbacks={}, dispatcher=SubscriberDispatcher()),
    )
    await subscriber.stop()
    await subscriber.finish()
    await subscriber.stop()  # Should be fine
    await subscriber.finish()  # Should be fine


async def test_finish_cancels_then_drains_retained_cleanup() -> None:
    server = InMemoryServer()
    await server.connect()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    block = asyncio.Event()

    async def callback() -> None:
        try:
            await block.wait()
        except asyncio.CancelledError:
            # Started tasks own their cancellation cleanup.
            cleanup_started.set()
            await allow_cleanup.wait()
            raise

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(channels_to_callbacks={}, dispatcher=SubscriberDispatcher()),
    )
    subscriber._admitted_tasks.start_task(callback)

    await subscriber.stop()

    draining_close = asyncio.create_task(subscriber.finish())
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not draining_close.done()

    allow_cleanup.set()
    await draining_close
    assert subscriber not in server._subscribers


async def test_subscribe_limits_hold_worker_and_channel_budgets() -> None:
    server = InMemoryServer()
    await server.connect()
    entered = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()
    payloads: list[bytes] = []

    async def callback(message: InMemoryReceivedMessage) -> None:
        payloads.append(message.payload)
        entered.set()
        await release.wait()
        await message.ack()
        if len(payloads) == 3:
            completed.set()

    subscriber = await server.subscribe(
        channels_to_callbacks={
            "a": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
            "b": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
        },
        dispatcher=SubscriberDispatcher(
            MessageLimits(max_payload_bytes=3),
            {"a": (MessageLimits(max_messages=1),)},
        ),
    )
    await server.publish(channel="a", message=InMemorySentMessage(payload=b"aa"))
    await server.publish(channel="a", message=InMemorySentMessage(payload=b"c"))
    await server.publish(channel="b", message=InMemorySentMessage(payload=b"bb"))

    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.sleep(0)
    assert payloads == [b"aa"]

    release.set()
    await asyncio.wait_for(completed.wait(), timeout=1)
    await subscriber.stop()
    await subscriber.finish()


async def test_subscriber_pause_channel_keeps_other_channels_active() -> None:
    server = InMemoryServer()
    await server.connect()
    received: list[str] = []

    async def callback(message: InMemoryReceivedMessage) -> None:
        received.append(message.channel)
        await message.ack()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channels_to_callbacks={
                "a": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
                "b": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
            },
            dispatcher=SubscriberDispatcher(),
        ),
    )
    await subscriber.pause_channel("a")
    await server.publish(channel="a", message=InMemorySentMessage(payload=b"a"))
    await server.publish(channel="b", message=InMemorySentMessage(payload=b"b"))

    for _ in range(10):
        await asyncio.sleep(0.01)
    assert received == ["b"]

    await subscriber.resume_channel("a")
    for _ in range(10):
        await asyncio.sleep(0.01)
    assert received == ["b", "a"]
    await subscriber.stop()
    await subscriber.finish()


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
            dispatcher=SubscriberDispatcher(),
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
    await subscriber.stop()
    await subscriber.finish()
    await asyncio.sleep(0)  # Let done callback run


async def test_server_disconnect_closes_subscribers_and_requeues_inflight_messages() -> None:
    server = InMemoryServer()
    finished: list[str] = []
    calls: list[str] = []
    callbacks_started = asyncio.Event()

    async def callback(message: InMemoryReceivedMessage) -> None:
        calls.append(message.channel)
        if len(calls) == 2:
            callbacks_started.set()
        try:
            await asyncio.Future()
        finally:
            finished.append(message.channel)

    async with server.connection():
        subscribers = [
            await server.subscribe(
                channels_to_callbacks={
                    channel: cast(
                        Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                        callback,
                    ),
                },
                dispatcher=SubscriberDispatcher(),
            )
            for channel in ("first", "second")
        ]
        for channel in ("first", "second"):
            await server.publish(
                channel=channel,
                message=InMemorySentMessage(payload=channel.encode()),
            )
        await asyncio.wait_for(callbacks_started.wait(), timeout=1)
        assert calls == ["first", "second"]

        await server.disconnect()

        assert not server._subscribers
        assert set(finished) == {"first", "second"}
        assert all(subscriber.task.done() for subscriber in subscribers)
        assert all(not queue.processing for queue in server.queues.values())
        assert {
            server.queues[channel].queue.get_nowait().payload for channel in ("first", "second")
        } == {b"first", b"second"}

        await server.connect()
        await server.publish(channel="first", message=InMemorySentMessage(payload=b"new"))
        await asyncio.sleep(0)
        assert calls == ["first", "second"]


async def test_subscriber_requeues_message_when_dynamic_oversized_policy_fails() -> None:
    server = InMemoryServer()
    await server.connect()

    async def invalid_policy(_: ReceivedMessageT) -> str:
        return "reject"

    subscriber = await server.subscribe(
        channels_to_callbacks={
            "orders": cast(
                Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                AsyncMock(),
            ),
        },
        dispatcher=SubscriberDispatcher(
            MessageLimits(
                max_payload_bytes=1,
                on_oversized_payload=cast(Any, invalid_policy),
            ),
        ),
    )
    await server.publish(channel="orders", message=InMemorySentMessage(payload=b"oversized"))

    with pytest.raises(TypeError, match="synchronous"):
        await subscriber.task

    queue = server.queues["orders"]
    assert not queue.processing
    assert queue.queue.get_nowait().payload == b"oversized"
    await subscriber.stop()
    await subscriber.finish()
    assert subscriber not in server._subscribers


async def test_server_disconnect_closes_admitted_work_after_supervisor_failure() -> None:
    server = InMemoryServer()
    callback_started = asyncio.Event()
    callback_finished = asyncio.Event()

    async def callback(_: InMemoryReceivedMessage) -> None:
        callback_started.set()
        try:
            await asyncio.Future()
        finally:
            callback_finished.set()

    async def invalid_policy(_: ReceivedMessageT) -> str:
        return "reject"

    async with server.connection():
        subscriber = await server.subscribe(
            channels_to_callbacks={
                "a": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
                "b": cast(Callable[[ReceivedMessageT], Coroutine[None, None, None]], callback),
            },
            dispatcher=SubscriberDispatcher(
                MessageLimits(
                    max_payload_bytes=1,
                    on_oversized_payload=cast(Any, invalid_policy),
                ),
            ),
        )
        await server.publish(channel="a", message=InMemorySentMessage(payload=b"a"))
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        await server.publish(channel="b", message=InMemorySentMessage(payload=b"oversized"))

        with pytest.raises(TypeError, match="synchronous"):
            await subscriber.task

        assert subscriber in server._subscribers
        assert not callback_finished.is_set()
        await server.disconnect()

        assert callback_finished.is_set()
        assert not server._subscribers


async def test_subscriber_reserve_error_logs_requeue_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailOnRemoveSet(set):
        def remove(self, item: Any) -> None:
            raise KeyError(item)

    server = InMemoryServer()
    await server.connect()
    server.queues["chan1"] = DummyQueue(processing=FailOnRemoveSet())

    dispatcher = SubscriberDispatcher()
    dispatcher.reserve = AsyncMock(side_effect=RuntimeError("reserve failed"))  # type: ignore[method-assign]

    subscriber = await server.subscribe(
        channels_to_callbacks={
            "chan1": cast(
                Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                AsyncMock(),
            ),
        },
        dispatcher=dispatcher,
    )
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"1"))

    with pytest.raises(RuntimeError, match="reserve failed"):
        await subscriber.task

    assert ("message.reject.error", logging.ERROR) in [
        (record.message, record.levelno) for record in caplog.records
    ]
    await subscriber.stop()
    await subscriber.finish()


async def test_server_disconnect_logs_subscriber_close_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = InMemoryServer()
    await server.connect()

    subscriber = cast(
        InMemorySubscriber,
        await server.subscribe(
            channels_to_callbacks={
                "chan1": cast(
                    Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                    AsyncMock(),
                ),
            },
            dispatcher=SubscriberDispatcher(),
        ),
    )
    subscriber.finish = AsyncMock(side_effect=RuntimeError("close failed"))  # type: ignore[method-assign]

    with caplog.at_level(logging.ERROR, logger="repid.connections.in_memory"):
        await server.disconnect()

    assert ("subscriber.close.error", logging.ERROR) in [
        (record.message, record.levelno) for record in caplog.records
    ]


async def test_subscriber_requeues_oversized_message_and_continues() -> None:
    server = InMemoryServer()
    await server.connect()

    async def callback(_: ReceivedMessageT) -> None:
        raise AssertionError("oversized message must not reach the callback")

    subscriber = await server.subscribe(
        channels_to_callbacks={
            "chan1": cast(
                Callable[[ReceivedMessageT], Coroutine[None, None, None]],
                callback,
            ),
        },
        dispatcher=SubscriberDispatcher(
            MessageLimits(
                max_payload_bytes=1,
                on_oversized_payload=cast(Any, lambda _: "reject"),
            ),
        ),
    )
    await server.publish(channel="chan1", message=InMemorySentMessage(payload=b"oversized"))

    requeued = asyncio.Event()

    async def poll() -> None:
        queue = server.queues["chan1"]
        while queue.queue.empty():
            await asyncio.sleep(0)
        requeued.set()

    poll_task = asyncio.create_task(poll())
    await asyncio.wait_for(requeued.wait(), timeout=1)
    poll_task.cancel()

    assert not server.queues["chan1"].processing
    assert server.queues["chan1"].queue.get_nowait().payload == b"oversized"
    await subscriber.stop()
    await subscriber.finish()
    assert subscriber not in server._subscribers
