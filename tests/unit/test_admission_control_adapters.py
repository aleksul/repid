from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiokafka.structs import TopicPartition
from nats.errors import BadSubscriptionError
from nats.js.api import ConsumerConfig
from nats.js.errors import NotFoundError

from repid.admission import MessageLimits
from repid.connections import SubscriberDispatcher
from repid.connections._subscriber import AdmittedTaskTracker
from repid.connections.abc import MessageAction
from repid.connections.in_memory import InMemoryServer
from repid.connections.kafka.message import KafkaReceivedMessage
from repid.connections.kafka.subscriber import KafkaSubscriber
from repid.connections.nats.message_broker import NatsReceivedMessage, NatsSubscriber
from repid.connections.sqs.subscriber import SqsSubscriber


async def test_admitted_task_tracker_returns_without_draining_blocked_prestart_cleanup() -> None:
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    first = Mock(channel="jobs", payload=b"first", keep_alive_interval=None)
    lease = await dispatcher.reserve(first)
    assert lease is not None
    tracker = AdmittedTaskTracker()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    callback = AsyncMock()

    async def on_cancel() -> None:
        cleanup_started.set()
        await release_cleanup.wait()

    task = tracker.start(dispatcher, lease, first, callback, on_cancel=on_cancel)

    await tracker.cancel_and_drain(drain=False)
    await cleanup_started.wait()

    second = asyncio.create_task(
        dispatcher.reserve(Mock(channel="jobs", payload=b"second", keep_alive_interval=None)),
    )
    await asyncio.sleep(0)
    assert not second.done()

    release_cleanup.set()
    second_lease = await asyncio.wait_for(second, timeout=1)
    assert second_lease is not None
    await second_lease.release()
    assert task.cancelled()
    callback.assert_not_awaited()


async def test_admitted_task_tracker_drain_does_not_recancel_started_cleanup() -> None:
    tracker = AdmittedTaskTracker()
    callback_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()

    async def run() -> None:
        callback_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cleanup_started.set()
            await allow_cleanup.wait()
            raise

    task = tracker.start_task(run)
    await callback_started.wait()
    await tracker.cancel_and_drain(drain=False)
    await cleanup_started.wait()

    draining = asyncio.create_task(tracker.cancel_and_drain())
    await asyncio.sleep(0)
    assert not draining.done()
    assert not task.done()

    allow_cleanup.set()
    await asyncio.wait_for(draining, timeout=1)
    assert task.cancelled()


async def test_admitted_task_tracker_drain_removes_completed_external_task() -> None:
    tracker = AdmittedTaskTracker()
    completed = asyncio.create_task(asyncio.sleep(0))
    await completed
    tracker.tasks.add(completed)

    await asyncio.wait_for(tracker.drain(), timeout=1)

    assert not tracker.tasks


async def test_admitted_task_tracker_cancels_tasks_started_during_shutdown() -> None:
    tracker = AdmittedTaskTracker()
    await tracker.cancel_and_drain(drain=False)
    cleanup = AsyncMock()
    run = AsyncMock()

    task = tracker.start_task(run, on_cancel=cleanup)
    await tracker.drain()

    assert task.cancelled()
    run.assert_not_awaited()
    cleanup.assert_awaited_once()


async def test_admitted_task_tracker_logs_started_task_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tracker = AdmittedTaskTracker()
    failed = asyncio.Event()

    async def fail() -> None:
        failed.set()
        raise RuntimeError("delivery failed")

    task = tracker.start_task(fail)
    await failed.wait()
    await asyncio.sleep(0)

    assert task.done()
    error = next(
        record for record in caplog.records if record.message == "subscriber.delivery.error"
    )
    assert error.exc_info is not None
    assert str(error.exc_info[1]) == "delivery failed"


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
                    MessageLimits(max_payload_bytes=1, on_oversized_payload="nack"),
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
        await subscriber.stop()
        await subscriber.finish()

    assert not called


async def test_in_memory_oversized_reject_can_be_closed() -> None:
    server = InMemoryServer()

    async with server.connection():
        subscriber = await server.subscribe(
            channels_to_callbacks={"jobs": AsyncMock()},
            dispatcher=SubscriberDispatcher(
                MessageLimits(max_payload_bytes=1, on_oversized_payload="reject"),
            ),
        )
        await server.publish(
            channel="jobs",
            message=Mock(payload=b"{}", headers=None, reply_to=None, content_type=None),
        )
        await asyncio.sleep(0)
        await subscriber.stop()
        await asyncio.wait_for(subscriber.finish(), timeout=1)


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

    subscriber = SqsSubscriber(server, {"jobs": callback}, dispatcher=SubscriberDispatcher())

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
    await subscriber.stop()
    await subscriber.finish()


def _nats_server_with_jsm(jsm: Any, *, consumer_paused: bool = False) -> Mock:
    """A NatsServer mock whose JetStream context subscribes and manages consumers."""
    subscription = Mock(
        unsubscribe=AsyncMock(),
        consumer_info=AsyncMock(
            return_value=Mock(
                config=Mock(ack_wait=None),
                stream="stream",
                paused=consumer_paused,
            ),
        ),
    )
    return Mock(_js=Mock(subscribe=AsyncMock(return_value=subscription)), _jsm=jsm)


async def test_nats_subscribe_clears_stale_consumer_pause() -> None:
    class JetStreamManager:
        def __init__(self) -> None:
            self.resumed: list[str] = []

        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def pause_consumer(self, *args: Any) -> None:  # noqa: ARG002
            return None

        async def resume_consumer(self, stream: str, consumer: str) -> None:  # noqa: ARG002
            self.resumed.append(consumer)

    jsm = JetStreamManager()
    subscriber = NatsSubscriber(
        _nats_server_with_jsm(jsm, consumer_paused=True),
        {},
        dispatcher=SubscriberDispatcher(),
    )

    try:
        await subscriber._subscribe_channel("jobs", AsyncMock())
        assert jsm.resumed == ["jobs_group"]
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_serializes_consumer_pause_and_resume() -> None:
    pause_started = asyncio.Event()
    allow_pause = asyncio.Event()

    class JetStreamManager:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def pause_consumer(self, stream: str, consumer: str, pause_until: str) -> None:  # noqa: ARG002
            self.calls.append(("pause", consumer))
            pause_started.set()
            await allow_pause.wait()

        async def resume_consumer(self, stream: str, consumer: str) -> None:  # noqa: ARG002
            self.calls.append(("resume", consumer))

    server = _nats_server_with_jsm(JetStreamManager())
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()}, dispatcher=SubscriberDispatcher())

    while not subscriber.is_active:
        await asyncio.sleep(0)

    pausing = asyncio.create_task(subscriber.pause())
    await pause_started.wait()
    resuming = asyncio.create_task(subscriber.resume())
    await asyncio.sleep(0)
    assert not resuming.done()

    allow_pause.set()
    await pausing
    await resuming

    assert subscriber.is_active
    assert set(subscriber._subs) == {"jobs"}
    await subscriber.stop()
    await subscriber.finish()


async def test_nats_recovers_from_partial_consumer_pause_failure() -> None:
    class JetStreamManager:
        def __init__(self) -> None:
            self.calls = 0

        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def pause_consumer(self, stream: str, consumer: str, pause_until: str) -> None:  # noqa: ARG002
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("pause failed")

        async def resume_consumer(self, stream: str, consumer: str) -> None:  # noqa: ARG002
            return None

    class Subscription:
        async def unsubscribe(self) -> None:
            return None

    server = _nats_server_with_jsm(JetStreamManager())
    subscriber = NatsSubscriber(
        server,
        {"first": AsyncMock(), "second": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    while not subscriber.is_active:
        await asyncio.sleep(0)

    with pytest.raises(ConnectionError, match="consumer pause requires nats-server"):
        await subscriber.pause()

    assert not subscriber.is_active
    assert set(subscriber._subs) == {"first", "second"}
    await subscriber.resume()
    assert subscriber.is_active
    assert set(subscriber._subs) == {"first", "second"}
    await subscriber.stop()
    await subscriber.finish()


async def test_nats_close_retries_failed_subscriptions_and_continues_shutdown() -> None:
    class Subscription:
        def __init__(self, *, fail_once: bool) -> None:
            self.fail_once = fail_once
            self.unsubscribe_calls = 0

        async def unsubscribe(self) -> None:
            self.unsubscribe_calls += 1
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("unsubscribe failed")

    subscriber = NatsSubscriber(Mock(), {}, dispatcher=SubscriberDispatcher())

    await asyncio.sleep(0)
    first = Subscription(fail_once=True)
    second = Subscription(fail_once=False)
    subscriber._subs = cast(Any, {"first": first, "second": second})
    subscriber._active = True

    async def full_close() -> None:
        await subscriber.stop()
        await subscriber.finish()

    try:
        with pytest.raises(RuntimeError, match="unsubscribe failed"):
            await full_close()

        assert first.unsubscribe_calls == 1
        assert second.unsubscribe_calls == 1
        assert subscriber._subs == {"first": first}

        await subscriber.stop()
        await subscriber.finish()

        assert first.unsubscribe_calls == 2
        assert second.unsubscribe_calls == 1
        assert subscriber._subs == {}
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_pause_logs_secondary_consumer_pause_failures(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[str] = []

    class JetStreamManager:
        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def pause_consumer(self, stream: str, consumer: str, pause_until: str) -> None:  # noqa: ARG002
            channel = consumer.removesuffix("_group")
            calls.append(channel)
            if channel in ("first", "second"):
                raise RuntimeError(f"{channel} failed")

    class Subscription:
        async def unsubscribe(self) -> None:
            return None

    server = _nats_server_with_jsm(JetStreamManager())
    subscriber = NatsSubscriber(
        server,
        {"first": AsyncMock(), "second": AsyncMock(), "tail": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )
    while not subscriber.is_active:
        await asyncio.sleep(0)
    try:
        with pytest.raises(ConnectionError, match="Consumer pause request failed"):
            await subscriber.pause()

        assert calls == ["first", "second", "tail"]
        secondary = next(
            record for record in caplog.records if record.getMessage() == "subscriber.pause.error"
        )
        assert secondary.__dict__["channel"] == "second"
        assert secondary.exc_info is not None
        assert secondary.exc_info[1] is not None
        assert str(secondary.exc_info[1].__cause__) == "second failed"
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_startup_failure_unsubscribes_created_subscriptions() -> None:
    created: list[Mock] = []

    class JetStream:
        async def subscribe(self, channel: str, **kwargs: Any) -> Mock:  # noqa: ARG002
            if channel == "second":
                raise RuntimeError("subscribe failed")
            sub = Mock(unsubscribe=AsyncMock(), consumer_info=AsyncMock())
            created.append(sub)
            return sub

    subscriber = NatsSubscriber(
        Mock(_js=JetStream(), _jsm=JetStream()),
        {"first": AsyncMock(), "second": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    with pytest.raises(RuntimeError, match="subscribe failed"):
        await subscriber.task

    assert len(created) == 1
    created[0].unsubscribe.assert_awaited_once()


@pytest.mark.parametrize(
    ("startup_phase", "mode"),
    [
        pytest.param("consumer_info", "stop", id="consumer_info_stop"),
        pytest.param("consumer_info", "finish", id="consumer_info_finish"),
        pytest.param("subscribe", "stop", id="subscribe_stop"),
        pytest.param("subscribe", "finish", id="subscribe_finish"),
    ],
)
async def test_nats_stop_cancels_blocked_startup_before_unsubscribing(
    startup_phase: str,
    mode: str,
) -> None:
    startup_blocked = asyncio.Event()
    startup_cancelled = asyncio.Event()
    release_startup = asyncio.Event()
    subscription = Mock(unsubscribe=AsyncMock())

    class JetStream:
        async def subscribe(self, channel: str, **kwargs: Any) -> Mock:  # noqa: ARG002
            if startup_phase == "subscribe":
                startup_blocked.set()
                try:
                    await release_startup.wait()
                except asyncio.CancelledError:
                    startup_cancelled.set()
                    raise
            return subscription

    async def consumer_info() -> Mock:
        if startup_phase == "consumer_info":
            startup_blocked.set()
            try:
                await release_startup.wait()
            except asyncio.CancelledError:
                startup_cancelled.set()
                raise
        return Mock(config=Mock(ack_wait=None))

    subscription.consumer_info = AsyncMock(side_effect=consumer_info)
    subscriber = NatsSubscriber(
        Mock(_js=JetStream(), _jsm=JetStream()),
        {"jobs": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    await asyncio.wait_for(startup_blocked.wait(), timeout=1)
    await asyncio.wait_for(subscriber.stop(), timeout=1)
    if mode == "finish":
        await asyncio.wait_for(subscriber.finish(), timeout=1)

    assert startup_cancelled.is_set()
    if mode == "finish" and startup_phase == "consumer_info":
        subscription.unsubscribe.assert_awaited_once()


async def test_kafka_global_pause_and_resume() -> None:
    consumer = Mock()
    consumer.assignment.return_value = ()

    async def getmany(**kwargs: Any) -> dict[object, object]:  # noqa: ARG001
        await asyncio.Future()
        return {}

    consumer.getmany = AsyncMock(side_effect=getmany)
    consumer.stop = AsyncMock()
    subscriber = KafkaSubscriber(Mock(), consumer, {}, dispatcher=SubscriberDispatcher())

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
    await subscriber.stop()
    await subscriber.finish()


async def test_kafka_channel_pause_keeps_other_topics_flowing() -> None:
    consumer = Mock()
    consumer.assignment.return_value = (
        TopicPartition("jobs", 0),
        TopicPartition("reports", 0),
    )

    async def getmany(**kwargs: Any) -> dict[object, object]:  # noqa: ARG001
        await asyncio.Future()
        return {}

    consumer.getmany = AsyncMock(side_effect=getmany)
    consumer.stop = AsyncMock()
    subscriber = KafkaSubscriber(Mock(), consumer, {}, dispatcher=SubscriberDispatcher())

    await subscriber.pause_channel("jobs")
    consumer.pause.assert_called_once_with(TopicPartition("jobs", 0))

    # A global resume must not un-pause a channel-paused topic.
    await subscriber.pause()
    await subscriber.resume()
    consumer.resume.assert_called_once_with(TopicPartition("reports", 0))

    await subscriber.resume_channel("jobs")
    consumer.resume.assert_called_with(TopicPartition("jobs", 0))
    await subscriber.stop()
    await subscriber.finish()
    first_commit_started = asyncio.Event()
    second_completion_waiting = asyncio.Event()
    release_first_commit = asyncio.Event()
    committed_offsets: list[int] = []

    class CompletionLock:
        def __init__(self) -> None:
            self._lock = asyncio.Lock()

        async def __aenter__(self) -> None:
            if self._lock.locked():
                second_completion_waiting.set()
            await self._lock.acquire()

        async def __aexit__(self, *_: object) -> None:
            self._lock.release()

    async def commit(offsets: dict[TopicPartition, Any]) -> None:
        offset = next(iter(offsets.values())).offset
        committed_offsets.append(offset)
        if offset == 1:
            first_commit_started.set()
            await release_first_commit.wait()

    consumer = Mock(
        assignment=Mock(return_value=()),
        commit=AsyncMock(side_effect=commit),
        stop=AsyncMock(),
    )
    subscriber = KafkaSubscriber(Mock(), consumer, {}, dispatcher=SubscriberDispatcher())

    tp = TopicPartition("jobs", 0)
    subscriber._offset_tracker[tp] = {0: False, 1: False}
    subscriber._completion_locks[tp] = CompletionLock()  # type: ignore[assignment]
    try:
        await subscriber.pause()
        first = asyncio.create_task(subscriber._mark_complete(Mock(offset=0), tp))
        await first_commit_started.wait()
        second = asyncio.create_task(subscriber._mark_complete(Mock(offset=1), tp))
        await second_completion_waiting.wait()

        assert committed_offsets == [1]
        release_first_commit.set()
        await asyncio.gather(first, second)

        assert committed_offsets == [1, 2]
        assert subscriber._offset_tracker[tp] == {}
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_kafka_received_message_ack_retry_does_not_rewind_committed_offset() -> None:
    committed_offsets: list[int] = []

    async def commit(offsets: dict[TopicPartition, Any]) -> None:
        committed_offsets.append(next(iter(offsets.values())).offset)
        if len(committed_offsets) == 1:
            raise ConnectionError("commit failed")

    consumer = Mock(
        assignment=Mock(return_value=()),
        commit=AsyncMock(side_effect=commit),
        stop=AsyncMock(),
    )
    subscriber = KafkaSubscriber(Mock(), consumer, {}, dispatcher=SubscriberDispatcher())

    tp = TopicPartition("jobs", 0)
    first = Mock(topic="jobs", partition=0, offset=0, value=b"", headers=[])
    second = Mock(topic="jobs", partition=0, offset=1, value=b"", headers=[])
    subscriber._offset_tracker[tp] = {0: False, 1: False}
    try:
        await subscriber.pause()
        first_message = KafkaReceivedMessage(
            Mock(),
            first,
            lambda record: subscriber._mark_complete(record, tp),
        )
        second_message = KafkaReceivedMessage(
            Mock(),
            second,
            lambda record: subscriber._mark_complete(record, tp),
        )

        with pytest.raises(ConnectionError, match="commit failed"):
            await first_message.ack()
        await second_message.ack()
        await first_message.ack()

        assert committed_offsets == [1, 2]
        assert subscriber._offset_tracker[tp] == {}
    finally:
        await subscriber.stop()
        await subscriber.finish()


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param("ack", id="ack"),
        pytest.param("nack", id="nack"),
        pytest.param("reject", id="reject"),
    ],
)
async def test_kafka_received_message_cancellation_during_completion_stays_reserved(
    operation: str,
) -> None:
    completion_started = asyncio.Event()
    completion_calls = 0

    async def mark_complete(_: Any) -> None:
        nonlocal completion_calls
        completion_calls += 1
        if completion_calls == 1:
            completion_started.set()
            await asyncio.Future()

    producer = Mock(send_and_wait=AsyncMock())
    server = Mock(
        _producer=producer,
        _dlq_topic_strategy=lambda _: "dlq" if operation == "nack" else None,
        _reject_topic_strategy=lambda _: "reject" if operation == "reject" else None,
    )
    record = Mock(topic="jobs", partition=0, offset=1, value=b"", headers=[])
    message = KafkaReceivedMessage(server, record, mark_complete)
    task = asyncio.create_task(getattr(message, operation)())

    await asyncio.wait_for(completion_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Cancellation may have left the RPC in flight, so the settlement stays
    # reserved and a retry must not issue a second broker operation.
    assert message.is_acted_on
    await getattr(message, operation)()
    assert message.is_acted_on
    assert completion_calls == 1


@pytest.mark.parametrize("operation", [pytest.param("nack"), pytest.param("reject")])
async def test_kafka_received_message_publish_failure_allows_retry(operation: str) -> None:
    producer = Mock(send_and_wait=AsyncMock(side_effect=[ConnectionError("failed"), None]))
    server = Mock(
        _producer=producer,
        _dlq_topic_strategy=lambda _: "dlq" if operation == "nack" else None,
        _reject_topic_strategy=lambda _: "reject" if operation == "reject" else None,
    )
    record = Mock(topic="jobs", partition=0, offset=1, value=b"", headers=[])
    mark_complete = AsyncMock()
    message = KafkaReceivedMessage(server, record, mark_complete)

    with pytest.raises(ConnectionError, match="failed"):
        await getattr(message, operation)()

    assert not message.is_acted_on
    await getattr(message, operation)()
    assert message.is_acted_on
    mark_complete.assert_awaited_once_with(record)


async def test_kafka_completion_retries_failed_commit_without_skipping_incomplete_offsets() -> None:
    committed_offsets: list[int] = []

    async def commit(offsets: dict[TopicPartition, Any]) -> None:
        committed_offsets.append(next(iter(offsets.values())).offset)
        if len(committed_offsets) == 1:
            raise ConnectionError("commit failed")

    consumer = Mock(
        assignment=Mock(return_value=()),
        commit=AsyncMock(side_effect=commit),
        stop=AsyncMock(),
    )
    subscriber = KafkaSubscriber(Mock(), consumer, {}, dispatcher=SubscriberDispatcher())

    tp = TopicPartition("jobs", 0)
    subscriber._offset_tracker[tp] = {0: False, 1: False}
    try:
        await subscriber.pause()
        await subscriber._mark_complete(Mock(offset=1), tp)
        assert committed_offsets == []

        with pytest.raises(ConnectionError, match="commit failed"):
            await subscriber._mark_complete(Mock(offset=0), tp)
        assert subscriber._offset_tracker[tp] == {0: True, 1: True}

        await subscriber._mark_complete(Mock(offset=0), tp)

        assert committed_offsets == [2, 2]
        assert subscriber._offset_tracker[tp] == {}
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_kafka_stop_returns_promptly_with_uncancellable_callback() -> None:
    consumer = Mock(assignment=Mock(return_value=()), stop=AsyncMock())

    async def getmany(**kwargs: Any) -> dict[object, object]:  # noqa: ARG001
        await asyncio.Future()
        return {}

    async def ignores_cancellation() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await asyncio.Future()

    consumer.getmany = AsyncMock(side_effect=getmany)
    subscriber = KafkaSubscriber(Mock(), consumer, {}, dispatcher=SubscriberDispatcher())

    callback_task = asyncio.create_task(ignores_cancellation())
    subscriber._admitted_tasks.tasks.add(callback_task)
    try:
        await asyncio.sleep(0)
        # stop() only cancels intake; an uncancellable callback cannot block it,
        # and consumer shutdown is deferred to finish().
        await asyncio.wait_for(subscriber.stop(), timeout=1)
        consumer.stop.assert_not_awaited()
        assert not callback_task.cancelled()
        assert callback_task in subscriber._admitted_tasks.tasks
    finally:
        # It ignores the first cancellation by design.
        callback_task.cancel()
        await asyncio.sleep(0)
        callback_task.cancel()
        await asyncio.gather(callback_task, return_exceptions=True)


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
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_subscriber_closes_before_start_and_preserves_unsubscribe_error() -> None:
    server = Mock(_js=Mock())
    closed = NatsSubscriber(server, {}, dispatcher=SubscriberDispatcher())

    closed._closed = True
    await closed.task

    failing = NatsSubscriber(server, {}, dispatcher=SubscriberDispatcher())

    await asyncio.sleep(0)

    async def full_close() -> None:
        await failing.stop()
        await failing.finish()

    with (
        patch.object(failing, "_unsubscribe_all", side_effect=RuntimeError("unsubscribe failed")),
        pytest.raises(RuntimeError, match="unsubscribe failed"),
    ):
        await full_close()


async def test_nats_stop_swallows_monitor_failure(caplog: pytest.LogCaptureFixture) -> None:
    """A monitor failing while `stop()` awaits it is logged, not raised."""
    server = Mock(_js=Mock())
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()}, dispatcher=SubscriberDispatcher())

    monitor_started = asyncio.Event()

    async def failing_subscribe(channel: str, callback: Any) -> None:  # noqa: ARG001
        monitor_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError as exc:
            # Startup teardown turns the requested cancellation into a failure.
            raise RuntimeError("monitor exploded") from exc

    subscriber._subscribe_channel = failing_subscribe  # type: ignore[method-assign]

    await monitor_started.wait()
    await subscriber.stop()

    error = next(
        record for record in caplog.records if record.message == "subscriber.stop.monitor.error"
    )
    assert error.exc_info is not None
    assert str(error.exc_info[1]) == "monitor exploded"
    assert subscriber._cleanup_ready.is_set()


async def test_nats_stop_reraises_when_stop_itself_is_cancelled() -> None:
    """`stop()` must not swallow its own cancellation as the monitor's."""
    server = Mock(_js=Mock())

    async def suppressing_start(self: NatsSubscriber) -> None:  # noqa: ARG001
        # Park the monitor until stop() cancels it, then swallow the
        # cancellation and complete normally.
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.Future()

    with patch.object(NatsSubscriber, "_start", suppressing_start):
        subscriber = NatsSubscriber(server, {}, dispatcher=SubscriberDispatcher())

        stop_holder: list[asyncio.Task[None]] = []

        def cancel_stopper(_: asyncio.Task[Any]) -> None:
            # Runs before stop()'s own wakeup callback (done callbacks are FIFO
            # and this one was registered first): by then the monitor is already
            # done, so the cancellation lands on the stop() caller itself.
            for task in stop_holder:
                task.cancel()

        subscriber._task.add_done_callback(cancel_stopper)
        stop_task = asyncio.create_task(subscriber.stop())
        stop_holder.append(stop_task)

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stop_task, timeout=1)

    # The monitor itself was never cancelled: it suppressed the request and
    # completed normally, so the error stop() saw was its own cancellation.
    assert not subscriber._task.cancelled()
    assert not subscriber._cleanup_ready.is_set()

    # A later stop() still completes the cleanup hand-off.
    await subscriber.stop()
    assert subscriber._cleanup_ready.is_set()


async def test_nats_waits_for_intake_before_detaching_callback_task() -> None:
    first_callback_started = asyncio.Event()
    release_first_callback = asyncio.Event()
    second_reservation_started = asyncio.Event()
    first_lease = AsyncMock()
    reservations = 0

    class JetStream:
        def __init__(self) -> None:
            self.callback: Callable[[Any], Awaitable[None]] | None = None

        async def consumer_info(self, channel: str, group: str) -> Mock:  # noqa: ARG002
            return Mock(config=Mock(ack_wait=None))

        async def subscribe(self, channel: str, **kwargs: Any) -> Mock:  # noqa: ARG002
            self.callback = kwargs["cb"]
            return Mock(unsubscribe=AsyncMock())

    async def reserve(message: object) -> AsyncMock:  # noqa: ARG001
        nonlocal reservations
        reservations += 1
        if reservations == 1:
            return first_lease
        second_reservation_started.set()
        await asyncio.Future()
        return AsyncMock()

    async def callback(message: object) -> None:  # noqa: ARG001
        first_callback_started.set()
        await release_first_callback.wait()

    server = Mock(_js=JetStream(), _jsm=JetStream())
    dispatcher = SubscriberDispatcher()
    dispatcher.reserve = AsyncMock(side_effect=reserve)  # type: ignore[method-assign]
    subscriber = NatsSubscriber(server, {"jobs": callback}, dispatcher=dispatcher)
    await asyncio.sleep(0)

    assert server._js.callback is not None
    first_delivery = asyncio.create_task(
        server._js.callback(Mock(data=b"first", headers=None)),
    )
    await asyncio.wait_for(first_callback_started.wait(), timeout=1)
    second_delivery = asyncio.create_task(
        server._js.callback(Mock(data=b"second", headers=None)),
    )
    try:
        assert first_delivery.done()
        await first_delivery
        await asyncio.wait_for(second_reservation_started.wait(), timeout=1)
        assert not second_delivery.done()
        assert len(subscriber._admitted_tasks.tasks) == 1
    finally:
        first_delivery.cancel()
        second_delivery.cancel()
        await asyncio.gather(first_delivery, second_delivery, return_exceptions=True)
        release_first_callback.set()
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_subscriber_rejects_message_arriving_after_close() -> None:
    subscriber = NatsSubscriber(Mock(), {}, dispatcher=SubscriberDispatcher())

    message = Mock(data=b"{}", headers=None, nak=AsyncMock())
    subscriber._closed = True
    try:
        await subscriber._handle_message("jobs", AsyncMock(), None, message)

        message.nak.assert_awaited_once()
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_subscriber_rejects_message_when_admission_fails() -> None:
    subscriber = NatsSubscriber(Mock(), {}, dispatcher=SubscriberDispatcher())

    message = Mock(data=b"{}", headers=None, nak=AsyncMock())
    subscriber._dispatcher = Mock(reserve=AsyncMock(side_effect=RuntimeError("admission failed")))
    try:
        with pytest.raises(RuntimeError, match="admission failed"):
            await subscriber._handle_message("jobs", AsyncMock(), None, message)

        message.nak.assert_awaited_once()
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_subscriber_rejects_and_releases_when_close_wins_after_admission() -> None:
    subscriber = NatsSubscriber(Mock(), {}, dispatcher=SubscriberDispatcher())

    message = Mock(data=b"{}", headers=None, nak=AsyncMock())
    lease = Mock(release=AsyncMock())

    async def reserve(_: object) -> Mock:
        subscriber._closed = True
        return lease

    subscriber._dispatcher = Mock(reserve=AsyncMock(side_effect=reserve))
    try:
        await subscriber._handle_message("jobs", AsyncMock(), None, message)

        message.nak.assert_awaited_once()
        lease.release.assert_awaited_once()
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_kafka_received_message_concurrent_settlement_is_deduplicated() -> None:
    completion_started = asyncio.Event()
    release_completion = asyncio.Event()

    async def mark_complete(_: object) -> None:
        completion_started.set()
        await release_completion.wait()

    record = Mock(topic="jobs", partition=0, offset=0, value=b"", headers=[])
    message = KafkaReceivedMessage(Mock(), record, mark_complete)
    first = asyncio.create_task(message.ack())
    await asyncio.wait_for(completion_started.wait(), timeout=1)

    # The action is reserved before the first await, so the second settlement
    # returns immediately instead of issuing a second broker operation.
    await asyncio.wait_for(message.ack(), timeout=1)

    release_completion.set()
    await first

    assert first.done()
    assert not first.exception()
    assert message.action is MessageAction.acked


async def test_kafka_subscriber_close_leaves_cancelled_message_uncommitted() -> None:
    callback_started = asyncio.Event()
    consumer = Mock(assignment=Mock(return_value=()), commit=AsyncMock(), stop=AsyncMock())

    async def getmany(**_: Any) -> dict[object, object]:
        await asyncio.Future()
        return {}

    async def callback(_: object) -> None:
        callback_started.set()
        await asyncio.Future()

    consumer.getmany = AsyncMock(side_effect=getmany)
    producer = Mock(send_and_wait=AsyncMock())
    subscriber = KafkaSubscriber(
        Mock(_producer=producer),
        consumer,
        {"jobs": callback},
        dispatcher=SubscriberDispatcher(),
    )

    tp = TopicPartition("jobs", 0)
    record = Mock(topic="jobs", partition=0, offset=0, value=b"", headers=[])
    subscriber._offset_tracker[tp] = {0: False}
    subscriber._dispatcher = Mock(
        reserve=AsyncMock(return_value=Mock(release=AsyncMock())),
        run_admitted=SubscriberDispatcher.run_admitted,
    )
    try:
        await subscriber._process_message(record, tp)
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        await subscriber.stop()
        await subscriber.finish()

        assert subscriber._offset_tracker[tp] == {0: False}
        consumer.commit.assert_not_awaited()
        producer.send_and_wait.assert_not_awaited()
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_received_message_cancellation_stays_reserved_and_blocks_retry() -> None:
    ack_started = asyncio.Event()

    async def ack() -> None:
        ack_started.set()
        await asyncio.Future()

    raw = Mock(data=b"", headers=None, ack=AsyncMock(side_effect=ack))
    message = NatsReceivedMessage(raw, Mock(_dlq_topic_strategy=None), "jobs")
    task = asyncio.create_task(message.ack())
    await asyncio.wait_for(ack_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Cancellation may have left the RPC in flight, so the settlement stays
    # reserved and a retry must not issue a second broker operation.
    assert message.action is MessageAction.acked
    raw.ack.side_effect = None
    await message.ack()
    raw.ack.assert_awaited_once()
    assert message.action is MessageAction.acked


async def test_nats_subscriber_close_rejects_cancelled_admitted_callback() -> None:
    callback_started = asyncio.Event()
    raw = Mock(data=b"{}", headers=None, nak=AsyncMock())
    lease = Mock(release=AsyncMock())

    async def callback(_: object) -> None:
        callback_started.set()
        await asyncio.Future()

    subscriber = NatsSubscriber(Mock(), {}, dispatcher=SubscriberDispatcher())

    subscriber._dispatcher = Mock(
        reserve=AsyncMock(return_value=lease),
        run_admitted=SubscriberDispatcher.run_admitted,
    )
    try:
        await subscriber._handle_message("jobs", callback, None, raw)
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        await subscriber.stop()
        await subscriber.finish()

        raw.nak.assert_awaited_once()
        lease.release.assert_awaited_once()
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_subscriber_close_rejects_prestart_admitted_callback() -> None:
    raw = Mock(data=b"{}", headers=None, nak=AsyncMock())
    lease = Mock(release=AsyncMock())
    subscriber = NatsSubscriber(Mock(), {}, dispatcher=SubscriberDispatcher())

    subscriber._task.cancel()
    await asyncio.gather(subscriber._task, return_exceptions=True)
    subscriber._dispatcher = Mock(
        reserve=AsyncMock(return_value=lease),
        run_admitted=SubscriberDispatcher.run_admitted,
    )
    try:
        await subscriber._handle_message("jobs", AsyncMock(), None, raw)
        await subscriber.stop()
        await subscriber.finish()

        raw.nak.assert_awaited_once()
        lease.release.assert_awaited_once()
    finally:
        await subscriber.stop()
        await subscriber.finish()


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
    subscriber = NatsSubscriber(server, {"jobs": callback}, dispatcher=SubscriberDispatcher())

    subscriber._dispatcher = Mock(reserve=AsyncMock(return_value=None))
    await asyncio.sleep(0)

    assert server._js.callback is not None
    await server._js.callback(Mock(data=b"{}", headers=None))

    callback.assert_not_awaited()
    await subscriber.stop()
    await subscriber.finish()


async def test_admitted_task_tracker_logs_cleanup_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tracker = AdmittedTaskTracker()
    await tracker.cancel_and_drain(drain=False)
    lease = Mock(release=AsyncMock())

    async def fail() -> None:
        raise RuntimeError("cleanup failed")

    task = tracker.start_task(AsyncMock(), lease=lease, on_cancel=fail)
    await tracker.drain()

    assert task.done()
    error = next(
        record for record in caplog.records if record.message == "subscriber.delivery.cleanup.error"
    )
    assert error.exc_info is not None
    assert str(error.exc_info[1]) == "cleanup failed"
    lease.release.assert_awaited_once()


async def test_nats_logs_lease_release_error_when_close_wins_after_admission(
    caplog: pytest.LogCaptureFixture,
) -> None:
    subscriber = NatsSubscriber(Mock(), {}, dispatcher=SubscriberDispatcher())

    message = Mock(data=b"{}", headers=None, nak=AsyncMock())
    lease = Mock(release=AsyncMock(side_effect=RuntimeError("release failed")))

    async def reserve(_: object) -> Mock:
        subscriber._closed = True
        return lease

    subscriber._dispatcher = Mock(reserve=AsyncMock(side_effect=reserve))
    try:
        await subscriber._handle_message("jobs", AsyncMock(), None, message)

        message.nak.assert_awaited_once()
        error = next(
            record
            for record in caplog.records
            if record.message == "subscriber.lease.release.error"
        )
        assert error.exc_info is not None
        assert str(error.exc_info[1]) == "release failed"
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_resume_raises_when_consumer_resume_fails() -> None:
    class JetStreamManager:
        calls = 0

        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def pause_consumer(self, *args: Any) -> None:  # noqa: ARG002
            return None

        async def resume_consumer(self, stream: str, consumer: str) -> None:  # noqa: ARG002
            JetStreamManager.calls += 1
            if JetStreamManager.calls == 1:
                raise RuntimeError("resume failed")

    server = _nats_server_with_jsm(JetStreamManager())
    subscriber = NatsSubscriber(server, {"jobs": AsyncMock()}, dispatcher=SubscriberDispatcher())

    while not subscriber.is_active:
        await asyncio.sleep(0)

    await subscriber.pause()
    with pytest.raises(ConnectionError, match="Consumer resume request failed"):
        await subscriber.resume()

    assert not subscriber.is_active

    await subscriber.resume()

    assert subscriber.is_active
    assert set(subscriber._subs) == {"jobs"}
    await subscriber.stop()
    await subscriber.finish()


async def test_admitted_task_tracker_cleanup_done_tolerates_cancelled_cleanup() -> None:
    tracker = AdmittedTaskTracker()
    await tracker.cancel_and_drain(drain=False)
    release_cleanup = asyncio.Event()

    async def block() -> None:
        await release_cleanup.wait()

    tracker.start_task(AsyncMock(), on_cancel=block)

    cleanup_task = next(iter(tracker._cleanup_tasks))
    cleanup_task.cancel()
    release_cleanup.set()
    await tracker.drain()

    assert not tracker._cleanup_tasks
    assert cleanup_task.cancelled()


async def test_nats_channel_pause_pauses_single_consumer() -> None:
    calls: list[tuple[str, str]] = []

    class JetStreamManager:
        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def pause_consumer(self, stream: str, consumer: str, pause_until: str) -> None:  # noqa: ARG002
            calls.append(("pause", consumer))

        async def resume_consumer(self, stream: str, consumer: str) -> None:  # noqa: ARG002
            calls.append(("resume", consumer))

    class Subscription:
        async def unsubscribe(self) -> None:
            return None

    server = _nats_server_with_jsm(JetStreamManager())
    subscriber = NatsSubscriber(
        server,
        {"first": AsyncMock(), "second": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )
    while not subscriber.is_active:
        await asyncio.sleep(0)

    await subscriber.pause_channel("first")
    assert calls == [("pause", "first_group")]
    assert set(subscriber._subs) == {"first", "second"}
    assert subscriber.is_active

    await subscriber.resume_channel("first")
    assert calls == [("pause", "first_group"), ("resume", "first_group")]

    await subscriber.stop()
    await subscriber.finish()


async def test_nats_subscribe_sets_max_ack_pending_from_native_limit() -> None:
    """Auto-created consumers carry the dispatcher's per-channel window."""
    subscribe_calls: list[dict[str, Any]] = []

    class JetStream:
        def __init__(self) -> None:
            self.consumer_info_calls = 0

        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def consumer_info(self, stream: str, consumer: str) -> Any:  # noqa: ARG002
            self.consumer_info_calls += 1
            raise NotFoundError()

        async def subscribe(self, channel: str, **kwargs: Any) -> Any:  # noqa: ARG002
            subscribe_calls.append(kwargs)
            return Mock(unsubscribe=AsyncMock(), consumer_info=AsyncMock())

    subscriber = NatsSubscriber(
        Mock(_js=JetStream(), _jsm=JetStream()),
        {"jobs": AsyncMock()},
        dispatcher=SubscriberDispatcher(
            limits=MessageLimits(max_messages=7),
        ),
    )
    await asyncio.sleep(0)
    while not subscriber.is_active:
        await asyncio.sleep(0)

    assert len(subscribe_calls) == 1
    config = subscribe_calls[0]["config"]
    assert config is not None
    assert config.max_ack_pending == 7

    await subscriber.stop()
    await subscriber.finish()


async def test_nats_subscribe_updates_stale_consumer_window() -> None:
    """An existing durable consumer is brought up to the intake window."""
    updates: list[int] = []

    class JetStream:
        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def consumer_info(self, stream: str, consumer: str) -> Any:  # noqa: ARG002
            return Mock(
                config=ConsumerConfig(max_ack_pending=1000, ack_wait=30),
                stream_name="stream",
            )

        async def add_consumer(self, stream: str, config: Any) -> Any:  # noqa: ARG002
            updates.append(config.max_ack_pending)
            return Mock()

        async def subscribe(self, channel: str, **kwargs: Any) -> Any:  # noqa: ARG002
            return Mock(unsubscribe=AsyncMock(), consumer_info=AsyncMock())

    js = JetStream()
    subscriber = NatsSubscriber(
        Mock(_js=js, _jsm=js),
        {"jobs": AsyncMock()},
        dispatcher=SubscriberDispatcher(limits=MessageLimits(max_messages=12)),
    )
    try:
        await subscriber._subscribe_channel("jobs", AsyncMock())
        assert updates == [12]
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_subscribe_without_native_limit_skips_window_config() -> None:
    subscribe_calls: list[dict[str, Any]] = []

    class JetStream:
        async def subscribe(self, channel: str, **kwargs: Any) -> Any:  # noqa: ARG002
            subscribe_calls.append(kwargs)
            return Mock(unsubscribe=AsyncMock(), consumer_info=AsyncMock())

    subscriber = NatsSubscriber(
        Mock(_js=JetStream(), _jsm=JetStream()),
        {"jobs": AsyncMock()},
        dispatcher=SubscriberDispatcher(),
    )

    await asyncio.sleep(0)
    while not subscriber.is_active:
        await asyncio.sleep(0)

    assert len(subscribe_calls) == 1
    assert subscribe_calls[0]["config"] is None

    await subscriber.stop()
    await subscriber.finish()


class _RecordingJetStreamManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
        return "stream"

    async def pause_consumer(self, stream: str, consumer: str, pause_until: str) -> None:  # noqa: ARG002
        self.calls.append(("pause", consumer))

    async def resume_consumer(self, stream: str, consumer: str) -> None:  # noqa: ARG002
        self.calls.append(("resume", consumer))


class _FailingResumeJetStreamManager:
    async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
        return "stream"

    async def pause_consumer(self, stream: str, consumer: str, pause_until: str) -> None:  # noqa: ARG002
        raise RuntimeError("pause failed")

    async def resume_consumer(self, stream: str, consumer: str) -> None:  # noqa: ARG002
        raise RuntimeError("resume failed")


async def _activated_nats_subscriber(
    server: Any,
    channels: tuple[str, ...],
) -> NatsSubscriber:
    subscriber = NatsSubscriber(
        server,
        {channel: AsyncMock() for channel in channels},
        dispatcher=SubscriberDispatcher(),
    )
    while not subscriber.is_active:
        await asyncio.sleep(0)
    return subscriber


async def test_nats_unsubscribe_drops_channel_on_bad_subscription() -> None:
    """Covers message_broker.py line 393: a dead subscription is dropped."""
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(_RecordingJetStreamManager()),
        ("jobs",),
    )
    try:
        subscriber._subs["jobs"] = Mock(
            unsubscribe=AsyncMock(side_effect=BadSubscriptionError("already closed")),
        )

        assert await subscriber._unsubscribe_all() is None
        assert "jobs" not in subscriber._subs
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_unsubscribe_returns_first_error_and_logs_rest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers message_broker.py lines 394-402: first error wins, rest are logged."""
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(_RecordingJetStreamManager()),
        ("first", "second"),
    )
    try:
        subscriber._subs["first"] = Mock(unsubscribe=AsyncMock(side_effect=ValueError("first")))
        subscriber._subs["second"] = Mock(unsubscribe=AsyncMock(side_effect=RuntimeError("second")))

        error = await subscriber._unsubscribe_all()

        assert isinstance(error, ValueError)
        assert any(
            record.message == "subscriber.unsubscribe.error"
            for record in caplog.get_records(when="call")
        )
        subscriber._subs.clear()
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_pause_noop_when_never_activated_without_subscriptions() -> None:
    """Covers message_broker.py line 455: pause without intake is a no-op."""
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(_RecordingJetStreamManager()),
        ("jobs",),
    )
    try:
        await subscriber.stop()
        subscriber._subs.clear()
        subscriber._active = False

        await subscriber.pause()

        assert not subscriber.is_active
    finally:
        await subscriber.finish()


async def test_nats_resume_resubscribes_channels_missing_subscriptions() -> None:
    """Covers message_broker.py lines 480-481: resume re-arms missing channels."""
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(_RecordingJetStreamManager()),
        ("first", "second"),
    )
    try:
        subscriber._active = False
        subscriber._subs.clear()

        with patch.object(
            NatsSubscriber,
            "_subscribe_channel",
            new_callable=AsyncMock,
        ) as mock_subscribe:
            await subscriber.resume()

        assert mock_subscribe.await_count == 2
        assert subscriber.is_active
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_resume_returns_first_error_and_logs_rest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers message_broker.py lines 484-492: first error wins, rest are logged."""
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(_FailingResumeJetStreamManager()),
        ("first", "second"),
    )
    try:
        subscriber._active = False

        with pytest.raises(ConnectionError, match="Consumer resume request failed"):
            await subscriber.resume()

        assert not subscriber.is_active
        assert any(
            record.message == "subscriber.resume.error"
            for record in caplog.get_records(when="call")
        )
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_pause_channel_unknown_channel_is_noop() -> None:
    """Covers message_broker.py line 502: pausing an unsubscribed channel is a no-op."""
    jsm = _RecordingJetStreamManager()
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(jsm),
        ("jobs",),
    )
    try:
        await subscriber.pause_channel("unknown")
        assert jsm.calls == []
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_resume_channel_noop_when_closed_or_unknown_callback() -> None:
    """Covers message_broker.py lines 509 and 512: closed subscriber and unknown callback."""
    jsm = _RecordingJetStreamManager()
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(jsm),
        ("jobs",),
    )
    try:
        subscriber._closed = True
        await subscriber.resume_channel("jobs")

        subscriber._closed = False
        await subscriber.resume_channel("unknown")

        assert jsm.calls == []
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_resume_channel_resubscribes_missing_subscription() -> None:
    """Covers message_broker.py lines 514-515: resume re-arms a missing channel."""
    subscriber = await _activated_nats_subscriber(
        _nats_server_with_jsm(_RecordingJetStreamManager()),
        ("jobs",),
    )
    try:
        subscriber._subs.clear()

        with patch.object(
            NatsSubscriber,
            "_subscribe_channel",
            new_callable=AsyncMock,
        ) as mock_subscribe:
            await subscriber.resume_channel("jobs")

        mock_subscribe.assert_awaited_once_with("jobs", subscriber._channels_to_callbacks["jobs"])
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_consumer_config_update_failure_warns_and_skips(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers message_broker.py lines 550-551: a failed window update is only warned."""

    class JetStream:
        async def find_stream_name_by_subject(self, channel: str) -> str:  # noqa: ARG002
            return "stream"

        async def consumer_info(self, stream: str, consumer: str) -> Any:  # noqa: ARG002
            return Mock(config=ConsumerConfig(max_ack_pending=1000), stream_name="stream")

        async def add_consumer(self, stream: str, config: Any) -> Any:  # noqa: ARG002
            raise RuntimeError("update failed")

        async def subscribe(self, channel: str, **kwargs: Any) -> Any:  # noqa: ARG002
            return Mock(unsubscribe=AsyncMock(), consumer_info=AsyncMock())

    js = JetStream()
    subscriber = NatsSubscriber(
        Mock(_js=js, _jsm=js),
        {"jobs": AsyncMock()},
        dispatcher=SubscriberDispatcher(limits=MessageLimits(max_messages=12)),
    )
    try:
        while not subscriber.is_active:
            await asyncio.sleep(0)

        assert await subscriber._consumer_config("jobs", "jobs_group") is None
        assert any(
            record.message == "subscriber.consumer_config.update.error"
            for record in caplog.get_records(when="call")
        )
    finally:
        await subscriber.stop()
        await subscriber.finish()
