from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, patch

import grpc.aio
import pytest

from repid.admission import ExecutionAdmission
from repid.connections import SubscriberDispatcher
from repid.connections.abc import ReceivedMessageT
from repid.connections.amqp.subscriber import AmqpSubscriber
from repid.connections.nats.message_broker import NatsReceivedMessage, NatsServer, NatsSubscriber
from repid.connections.pubsub.message_broker import PubsubServer
from repid.connections.pubsub.protocol._helpers import ChannelConfig
from repid.connections.pubsub.protocol.credentials import InsecureCredentials
from repid.connections.pubsub.protocol.proto import (
    PubsubMessage,
    ReceivedMessage,
    StreamingPullResponse,
)
from repid.connections.pubsub.protocol.received_message import PubsubReceivedMessage
from repid.connections.pubsub.protocol.resilience import ResilienceState
from repid.connections.pubsub.protocol.subscriber import PubsubSubscriber
from repid.connections.redis.message_broker import ChannelConfig as RedisChannelConfig
from repid.connections.redis.message_broker import RedisReceivedMessage, RedisSubscriber
from repid.connections.sqs.message import SqsReceivedMessage
from repid.connections.sqs.subscriber import SqsSubscriber
from repid.limits import BackpressurePolicy, MessageLimits


def _pipeline_client() -> tuple[MagicMock, MagicMock]:
    pipe = MagicMock(execute=AsyncMock())
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(pipeline=MagicMock(return_value=pipe), xack=AsyncMock()), pipe


class _PubsubBatcher:
    def __init__(self) -> None:
        self.deadlines: list[tuple[str, str, int]] = []

    async def add_modify_deadline(self, subscription: str, ack_id: str, deadline: int) -> None:
        self.deadlines.append((subscription, ack_id, deadline))


def _pubsub_subscriber(
    dispatcher: SubscriberDispatcher,
    server: PubsubServer | None = None,
) -> PubsubSubscriber:
    return PubsubSubscriber(
        channel=MagicMock(spec=grpc.aio.Channel),
        channel_configs=[],
        credentials_provider=InsecureCredentials(),
        resilience_state=MagicMock(spec=ResilienceState),
        server=server or MagicMock(spec=PubsubServer),
        stream_ack_deadline_seconds=10,
        client_id="test-client",
        dispatcher=dispatcher,
    )


async def test_redis_consume_batch_renews_all_prefetched_streams_before_admission() -> None:
    client, pipe = _pipeline_client()
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    first = RedisChannelConfig(stream="first-stream", group="group", dlq=None)
    second = RedisChannelConfig(stream="second-stream", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"first": first, "second": second},
        callbacks={"first": AsyncMock(), "second": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )
    blocker = await dispatcher.reserve(
        Mock(channel="first", payload=b"block", keep_alive_interval=None),
    )
    assert blocker is not None
    renewed: dict[str, asyncio.Event] = {
        "first:1-0": asyncio.Event(),
        "first:2-0": asyncio.Event(),
        "second:3-0": asyncio.Event(),
    }

    async def keep_alive(message: RedisReceivedMessage) -> None:
        renewed[f"{message.channel}:{message.message_id}"].set()

    client.xreadgroup = AsyncMock(
        return_value=[
            [b"first-stream", [(b"1-0", {b"payload": b"one"}), (b"2-0", {b"payload": b"two"})]],
            [b"second-stream", [(b"3-0", {b"payload": b"three"})]],
        ],
    )
    with (
        patch.object(RedisReceivedMessage, "keep_alive", new=keep_alive),
        patch.object(
            RedisReceivedMessage,
            "keep_alive_interval",
            new_callable=PropertyMock,
            return_value=0.001,
        ),
    ):
        consuming = asyncio.create_task(
            subscriber._consume_batch("group", {"first": first, "second": second}),
        )
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in renewed.values())),
            timeout=1,
        )
        consuming.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consuming
    assert subscriber.in_flight_count == 0
    assert pipe.xadd.call_count == 3
    assert not dispatcher._keepalive_tasks
    await blocker.release()
    await subscriber.stop()
    await subscriber.finish()


async def test_redis_reclaim_batch_renews_all_prefetched_messages_before_admission() -> None:
    client, pipe = _pipeline_client()
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    config = RedisChannelConfig(stream="jobs", group="group", dlq=None)
    subscriber = RedisSubscriber(
        redis_client=client,
        channels={"jobs": config},
        callbacks={"jobs": AsyncMock()},
        consumer_name="consumer",
        dispatcher=dispatcher,
    )
    blocker = await dispatcher.reserve(
        Mock(channel="jobs", payload=b"block", keep_alive_interval=None),
    )
    assert blocker is not None
    renewed = {message_id: asyncio.Event() for message_id in ("1-0", "2-0")}

    async def keep_alive(message: RedisReceivedMessage) -> None:
        renewed[cast(str, message.message_id)].set()

    client.xautoclaim = AsyncMock(
        return_value=[b"0-0", [(b"1-0", {b"payload": b"one"}), (b"2-0", {b"payload": b"two"})], []],
    )
    with (
        patch.object(RedisReceivedMessage, "keep_alive", new=keep_alive),
        patch.object(
            RedisReceivedMessage,
            "keep_alive_interval",
            new_callable=PropertyMock,
            return_value=0.001,
        ),
    ):
        reclaiming = asyncio.create_task(subscriber._reclaim_pending(config, "jobs", AsyncMock()))
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in renewed.values())),
            timeout=1,
        )
        reclaiming.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reclaiming
    assert subscriber.in_flight_count == 0
    assert pipe.xadd.call_count == 2
    assert not dispatcher._keepalive_tasks
    await blocker.release()
    await subscriber.stop()
    await subscriber.finish()


async def test_pubsub_paused_response_renews_every_delivery_and_finish_cleans_up() -> None:
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    batcher = _PubsubBatcher()
    server = Mock(spec=PubsubServer, _control_batcher=batcher)
    subscriber = _pubsub_subscriber(dispatcher, server)
    await subscriber.pause()
    config = ChannelConfig(
        channel="jobs",
        subscription_path="projects/p/subscriptions/s",
        callback=AsyncMock(),
    )
    response = StreamingPullResponse(
        received_messages=[
            ReceivedMessage(message=PubsubMessage(data=b"one", message_id="one"), ack_id="one"),
            ReceivedMessage(message=PubsubMessage(data=b"two", message_id="two"), ack_id="two"),
        ],
    )
    renewed = {"one": asyncio.Event(), "two": asyncio.Event()}

    async def keep_alive(message: Any) -> None:
        renewed[message.message_id].set()

    with (
        patch.object(
            PubsubReceivedMessage,
            "keep_alive",
            new=keep_alive,
        ),
        patch.object(
            PubsubReceivedMessage,
            "keep_alive_interval",
            new_callable=PropertyMock,
            return_value=0.001,
        ),
    ):
        processing = asyncio.create_task(subscriber._process_response(response, config))
        subscriber._task = processing
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in renewed.values())),
            timeout=1,
        )
        await asyncio.wait_for(subscriber.stop(), timeout=1)
        await asyncio.gather(processing, return_exceptions=True)
        assert processing.cancelled()
        await subscriber.finish()
    assert set(batcher.deadlines) == {
        ("projects/p/subscriptions/s", "one", 1),
        ("projects/p/subscriptions/s", "two", 1),
    }
    assert not subscriber._in_flight_messages
    assert not dispatcher._keepalive_tasks


@pytest.mark.parametrize(
    "drain",
    [pytest.param(True, id="drain"), pytest.param(False, id="no_drain")],
)
async def test_sqs_fetch_batch_renews_before_blocked_admission_and_close_drains_requeues(
    drain: bool,
) -> None:
    renewed = {"one": asyncio.Event(), "two": asyncio.Event()}
    requeue_started = asyncio.Event()
    allow_requeue = asyncio.Event()

    async def visibility(**_: Any) -> None:
        requeue_started.set()
        await allow_requeue.wait()

    client = Mock(
        receive_message=AsyncMock(
            return_value={
                "Messages": [
                    {"MessageId": "one", "ReceiptHandle": "one", "Body": "one"},
                    {"MessageId": "two", "ReceiptHandle": "two", "Body": "two"},
                ],
            },
        ),
        change_message_visibility=AsyncMock(side_effect=visibility),
    )
    server = Mock(
        _client=client,
        _visibility_timeout=30,
        _batch_size=2,
        _receive_wait_time_seconds=0,
        _active_subscribers=set(),
        _get_queue_url=AsyncMock(return_value="queue"),
    )
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    blocker = await dispatcher.reserve(
        Mock(channel="jobs", payload=b"block", keep_alive_interval=None),
    )
    assert blocker is not None
    subscriber = SqsSubscriber(
        server,
        {"jobs": AsyncMock()},
        dispatcher,
    )

    async def keep_alive(message: Any) -> None:
        renewed[message.message_id].set()

    with (
        patch.object(SqsReceivedMessage, "keep_alive", new=keep_alive),
        patch.object(
            SqsReceivedMessage,
            "keep_alive_interval",
            new_callable=PropertyMock,
            return_value=0.001,
        ),
    ):
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in renewed.values())),
            timeout=1,
        )
        await subscriber.stop()
        closing = asyncio.create_task(subscriber.finish()) if drain else None
        await asyncio.wait_for(requeue_started.wait(), timeout=1)
        if drain:
            assert closing is not None
            assert not closing.done()
        allow_requeue.set()
        if drain:
            assert closing is not None
            await closing
        else:
            await subscriber._drain_requeues()
    assert client.change_message_visibility.await_count == 2
    assert not subscriber._requeue_tasks
    assert not dispatcher._keepalive_tasks
    await blocker.release()


async def test_sqs_non_draining_close_can_be_upgraded_to_drain_cancel_cleanup() -> None:
    requeue_started = asyncio.Event()
    allow_requeue = asyncio.Event()
    message_received = asyncio.Event()

    async def receive(**_: Any) -> dict[str, Any]:
        message_received.set()
        return {"Messages": [{"MessageId": "one", "ReceiptHandle": "one", "Body": "one"}]}

    async def visibility(**_: Any) -> None:
        requeue_started.set()
        await allow_requeue.wait()

    client = Mock(
        receive_message=AsyncMock(side_effect=receive),
        change_message_visibility=AsyncMock(side_effect=visibility),
    )
    server = Mock(
        _client=client,
        _visibility_timeout=30,
        _batch_size=1,
        _receive_wait_time_seconds=0,
        _active_subscribers=set(),
        _get_queue_url=AsyncMock(return_value="queue"),
    )
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1))
    blocker = await dispatcher.reserve(
        Mock(channel="jobs", payload=b"block", keep_alive_interval=None),
    )
    assert blocker is not None
    subscriber = SqsSubscriber(
        server,
        {"jobs": AsyncMock()},
        dispatcher,
    )

    server._active_subscribers.add(subscriber)

    try:
        await asyncio.wait_for(message_received.wait(), timeout=1)
        while not dispatcher._pre_admission_keepalives:
            await asyncio.sleep(0)
        await asyncio.wait_for(subscriber.stop(), timeout=1)
        main_task = subscriber._main_task
        assert main_task is not None
        assert subscriber in server._active_subscribers

        await asyncio.wait_for(requeue_started.wait(), timeout=1)
        draining_close = asyncio.create_task(subscriber.finish())
        await asyncio.sleep(0)
        assert not draining_close.done()

        allow_requeue.set()
        await asyncio.wait_for(draining_close, timeout=1)
        assert client.change_message_visibility.await_count == 1
        assert not subscriber._requeue_tasks
        assert subscriber._main_task is None
        assert subscriber not in server._active_subscribers
    finally:
        allow_requeue.set()
        await subscriber.stop()
        await subscriber.finish()
        await blocker.release()


async def test_sqs_stays_registered_until_running_cleanup_is_drained() -> None:
    callback_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    block = asyncio.Event()

    async def callback(message: Any) -> None:  # noqa: ARG001
        callback_started.set()
        try:
            await block.wait()
        except asyncio.CancelledError:
            # Started tasks own their cancellation cleanup.
            cleanup_started.set()
            await allow_cleanup.wait()
            raise

    async def receive(**_: Any) -> dict[str, Any]:
        await asyncio.Future()
        return {}

    server = Mock(
        _client=Mock(receive_message=AsyncMock(side_effect=receive)),
        _visibility_timeout=30,
        _batch_size=1,
        _receive_wait_time_seconds=0,
        _active_subscribers=set(),
        _get_queue_url=AsyncMock(return_value="queue"),
    )
    subscriber = SqsSubscriber(server, {"jobs": AsyncMock()}, dispatcher=SubscriberDispatcher())

    server._active_subscribers.add(subscriber)
    tracked = subscriber._admitted_tasks.start(
        subscriber._dispatcher,
        AsyncMock(),
        MagicMock(),
        callback,
    )

    try:
        await subscriber.stop()
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        await asyncio.gather(subscriber.task, return_exceptions=True)

        # stop() leaves the running callback alone; it is still registered.
        assert not tracked.done()
        assert subscriber in server._active_subscribers

        draining_close = asyncio.create_task(subscriber.finish())
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not draining_close.done()
        assert subscriber in server._active_subscribers

        allow_cleanup.set()
        await asyncio.wait_for(draining_close, timeout=1)
        assert tracked.cancelled()
        assert subscriber not in server._active_subscribers
    finally:
        block.set()
        allow_cleanup.set()
        await subscriber.stop()
        await subscriber.finish()


async def test_sqs_reserve_error_requeues_entire_prefetched_batch() -> None:
    renewed = {"one": asyncio.Event(), "two": asyncio.Event()}
    requeued = asyncio.Event()
    requeue_count = 0

    async def visibility(**_: Any) -> None:
        nonlocal requeue_count
        requeue_count += 1
        if requeue_count == 2:
            requeued.set()

    client = Mock(
        receive_message=AsyncMock(
            return_value={
                "Messages": [
                    {"MessageId": "one", "ReceiptHandle": "one", "Body": "one"},
                    {"MessageId": "two", "ReceiptHandle": "two", "Body": "two"},
                ],
            },
        ),
        change_message_visibility=AsyncMock(side_effect=visibility),
    )
    server = Mock(
        _client=client,
        _visibility_timeout=30,
        _batch_size=2,
        _receive_wait_time_seconds=0,
        _active_subscribers=set(),
        _get_queue_url=AsyncMock(return_value="queue"),
    )
    dispatcher = SubscriberDispatcher()
    subscriber = SqsSubscriber(
        server,
        {"jobs": AsyncMock()},
        dispatcher,
    )

    async def keep_alive(message: Any) -> None:
        renewed[message.message_id].set()

    async def raise_after_prefetch(_: ReceivedMessageT) -> Any:
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in renewed.values())),
            timeout=1,
        )
        raise RuntimeError("reserve failed")

    with (
        patch.object(SqsReceivedMessage, "keep_alive", new=keep_alive),
        patch.object(
            SqsReceivedMessage,
            "keep_alive_interval",
            new_callable=PropertyMock,
            return_value=0.001,
        ),
        patch.object(dispatcher, "reserve", side_effect=raise_after_prefetch),
    ):
        await asyncio.wait_for(requeued.wait(), timeout=1)
        await subscriber.stop()
        await asyncio.wait_for(subscriber.finish(), timeout=1)
    assert client.change_message_visibility.await_count == 2
    assert not subscriber._requeue_tasks
    assert not dispatcher._keepalive_tasks


async def test_nats_disconnect_drains_cleanup_after_non_draining_close() -> None:
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    nats_client = Mock(is_connected=True, close=AsyncMock())
    server = NatsServer("nats://localhost:4222", dlq_topic_strategy=None)
    server._nc = nats_client
    server._js = Mock()
    subscriber = cast(
        NatsSubscriber,
        await server.subscribe(channels_to_callbacks={}, dispatcher=SubscriberDispatcher()),
    )
    # Let the monitor reach its steady-state wait before admitting callback work.
    await asyncio.sleep(0)
    message_transport = Mock(nak=AsyncMock())
    message = NatsReceivedMessage(message_transport, server, "orders.subject")
    callback_started = asyncio.Event()

    async def callback() -> None:
        callback_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cleanup_started.set()
            await allow_cleanup.wait()
            await message.reject()
            raise

    subscriber._admitted_tasks.start_task(callback, message=message)

    try:
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        await asyncio.wait_for(subscriber.stop(), timeout=1)
        # stop() leaves the running callback alone.
        assert not cleanup_started.is_set()
        assert subscriber in server._active_subscribers

        disconnecting = asyncio.create_task(server.disconnect())
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not nats_client.close.await_count

        allow_cleanup.set()
        await asyncio.wait_for(disconnecting, timeout=1)
        message_transport.nak.assert_awaited_once()
        nats_client.close.assert_awaited_once()
    finally:
        allow_cleanup.set()
        await subscriber.stop()
        await subscriber.finish()


async def test_nats_callback_waits_for_subscription_metadata_before_dispatch() -> None:
    metadata_ready = asyncio.Event()
    captured: Callable[[Any], Coroutine[Any, Any, None]] | None = None

    class Subscription:
        async def consumer_info(self) -> Any:
            await metadata_ready.wait()
            return SimpleNamespace(config=SimpleNamespace(ack_wait=9))

        async def unsubscribe(self) -> None:
            return None

    async def subscribe(
        *_: Any,
        cb: Callable[[Any], Coroutine[Any, Any, None]],
        **__: Any,
    ) -> Subscription:
        nonlocal captured
        captured = cb
        return Subscription()

    delivered = asyncio.Event()
    received_messages: list[ReceivedMessageT] = []

    async def callback(message: ReceivedMessageT) -> None:
        received_messages.append(message)
        delivered.set()

    server = Mock(spec=NatsServer, _js=Mock(subscribe=AsyncMock(side_effect=subscribe)))
    subscriber = NatsSubscriber(
        server,
        {"orders.subject": callback},
        dispatcher=SubscriberDispatcher(),
    )

    try:
        await asyncio.wait_for(asyncio.sleep(0), timeout=1)
        assert captured is not None
        callback_task = asyncio.create_task(
            captured(Mock(data=b"x", headers=None, reply=None, subject="orders.subject")),
        )
        await asyncio.sleep(0)
        assert not callback_task.done()
        metadata_ready.set()
        await asyncio.wait_for(callback_task, timeout=1)
        await asyncio.wait_for(delivered.wait(), timeout=1)
        assert received_messages[0].keep_alive_interval == 3
    finally:
        await subscriber.stop()
        await subscriber.finish()


async def test_amqp_native_flow_requires_each_channel_to_have_an_independent_limit() -> None:
    managed = Mock(receiver_pool=Mock(unsubscribe=AsyncMock()))

    async def callback(_: ReceivedMessageT) -> None:
        return None

    callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]] = {
        "first": callback,
        "second": callback,
    }
    shared = MessageLimits(max_messages=1)
    subscribers = [
        AmqpSubscriber(
            managed_session=managed,
            queues_to_callbacks=callbacks,
            dispatcher=SubscriberDispatcher(
                channel_limits={"first": (shared,), "second": (shared,)},
            ),
            naming_strategy=str,
        ),
        AmqpSubscriber(
            managed_session=managed,
            queues_to_callbacks=callbacks,
            dispatcher=SubscriberDispatcher(
                MessageLimits(max_messages=2),
                {
                    "first": (MessageLimits(max_messages=1),),
                    "second": (MessageLimits(max_messages=1),),
                },
            ),
            naming_strategy=str,
        ),
        AmqpSubscriber(
            managed_session=managed,
            queues_to_callbacks=callbacks,
            dispatcher=SubscriberDispatcher(
                channel_limits={
                    "first": (MessageLimits(max_messages=1),),
                    "second": (MessageLimits(max_messages=1),),
                },
            ),
            naming_strategy=str,
        ),
    ]
    try:
        # Native flow control is claimed through server capabilities; whether the
        # configured limits map onto an independent per-link window is decided by
        # the dispatcher.
        assert not subscribers[0]._dispatcher.native_limit_is_independent(
            "first",
            ("first", "second"),
            "messages",
        )
        assert not subscribers[1]._dispatcher.native_limit_is_independent(
            "first",
            ("first", "second"),
            "messages",
        )
        assert subscribers[2]._dispatcher.native_limit_is_independent(
            "first",
            ("first", "second"),
            "messages",
        )
        assert subscribers[2]._dispatcher.native_limit_is_independent(
            "second",
            ("first", "second"),
            "messages",
        )
    finally:
        for subscriber in subscribers:
            await subscriber.stop()
            await subscriber.finish()


async def test_execution_admission_native_only_requires_independent_amqp_caps() -> None:
    managed = Mock(receiver_pool=Mock(unsubscribe=AsyncMock()))

    async def callback(_: ReceivedMessageT) -> None:
        return None

    callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]] = {
        "first": callback,
        "second": callback,
    }
    server = Mock(
        capabilities={
            "supports_pause_per_channel": False,
            "supports_pause": False,
            "supports_native_message_flow_control": True,
            "supports_native_message_flow_control_per_channel": True,
            "supports_native_payload_flow_control": False,
            "supports_native_payload_flow_control_per_channel": False,
        },
    )
    strict = BackpressurePolicy(strategies=("native",), on_unavailable="error")
    shared_channel = MessageLimits(max_messages=1)
    cases = (
        (
            MessageLimits(backpressure=strict),
            {"first": shared_channel, "second": shared_channel},
            True,
        ),
        (
            MessageLimits(max_messages=2, backpressure=strict),
            {"first": MessageLimits(max_messages=1), "second": MessageLimits(max_messages=1)},
            True,
        ),
        (
            MessageLimits(backpressure=strict),
            {"first": MessageLimits(max_messages=1), "second": MessageLimits(max_messages=1)},
            False,
        ),
    )
    for worker_limits, channel_limits, should_fail in cases:
        admission = ExecutionAdmission(
            server=server,
            limits=worker_limits,
            channel_limits=channel_limits,
        )
        prepared = admission.prepare({"first": [], "second": []})
        admission.dispatcher = SubscriberDispatcher(worker_limits, prepared)
        subscriber = AmqpSubscriber(
            managed_session=managed,
            queues_to_callbacks=callbacks,
            dispatcher=SubscriberDispatcher(worker_limits, prepared),
            naming_strategy=str,
        )
        admission.server_subscriber = subscriber
        try:
            if should_fail:
                with pytest.raises(ValueError, match="no available strategy"):
                    admission.validate_backpressure()
            else:
                admission.validate_backpressure()
        finally:
            await subscriber.stop()
            await subscriber.finish()


async def test_dispatcher_cancellation_waits_for_real_keepalive_then_releases_lease() -> None:
    dispatcher = SubscriberDispatcher(MessageLimits(max_messages=1), active=False)
    keepalive_started = asyncio.Event()
    cancelled = asyncio.Event()
    release_keepalive = asyncio.Event()

    class Message:
        channel = "jobs"
        payload = b"message"
        keep_alive_interval = 0.001
        is_acted_on = False

        async def keep_alive(self) -> None:
            keepalive_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                await release_keepalive.wait()
                raise

    reserving = asyncio.create_task(dispatcher.reserve(cast(ReceivedMessageT, Message())))
    try:
        await asyncio.wait_for(keepalive_started.wait(), timeout=1)
        dispatcher.activate()
        await asyncio.wait_for(cancelled.wait(), timeout=1)
        retained = tuple(dispatcher._keepalive_tasks)
        reserving.cancel()
        release_keepalive.set()
        with pytest.raises(asyncio.CancelledError):
            await reserving
        await asyncio.gather(*retained, return_exceptions=True)
        next_lease = await asyncio.wait_for(
            dispatcher.reserve(Mock(channel="jobs", payload=b"next", keep_alive_interval=None)),
            timeout=1,
        )
        assert next_lease is not None
        await next_lease.release()
        assert not dispatcher._keepalive_tasks
    finally:
        release_keepalive.set()
        reserving.cancel()
        await asyncio.gather(reserving, return_exceptions=True)
