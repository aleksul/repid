from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from repid.connections.amqp.subscriber import AmqpSubscriber
from repid.connections.in_memory import InMemoryServer
from repid.connections.kafka.subscriber import KafkaSubscriber
from repid.connections.nats.message_broker import NatsServer, NatsSubscriber
from repid.connections.pubsub.protocol.subscriber import PubsubSubscriber
from repid.connections.redis.message_broker import ChannelConfig, RedisSubscriber
from repid.connections.sqs.subscriber import SqsSubscriber
from repid.data import MessageData
from repid.limits import NativeFlow, NativeWindow


@pytest.mark.parametrize("adapter", ["amqp", "kafka", "nats", "pubsub", "redis"])
async def test_worker_pause_only_adapters_reject_channel_operations(adapter: str) -> None:
    subscriber: Any
    if adapter == "amqp":
        subscriber = AmqpSubscriber(
            managed_session=Mock(
                receiver_pool=Mock(pause_intake=AsyncMock(), resume_intake=AsyncMock()),
            ),
            queues_to_callbacks={},
            paused_event=asyncio.Event(),
            naming_strategy=str,
        )
    elif adapter == "kafka":
        subscriber = KafkaSubscriber(
            Mock(),
            Mock(stop=AsyncMock(), assignment=Mock(return_value=())),
            {},
        )
    elif adapter == "nats":
        subscriber = NatsSubscriber(NatsServer("nats://localhost"), {})
    elif adapter == "pubsub":
        subscriber = PubsubSubscriber(
            channel=Mock(),
            channel_configs=[],
            credentials_provider=Mock(),
            resilience_state=Mock(),
            stream_ack_deadline_seconds=10,
            client_id="test",
            server=Mock(),
        )
    else:
        subscriber = RedisSubscriber(
            redis_client=Mock(),
            channels={},
            callbacks={},
            consumer_name="test",
            server=Mock(),
        )
    with pytest.raises(ValueError, match="worker pause only"):
        await subscriber.pause("jobs")
    with pytest.raises(ValueError, match="worker pause only"):
        await subscriber.resume("jobs")
    await subscriber.finish()


async def test_amqp_partial_subscription_failure_releases_all_links() -> None:
    link = Mock()
    pool = Mock(
        subscribe=AsyncMock(side_effect=[link, RuntimeError("subscribe failed")]),
        unsubscribe=AsyncMock(),
    )
    session = Mock(receiver_pool=pool)
    with pytest.raises(RuntimeError, match="subscribe failed"):
        await AmqpSubscriber.create(
            managed_session=session,
            queues_to_callbacks={"one": AsyncMock(), "two": AsyncMock()},
            naming_strategy=str,
            publish_fn=AsyncMock(),
        )
    assert pool.unsubscribe.await_count == 2
    pool.unsubscribe.assert_any_await("one")
    pool.unsubscribe.assert_any_await("two")


@pytest.mark.parametrize("failed", [False, True])
async def test_amqp_dispatcher_completion_is_reported_as_worker_failure(failed: bool) -> None:
    async def dispatch() -> None:
        if failed:
            raise RuntimeError("dispatch failed")

    dispatcher = asyncio.create_task(dispatch())
    await asyncio.gather(dispatcher, return_exceptions=True)
    subscriber = AmqpSubscriber(
        managed_session=Mock(
            receiver_pool=Mock(pause_intake=AsyncMock(), resume_intake=AsyncMock()),
        ),
        queues_to_callbacks={},
        paused_event=asyncio.Event(),
        naming_strategy=str,
    )
    subscriber._dispatch_tasks = [dispatcher]
    with pytest.raises(RuntimeError, match="dispatch"):
        await asyncio.wait_for(subscriber.task, 2)
    with pytest.raises(RuntimeError, match="dispatch"):
        await subscriber.finish()


async def test_amqp_pause_failure_still_disposes_every_buffer() -> None:
    pool = Mock(pause_intake=AsyncMock(side_effect=RuntimeError("pause failed")))
    subscriber = AmqpSubscriber(
        managed_session=Mock(receiver_pool=pool),
        queues_to_callbacks={},
        paused_event=asyncio.Event(),
        naming_strategy=str,
    )
    first = Mock(dispose=AsyncMock(side_effect=RuntimeError("reject failed")))
    second = Mock(dispose=AsyncMock())
    subscriber._buffers = [first, second]
    with pytest.raises(RuntimeError, match="pause failed"):
        await subscriber.stop()
    second.dispose.assert_awaited_once()
    await subscriber.finish()


async def test_amqp_delivery_after_stop_is_disposed_without_handoff() -> None:
    link = Mock(pause_intake=AsyncMock(), settle_delivery=AsyncMock())
    pool = Mock(
        subscribe=AsyncMock(return_value=link),
        unsubscribe=AsyncMock(),
        pause_intake=AsyncMock(),
    )
    session = Mock(receiver_pool=pool)
    callback = AsyncMock()
    subscriber = await AmqpSubscriber.create(
        managed_session=session,
        queues_to_callbacks={"jobs": callback},
        naming_strategy=str,
        publish_fn=AsyncMock(),
    )
    raw_callback = pool.subscribe.await_args.args[1]
    await subscriber.stop()
    await raw_callback(b"body", None, 1, b"tag", link)
    callback.assert_not_awaited()
    link.settle_delivery.assert_awaited_once()
    await subscriber.finish()


async def test_native_count_credit_is_released_by_reply() -> None:

    server = InMemoryServer()
    replied = asyncio.Event()
    count = 0

    async def reply(message: Any) -> None:
        nonlocal count
        await message.reply(payload=b"reply", channel="replies")
        count += 1
        if count == 2:
            replied.set()

    async with server.connection():
        for _ in range(2):
            await server.publish(channel="jobs", message=MessageData(payload=b"body"))
        subscriber = await server.subscribe(
            channels_to_callbacks={"jobs": reply},
            native_flow=NativeFlow(worker=NativeWindow(max_messages=1)),
        )
        await asyncio.wait_for(replied.wait(), 2)
        await subscriber.finish()
    assert count == 2


@pytest.mark.parametrize("adapter", ["amqp", "nats"])
async def test_finish_attempts_all_resources_and_propagates_release_failure(adapter: str) -> None:
    subscriber: Any
    failed = AsyncMock(side_effect=RuntimeError("release failed"))
    good = AsyncMock()
    if adapter == "amqp":
        pool = Mock(unsubscribe=AsyncMock(side_effect=[RuntimeError("release failed"), None]))
        subscriber = AmqpSubscriber(
            managed_session=Mock(receiver_pool=pool),
            queues_to_callbacks={"one": AsyncMock(), "two": AsyncMock()},
            paused_event=asyncio.Event(),
            naming_strategy=str,
        )
    else:
        subscriber = NatsSubscriber(NatsServer("nats://localhost"), {})
        subscriber._subs = {
            "one": Mock(_sub=Mock(drain=AsyncMock()), unsubscribe=failed),
            "two": Mock(_sub=Mock(drain=AsyncMock()), unsubscribe=good),
        }
    with pytest.raises(RuntimeError, match="release failed"):
        await subscriber.finish()
    if adapter == "amqp":
        assert pool.unsubscribe.await_count == 2
    else:
        good.assert_awaited_once()


async def test_redis_stop_surfaces_failed_intake_task_after_disposal() -> None:
    subscriber = RedisSubscriber(
        redis_client=Mock(),
        channels={},
        callbacks={},
        consumer_name="test",
        server=Mock(),
    )

    async def fail() -> None:
        raise RuntimeError("intake failed")

    subscriber._task = asyncio.create_task(fail())
    await asyncio.gather(subscriber._task, return_exceptions=True)
    with pytest.raises(RuntimeError, match="intake failed"):
        await subscriber.stop()
    assert not subscriber._buffer.owned
    await subscriber.finish()


@pytest.mark.parametrize("shutdown_in_receive", [False, True])
async def test_sqs_receive_retry_and_shutdown_disposal(shutdown_in_receive: bool) -> None:
    calls = 0
    client = Mock(change_message_visibility=AsyncMock())
    server = Mock(
        _client=client,
        _get_queue_url=AsyncMock(return_value="url"),
        _batch_size=1,
        _receive_wait_time_seconds=0,
        _visibility_timeout=30,
        _active_subscribers=set(),
    )
    callback = AsyncMock()

    async def receive(**_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if not shutdown_in_receive and calls == 1:
            raise RuntimeError("receive failed")
        subscriber._shutdown_event.set()
        return {"Messages": [{"ReceiptHandle": "handle"}]} if shutdown_in_receive else {}

    client.receive_message = receive
    subscriber = SqsSubscriber(server, {"jobs": callback})
    await asyncio.wait_for(subscriber.task, 2)
    await subscriber.finish()
    callback.assert_not_awaited()
    if shutdown_in_receive:
        client.change_message_visibility.assert_awaited_once_with(
            QueueUrl="url",
            ReceiptHandle="handle",
            VisibilityTimeout=0,
        )
    else:
        assert calls == 2


@pytest.mark.parametrize("adapter", ["sqs", "pubsub", "redis"])
async def test_buffered_renewal_failure_stops_paused_adapter(adapter: str) -> None:
    waiting = Mock(
        is_acted_on=False,
        keep_alive_interval=0.001,
        keep_alive=AsyncMock(side_effect=RuntimeError("renewal failed")),
        reject=AsyncMock(),
    )
    callback = AsyncMock()
    subscriber: Any
    if adapter == "sqs":

        async def receive(**_kwargs: Any) -> dict[str, Any]:
            await subscriber.pause()
            return {"Messages": [{}]}

        server = Mock(
            _client=Mock(receive_message=receive),
            _get_queue_url=AsyncMock(return_value="url"),
            _batch_size=1,
            _receive_wait_time_seconds=0,
            _visibility_timeout=30,
            _active_subscribers=set(),
        )
        with patch("repid.connections.sqs.subscriber.SqsReceivedMessage", return_value=waiting):
            subscriber = SqsSubscriber(server, {"jobs": callback})
            with pytest.raises(RuntimeError, match="renewal failed"):
                await asyncio.wait_for(subscriber.task, 2)
    elif adapter == "redis":
        subscriber = RedisSubscriber(
            redis_client=Mock(),
            channels={"jobs": ChannelConfig(stream="jobs", group="group", dlq=None)},
            callbacks={"jobs": callback},
            consumer_name="consumer",
            server=Mock(),
        )
        await subscriber.pause()
        subscriber._buffer.track((waiting,))
        subscriber.start()
        with pytest.raises(RuntimeError, match="renewal failed"):
            await asyncio.wait_for(subscriber.task, 2)
    else:
        subscriber = PubsubSubscriber(
            channel=Mock(),
            channel_configs=[Mock(callback=callback)],
            credentials_provider=Mock(),
            resilience_state=Mock(),
            stream_ack_deadline_seconds=10,
            client_id="test",
            server=Mock(),
        )
        subscriber._pause_event.clear()
        subscriber._buffer.track((waiting,))
        subscriber._start_background_tasks()
        with pytest.raises(RuntimeError, match="renewal failed"):
            await asyncio.wait_for(subscriber.task, 2)
    with pytest.raises(RuntimeError, match="renewal failed"):
        await subscriber.finish()
    callback.assert_not_awaited()
    waiting.reject.assert_awaited_once()


@pytest.mark.parametrize("release_fails", [False, True])
async def test_amqp_replacement_overflow_is_released_or_fails_intake(release_fails: bool) -> None:
    old = Mock(defer_delivery_credit=Mock(), settle_delivery=AsyncMock())
    replacement = Mock(
        defer_delivery_credit=Mock(),
        settle_delivery=AsyncMock(
            side_effect=RuntimeError("release failed") if release_fails else None,
        ),
    )
    pool = Mock(
        subscribe=AsyncMock(return_value=old),
        pause_intake=AsyncMock(),
        resume_intake=AsyncMock(),
        unsubscribe=AsyncMock(),
    )
    callback = AsyncMock()
    subscriber = await AmqpSubscriber.create(
        managed_session=Mock(receiver_pool=pool),
        queues_to_callbacks={"jobs": callback},
        native_flow=NativeFlow(channels={"jobs": NativeWindow(max_messages=2)}),
        naming_strategy=str,
        publish_fn=AsyncMock(),
    )
    await subscriber.pause()
    raw_callback = pool.subscribe.await_args.args[1]
    for delivery_id in range(2):
        await raw_callback(b"old", None, delivery_id, b"tag", old)
    await raw_callback(b"new", None, 2, b"tag", replacement)
    replacement.settle_delivery.assert_awaited_once()
    assert len(subscriber._buffers[0].owned) == 2
    if release_fails:
        with pytest.raises(RuntimeError, match="release failed"):
            await asyncio.wait_for(subscriber.task, 2)
        with pytest.raises(RuntimeError, match="release failed"):
            await subscriber.finish()
    else:
        await subscriber.resume()
        dispatched = asyncio.Event()
        count = 0

        async def received(_message: Any) -> None:
            nonlocal count
            count += 1
            if count == 2:
                dispatched.set()

        callback.side_effect = received
        await asyncio.wait_for(dispatched.wait(), 2)
        assert not subscriber._buffers[0].owned
        await subscriber.finish()
        replacement.settle_delivery.assert_awaited_once()


async def test_sqs_shutdown_while_paused_does_not_receive_again() -> None:
    waiting, released = asyncio.Event(), asyncio.Event()
    client = Mock(receive_message=AsyncMock())
    server = Mock(
        _client=client,
        _get_queue_url=AsyncMock(return_value="url"),
        _active_subscribers=set(),
    )
    subscriber = SqsSubscriber(server, {})

    async def wait_for_resume() -> None:
        waiting.set()
        await released.wait()

    with patch.object(subscriber._paused_event, "wait", side_effect=wait_for_resume):
        consuming = asyncio.create_task(subscriber._consume_channel("jobs"))
        await asyncio.wait_for(waiting.wait(), 2)
        subscriber._shutdown_event.set()
        released.set()
        await asyncio.wait_for(consuming, 2)
    await subscriber.finish()
    client.receive_message.assert_not_awaited()
