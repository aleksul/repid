from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from aiokafka import OffsetAndMetadata
from aiokafka.structs import TopicPartition

from repid import Concurrency, LeaseRenewal, Repid, Router
from repid.connections.abc import MessageAction, ReceivedMessageT
from repid.connections.kafka import message_broker
from repid.connections.kafka.message import KafkaReceivedMessage
from repid.connections.kafka.message_broker import KafkaServer
from repid.connections.kafka.subscriber import KafkaSubscriber, _PartitionDeliveryState


@patch.object(message_broker, "AIOKafkaProducer")
async def test_kafka_connect_failed_start_is_stopped_and_can_retry(
    mock_producer_cls: MagicMock,
) -> None:
    failed_producer = MagicMock(
        start=AsyncMock(side_effect=ConnectionError("offline")),
        stop=AsyncMock(),
    )
    connected_producer = MagicMock(start=AsyncMock(), stop=AsyncMock())
    mock_producer_cls.side_effect = [failed_producer, connected_producer]
    server = KafkaServer("localhost:9092")

    with pytest.raises(ConnectionError, match="offline"):
        await server.connect()

    assert not server.is_connected
    failed_producer.stop.assert_awaited_once()

    await server.connect()

    assert server.is_connected
    assert mock_producer_cls.call_count == 2
    connected_producer.start.assert_awaited_once()


@patch.object(message_broker, "AIOKafkaProducer")
async def test_kafka_connect_cancelled_start_is_stopped_and_not_published(
    mock_producer_cls: MagicMock,
) -> None:
    producer = MagicMock(start=AsyncMock(side_effect=asyncio.CancelledError), stop=AsyncMock())
    mock_producer_cls.return_value = producer
    server = KafkaServer("localhost:9092")

    with pytest.raises(asyncio.CancelledError):
        await server.connect()

    assert not server.is_connected
    producer.stop.assert_awaited_once()


@patch.object(message_broker, "AIOKafkaProducer")
async def test_kafka_connect_preserves_start_error_when_cleanup_fails(
    mock_producer_cls: MagicMock,
) -> None:
    producer = MagicMock(
        start=AsyncMock(side_effect=ConnectionError("startup failed")),
        stop=AsyncMock(side_effect=RuntimeError("cleanup failed")),
    )
    mock_producer_cls.return_value = producer
    server = KafkaServer("localhost:9092")

    with pytest.raises(ConnectionError, match="startup failed"):
        await server.connect()

    assert not server.is_connected
    producer.stop.assert_awaited_once()


async def test_kafka_commit_failure_and_concurrent_completion() -> None:
    consumer = MagicMock(commit=AsyncMock(), stop=AsyncMock())
    subscriber = KafkaSubscriber(MagicMock(), consumer, {})
    partition = TopicPartition("topic", 0)
    state = _PartitionDeliveryState(offsets={0: False, 1: False, 2: False})
    subscriber._partitions[partition] = state
    await subscriber._mark_complete(1, partition, state)
    consumer.commit.assert_not_awaited()
    consumer.commit.side_effect = ConnectionError("commit failed")
    with pytest.raises(ConnectionError, match="commit failed"):
        await subscriber._mark_complete(0, partition, state)
    assert state.offsets == {0: True, 1: True, 2: False}
    consumer.commit.side_effect = None
    await asyncio.gather(
        subscriber._mark_complete(0, partition, state),
        subscriber._mark_complete(2, partition, state),
    )
    assert not state.offsets
    await subscriber._mark_complete(0, partition, state)
    assert consumer.commit.call_args.args[0][partition].offset == 3
    await subscriber.close()


async def test_kafka_commit_finishes_before_next_commit_when_caller_is_cancelled() -> None:
    partition = TopicPartition("topic", 0)
    started = asyncio.Event()
    release = asyncio.Event()

    async def commit(offsets: dict[Any, Any]) -> None:
        if offsets[partition].offset == 1:
            started.set()
            await release.wait()

    consumer = MagicMock(commit=AsyncMock(side_effect=commit), stop=AsyncMock())
    subscriber = KafkaSubscriber(MagicMock(), consumer, {})
    state = _PartitionDeliveryState(offsets={0: False, 1: False})
    subscriber._partitions[partition] = state
    first = asyncio.create_task(subscriber._mark_complete(0, partition, state))
    try:
        async with asyncio.timeout(1):
            await started.wait()
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            second = asyncio.create_task(subscriber._mark_complete(1, partition, state))
            await asyncio.sleep(0)
            consumer.commit.assert_awaited_once_with({partition: OffsetAndMetadata(1, "")})
            release.set()
            await second
        assert consumer.commit.await_args_list[-1].args == ({partition: OffsetAndMetadata(2, "")},)
    finally:
        release.set()
        await subscriber.close()


async def test_kafka_rebalance_revokes_pending_commit() -> None:
    partition = TopicPartition("topic", 0)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def commit(_offsets: dict[Any, Any]) -> None:
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    consumer = MagicMock(commit=AsyncMock(side_effect=commit), stop=AsyncMock())
    subscriber = KafkaSubscriber(MagicMock(), consumer, {})
    state = _PartitionDeliveryState(offsets={0: False})
    subscriber._partitions[partition] = state
    old = asyncio.create_task(subscriber._mark_complete(0, partition, state))
    try:
        async with asyncio.timeout(1):
            await started.wait()
            subscriber.on_partitions_revoked({partition})
            await cancelled.wait()
            await old
        assert not state.owned
        assert partition not in subscriber._partitions
    finally:
        await subscriber.close()


@pytest.mark.parametrize(
    ("disposition", "action"),
    [("reject", MessageAction.rejected), ("nack", MessageAction.nacked)],
)
async def test_kafka_concurrent_dispositions_publish_only_once(
    disposition: str,
    action: MessageAction,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def publish(**_: Any) -> None:
        started.set()
        await release.wait()

    server = KafkaServer("localhost:9092")
    producer = MagicMock(send_and_wait=AsyncMock(side_effect=publish))
    server._producer = producer
    commit = AsyncMock()
    record = MagicMock(topic="topic", partition=0, offset=0, value=b"job", headers=None)
    message = KafkaReceivedMessage(server, record, commit, is_owned=lambda: True)
    settlement = asyncio.create_task(getattr(message, disposition)())
    try:
        async with asyncio.timeout(1):
            await started.wait()
            await message.ack()
            await message.reject()
            await message.nack()
            commit.assert_not_awaited()
    finally:
        release.set()
        await settlement
    assert message.action is action
    producer.send_and_wait.assert_awaited_once()
    commit.assert_awaited_once()


@pytest.mark.parametrize("disposition", ["nack", "reject"])
@pytest.mark.parametrize("publish_fails", [False, True])
async def test_kafka_cancelled_publication_completes_or_releases_its_settlement(
    disposition: str,
    publish_fails: bool,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def publish(**_: Any) -> None:
        started.set()
        await release.wait()
        if publish_fails:
            raise ConnectionError("publication failed")

    server = KafkaServer("localhost:9092")
    producer = MagicMock(send_and_wait=AsyncMock(side_effect=publish))
    server._producer = producer
    consumer = MagicMock(commit=AsyncMock(), stop=AsyncMock())
    subscriber = KafkaSubscriber(server, consumer, {})
    partition = TopicPartition("topic", 0)
    state = _PartitionDeliveryState(offsets={0: False, 1: False})
    subscriber._partitions[partition] = state
    record = MagicMock(topic="topic", partition=0, offset=0, value=b"job", headers=None)
    message = KafkaReceivedMessage(
        server,
        record,
        lambda: subscriber._mark_complete(0, partition, state),
        is_owned=lambda: state.owned,
    )
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    errors: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _, context: errors.append(context))
    settlement = asyncio.create_task(getattr(message, disposition)())
    try:
        async with asyncio.timeout(1):
            await started.wait()
            settlement.cancel()
            await asyncio.sleep(0)
            assert not settlement.done()
            await getattr(message, disposition)()
            await subscriber._mark_complete(1, partition, state)
            consumer.commit.assert_not_awaited()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await settlement
            if publish_fails:
                assert message.action is None
                assert state.offsets == {0: False, 1: True}
                producer.send_and_wait.side_effect = None
                await getattr(message, disposition)()
            assert not state.offsets
            consumer.commit.assert_awaited_once_with({partition: OffsetAndMetadata(2, "")})
            assert producer.send_and_wait.await_count == (2 if publish_fails else 1)
            assert not errors
    finally:
        release.set()
        await asyncio.gather(settlement, return_exceptions=True)
        loop.set_exception_handler(previous_handler)
        await subscriber.close()


async def test_kafka_cancelled_ack_waiting_for_commit_lock_still_completes() -> None:
    server = KafkaServer("localhost:9092")
    consumer = MagicMock(commit=AsyncMock(), stop=AsyncMock())
    subscriber = KafkaSubscriber(server, consumer, {})
    partition = TopicPartition("topic", 0)
    state = _PartitionDeliveryState(offsets={0: False, 1: False})
    subscriber._partitions[partition] = state
    record = MagicMock(topic="topic", partition=0, offset=0, value=b"job", headers=None)
    message = KafkaReceivedMessage(
        server,
        record,
        lambda: subscriber._mark_complete(0, partition, state),
        is_owned=lambda: state.owned,
    )
    await state.lock.acquire()
    settlement = asyncio.create_task(message.ack())
    try:
        async with asyncio.timeout(1):
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert message.action is MessageAction.acked
            assert state.offsets == {0: False, 1: False}
            settlement.cancel()
            await asyncio.sleep(0)
            assert not settlement.done()
            state.lock.release()
            with pytest.raises(asyncio.CancelledError):
                await settlement
            await subscriber._mark_complete(1, partition, state)
            assert not state.offsets
            assert consumer.commit.await_args_list[-1].args == (
                {partition: OffsetAndMetadata(2, "")},
            )
    finally:
        if state.lock.locked():
            state.lock.release()
        await asyncio.gather(settlement, return_exceptions=True)
        await subscriber.close()


@pytest.mark.parametrize("revoke_before_publication", [False, True])
async def test_kafka_reserved_publication_cannot_commit_a_revoked_assignment(
    revoke_before_publication: bool,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def publish(**_: Any) -> None:
        started.set()
        await release.wait()

    server = KafkaServer("localhost:9092")
    producer = MagicMock(send_and_wait=AsyncMock(side_effect=publish))
    server._producer = producer
    consumer = MagicMock(commit=AsyncMock(), stop=AsyncMock())
    subscriber = KafkaSubscriber(server, consumer, {})
    partition = TopicPartition("topic", 0)
    state = _PartitionDeliveryState(offsets={0: False})
    subscriber._partitions[partition] = state
    record = MagicMock(topic="topic", partition=0, offset=0, value=b"job", headers=None)
    message = KafkaReceivedMessage(
        server,
        record,
        lambda: subscriber._mark_complete(0, partition, state),
        is_owned=lambda: state.owned,
    )
    settlement = asyncio.create_task(message.reject())
    try:
        async with asyncio.timeout(1):
            if revoke_before_publication:
                await asyncio.sleep(0)
            else:
                await started.wait()
            subscriber.on_partitions_revoked({partition})
            release.set()
            await settlement
            consumer.commit.assert_not_awaited()
            assert state.offsets == {0: False}
            assert producer.send_and_wait.await_count == (0 if revoke_before_publication else 1)
    finally:
        release.set()
        await asyncio.gather(settlement, return_exceptions=True)
        await subscriber.close()


@patch.object(message_broker, "AIOKafkaConsumer")
async def test_kafka_rebalance_fences_stale_delivery_and_cancels_pending_callback(
    mock_consumer: MagicMock,
) -> None:
    partition = TopicPartition("topic", 0)
    deliveries: asyncio.Queue[ReceivedMessageT] = asyncio.Queue()
    reassigned = asyncio.Event()
    callback_cancelled = asyncio.Event()
    paused: set[TopicPartition] = set()
    record = MagicMock(topic="topic", partition=0, offset=0, value=b"job")

    async def getmany(**_: Any) -> dict[TopicPartition, list[MagicMock]]:
        if consumer.getmany.await_count == 1:
            return {partition: [record]}
        if consumer.getmany.await_count == 2:
            await reassigned.wait()
            return {partition: [record]}
        await asyncio.Future()
        return {}

    async def on_message(message: ReceivedMessageT, _renewal: LeaseRenewal | None = None) -> None:
        deliveries.put_nowait(message)
        if not reassigned.is_set():
            try:
                await asyncio.Future()
            finally:
                callback_cancelled.set()

    consumer = MagicMock(
        start=AsyncMock(),
        stop=AsyncMock(),
        getmany=AsyncMock(side_effect=getmany),
        commit=AsyncMock(),
        assignment=Mock(return_value={partition}),
        paused=Mock(side_effect=paused.copy),
        pause=Mock(side_effect=lambda *partitions: paused.update(partitions)),  # pyrefly: ignore[implicit-any-lambda]
        resume=Mock(side_effect=lambda *partitions: paused.difference_update(partitions)),  # pyrefly: ignore[implicit-any-lambda]
    )
    mock_consumer.return_value = consumer
    server = KafkaServer("localhost:9092")
    producer = MagicMock(send_and_wait=AsyncMock())
    server._producer = producer
    subscriber = await server.subscribe(
        channels_to_callbacks={"topic": on_message},
        concurrency=Concurrency(messages=1),
        channel_concurrency={},
    )
    listener = consumer.subscribe.call_args.kwargs["listener"]
    try:
        async with asyncio.timeout(1):
            old = await deliveries.get()
            await subscriber.pause()
            listener.on_partitions_revoked({partition})
            await callback_cancelled.wait()

            paused.clear()  # aiokafka creates new pause state on assignment.
            listener.on_partitions_assigned({partition})
            assert paused == {partition}
            await subscriber.resume()
            assert not paused
            reassigned.set()
            current = await deliveries.get()

            await old.ack()
            await old.reject()
            await old.nack()
            producer.send_and_wait.assert_not_awaited()
            consumer.commit.assert_not_awaited()
            await current.ack()
        consumer.commit.assert_awaited_once_with({partition: OffsetAndMetadata(1, "")})
    finally:
        await subscriber.close()


@pytest.mark.parametrize("cleanup_fails", [False, True])
@patch.object(message_broker, "AIOKafkaConsumer")
async def test_kafka_subscribe_closes_consumer_after_cancelled_start(
    mock_consumer: MagicMock,
    cleanup_fails: bool,
) -> None:
    consumer = MagicMock(
        start=AsyncMock(side_effect=asyncio.CancelledError),
        stop=AsyncMock(side_effect=RuntimeError("stop failed") if cleanup_fails else None),
    )
    mock_consumer.return_value = consumer
    server = KafkaServer("localhost:9092")
    server._producer = MagicMock()
    with pytest.raises(asyncio.CancelledError):
        await server.subscribe(
            channels_to_callbacks={"topic": AsyncMock()},
            concurrency=Concurrency(),
            channel_concurrency={},
        )
    consumer.stop.assert_awaited_once()
    assert not server._active_subscribers


@patch.object(message_broker, "AIOKafkaConsumer")
async def test_kafka_disconnect_stops_consumer_after_poll_failure(mock_consumer: MagicMock) -> None:
    consumer = MagicMock(
        start=AsyncMock(),
        stop=AsyncMock(),
        getmany=AsyncMock(side_effect=ConnectionError("poll failed")),
    )
    mock_consumer.return_value = consumer
    server = KafkaServer("localhost:9092")
    server._producer = MagicMock(stop=AsyncMock())
    subscriber = await server.subscribe(
        channels_to_callbacks={"topic": AsyncMock()},
        concurrency=Concurrency(),
        channel_concurrency={},
    )
    with pytest.raises(ConnectionError, match="poll failed"):
        await subscriber.task
    assert not subscriber.is_active
    await server.disconnect()
    await subscriber.close()
    consumer.stop.assert_awaited_once()
    assert not server._active_subscribers


@pytest.mark.parametrize(
    ("limit", "channel_concurrency", "count", "fetch_max_bytes"),
    [
        (3, None, 3, None),
        (None, None, 1000, None),
        (1000, {"topic": Concurrency(messages=3, payload_bytes=10240)}, 3, 10240),
    ],
)
@patch.object(message_broker, "AIOKafkaConsumer")
async def test_kafka_poll_batch_limit(
    mock_consumer: MagicMock,
    limit: int | None,
    channel_concurrency: dict[str, Concurrency] | None,
    count: int,
    fetch_max_bytes: int | None,
) -> None:
    consumer = MagicMock(
        start=AsyncMock(),
        stop=AsyncMock(),
        assignment=Mock(return_value=set()),
        paused=Mock(return_value=set()),
    )
    consumer.getmany = AsyncMock(side_effect=asyncio.CancelledError)
    mock_consumer.return_value = consumer
    server = KafkaServer("localhost:9092")
    server._producer = MagicMock()
    subscriber = await server.subscribe(
        channels_to_callbacks={"topic": AsyncMock(), "other": AsyncMock()},
        concurrency=Concurrency(messages=limit),
        channel_concurrency=channel_concurrency or {},
    )
    await asyncio.sleep(0)
    consumer.getmany.assert_awaited_once_with(timeout_ms=1000, max_records=count)
    assert mock_consumer.call_args.kwargs.get("fetch_max_bytes") == fetch_max_bytes
    await subscriber.close()


@patch.object(message_broker, "AIOKafkaConsumer")
async def test_kafka_saturated_topic_does_not_block_other_topics(mock_consumer: MagicMock) -> None:
    first, other = TopicPartition("a", 0), TopicPartition("b", 0)
    paused: set[TopicPartition] = set()
    release = asyncio.Event()
    other_started = asyncio.Event()
    resumed = asyncio.Event()
    delivered = set()

    async def getmany(**_: Any) -> dict[TopicPartition, list[MagicMock]]:
        for partition in (first, other):
            if partition not in delivered and partition not in paused:
                delivered.add(partition)
                return {partition: [MagicMock(topic=partition.topic, offset=0)]}
        await asyncio.Event().wait()
        return {}

    def resume(*partitions: Any) -> None:
        paused.difference_update(partitions)
        if first in partitions:
            resumed.set()

    async def blocked(_message: Any, _renewal: LeaseRenewal | None = None) -> None:
        await release.wait()

    consumer = MagicMock(
        start=AsyncMock(),
        stop=AsyncMock(),
        getmany=AsyncMock(side_effect=getmany),
        assignment=Mock(return_value={first, other}),
        paused=Mock(side_effect=paused.copy),
        pause=Mock(side_effect=lambda *partitions: paused.update(partitions)),  # pyrefly: ignore[implicit-any-lambda]
        resume=Mock(side_effect=resume),
    )
    mock_consumer.return_value = consumer
    server = KafkaServer("localhost:9092")
    server._producer = MagicMock()
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
        assert first in paused
        await subscriber.pause()
        await subscriber.resume()
        assert first in paused
        release.set()
        await asyncio.wait_for(resumed.wait(), 1)
    finally:
        release.set()
        await subscriber.close()


@patch.object(message_broker, "AIOKafkaConsumer")
async def test_kafka_republishes_unadmitted_record_without_committed_offset(
    mock_consumer: MagicMock,
) -> None:
    admitted, refused = TopicPartition("jobs", 0), TopicPartition("jobs", 1)
    headers = [("topic", b"job"), ("content-type", b"application/json")]
    records = {
        partition: [
            MagicMock(
                topic="jobs",
                partition=partition.partition,
                offset=0,
                value=b"{}",
                headers=headers,
            ),
        ]
        for partition in (admitted, refused)
    }
    returning = asyncio.Event()

    async def poll(**_: Any) -> dict[TopicPartition, list[MagicMock]]:
        if consumer.getmany.await_count == 1:
            return records
        await asyncio.Event().wait()
        return {}

    consumer = MagicMock(
        start=AsyncMock(),
        stop=AsyncMock(),
        getmany=AsyncMock(side_effect=poll),
        commit=AsyncMock(),
    )
    mock_consumer.return_value = consumer
    producer = MagicMock(send_and_wait=AsyncMock(side_effect=lambda *_, **__: returning.set()))  # pyrefly: ignore[implicit-any-lambda]
    server = KafkaServer("localhost:9092", auto_offset_reset="latest")
    server._producer = producer
    app = Repid()
    app.servers.register_server("default", server, is_default=True)
    router = Router()

    @router.actor(channel="jobs")
    async def job() -> None:
        await asyncio.wait_for(returning.wait(), 1)

    app.include_router(router)
    result = await asyncio.wait_for(
        app.run_worker(concurrency=Concurrency(messages=2), messages_limit=1, register_signals=[]),
        2,
    )

    assert result.processed == 1
    producer.send_and_wait.assert_awaited_once()
    consumer.commit.assert_any_await({admitted: OffsetAndMetadata(1, "")})
    consumer.commit.assert_any_await({refused: OffsetAndMetadata(1, "")})


@patch.object(message_broker, "AIOKafkaConsumer")
async def test_kafka_keeps_polling_paused_partitions(mock_consumer: MagicMock) -> None:
    partition = TopicPartition("jobs", 0)
    first_poll = asyncio.Event()
    finish_poll = asyncio.Event()
    paused_poll = asyncio.Event()
    paused: set[TopicPartition] = set()

    async def poll(**_: object) -> dict[TopicPartition, list[MagicMock]]:
        if not first_poll.is_set():
            first_poll.set()
            await finish_poll.wait()
            return {}
        assert paused == {partition}
        paused_poll.set()
        return await asyncio.Future()

    consumer = MagicMock(
        start=AsyncMock(),
        stop=AsyncMock(),
        getmany=AsyncMock(side_effect=poll),
        assignment=Mock(return_value={partition}),
        paused=Mock(side_effect=paused.copy),
        pause=Mock(side_effect=lambda *partitions: paused.update(partitions)),  # pyrefly: ignore[implicit-any-lambda]
    )
    mock_consumer.return_value = consumer
    server = KafkaServer("localhost:9092")
    server._producer = MagicMock()
    subscriber = await server.subscribe(
        channels_to_callbacks={"jobs": AsyncMock()},
        concurrency=Concurrency(messages=1),
        channel_concurrency={},
    )
    try:
        async with asyncio.timeout(1):
            await first_poll.wait()
            await subscriber.pause()
            finish_poll.set()
            await paused_poll.wait()
    finally:
        await subscriber.close()
