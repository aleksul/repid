import asyncio
from datetime import timedelta
from typing import AsyncIterator

import pytest

from repid import Connection, Job, Worker


@pytest.fixture(autouse=True)
async def seed_conn(
    fake_connection: Connection,
    request: pytest.FixtureRequest,
) -> AsyncIterator[Connection]:
    j = Job("awesome_job")

    await j.queue.declare()
    await j.queue.flush()

    amount_of_jobs = getattr(request, "param", 1)

    if amount_of_jobs > 0:
        await asyncio.gather(*[j.enqueue() for _ in range(amount_of_jobs)])

    yield fake_connection

    await j.queue.delete()


@pytest.mark.parametrize("seed_conn", [15], indirect=True)
async def test_more_concurrent_tasks_than_limit() -> None:
    w = Worker(messages_limit=15, tasks_limit=10)

    hit = 0

    @w.actor
    async def awesome_job() -> None:
        nonlocal hit
        await asyncio.sleep(1.0)
        hit += 1

    await asyncio.wait_for(w.run(), timeout=5.0)

    assert hit == 15


@pytest.mark.parametrize("seed_conn", [15], indirect=True)
async def test_rejected_after_timeout(seed_conn: Connection) -> None:
    consumer = seed_conn.message_broker.get_consumer("default", ["awesome_job"])
    async with consumer:
        await asyncio.sleep(0)

    with pytest.raises(RuntimeError):
        # can't consume finished consumer
        await consumer.consume()


@pytest.mark.parametrize("seed_conn", [15], indirect=True)
async def test_paused_consumer(seed_conn: Connection) -> None:
    consumer = seed_conn.message_broker.get_consumer("default", ["awesome_job"])
    await consumer.pause()
    async with consumer:
        with pytest.raises(asyncio.TimeoutError):
            # there should be nothing to consume, since the consumer has been paused
            await asyncio.wait_for(consumer.consume(), 0.2)


async def test_finishing_consumer_without_start(seed_conn: Connection) -> None:
    consumer = seed_conn.message_broker.get_consumer("default", ["awesome_job"])
    await consumer.finish()


async def test_another_topic_is_not_consumed(seed_conn: Connection) -> None:
    consumer = seed_conn.message_broker.get_consumer("default", ["another_topic"])
    await consumer.start()
    with pytest.raises(asyncio.TimeoutError):
        # there should be nothing to consume, since the consumer is interested in another topic
        await asyncio.wait_for(consumer.consume(), 0.2)
    await consumer.finish()


async def test_consume_without_ack(seed_conn: Connection) -> None:
    consumer = seed_conn.message_broker.get_consumer("default", ["awesome_job"])
    async with consumer:
        key, _, _ = await asyncio.wait_for(consumer.consume(), 0.1)
        assert key.topic == "awesome_job"

    async with consumer:
        key, _, _ = await asyncio.wait_for(consumer.consume(), 0.1)
        assert key.topic == "awesome_job"
        await seed_conn.message_broker.ack(key)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(consumer.consume(), 0.2)


@pytest.mark.parametrize("seed_conn", [0], indirect=True)
async def test_ttl(seed_conn: Connection) -> None:
    j = Job("awesome_job", ttl=timedelta(seconds=4))
    await asyncio.gather(*[j.enqueue() for _ in range(2)])

    consumer = seed_conn.message_broker.get_consumer(j.queue.name, [j.name])
    async with consumer:
        key, _, _ = await asyncio.wait_for(consumer.consume(), 1.0)
        assert key.topic == "awesome_job"
        await seed_conn.message_broker.ack(key)

    await asyncio.sleep(4.0)  # wait for TTL to expire on the second job

    consumer = seed_conn.message_broker.get_consumer(j.queue.name, [j.name])
    async with consumer:
        with pytest.raises(asyncio.TimeoutError):
            # there should be nothing to consume, since the second job has expired
            await asyncio.wait_for(consumer.consume(), 1.0)
