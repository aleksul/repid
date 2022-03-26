from datetime import datetime

import pytest
from redis.asyncio import Redis

from repid import Queue, Repid


@pytest.mark.asyncio()
async def test_doesnt_decode_responses():
    redis = Redis()
    with pytest.raises(ValueError, match="Redis instance must decode responses."):
        Repid(redis)
    await redis.close()


@pytest.mark.asyncio()
async def test_no_queues(redis: Redis):
    r = Repid(redis)
    queues = await r.get_all_queues()
    assert len(queues) == 0


@pytest.mark.asyncio()
async def test_enqueue_job(redis: Redis):
    r = Repid(redis)
    job = await r.enqueue_job("super_job")
    assert await r.get_all_queues() == [Queue(redis, "default")]
    assert await r.get_queue("default").ids == [job._id]
    assert await r.get_job(job._id) == job


@pytest.mark.asyncio()
async def test_get_non_existing_job(redis: Redis):
    r = Repid(redis)
    assert await r.get_job("some_job") is None


@pytest.mark.asyncio()
async def test_pop_no_job(redis: Redis):
    r = Repid(redis)
    await r.pop_job("default")


@pytest.mark.asyncio()
async def test_pop_job(redis: Redis):
    r = Repid(redis)
    job = await r.enqueue_job("super_job")
    popped = await r.pop_job("default")
    assert job == popped


@pytest.mark.asyncio()
async def test_pop_deferred_until_job(redis: Redis):
    r = Repid(redis)
    job = await r.enqueue_job("deferred_job", defer_until=int(datetime.utcnow().timestamp()))
    popped = await r._pop_deferred_job("default")
    assert job == popped


@pytest.mark.asyncio()
async def test_pop_normal_job_from_deferred_queue(redis: Redis):
    r = Repid(redis)
    job = await r.enqueue_job("not_a_deferred_job")
    await job.queue.remove_job(job._id)
    await job.queue.add_job(job._id, deferred=True)
    popped = await r._pop_deferred_job("default")
    assert popped is None
    assert len(await job.queue.deferred_queue_ids) == 0
