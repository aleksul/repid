import pytest
from aioredis import Redis

from repid import Queue, Repid


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
