import asyncio

import pytest
from aioredis import Redis

from repid import Repid


@pytest.mark.asyncio()
async def test_no_queues(redis: Redis):
    r = Repid(redis)
    queues = await r._get_all_queues()
    assert len(queues) == 0


@pytest.mark.asyncio()
async def test_enqueue_job(redis: Redis):
    r = Repid(redis)
    job = await r.enqueue_job("super_job")
    assert await r._get_all_queues() == ["default"]
    assert await r._get_queued_jobs_ids("default") == [job._id_redis]
    assert await r._get_queued_jobs("default") == [job]


@pytest.mark.asyncio()
async def test_job_expired_but_still_in_queue(redis: Redis):
    r = Repid(redis)
    job = await r.enqueue_job("super_job", expires_in=1)
    assert await r._get_queued_jobs("default") == [job]
    await asyncio.sleep(1)
    assert await r._get_queued_jobs("default") == []
