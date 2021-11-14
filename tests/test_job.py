import asyncio
from datetime import datetime, timedelta

import pytest
from aioredis import Redis

from repid import Job, JobStatus


@pytest.mark.asyncio()
async def test_job_defer_until_and_by(redis: Redis):
    with pytest.raises(ValueError, match=".*'defer_until' AND 'defer_by'.*"):
        Job(redis, "awesome_job", defer_until=datetime.now(), defer_by=timedelta(seconds=3))


@pytest.mark.asyncio()
async def test_expire(redis: Redis):
    j = Job(redis, "awesome_job")
    await j.enqueue(expires_in=10)
    seconds_left = await j.expires_in
    assert type(seconds_left) is int
    assert 0 < seconds_left <= 10


@pytest.mark.asyncio()
async def test_no_expire(redis: Redis):
    j = Job(redis, "awesome_job")
    await j.enqueue(expires_in=None)
    assert (await j.expires_in) is None


@pytest.mark.asyncio()
async def test_no_reccuring_no_schedule(redis: Redis):
    j = Job(redis, "awesome_job")
    assert bool(j.is_scheduled_now())
    assert bool(await j.is_reccuring_now())


@pytest.mark.asyncio()
async def test_schedule(redis: Redis):
    j = Job(redis, "awesome_job", defer_until=datetime.now() + timedelta(seconds=1))
    assert not bool(j.is_scheduled_now())
    await asyncio.sleep(1)
    assert bool(j.is_scheduled_now())


@pytest.mark.asyncio()
async def test_reccuring(redis: Redis):
    j = Job(redis, "awesome_job", defer_by=timedelta(seconds=10))
    assert bool(await j.is_reccuring_now())


@pytest.mark.asyncio()
async def test_wrong_equals(redis: Redis):
    j = Job(redis, "awesome_job")
    assert not (j == 123)


@pytest.mark.asyncio()
async def test_status(redis: Redis):
    j = Job(redis, "awesome_job")
    assert await j.status == JobStatus.NOT_FOUND
    await j.enqueue()
    assert await j.status == JobStatus.QUEUED
