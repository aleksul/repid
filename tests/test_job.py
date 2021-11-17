from datetime import datetime, timedelta

import pytest
from aioredis import Redis

from repid import Job, JobStatus


@pytest.mark.asyncio()
async def test_job_defer_until_and_by(redis: Redis):
    with pytest.raises(ValueError, match=".*'defer_until' AND 'defer_by'.*"):
        Job(redis, "awesome_job", defer_until=datetime.now(), defer_by=timedelta(seconds=3))


@pytest.mark.asyncio()
async def test_no_reccuring_no_schedule(redis: Redis):
    j = Job(redis, "awesome_job")
    assert not j.is_defered


@pytest.mark.asyncio()
async def test_schedule(redis: Redis):
    defer_until = int(datetime.utcnow().timestamp())
    j = Job(redis, "awesome_job", defer_until=defer_until + 1)
    while j.defer_until >= int(datetime.utcnow().timestamp()):  # type: ignore
        pass
    assert bool(j.is_defer_until)


@pytest.mark.asyncio()
async def test_reccuring(redis: Redis):
    j = Job(redis, "awesome_job", defer_by=timedelta(seconds=10))
    assert bool(await j.is_defer_by)


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
