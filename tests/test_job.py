from datetime import datetime, timedelta

import pytest
from aioredis import Redis

from repid import Job, JobStatus
from repid.queue import JOB_PREFIX, Queue


@pytest.mark.asyncio()
async def test_job_defer_until_and_by(redis: Redis):
    with pytest.raises(ValueError, match=".*'defer_until' AND 'defer_by'.*"):
        Job(redis, "awesome_job", defer_until=datetime.now(), defer_by=timedelta(seconds=3))


@pytest.mark.asyncio()
async def test_no_reccuring_no_schedule(redis: Redis):
    j = Job(redis, "awesome_job")
    assert not j.is_deferred


@pytest.mark.asyncio()
async def test_schedule(redis: Redis):
    defer_until = int(datetime.utcnow().timestamp())
    j1 = Job(redis, "awesome_job", defer_until=defer_until + 1)
    j2 = Job(redis, "awesome_job", defer_until=datetime.utcnow() + timedelta(seconds=1))
    assert not await j1.is_deferred_already
    assert not await j2.is_deferred_already
    while j2.defer_until >= int(datetime.utcnow().timestamp()):  # type: ignore
        pass
    assert await j1.is_deferred_already
    assert await j2.is_deferred_already


@pytest.mark.asyncio()
async def test_reccuring(redis: Redis):
    j = Job(redis, "awesome_job", defer_by=timedelta(seconds=10))
    assert await j.is_deferred_already


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


@pytest.mark.asyncio()
async def test_delete(redis: Redis):
    j = Job(redis, "awesome_job")
    await j.enqueue()
    assert await j.queue.is_job_queued(j._id)
    assert await redis.exists("job:" + j._id)
    await j.delete()
    assert not await j.queue.is_job_queued(j._id)


def test_inappropriate_job_name(redis: Redis):
    with pytest.raises(ValueError, match="Job name must"):
        Job(redis, "some!@#$%^inappropriate_name")


def test_non_default_queue(redis: Redis):
    Job(redis, "awesome_job", queue="nondefault")
    Job(redis, "second_job", queue=Queue(redis, "nondefault"))
    with pytest.raises(ValueError, match="Queue name must"):
        Job(redis, "third_job", queue="some!@#$%^inappropriate_name")


@pytest.mark.asyncio()
async def test_update(redis: Redis):
    j = Job(redis, "awesome_job")
    await j.enqueue()
    assert await j.queue.normal_queue_ids == [j._id]
    assert await j.queue.deferred_queue_ids == []
    j.defer_by = 3
    await j.update()
    assert await j.queue.normal_queue_ids == []
    assert await j.queue.deferred_queue_ids == [j._id]


def test_job_str(redis: Redis):
    j = Job(redis, "awesome_job")
    assert str(j) == f"Job(name=awesome_job, queue=default, _id={JOB_PREFIX}{j._id})"
