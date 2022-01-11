import orjson
import pytest
from aioredis import Redis

from repid import JOB_PREFIX, QUEUE_PREFIX, Job, Queue


def test_inappropriate_queue_name(redis: Redis):
    with pytest.raises(ValueError, match="Queue name must"):
        Queue(redis, "some!@#$%^inappropriate_name")


@pytest.mark.asyncio()
async def test_add_job(redis: Redis):
    j = Job(redis, "some_name")
    await redis.set(
        JOB_PREFIX + j._id,
        orjson.dumps(j.__as_dict__()),
        nx=True,
    )
    q = Queue(redis)
    await q.add_job(j._id)
    assert await redis.lrange(QUEUE_PREFIX + "default", 0, -1) == [j._id]


@pytest.mark.asyncio()
async def test_is_job_queued(redis: Redis):
    j1 = Job(redis, "some_name")
    j2 = Job(redis, "some_name", defer_by=3000)
    q = Queue(redis)
    assert await q.is_job_queued(j1._id) is False
    assert await q.is_job_queued(j2._id) is False
    await j1.enqueue()
    assert await q.is_job_queued(j1._id) is True
    assert await q.is_job_queued(j2._id) is False
    await j2.enqueue()
    assert await q.is_job_queued(j1._id) is True
    assert await q.is_job_queued(j2._id) is True
    await j1.delete()
    assert await q.is_job_queued(j1._id) is False
    assert await q.is_job_queued(j2._id) is True


def test_different_types_not_equal(redis: Redis):
    q = Queue(redis)
    assert not (q == 123)


def test_str(redis: Redis):
    q = Queue(redis)
    assert str(q) == "Queue(default)"


@pytest.mark.asyncio()
async def test_queue_ids(redis: Redis):
    q = Queue(redis)
    j1 = Job(redis, "some_name")
    j2 = Job(redis, "some_name", defer_by=3000)
    assert await q.ids == []
    assert await q.normal_queue_ids == []
    assert await q.deferred_queue_ids == []
    await j1.enqueue()
    assert await q.ids == [j1._id]
    assert await q.normal_queue_ids == [j1._id]
    assert await q.deferred_queue_ids == []
    await j2.enqueue()
    assert await q.ids == [j1._id, j2._id]
    assert await q.normal_queue_ids == [j1._id]
    assert await q.deferred_queue_ids == [j2._id]
    await j1.delete()
    assert await q.ids == [j2._id]
    assert await q.normal_queue_ids == []
    assert await q.deferred_queue_ids == [j2._id]


@pytest.mark.asyncio()
async def test_clean_queue(redis: Redis):
    q = Queue(redis)
    await q.add_job("unreal_job")
    assert await q.ids == ["unreal_job"]
    await q.clean()
    assert await q.ids == []


@pytest.mark.asyncio()
async def test_clear_queue(redis: Redis):
    q = Queue(redis)
    j = Job(redis, "some_name")
    await j.enqueue()
    assert await q.ids == [j._id]
    await q.clear()
    assert await q.ids == []


@pytest.mark.asyncio()
async def test_remove_job(redis: Redis):
    q = Queue(redis)
    j1 = Job(redis, "some_name")
    j2 = Job(redis, "some_name", defer_by=3000)
    await j1.enqueue()
    await j2.enqueue()
    assert await q.ids == [j1._id, j2._id]
    await q.remove_job(j2._id)
    assert await q.ids == [j1._id]
