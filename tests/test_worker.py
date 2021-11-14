import asyncio

import pytest
from aioredis import Redis

from repid import JobStatus, Worker


@pytest.mark.asyncio()
async def test_actor_and_its_result(redis: Redis):
    w = Worker(redis)

    @w.actor()
    async def super_duper_job():
        await asyncio.sleep(0)
        return 1

    j = await w.enqueue_job("super_duper_job")
    assert await w._get_queued_jobs("default") == [j]
    await w.run()
    assert await w._get_queued_jobs("default") == []
    assert await j.status == JobStatus.DONE
    assert (await j.result).result == 1  # type: ignore


@pytest.mark.asyncio()
async def test_sync_actor(redis: Redis):
    w = Worker(redis)

    @w.actor(name="renamed_job")
    def super_duper_job_2():
        return 11

    j = await w.enqueue_job("renamed_job")
    assert await w._get_queued_jobs("default") == [j]
    await w.run()
    assert (await j.result).result == 11  # type: ignore


@pytest.mark.asyncio()
async def test_no_actor(redis: Redis):
    w = Worker(redis)
    j = await w.enqueue_job("non-existing-job")
    assert await w._get_queued_jobs("default") == [j]
    await w.run()
    assert await w._get_queued_jobs("default") == [j]

    @w.actor(name="non-existing-job")
    async def now_existing_job():
        return 111

    await w.run()
    assert (await j.result).result == 111  # type: ignore
    assert await w._get_queued_jobs("default") == []


@pytest.mark.asyncio()
async def test_actor_with_exception(redis: Redis):
    w = Worker(redis)

    @w.actor(retries=3)
    def exception_job():
        raise Exception("Some unhandled exception.")

    j = await w.enqueue_job("exception_job")
    await w.run()
    assert (await j.result).success is False  # type: ignore


@pytest.mark.asyncio()
async def test_pop_job(redis: Redis):
    w = Worker(redis)
    assert await w._pop_job("queue:default") is None

    await w.enqueue_job("expired_job", expires_in=1)
    await asyncio.sleep(1)
    assert await w._pop_job("queue:default") is None

    j = await w.enqueue_job("normal_job")
    assert await w._pop_job("queue:default") == j


@pytest.mark.asyncio()
async def test_expired_job(redis: Redis):
    w = Worker(redis)

    @w.actor()
    def absolutely_normal_job():
        return 1

    j = await w.enqueue_job("absolutely_normal_job", expires_in=1)
    await asyncio.sleep(1)
    assert await w._get_queued_jobs_ids("default") == [j._id_redis]
    await w.run()
    assert await w._get_queued_jobs_ids("default") == []


"""
@pytest.mark.asyncio()
async def test_reccuring_job(redis: Redis):
    w = Worker(redis)

    @w.actor()
    def reccuring_job():
        return datetime.now()

    j = await w.enqueue_job("reccuring_job", defer_by=timedelta(seconds=3))
    assert await w._get_queued_jobs_ids("default") == [j._id_redis]
    await w.run()
    first_result = await j.result
    assert await w._get_queued_jobs_ids("default") == [j._id_redis]
    assert not await j.is_reccuring_now()
    await asyncio.sleep(3)
    await w.run()
    second_result = await j.result
    assert first_result.result != second_result.result
"""
