import asyncio
from datetime import datetime, timedelta

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
    assert await w.get_queue("default").jobs == [j]
    await w.run()
    assert await w.get_queue("default").jobs == []
    assert await j.status == JobStatus.DONE
    assert (await j.result).result == 1  # type: ignore


@pytest.mark.asyncio()
async def test_sync_actor(redis: Redis):
    w = Worker(redis)

    @w.actor(name="renamed_job")
    def super_duper_job_2():
        return 11

    j = await w.enqueue_job("renamed_job")
    assert await w.get_queue("default").jobs == [j]
    await w.run()
    assert (await j.result).result == 11  # type: ignore


@pytest.mark.asyncio()
async def test_no_actor(redis: Redis):
    w = Worker(redis)
    j = await w.enqueue_job("non-existing-job")
    assert await w.get_queue("default").jobs == [j]
    await w.run()
    assert await w.get_queue("default").jobs == [j]

    @w.actor(name="non-existing-job")
    async def now_existing_job():
        return 111

    await w.run()
    assert (await j.result).result == 111  # type: ignore
    assert await w.get_queue("default").jobs == []


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
async def test_reccuring_job(redis: Redis):
    w = Worker(redis)

    @w.actor()
    def reccuring_job():
        return datetime.now().timestamp()

    j = await w.enqueue_job("reccuring_job", defer_by=timedelta(seconds=3))
    assert await w.get_queue("default").jobs == [j]
    await w.run()
    first_result = await j.result
    assert await w.get_queue("default").jobs == [j]
    assert not await j.is_defer_by
    await asyncio.sleep(4)
    assert await j.is_defer_by
    await w.run()
    second_result = await j.result
    assert first_result.result != second_result.result  # type: ignore
