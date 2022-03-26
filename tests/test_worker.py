import asyncio
from datetime import datetime, timedelta

import pytest
from redis.asyncio import Redis

from repid import JobStatus, Queue, Worker


@pytest.mark.asyncio()
async def test_actor_and_its_result(redis: Redis):
    w = Worker(redis)

    @w.actor()
    async def super_duper_job():
        await asyncio.sleep(0)
        return 1

    j = await w.enqueue_job("super_duper_job")
    assert await j.queue.is_job_queued(j._id)
    await w.run()
    assert not await j.queue.is_job_queued(j._id)
    assert await j.status == JobStatus.DONE
    assert (await j.result).result == 1  # type: ignore


@pytest.mark.asyncio()
async def test_sync_actor(redis: Redis):
    w = Worker(redis)

    @w.actor(name="renamed_job")
    def super_duper_job_2():
        return 11

    j = await w.enqueue_job("renamed_job")
    await w.run()
    assert (await j.result).result == 11  # type: ignore


@pytest.mark.asyncio()
async def test_no_actor(redis: Redis):
    w = Worker(redis)
    j = await w.enqueue_job("non-existing-job")
    assert await j.queue.is_job_queued(j._id)
    await w.run()
    assert await j.queue.is_job_queued(j._id)

    @w.actor(name="non-existing-job")
    async def now_existing_job():
        return 111

    await w.run()
    assert (await j.result).result == 111  # type: ignore
    assert not await j.queue.is_job_queued(j._id)


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
    assert await j.queue.is_job_queued(j._id)
    await w.run()
    first_result = await j.result
    assert await j.queue.is_job_queued(j._id)
    assert not await j.is_deferred_already
    await asyncio.sleep(4)
    assert await j.is_deferred_already
    await w.run()
    second_result = await j.result
    assert first_result.result != second_result.result  # type: ignore


@pytest.mark.asyncio()
async def test_no_jobs(redis: Redis):
    w = Worker(redis)
    await w.run_one_queue(Queue(redis, "empty_queue"))
