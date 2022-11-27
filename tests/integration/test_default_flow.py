import asyncio
from datetime import timedelta

import pytest

from repid import Job, Worker


async def test_simple_job():
    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(messages_limit=1)

    hit = False

    @myworker.actor()
    async def awesome_job():
        nonlocal hit
        hit = True

    await myworker.run()
    assert hit


async def test_deferred_by_job():
    j = Job("awesome_job", deferred_by=timedelta(seconds=1))
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(messages_limit=3)

    hit = 0

    @myworker.actor()
    async def awesome_job():
        nonlocal hit
        hit += 1

    await myworker.run()
    assert hit == 3


async def test_retries():
    j = Job("awesome_job", retries=2)
    await j.queue.declare()
    await j.enqueue()

    myworker = Worker(messages_limit=2)

    hit = 0

    @myworker.actor()
    async def awesome_job():
        nonlocal hit
        hit += 1
        if hit < 2:
            raise Exception("Some stupid exception.")

    await myworker.run()
    assert hit == 2


async def test_worker_no_queue():
    myworker = Worker(gracefull_shutdown_time=1)

    @myworker.actor()
    async def awesome_job():
        pass

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(myworker.run(), 3.0)
