import asyncio
from datetime import timedelta
from random import random

from repid import Job, Repid, Router, Worker


async def test_simple_job(autoconn: Repid):
    async with autoconn.connect():
        j = Job("awesome_job")
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job():
        nonlocal hit
        hit = True

    async with autoconn.connect():
        myworker = Worker(routers=[router], messages_limit=1)
        await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit


async def test_args_job(autoconn: Repid):
    assertion1 = random()
    assertion2 = random()

    async with autoconn.connect():
        j = Job("awesome_job", args=dict(my_arg1=assertion1, my_arg2=assertion2))
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job(my_arg1: float, my_arg2: float):
        nonlocal hit, assertion1, assertion2
        assert my_arg1 == assertion1
        assert my_arg2 == assertion2
        hit = True

    async with autoconn.connect():
        myworker = Worker(routers=[router], messages_limit=1)
        await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit


async def test_deferred_by_job(autoconn: Repid):
    async with autoconn.connect():
        j = Job("awesome_job", deferred_by=timedelta(seconds=1))
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = 0

    @router.actor
    async def awesome_job():
        nonlocal hit
        hit += 1

    async with autoconn.connect():
        myworker = Worker(routers=[router], messages_limit=3)
        await asyncio.wait_for(myworker.run(), timeout=5.0)
        await j.queue.flush()

    assert hit == 3


async def test_retries(autoconn: Repid):
    async with autoconn.connect():
        j = Job("awesome_job", retries=3)
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = 0

    @router.actor(retry_policy=lambda retry_number: timedelta(seconds=0))
    async def awesome_job():
        nonlocal hit
        hit += 1
        if hit < 2:
            raise Exception("Some stupid exception.")

    async with autoconn.connect():
        myworker = Worker(routers=[router], messages_limit=2)
        await asyncio.wait_for(myworker.run(), timeout=5.0)
    assert hit == 2


async def test_worker_no_routers(autoconn: Repid):
    async with autoconn.connect():
        myworker = Worker()
        await myworker.run()
