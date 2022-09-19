import asyncio
import os
from signal import SIGINT

from repid import Job, Router, Worker


async def test_worker_sigint(fake_connection):
    r = Router()

    @r.actor
    async def awesome_job():
        pass

    myworker = Worker(routers=[r], gracefull_shutdown_time=1)

    task = asyncio.Task(myworker.run())
    await asyncio.sleep(0.3)
    assert not task.done()
    pid = os.getpid()
    os.kill(pid, SIGINT)
    await task


async def test_worker_long_task_reject(fake_connection):
    r = Router()
    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()
    hit = False
    never_hit = False

    @r.actor
    async def awesome_job():
        nonlocal hit, never_hit
        hit = True
        await asyncio.sleep(10.0)
        never_hit = True

    myworker = Worker(routers=[r], gracefull_shutdown_time=1)
    task = asyncio.Task(myworker.run())
    await asyncio.sleep(0.9)
    assert not task.done()
    pid = os.getpid()
    os.kill(pid, SIGINT)
    await task
    assert hit
    assert not never_hit


async def test_worker_short_task_finishes(fake_connection):
    r = Router()

    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()

    hit = False

    @r.actor
    async def awesome_job():
        nonlocal hit
        await asyncio.sleep(1.9)
        hit = True

    myworker = Worker(routers=[r], gracefull_shutdown_time=2)
    task = asyncio.Task(myworker.run())
    await asyncio.sleep(0.9)
    assert not task.done()
    pid = os.getpid()
    os.kill(pid, SIGINT)
    await task
    assert task.exception() is None
    assert hit
