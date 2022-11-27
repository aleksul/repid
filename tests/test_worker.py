import asyncio
import os
from signal import SIGINT

from repid import Job, Worker


async def test_worker_sigint(fake_connection):
    myworker = Worker(gracefull_shutdown_time=1)

    @myworker.actor()
    async def awesome_job():
        pass

    task = asyncio.Task(myworker.run())
    await asyncio.sleep(0.1)
    assert not task.done()
    pid = os.getpid()
    os.kill(pid, SIGINT)
    await asyncio.sleep(1.1)
    assert task.done()
    assert task.exception() is None


async def test_worker_long_task_reject(fake_connection):
    myworker = Worker(gracefull_shutdown_time=1)
    await Job("awesome_job").enqueue()

    hit = False
    never_hit = False

    @myworker.actor()
    async def awesome_job():
        nonlocal hit, never_hit
        hit = True
        await asyncio.sleep(10.0)
        never_hit = True

    task = asyncio.Task(myworker.run())
    await asyncio.sleep(0.1)
    assert not task.done()
    pid = os.getpid()
    os.kill(pid, SIGINT)
    await asyncio.sleep(1.1)
    assert task.done()
    assert task.exception() is None
    assert hit
    assert not never_hit


async def test_worker_short_task_finishes(fake_connection):
    myworker = Worker(gracefull_shutdown_time=2)
    await Job("awesome_job").enqueue()

    hit = False

    @myworker.actor()
    async def awesome_job():
        nonlocal hit
        await asyncio.sleep(1)
        hit = True

    task = asyncio.Task(myworker.run())
    await asyncio.sleep(0.1)
    assert not task.done()
    pid = os.getpid()
    os.kill(pid, SIGINT)
    await asyncio.sleep(2.1)
    assert task.done()
    assert task.exception() is None
    assert hit
