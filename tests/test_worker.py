import asyncio
import json
import os
import time
from datetime import timedelta
from random import random
from signal import SIGINT

import pytest

from repid import Connection, Job, Router, Worker

pytestmark = pytest.mark.usefixtures("fake_connection")


async def test_worker_include_router() -> None:
    r1 = Router()

    @r1.actor
    async def first_job() -> None:
        pass

    r2 = Router()

    @r2.actor
    async def second_job() -> None:
        pass

    myworker = Worker()

    assert "first_job" not in myworker.actors
    assert "second_job" not in myworker.actors

    myworker.include_router(r1)
    assert "first_job" in myworker.actors
    assert "second_job" not in myworker.actors

    myworker.include_router(r2)
    assert "first_job" in myworker.actors
    assert "second_job" in myworker.actors


async def test_worker_sigint() -> None:
    r = Router()

    @r.actor
    async def awesome_job() -> None:
        pass

    myworker = Worker(routers=[r], gracefull_shutdown_time=1)

    task = asyncio.Task(myworker.run())
    await asyncio.sleep(0.3)
    assert not task.done()
    pid = os.getpid()
    os.kill(pid, SIGINT)
    await task


async def test_worker_long_task_reject() -> None:
    r = Router()
    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()
    hit = False
    never_hit = False

    @r.actor
    async def awesome_job() -> None:
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


async def test_worker_short_task_finishes() -> None:
    r = Router()

    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()

    hit = False

    @r.actor
    async def awesome_job() -> None:
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


async def test_dead(fake_connection: Connection) -> None:
    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()

    router = Router()

    hit = 0

    @router.actor
    async def awesome_job() -> None:
        nonlocal hit
        hit += 1
        raise Exception("Some stupid exception.")

    myworker = Worker(routers=[router], messages_limit=1)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit == 1

    assert fake_connection.message_broker.queues["default"].dead[0].key.topic == "awesome_job"  # type: ignore[attr-defined]  # noqa: E501


async def test_retries() -> None:
    j = Job("awesome_job", retries=3)
    await j.queue.declare()
    await j.enqueue()

    router = Router()

    hit = 0

    def zero_retry_policy(retry_number: int = 1) -> timedelta:
        return timedelta(seconds=0)

    @router.actor(retry_policy=zero_retry_policy)
    async def awesome_job() -> None:
        nonlocal hit
        hit += 1
        if hit < 2:
            raise Exception("Some stupid exception.")

    myworker = Worker(routers=[router], messages_limit=2)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit == 2


async def test_tasks_limit() -> None:
    j = Job("awesome_job")
    await j.queue.declare()
    [await j.enqueue() for _ in range(5)]

    router = Router()

    sync = 0
    hit = 0

    @router.actor
    async def awesome_job() -> None:
        nonlocal sync, hit
        sync += 1
        await asyncio.sleep(1.0)
        assert sync <= 2
        sync -= 1
        hit += 1

    myworker = Worker(routers=[router], messages_limit=5, tasks_limit=2)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit == 5
    assert sync == 0


async def test_args_job() -> None:
    assertion1 = random()
    assertion2 = random()

    j = Job("awesome_job", args=dict(my_arg1=assertion1, my_arg2=assertion2))
    await j.queue.declare()
    await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job(my_arg1: float, my_arg2: float) -> None:
        nonlocal hit, assertion1, assertion2
        assert my_arg1 == assertion1
        assert my_arg2 == assertion2
        hit = True

    myworker = Worker(routers=[router], messages_limit=1)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit


async def test_all_args_job() -> None:
    assertion1 = random()
    assertion2 = random()

    j = Job("awesome_job", args=dict(my_arg1=assertion1, my_arg2=assertion2))
    await j.queue.declare()
    await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job(*args: tuple) -> None:
        nonlocal hit, assertion1, assertion2
        assert args[0] == assertion1
        assert args[1] == assertion2
        hit = True

    myworker = Worker(routers=[router], messages_limit=1)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit


async def test_all_kwargs_job() -> None:
    assertion1 = random()
    assertion2 = random()

    j = Job("awesome_job", args=dict(my_arg1=assertion1, my_arg2=assertion2))
    await j.queue.declare()
    await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job(**kwargs: dict) -> None:
        nonlocal hit, assertion1, assertion2
        assert kwargs["my_arg1"] == assertion1
        assert kwargs["my_arg2"] == assertion2
        hit = True

    myworker = Worker(routers=[router], messages_limit=1)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit


async def test_result_job() -> None:
    result = random()

    j = Job("awesome_job")
    await j.queue.declare()
    await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job() -> float:
        nonlocal hit, result
        hit = True
        return result

    myworker = Worker(routers=[router], messages_limit=1)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit
    r = await j.result
    assert r is not None
    assert r.success
    assert r.data == json.dumps(result)


async def test_sync_job() -> None:
    j = Job("awesome_job")
    await j.queue.declare()
    [await j.enqueue() for _ in range(5)]

    router = Router()

    hit = 0

    @router.actor
    def awesome_job() -> None:
        nonlocal hit
        time.sleep(1.5)
        hit += 1

    myworker = Worker(routers=[router], messages_limit=5)
    await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit == 5
