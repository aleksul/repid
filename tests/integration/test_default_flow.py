import asyncio
from datetime import timedelta
from random import random

from repid import Job, RabbitMessageBroker, RedisMessageBroker, Repid, Router, Worker


async def test_simple_job(autoconn: Repid) -> None:
    async with autoconn.magic():
        j = Job("awesome_job")
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job() -> None:
        nonlocal hit
        hit = True

    async with autoconn.magic():
        myworker = Worker(routers=[router], messages_limit=1)
        await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit


async def test_args_job(autoconn: Repid) -> None:
    assertion1 = random()
    assertion2 = random()

    async with autoconn.magic():
        j = Job("awesome_job", args={"my_arg1": assertion1, "my_arg2": assertion2})
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = False

    @router.actor
    async def awesome_job(my_arg1: float, my_arg2: float) -> None:
        nonlocal hit, assertion1, assertion2
        assert my_arg1 == assertion1
        assert my_arg2 == assertion2
        hit = True

    async with autoconn.magic():
        myworker = Worker(routers=[router], messages_limit=1)
        await asyncio.wait_for(myworker.run(), timeout=5.0)

    assert hit


async def test_deferred_by_job(autoconn: Repid) -> None:
    async with autoconn.magic():
        j = Job("awesome_job", deferred_by=timedelta(seconds=1))
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = 0

    @router.actor
    async def awesome_job() -> None:
        nonlocal hit
        hit += 1

    async with autoconn.magic():
        myworker = Worker(routers=[router], messages_limit=3)
        await asyncio.wait_for(myworker.run(), timeout=5.0)
        await j.queue.flush()

    assert hit == 3


async def test_retries(autoconn: Repid) -> None:
    async with autoconn.magic():
        j = Job("awesome_job", retries=2)
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = 0

    def zero_retry_policy(retry_number: int = 1) -> timedelta:  # noqa: ARG001
        return timedelta(seconds=0)

    @router.actor(retry_policy=zero_retry_policy)
    async def awesome_job() -> None:
        nonlocal hit
        hit += 1
        if hit < 2:
            raise Exception("Some stupid exception.")

    async with autoconn.magic():
        myworker = Worker(routers=[router], messages_limit=2)
        await asyncio.wait_for(myworker.run(), timeout=5.0)
    assert hit == 2


async def test_dead_queue(autoconn: Repid) -> None:
    async with autoconn.magic():
        j = Job("awesome_job")
        await j.queue.declare()
        await j.queue.flush()
        await j.enqueue()

    router = Router()

    hit = 0

    @router.actor
    async def awesome_job() -> None:
        nonlocal hit
        hit += 1
        if hit == 1:
            raise Exception("Only first.")

    async with autoconn.magic():
        myworker = Worker(routers=[router], messages_limit=1)
        await asyncio.wait_for(myworker.run(), timeout=5.0)

        assert hit == 1

        broker = autoconn.connection.message_broker
        if type(broker) is RedisMessageBroker:
            assert await broker.conn.llen("q:default:5:dead") == 1
        elif type(broker) is RabbitMessageBroker:
            msg = await broker._channel.basic_get("default:dead")
            assert msg is not None
            assert msg.header.properties.headers is not None
            assert msg.header.properties.headers.get("topic") == "awesome_job"
        else:
            raise Exception("This broker is not tested!")


async def test_worker_no_routers(autoconn: Repid) -> None:
    async with autoconn.magic():
        myworker = Worker()
        await myworker.run()
