import asyncio
import time
from datetime import datetime, timedelta

import pytest

from repid import (
    Connection,
    GetInMemoryQueueT,
    InMemoryMessageBroker,
    Job,
    Message,
    Repid,
    Router,
    Worker,
)

pytestmark = pytest.mark.usefixtures("fake_connection")


async def test_simple_message_dependency() -> None:
    w = Worker(messages_limit=1)

    received = None

    @w.actor
    async def myactor(arg1: str, m: Message) -> None:  # noqa: ARG001
        nonlocal received
        received = m

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello"}).enqueue()
    await w.run()

    assert isinstance(received, Message)


async def test_message_dependency_ack(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    w = Worker(messages_limit=1)

    @w.actor
    async def myactor(m: Message) -> None:
        await m.ack()

    await w.declare_all_queues()
    await Job("myactor").enqueue()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 1

    await w.run()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 0
    assert len(q.dead) == 0
    assert len(q.delayed) == 0
    assert len(q.processing) == 0


async def test_message_dependency_nack(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    w = Worker(messages_limit=1)

    @w.actor
    async def myactor(m: Message) -> None:
        await m.nack()

    await w.declare_all_queues()
    await Job("myactor").enqueue()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 1

    await w.run()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 0
    assert len(q.dead) == 1
    assert len(q.delayed) == 0
    assert len(q.processing) == 0


async def test_message_dependency_reject(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    w = Worker(messages_limit=1)

    @w.actor
    async def myactor(m: Message) -> None:
        await m.reject()

    await w.declare_all_queues()
    await Job("myactor").enqueue()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 1

    await w.run()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 1
    assert len(q.dead) == 0
    assert len(q.delayed) == 0
    assert len(q.processing) == 0


async def test_message_dependency_force_retry(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    w = Worker(messages_limit=1)

    counter = 0

    @w.actor(retry_policy=lambda retry_number=1: timedelta(seconds=1))  # noqa: ARG005
    async def myactor(m: Message) -> None:
        nonlocal counter
        counter += 1
        await m.force_retry()

    await w.declare_all_queues()
    await Job("myactor").enqueue()  # message doesn't allow for retries

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 1

    await w.run()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 0
    assert len(q.dead) == 0
    assert len(q.delayed) == 1
    assert len(q.processing) == 0

    await w.run()

    assert counter == 2


async def test_message_dependency_reschedule(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    r = Router()

    counter = 0

    @r.actor(retry_policy=lambda retry_number=1: timedelta(seconds=1))  # noqa: ARG005
    async def myactor(m: Message) -> None:
        nonlocal counter
        await asyncio.sleep(0.1)
        counter += 1
        if counter == 1:
            raise Exception
        if counter == 2:
            await m.reschedule()

    j = Job("myactor", retries=1)
    await j.queue.declare()
    await j.enqueue()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 1

    await Worker(routers=[r], messages_limit=1, tasks_limit=1).run()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 0
    assert len(q.dead) == 0
    assert len(q.delayed) == 1
    assert len(q.processing) == 0

    await Worker(routers=[r], messages_limit=2, tasks_limit=1).run()

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 0
    assert len(q.dead) == 0
    assert len(q.delayed) == 0
    assert len(q.processing) == 0


async def test_message_dependency_set_result() -> None:
    w = Worker(messages_limit=1)

    @w.actor
    async def myactor(arg1: str, m: Message) -> None:
        m.set_result(arg1)
        await m.ack()

    await w.declare_all_queues()
    j = Job("myactor", args={"arg1": "hello"})
    await j.enqueue()
    await w.run()

    r = await j.result
    assert r is not None
    assert r.success is True
    assert r.data == '"hello"'


async def test_message_dependency_set_exception() -> None:
    w = Worker(messages_limit=1)

    @w.actor
    async def myactor(arg1: str, m: Message) -> None:
        m.set_result(arg1)  # result won't be set since setting exception will override it
        m.set_exception(ValueError(arg1))
        await m.ack()

    await w.declare_all_queues()
    j = Job("myactor", args={"arg1": "hello"})
    await j.enqueue()
    await w.run()

    r = await j.result
    assert r is not None
    assert r.success is False
    assert r.data == "hello"
    assert r.exception == "ValueError"


async def test_message_dependency_add_callback() -> None:
    w = Worker(messages_limit=1)

    callback_time_1 = None
    callback_time_2 = None

    async def callback_1() -> None:
        nonlocal callback_time_1
        callback_time_1 = datetime.now()
        await asyncio.sleep(0.1)

    def callback_2() -> None:
        nonlocal callback_time_2
        callback_time_2 = datetime.now()
        time.sleep(0.1)

    @w.actor
    async def myactor(arg1: str, m: Message) -> None:
        m.add_callback(callback_1)
        m.set_result(arg1)
        m.add_callback(callback_2)
        await m.ack()

    await w.declare_all_queues()
    j = Job("myactor", args={"arg1": "hello"})
    await j.enqueue()
    await w.run()

    r = await j.result
    assert r is not None
    assert r.success is True
    assert r.data == '"hello"'

    assert callback_time_1 is not None
    assert callback_time_2 is not None
    assert callback_time_1 < r.timestamp < callback_time_2


@pytest.mark.parametrize(
    "command",
    [
        "m.set_result('hello')",
        "m.set_exception(ConnectionError('Hi!'))",
    ],
)
async def test_message_dependency_result_params_are_not_set(command: str) -> None:
    w = Worker(messages_limit=1)

    was_raised = False

    @w.actor
    async def myactor(m: Message) -> str:
        nonlocal was_raised
        try:
            exec(command)
        except ValueError as exc:
            if exc.args == ("parameters.result is not set.",):
                was_raised = True
        await m.ack()

    await w.declare_all_queues()
    j = Job("myactor", store_result=False)
    await j.enqueue()
    await w.run()

    assert was_raised is True


@pytest.mark.parametrize(
    "command",
    [
        "m.set_result('hello')",
        "m.set_exception(ConnectionError('Hi!'))",
    ],
)
async def test_message_dependency_result_bucket_broker_not_available(
    command: str,
    fake_connection: Connection,
) -> None:
    await fake_connection.disconnect()

    app = Repid(Connection(InMemoryMessageBroker()))

    was_raised = False

    async with app.magic(auto_disconnect=True):
        w = Worker(messages_limit=1)

        @w.actor
        async def myactor(m: Message) -> str:
            nonlocal was_raised
            try:
                exec(command)
            except ValueError as exc:
                if exc.args == ("Results bucket broker is not configured.",):
                    was_raised = True
            await m.ack()

        await w.declare_all_queues()
        j = Job("myactor", store_result=True)
        await j.enqueue()
        await w.run()

    assert was_raised is True
