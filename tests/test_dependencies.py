import asyncio
import sys
import time
from datetime import datetime, timedelta

import pytest
from annotated_types import Ge

from repid import (
    Connection,
    Depends,
    GetInMemoryQueueT,
    InMemoryMessageBroker,
    Job,
    MessageDependency,
    Repid,
    Router,
    Worker,
)

if sys.version_info >= (3, 10):
    from typing import Annotated
else:
    from typing_extensions import Annotated

pytestmark = pytest.mark.usefixtures("fake_connection")


async def test_simple_message_dependency() -> None:
    w = Worker(messages_limit=1)

    received = None

    @w.actor
    async def myactor(arg1: str, m: MessageDependency) -> None:  # noqa: ARG001
        nonlocal received
        received = m

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello"}).enqueue()
    await w.run()

    assert isinstance(received, MessageDependency)


async def test_message_dependency_ack(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    w = Worker(messages_limit=1)

    @w.actor
    async def myactor(m: MessageDependency) -> None:
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
    async def myactor(m: MessageDependency) -> None:
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
    async def myactor(m: MessageDependency) -> None:
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


async def test_message_dependency_retry(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    w = Worker(messages_limit=1)

    counter = 0

    @w.actor(retry_policy=lambda retry_number=1: timedelta(seconds=1))  # noqa: ARG005
    async def myactor(m: MessageDependency) -> None:
        nonlocal counter
        counter += 1
        await m.retry()

    await w.declare_all_queues()
    await Job("myactor", retries=1).enqueue()  # message sets some amount of retries

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

    q = repid_get_in_memory_queue("default")
    assert q is not None
    assert q.simple.qsize() == 0
    assert len(q.dead) == 1  # failed as exceeded amount of retries
    assert len(q.delayed) == 0
    assert len(q.processing) == 0


async def test_message_dependency_force_retry(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    w = Worker(messages_limit=1)

    counter = 0

    @w.actor(retry_policy=lambda retry_number=1: timedelta(seconds=1))  # noqa: ARG005
    async def myactor(m: MessageDependency) -> None:
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
    async def myactor(m: MessageDependency) -> None:
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
    async def myactor(arg1: str, m: MessageDependency) -> None:
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
    async def myactor(arg1: str, m: MessageDependency) -> None:
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
    async def myactor(arg1: str, m: MessageDependency) -> None:
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
    async def myactor(m: MessageDependency) -> str:
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
        async def myactor(m: MessageDependency) -> str:
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


def _mydependency() -> str:
    return "aaa"


async def test_depends() -> None:
    w = Worker(messages_limit=1)

    received = None

    @w.actor
    async def myactor(arg1: str, d: Annotated[str, Depends(_mydependency)]) -> None:  # noqa: ARG001
        nonlocal received
        received = d

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello"}).enqueue()
    await w.run()

    assert received == "aaa"


async def test_depends_run_in_process() -> None:
    w = Worker(messages_limit=1)

    received = None

    @w.actor
    async def myactor(
        arg1: str,  # noqa: ARG001
        d: Annotated[
            str,
            Depends(_mydependency, run_in_process=True),  # has to be top-level function
        ],
    ) -> None:
        nonlocal received
        received = d

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello"}).enqueue()
    await w.run()

    assert received == "aaa"


async def test_sub_depends() -> None:
    w = Worker(messages_limit=1)

    received = None

    def mysubdependency(m: MessageDependency) -> str:
        return m.raw_payload

    async def mydependency(rand: Annotated[int, Depends(mysubdependency)]) -> str:
        return str(rand) + "aaa"

    @w.actor
    async def myactor(arg1: str, d: Annotated[str, Depends(mydependency)]) -> None:  # noqa: ARG001
        nonlocal received
        received = d

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello"}).enqueue()
    await w.run()

    assert received == '{"arg1":"hello"}aaa'


def test_positional_only_dependency() -> None:
    def mysubdependency(m: MessageDependency, /) -> str:
        return m.raw_payload

    with pytest.raises(
        ValueError,
        match="Dependencies in positional-only arguments are not supported.",
    ):
        Depends(mysubdependency)


def test_non_default_arg() -> None:
    def mysubdependency(m: None) -> str:
        return f"{m}"

    with pytest.raises(
        ValueError,
        match="Non-dependency arguments without defaults are not supported.",
    ):
        Depends(mysubdependency)


async def test_multiple_depends() -> None:
    w = Worker(messages_limit=1)

    received = None
    received2 = None

    async def mydependency() -> str:
        return "aaa"

    def myseconddependency() -> int:
        return 123

    @w.actor
    async def myactor(
        arg1: str,  # noqa: ARG001
        d: Annotated[str, Depends(mydependency)],
        d2: Annotated[int, Depends(myseconddependency)],
    ) -> None:
        nonlocal received, received2
        received = d
        received2 = d2

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello"}).enqueue()
    await w.run()

    assert received == "aaa"
    assert received2 == 123


async def test_dependency_override() -> None:
    w = Worker(messages_limit=1)

    received = None

    async def mydependency() -> str:
        return "aaa"

    def another_dependency() -> str:
        return "bbb"

    dep = Depends(mydependency)

    @w.actor
    async def myactor(arg1: str, d: Annotated[str, dep]) -> None:  # noqa: ARG001
        nonlocal received
        received = d

    dep.override(another_dependency)

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello"}).enqueue()
    await w.run()

    assert received == "bbb"


async def test_fake_depends() -> None:
    w = Worker(messages_limit=1)

    received = None

    async def mydependency() -> str:
        return "aaa"

    class MyDepends:
        __repid_dependency__ = "annotated"

    @w.actor
    async def myactor(arg1: str, d: Annotated[str, MyDepends()]) -> None:  # noqa: ARG001
        nonlocal received
        received = d

    await w.declare_all_queues()
    j = Job("myactor", args={"arg1": "hello"})
    await j.enqueue()
    await w.run()

    assert received is None
    result = await j.result
    assert result is not None
    assert result.success is False
    assert result.exception == "AttributeError"


async def test_not_depends_but_annotated() -> None:
    w = Worker(messages_limit=1)

    received = None

    @w.actor
    async def myactor(arg1: str, d: Annotated[str, Ge(2)]) -> None:  # noqa: ARG001
        nonlocal received
        received = d

    await w.declare_all_queues()
    await Job("myactor", args={"arg1": "hello", "d": 3}).enqueue()
    await w.run()

    assert received == 3
