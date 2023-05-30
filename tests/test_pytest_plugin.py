import pytest

from repid import GetEventLogT, GetInMemoryQueueT, GetMockedActorT, Job, Queue, Repid, Router

r = Router()


@r.actor
def my_super_function(a: str) -> str:
    return a


@pytest.mark.repid()
async def test_repid_marker_provides_connection() -> None:
    Repid.get_magic_connection()


async def test_no_repid_marker_no_connection() -> None:
    with pytest.raises(ValueError, match="Default connection isn't set."):
        Repid.get_magic_connection()


@pytest.mark.repid.with_args(autoconnection=False)
async def test_repid_marker_without_connection() -> None:
    with pytest.raises(ValueError, match="Default connection isn't set."):
        Repid.get_magic_connection()


@pytest.mark.repid.with_args(routers=[r])
async def test_job_execution() -> None:
    j = Job("my_super_function", args={"a": "hello"}, store_result=True)
    await j.queue.declare()
    assert (await j.result) is None
    await j.enqueue()
    result = await j.result
    assert result is not None
    assert result.data == '"hello"'


@pytest.mark.xfail(
    reason="`routers` should only accept Router type.",
    strict=True,
)
@pytest.mark.repid.with_args(routers=[123])
async def test_non_router() -> None:
    pass


@pytest.mark.repid.with_args(routers=[r])
async def test_enqueue_not_in_router() -> None:
    await Queue().declare()

    j1 = Job("my_other_function")
    [await j1.enqueue() for _ in range(10)]

    j2 = Job("my_super_function", args={"a": "hello"})
    await j2.enqueue()
    result = await j2.result
    assert result is not None
    assert result.data == '"hello"'


@pytest.mark.repid.with_args(routers=[r])
async def test_event_log(repid_get_event_log: GetEventLogT) -> None:
    j = Job("my_super_function", args={"a": "hello"})
    await j.queue.declare()
    await j.enqueue()

    eventlog = repid_get_event_log()
    assert len(eventlog) == 7
    assert eventlog[0].function == "queue_declare"
    assert eventlog[1].function == "store_bucket"
    assert eventlog[2].function == "enqueue"
    assert eventlog[3].function == "consume"
    assert eventlog[4].function == "get_bucket"
    assert eventlog[5].function == "ack"
    assert eventlog[6].function == "store_bucket"


@pytest.mark.repid.with_args(routers=[r])
async def test_actor_mock(repid_get_mocked_actor: GetMockedActorT) -> None:
    j = Job("my_super_function", args={"a": "hello"})
    await j.queue.declare()

    m = repid_get_mocked_actor("my_super_function")
    assert m is not None
    m.assert_not_called()

    await j.enqueue()

    m.assert_called_once_with(a="hello")


@pytest.mark.repid.with_args(routers=[r], declare_all_known_queues=True)
async def test_declare_all_queues() -> None:
    j1 = Job("other_actor", queue="unknown_queue")
    with pytest.raises(KeyError, match="unknown_queue"):
        await j1.enqueue()

    j2 = Job("my_super_function", args={"a": "hello"})
    await j2.enqueue()


@pytest.mark.xfail(
    reason="Can not declare queues without connection.",
    raises=ValueError,
    strict=True,
)
@pytest.mark.repid.with_args(routers=[r], declare_all_known_queues=True, autoconnection=False)
async def test_no_connection_declare_all_queues() -> None:
    pass


@pytest.mark.repid()
async def test_get_queue(repid_get_in_memory_queue: GetInMemoryQueueT) -> None:
    q = Queue("nondefault")
    await q.declare()

    j1 = Job("my_other_function", queue=q)
    [await j1.enqueue() for _ in range(10)]

    in_memory_queue = repid_get_in_memory_queue("default")
    assert in_memory_queue is None

    in_memory_queue = repid_get_in_memory_queue(q)
    assert in_memory_queue is not None
    assert in_memory_queue.simple.qsize() == 10

    in_memory_queue = repid_get_in_memory_queue("nondefault")
    assert in_memory_queue is not None
    assert in_memory_queue.simple.qsize() == 10
