import asyncio
from datetime import datetime, timedelta

import pytest

from repid import Job, Message, MessageCategory, Queue, Worker

pytestmark = pytest.mark.usefixtures("fake_connection")


@pytest.mark.parametrize("category", list(MessageCategory))
async def test_get_message_from_empty_queue(category: MessageCategory) -> None:
    q = Queue()
    await q.declare()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            q.get_messages(category=category).__anext__(),
            timeout=1.0,
        )


async def test_get_normal_messages() -> None:
    q = Queue()
    await q.declare()

    await Job("myjob").enqueue()

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages().__anext__()

    assert isinstance(m, Message)
    assert m.category == MessageCategory.NORMAL
    assert m.key.topic == "myjob"

    await m.ack()


async def test_get_delayed_messages() -> None:
    q = Queue()
    await q.declare()

    await Job("myjob", deferred_until=datetime.now() + timedelta(minutes=1)).enqueue()

    # can't get via normal category
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            q.get_messages(category=MessageCategory.NORMAL).__anext__(),
            timeout=1.0,
        )

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages(category=MessageCategory.DELAYED).__anext__()

    assert isinstance(m, Message)
    assert m.category == MessageCategory.DELAYED
    assert m.key.topic == "myjob"

    await m.ack()


async def test_get_earliest_delayed_messages() -> None:
    q = Queue()
    await q.declare()

    await Job("myjob", deferred_until=datetime.now() + timedelta(minutes=15)).enqueue()
    await Job("myanotherjob", deferred_until=datetime.now() + timedelta(minutes=1)).enqueue()

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages(category=MessageCategory.DELAYED).__anext__()

    assert isinstance(m, Message)
    assert m.category == MessageCategory.DELAYED
    assert m.key.topic == "myanotherjob"

    await m.ack()


async def test_get_earliest_delayed_with_same_time_messages() -> None:
    q = Queue()
    await q.declare()

    same_time = datetime.now() + timedelta(minutes=1)

    await Job("myjob", deferred_until=datetime.now() + timedelta(minutes=15)).enqueue()
    await Job("myfirstjob", deferred_until=same_time).enqueue()
    await Job("mysecondjob", deferred_until=same_time).enqueue()

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages(category=MessageCategory.DELAYED).__anext__()

    assert isinstance(m, Message)
    assert m.category == MessageCategory.DELAYED
    assert m.key.topic == "myfirstjob"

    await m.ack()


async def test_get_dead_messages() -> None:
    q = Queue()
    await q.declare()

    await Job("myjob").enqueue()

    w = Worker(messages_limit=1)

    @w.actor
    async def myjob() -> None:
        raise RuntimeError

    await asyncio.wait_for(w.run(), timeout=5.0)

    # can't get via normal category
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            q.get_messages(category=MessageCategory.NORMAL).__anext__(),
            timeout=1.0,
        )

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages(category=MessageCategory.DEAD).__anext__()

    assert isinstance(m, Message)
    assert m.category == MessageCategory.DEAD
    assert m.key.topic == "myjob"

    await m.ack()


async def test_read_only_message() -> None:
    q = Queue()
    await q.declare()

    await Job("myjob").enqueue()

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages().__anext__()

    assert isinstance(m, Message)
    assert m.category == MessageCategory.NORMAL
    assert m.key.topic == "myjob"
    assert m.read_only is False

    await m.ack()

    assert m.read_only is True

    with pytest.raises(ValueError, match="Message is read only."):
        await m.ack()

    with pytest.raises(ValueError, match="Message is read only."):
        await m.nack()

    with pytest.raises(ValueError, match="Message is read only."):
        await m.reschedule()

    with pytest.raises(ValueError, match="Message is read only."):
        await m.reject()

    with pytest.raises(ValueError, match="Message is read only."):
        await m.retry()

    with pytest.raises(ValueError, match="Message is read only."):
        await m.force_retry()


@pytest.mark.parametrize("category", [MessageCategory.DELAYED, MessageCategory.DEAD])
async def test_message_non_normal_category(category: MessageCategory) -> None:
    q = Queue()
    await q.declare()

    if category == MessageCategory.DELAYED:
        await Job("myjob", deferred_until=datetime.now() + timedelta(minutes=1)).enqueue()
    elif category == MessageCategory.DEAD:
        await Job("myjob").enqueue()
        _m = await q.get_messages().__anext__()
        await _m.nack()
    else:
        pytest.fail(reason=f"Unknown {category=}.")

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages(category=category).__anext__()

    assert isinstance(m, Message)
    assert m.category == category
    assert m.key.topic == "myjob"

    with pytest.raises(ValueError, match=f"Can not nack message with category {category}."):
        await m.nack()

    with pytest.raises(ValueError, match=f"Can not retry message with category {category}."):
        await m.retry()

    with pytest.raises(ValueError, match=f"Can not force retry message with category {category}."):
        await m.force_retry()
