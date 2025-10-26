import asyncio
from datetime import datetime, timedelta

import pytest

from repid import Job, Message, MessageCategory, Queue, Repid, Worker


async def test_get_normal_messages(autoconn: Repid) -> None:
    async with autoconn.magic(auto_disconnect=True):
        q = Queue()
        await q.declare()

        await Job("myjob").enqueue()

    async with autoconn.magic(auto_disconnect=True):
        # can be replaced with anext() in Python 3.10+
        m = await q.get_messages().__anext__()

        assert isinstance(m, Message)

        assert m.key.topic == "myjob"

        await m.ack()


async def test_get_delayed_messages(autoconn: Repid) -> None:
    async with autoconn.magic(auto_disconnect=True):
        q = Queue()
        await q.declare()

        await Job("myjob", deferred_until=datetime.now() + timedelta(minutes=1)).enqueue()

    async with autoconn.magic(auto_disconnect=True):
        # can't get via normal category
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                q.get_messages(category=MessageCategory.NORMAL).__anext__(),
                timeout=1.0,
            )

    async with autoconn.magic(auto_disconnect=True):
        # can be replaced with anext() in Python 3.10+
        m = await q.get_messages(category=MessageCategory.DELAYED).__anext__()

        assert isinstance(m, Message)

        assert m.key.topic == "myjob"

        await m.ack()


async def test_get_dead_messages(autoconn: Repid) -> None:
    async with autoconn.magic(auto_disconnect=True):
        q = Queue()
        await q.declare()

        await Job("myjob").enqueue()

    async with autoconn.magic(auto_disconnect=True):
        w = Worker(messages_limit=1)

        @w.actor
        async def myjob() -> None:
            raise RuntimeError

        await asyncio.wait_for(w.run(), timeout=5.0)

    async with autoconn.magic(auto_disconnect=True):
        # can't get via normal category
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                q.get_messages(category=MessageCategory.NORMAL).__anext__(),
                timeout=1.0,
            )

    async with autoconn.magic(auto_disconnect=True):
        # can be replaced with anext() in Python 3.10+
        m = await q.get_messages(category=MessageCategory.DEAD).__anext__()

        assert isinstance(m, Message)

        assert m.key.topic == "myjob"

        await m.ack()


@pytest.mark.parametrize("category", [MessageCategory.DELAYED, MessageCategory.DEAD])
async def test_reject_delayed_and_dead_messages(category: MessageCategory, autoconn: Repid) -> None:
    async with autoconn.magic(auto_disconnect=True):
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

    async with autoconn.magic(auto_disconnect=True):
        # can be replaced with anext() in Python 3.10+
        m = await q.get_messages(category=category).__anext__()

        assert isinstance(m, Message)
        assert m.category == category
        assert m.key.topic == "myjob"

        await m.reject()

    async with autoconn.magic(auto_disconnect=True):
        # can be replaced with anext() in Python 3.10+
        m = await q.get_messages(category=category).__anext__()

        assert isinstance(m, Message)
        assert m.category == category
        assert m.key.topic == "myjob"

        await m.ack()


async def test_reschedule_dead_messages(autoconn: Repid) -> None:
    async with autoconn.magic(auto_disconnect=True):
        q = Queue()
        await q.declare()

        await Job("myjob").enqueue()
        _m = await q.get_messages().__anext__()
        await _m.nack()

    async with autoconn.magic(auto_disconnect=True):
        # can be replaced with anext() in Python 3.10+
        m = await q.get_messages(category=MessageCategory.DEAD).__anext__()

        assert isinstance(m, Message)
        assert m.category == MessageCategory.DEAD
        assert m.key.topic == "myjob"

        await m.reschedule()

    async with autoconn.magic(auto_disconnect=True):
        # can be replaced with anext() in Python 3.10+
        m = await q.get_messages().__anext__()

        assert isinstance(m, Message)
        assert m.category == MessageCategory.NORMAL
        assert m.key.topic == "myjob"

        await m.ack()
