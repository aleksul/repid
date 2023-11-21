import pytest

from repid import Job, Message, Queue

pytestmark = pytest.mark.usefixtures("fake_connection")


async def test_get_normal_messages() -> None:
    q = Queue()
    await q.declare()

    await Job("myjob").enqueue()

    # can be replaced with anext() in Python 3.10+
    m = await q.get_messages().__anext__()

    assert isinstance(m, Message)

    assert m.key.topic == "myjob"

    await m.ack()
