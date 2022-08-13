import asyncio

import pytest
import uvloop

from repid import Repid
from repid.connection import Connection


@pytest.fixture(scope="session")
def event_loop():
    uvloop.install()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def fake_connection() -> Connection:
    repid = Repid(
        "amqp://user:testtest@localhost:5672",
        "redis://:test@localhost:6379/0",
        "redis://:test@localhost:6379/1",
    )
    assert Repid._Repid__default_connection is not None
    return repid._Repid__conn
