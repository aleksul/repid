import asyncio

import pytest
import uvloop

from repid import Repid
from repid.connection import Connection
from repid.main import DEFAULT_CONNECTION


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
    assert DEFAULT_CONNECTION.get()
    return repid._Repid__conn
