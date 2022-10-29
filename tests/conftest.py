import asyncio
from typing import AsyncIterator, Iterator

import pytest

from repid import (
    Connection,
    DummyBucketBroker,
    DummyMessageBroker,
    DummyResultBucketBroker,
    Repid,
)
from repid.main import DEFAULT_CONNECTION


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture()
def fake_repid() -> Repid:
    repid = Repid(Connection(DummyMessageBroker(), DummyBucketBroker(), DummyResultBucketBroker()))
    return repid


@pytest.fixture()
async def __fake_connection(fake_repid: Repid) -> AsyncIterator[Connection]:
    await fake_repid._conn.connect()
    yield fake_repid._conn
    await fake_repid._conn.disconnect()


@pytest.fixture()
def fake_connection(__fake_connection: Connection) -> Iterator[Connection]:
    token = DEFAULT_CONNECTION.set(__fake_connection)
    yield __fake_connection
    DEFAULT_CONNECTION.reset(token)
