import asyncio
from typing import AsyncIterator, Iterator

import pytest

from repid import Connection, DummyBucketBroker, DummyMessageBroker, Repid


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture()
def fake_repid() -> Repid:
    repid = Repid(Connection(DummyMessageBroker(), DummyBucketBroker(), DummyBucketBroker(True)))
    return repid


@pytest.fixture()
async def fake_connection(fake_repid: Repid) -> AsyncIterator[Connection]:
    async with fake_repid.magic(auto_disconnect=True) as conn:
        yield conn
