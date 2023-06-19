import asyncio
from typing import AsyncIterator, Iterator

import pytest

from repid import (
    BasicConverter,
    Config,
    Connection,
    InMemoryBucketBroker,
    InMemoryMessageBroker,
    Repid,
)

Config.CONVERTER = BasicConverter


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture()
def fake_repid() -> Repid:
    return Repid(
        Connection(
            InMemoryMessageBroker(),
            InMemoryBucketBroker(),
            InMemoryBucketBroker(use_result_bucket=True),
        ),
    )


@pytest.fixture()
async def fake_connection(fake_repid: Repid) -> AsyncIterator[Connection]:
    async with fake_repid.magic(auto_disconnect=True) as conn:
        yield conn
