import asyncio

import aioredis
import pytest


@pytest.fixture()
async def redis():
    redis = await aioredis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    await redis.flushdb()
    yield redis
    await redis.close()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
