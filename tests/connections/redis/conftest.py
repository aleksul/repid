from typing import AsyncIterator

import pytest
from pytest_docker_tools import container
from redis.asyncio import Redis

redis_container = container(
    image="redis:6.2-alpine",
    ports={"6379/tcp": None},
    command="redis-server --requirepass test",
)


@pytest.fixture()
async def redis(redis_container) -> AsyncIterator[Redis]:
    assert "Ready to accept connections" in redis_container.logs()
    url = f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}"
    redis = Redis.from_url(url)
    await redis.flushdb()
    yield redis
    await redis.close()
