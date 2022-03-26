import asyncio

import pytest
from redis.asyncio import Redis


async def test_redis_connection(url: str) -> bool:
    try:
        r = Redis.from_url(url)
        await r.ping()
    except Exception:
        return False
    else:
        return True


@pytest.fixture(scope="session")
def redis_service(docker_services):
    port = docker_services.port_for("redis", 6379)
    url = f"redis://:pass@localhost:{port}"
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: asyncio.run(test_redis_connection(url))
    )
    return url


@pytest.fixture()
async def redis(redis_service):
    redis = Redis.from_url(redis_service, decode_responses=True)
    await redis.flushdb()
    yield redis
    await redis.close()
