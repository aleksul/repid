import anyio
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
def docker_compose_file(pytestconfig, tmp_path_factory: pytest.TempPathFactory):
    compose = """
    version: '3.8'
    services:
        redis:
            image: "redis:6.2-alpine"
            restart: unless-stopped
            command: redis-server --requirepass pass
            ports:
            - 6379
    """
    temp_file = tmp_path_factory.mktemp("compose") / "docker-compose.yml"
    temp_file.write_text(compose)
    return temp_file


@pytest.fixture(scope="session")
def redis_service(docker_services):
    port = docker_services.port_for("redis", 6379)
    url = f"redis://:pass@localhost:{port}"
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: anyio.run(test_redis_connection, url)
    )
    return url


@pytest.fixture()
async def redis(redis_service):
    redis = Redis.from_url(redis_service)
    await redis.flushdb()
    yield redis
    await redis.close()
