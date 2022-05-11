from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator

import pytest
from redis.asyncio import Redis

if TYPE_CHECKING:
    from _pytest.config import Config
    from pytest import TempPathFactory
    from pytest_docker.plugin import Services


def test_redis_connection(url: str) -> bool:
    try:
        r = Redis.from_url(url)
        r.ping()
    except Exception:  # pylint: disable=broad-except
        return False
    else:
        return True


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig: "Config", tmp_path_factory: "TempPathFactory") -> Path:
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
def redis_service_url(docker_services: "Services") -> str:  # type: ignore[no-any-unimported]
    port = docker_services.port_for("redis", 6379)
    url = f"redis://:pass@localhost:{port}"
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: test_redis_connection(url)
    )
    return url


@pytest.fixture()
async def redis(redis_service_url: str) -> AsyncIterator[Redis]:
    redis = Redis.from_url(redis_service_url)
    redis.flushdb()
    yield redis
    await redis.close()
