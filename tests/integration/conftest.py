import pytest
from pytest_docker_tools import container

from repid import Repid
from repid.connection import Connection
from repid.main import DEFAULT_CONNECTION

redis_container = container(
    image="redis:7.0-alpine",
    ports={"6379/tcp": None},
    command="redis-server --requirepass test",
    scope="session",
)

rabbitmq_container = container(
    image="rabbitmq:3.10-management-alpine",
    ports={"5672/tcp": None, "15672/tcp": None},
    environment={
        "RABBITMQ_DEFAULT_USER": "user",
        "RABBITMQ_DEFAULT_PASS": "testtest",
    },
    scope="session",
)


@pytest.fixture(scope="session", autouse=True)
def connection(rabbitmq_container, redis_container) -> Connection:
    repid = Repid(
        f"amqp://user:testtest@localhost:{rabbitmq_container.ports['5672/tcp'][0]}",
        f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/0",
        f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/1",
    )
    assert DEFAULT_CONNECTION.get()
    return repid._Repid__conn
