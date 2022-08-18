import pytest
from pytest_docker_tools import container
from pytest_lazyfixture import lazy_fixture

from repid import Repid
from repid.connection import Connection
from repid.connections.redis.redis_messaging import RedisMessaging

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


@pytest.fixture(scope="session")
def standart_connection(rabbitmq_container, redis_container) -> Connection:
    repid = Repid(
        f"amqp://user:testtest@localhost:{rabbitmq_container.ports['5672/tcp'][0]}",
        f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/0",
        f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/1",
    )
    assert Repid._Repid__default_connection is not None
    return repid._conn


@pytest.fixture(scope="session")
def redis_connection(redis_container) -> Connection:
    RedisMessaging.cls.run_maintenance_every = 0
    repid = Repid(
        f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/2",
        f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/3",
        f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/4",
    )
    assert Repid._Repid__default_connection is not None
    return repid._conn


@pytest.fixture(
    scope="session",
    autouse=True,
    params=[
        lazy_fixture("standart_connection"),
        lazy_fixture("redis_connection"),
    ],
)
def autoconnection(request):
    return request.param
