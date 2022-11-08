from time import sleep
from typing import TYPE_CHECKING, Any

import pytest
from pytest_docker_tools import container
from pytest_lazyfixture import lazy_fixture

from repid import Connection, Repid
from repid.connections import RabbitMessageBroker, RedisBucketBroker, RedisMessageBroker

if TYPE_CHECKING:
    from pytest_docker_tools import wrappers

redis_container = container(
    image="redis:7.0-alpine",
    ports={"6379/tcp": None},
    command="redis-server --requirepass test",
    scope="session",
)

rabbitmq_container = container(
    image="rabbitmq:3.11-alpine",
    ports={"5672/tcp": None},
    environment={
        "RABBITMQ_DEFAULT_USER": "user",
        "RABBITMQ_DEFAULT_PASS": "testtest",
    },
    scope="session",
)

rabbitmq_container_2 = container(
    image="rabbitmq:3.11-alpine",
    ports={"5672/tcp": None},
    environment={
        "RABBITMQ_DEFAULT_USER": "user",
        "RABBITMQ_DEFAULT_PASS": "testtest",
    },
    scope="session",
)


@pytest.fixture(scope="session")
def rabbitmq_connection(rabbitmq_container: "wrappers.Container") -> Repid:
    while not rabbitmq_container.ready():
        sleep(0.1)
    repid = Repid(
        Connection(
            RabbitMessageBroker(
                f"amqp://user:testtest@localhost:{rabbitmq_container.ports['5672/tcp'][0]}"
            )
        )
    )
    return repid


@pytest.fixture(scope="session")
def redis_connection(redis_container: "wrappers.Container") -> Repid:
    while not redis_container.ready():
        sleep(0.1)
    repid = Repid(
        Connection(
            RedisMessageBroker(f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/0"),
            RedisBucketBroker(f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/1"),
            RedisBucketBroker(
                f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/1",
                use_result_bucket=True,
            ),
        )
    )
    return repid


@pytest.fixture(scope="session")
def rabbitmq_with_redis_connection(
    rabbitmq_container_2: "wrappers.Container",
    redis_container: "wrappers.Container",
) -> Repid:
    while not rabbitmq_container_2.ready() or not redis_container.ready():
        sleep(0.1)
    repid = Repid(
        Connection(
            RabbitMessageBroker(
                f"amqp://user:testtest@localhost:{rabbitmq_container_2.ports['5672/tcp'][0]}"
            ),
            RedisBucketBroker(f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/2"),
            RedisBucketBroker(
                f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/2",
                use_result_bucket=True,
            ),
        )
    )
    return repid


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("rabbitmq_connection"),
        lazy_fixture("redis_connection"),
        lazy_fixture("rabbitmq_with_redis_connection"),
    ],
)
def autoconn(request: pytest.FixtureRequest) -> Any:
    return request.param
