from time import sleep
from typing import TYPE_CHECKING, Any

import pytest
from pytest_docker_tools import container
from pytest_lazyfixture import lazy_fixture

from repid import Connection, Repid
from repid.connections import RabbitBroker

if TYPE_CHECKING:
    from pytest_docker_tools import wrappers

# redis_container = container(
#     image="redis:7.0-alpine",
#     ports={"6379/tcp": None},
#     command="redis-server --requirepass test",
#     scope="session",
# )

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
def rabbitmq_connection(rabbitmq_container: "wrappers.Container") -> Repid:
    while not rabbitmq_container.ready():
        sleep(0.1)
    repid = Repid(
        Connection(
            RabbitBroker(
                f"amqp://user:testtest@localhost:{rabbitmq_container.ports['5672/tcp'][0]}"
            )
        )
    )
    return repid


@pytest.fixture(scope="session")
def redis_connection(redis_container: "wrappers.Container") -> Repid:
    while not redis_container.ready():
        sleep(0.1)

    repid = None
    return repid  # type: ignore[return-value]


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("rabbitmq_connection"),
        # lazy_fixture("redis_connection"),
    ],
)
def autoconn(request: pytest.FixtureRequest) -> Any:
    return request.param
