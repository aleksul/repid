from pathlib import Path
from time import sleep
from typing import Any

import pytest
from pytest_docker_tools import container, wrappers
from pytest_lazy_fixtures import lf as lazy_fixture

from repid import Repid
from repid.connections.amqp import AmqpServer

RABBITMQ_DEFINITIONS_JSON = Path(__file__).parent / "rabbitmq_definitions.json"


redis_container = container(
    image="redis:8.4-alpine",
    ports={"6379/tcp": None},
    command="redis-server --requirepass test",
    scope="session",
)

rabbitmq_container = container(
    image="rabbitmq:4.2-alpine",
    ports={"5672/tcp": None},
    volumes={
        str(RABBITMQ_DEFINITIONS_JSON): {
            "bind": "/etc/rabbitmq/definitions.json",
            "mode": "ro",
        },
    },
    scope="session",
)


@pytest.fixture(scope="session")
def rabbitmq_connection(rabbitmq_container: "wrappers.Container") -> Repid:
    while (
        not rabbitmq_container.ready()
        and "Server startup complete" not in rabbitmq_container.logs()
    ):
        sleep(0.1)

    rabbitmq_container.exec_run("rabbitmqctl import_definitions /etc/rabbitmq/definitions.json")

    while "Successfully set permissions for user 'user'" not in rabbitmq_container.logs():
        sleep(0.1)

    server = AmqpServer(
        dsn=f"amqp://user:testtest@localhost:{rabbitmq_container.ports['5672/tcp'][0]}",
    )

    app = Repid()
    app.servers.register_server("default", server, is_default=True)

    return app


@pytest.fixture(scope="session")
def redis_connection(redis_container: "wrappers.Container") -> Repid:
    while not redis_container.ready():
        sleep(0.1)
    # server = RedisServer(f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/0")
    app = Repid()
    # app.servers.register_server("default", server, is_default=True)
    return app  # noqa: RET504


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("rabbitmq_connection"),
        # lazy_fixture("redis_connection"),
    ],
)
def autoconn(request: pytest.FixtureRequest) -> Any:
    return request.param
