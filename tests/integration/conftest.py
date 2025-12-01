from pathlib import Path
from time import sleep

import pytest
from pytest_docker_tools import container, wrappers
from pytest_lazy_fixtures import lf as lazy_fixture

from repid import Repid
from repid.connections.abc import ServerT
from repid.connections.amqp import AmqpServer
from repid.connections.pubsub import PubsubServer
from repid.connections.redis import RedisServer

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

pubsub_container = container(
    image="messagebird/gcloud-pubsub-emulator:latest",
    ports={"8681/tcp": None},
    environment={"PUBSUB_PROJECT1": "my-project,default:default,another:another"},
    scope="session",
)


@pytest.fixture(scope="session")
def rabbitmq_connection(rabbitmq_container: "wrappers.Container") -> ServerT:
    while "Server startup complete" not in rabbitmq_container.logs():
        sleep(0.1)

    import_definitions_result = rabbitmq_container.exec_run(
        "rabbitmqctl import_definitions /etc/rabbitmq/definitions.json",
    )
    assert import_definitions_result.exit_code == 0, (
        f"Failed to import definitions: {import_definitions_result.output.decode()}"
    )

    while "Successfully set permissions for user 'user'" not in rabbitmq_container.logs():
        sleep(0.1)

    return AmqpServer(
        dsn=f"amqp://user:testtest@localhost:{rabbitmq_container.ports['5672/tcp'][0]}",
    )


@pytest.fixture(scope="session")
def pubsub_connection(pubsub_container: "wrappers.Container") -> ServerT:
    patterns = [
        "Server started, listening on",
        'Creating topic "default"',
        'Creating subscription "default"',
        'Creating topic "another"',
        'Creating subscription "another"',
    ]
    while not all(pattern in pubsub_container.logs() for pattern in patterns):
        sleep(0.1)

    return PubsubServer(
        dsn=f"http://localhost:{pubsub_container.ports['8681/tcp'][0]}/v1",
        default_project="my-project",
    )


@pytest.fixture(scope="session")
def redis_connection(redis_container: "wrappers.Container") -> ServerT:
    return RedisServer(f"redis://:test@localhost:{redis_container.ports['6379/tcp'][0]}/0")


@pytest.fixture(
    params=[
        lazy_fixture("rabbitmq_connection"),
        lazy_fixture("pubsub_connection"),
        lazy_fixture("redis_connection"),
    ],
)
def autoconn(request: pytest.FixtureRequest) -> Repid:
    app = Repid()
    app.servers.register_server("default", request.param, is_default=True)
    return app
