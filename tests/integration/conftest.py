from pathlib import Path
from time import sleep

import grpc
import pytest
from pytest_docker_tools import container, wrappers
from pytest_lazy_fixtures import lf as lazy_fixture

from repid import Repid
from repid.connections.abc import ServerT
from repid.connections.amqp import AmqpServer
from repid.connections.pubsub import PubsubServer
from repid.connections.redis import RedisServer
from tests.integration.pubsub_proto_helpers import Subscription, Topic

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


class DeltioContainer(wrappers.Container):
    def ready(self) -> bool:
        return "listening on" in self.logs()


pubsub_container = container(
    image="ghcr.io/jeffijoe/deltio:latest",
    ports={"8085/tcp": None},
    scope="session",
    wrapper_class=DeltioContainer,
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
    # Wait for container ports to be assigned
    port = None
    for _ in range(100):
        try:
            pubsub_container._container.reload()
            port = pubsub_container.ports["8085/tcp"][0]
            break
        except (KeyError, AttributeError):
            sleep(0.1)

    if port is None:
        raise RuntimeError("Could not get port from deltio container")

    target = f"localhost:{port}"

    # Wait for readiness
    for _ in range(100):
        try:
            with grpc.insecure_channel(target) as channel:
                grpc.channel_ready_future(channel).result(timeout=0.1)
            break
        except Exception:
            sleep(0.1)
    else:
        raise TimeoutError("Deltio container failed to start")

    # Create topics and subscriptions
    with grpc.insecure_channel(target) as channel:
        create_topic = channel.unary_unary(
            "/google.pubsub.v1.Publisher/CreateTopic",
            request_serializer=Topic.serialize,
            response_deserializer=lambda x: x,
        )
        create_subscription = channel.unary_unary(
            "/google.pubsub.v1.Subscriber/CreateSubscription",
            request_serializer=Subscription.serialize,
            response_deserializer=lambda x: x,
        )

        create_topic(Topic(name="projects/my-project/topics/default"))
        create_subscription(
            Subscription(
                name="projects/my-project/subscriptions/default",
                topic="projects/my-project/topics/default",
                ack_deadline_seconds=10,
            ),
        )

        create_topic(Topic(name="projects/my-project/topics/another"))
        create_subscription(
            Subscription(
                name="projects/my-project/subscriptions/another",
                topic="projects/my-project/topics/another",
                ack_deadline_seconds=10,
            ),
        )

    return PubsubServer(
        dsn=f"http://{target}",
        default_project="my-project",
    )


@pytest.fixture(scope="session")
def redis_connection(redis_container: "wrappers.Container") -> ServerT:
    while "Ready to accept connections tcp" not in redis_container.logs():
        sleep(0.1)

    sleep(3)

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
