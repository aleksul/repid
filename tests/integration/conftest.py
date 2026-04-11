import asyncio
import contextlib
from pathlib import Path
from time import sleep
from typing import Any

import aiobotocore.session
import grpc
import nats
import pytest
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError
from pytest_docker_tools import container, wrappers
from pytest_lazy_fixtures import lf as lazy_fixture

from repid import Repid
from repid.connections.abc import ServerT
from repid.connections.amqp import AmqpServer
from repid.connections.kafka import KafkaServer
from repid.connections.nats import NatsServer
from repid.connections.pubsub import PubsubServer
from repid.connections.redis import RedisServer
from repid.connections.sqs import SqsServer
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
    image="ghcr.io/jeffijoe/deltio:0.7.0",
    ports={"8085/tcp": None},
    scope="session",
    wrapper_class=DeltioContainer,
)


class KafkaContainer(wrappers.Container):
    def ready(self) -> bool:
        self._container.reload()
        return bool(self.status == "running")


kafka_container = container(
    image="redpandadata/redpanda:v26.1.4",
    entrypoint="/bin/sh",
    command=[
        "-c",
        "while [ ! -f /tmp/port ]; do sleep 0.1; done && "
        "PORT=$(cat /tmp/port) && "
        "exec /usr/bin/rpk redpanda start --smp 1 --reserve-memory 0M --overprovisioned --node-id 0 "
        "--kafka-addr PLAINTEXT://0.0.0.0:29092 "
        "--advertise-kafka-addr PLAINTEXT://127.0.0.1:$PORT",
    ],
    ports={"29092/tcp": None},
    scope="session",
    wrapper_class=KafkaContainer,
)


class NatsContainer(wrappers.Container):
    def ready(self) -> bool:
        return "Server is ready" in self.logs()


nats_container = container(
    image="nats:2.12.6",
    command="-js",
    ports={"4222/tcp": None},
    scope="session",
    wrapper_class=NatsContainer,
)

elasticmq_container = container(
    image="softwaremill/elasticmq-native:1.7.1",
    ports={"9324/tcp": None},
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


@pytest.fixture(scope="session")
async def kafka_connection(kafka_container: "wrappers.Container") -> ServerT:  # noqa: C901, PLR0912
    # Wait for container ports to be assigned
    port = None
    for _ in range(100):
        try:
            kafka_container._container.reload()
            port = kafka_container.ports["29092/tcp"][0]
            break
        except (KeyError, AttributeError):
            sleep(0.1)

    if port is None:
        raise RuntimeError("Could not get port from kafka container")

    kafka_container.exec_run(f"sh -c 'echo {port} > /tmp/port'")

    while "Started Kafka API server" not in kafka_container.logs():
        await asyncio.sleep(0.1)

    dsn = f"127.0.0.1:{port}"
    server = KafkaServer(dsn, client_id="repid-test-client")

    # Retry connection until redpanda is fully up
    for _ in range(100):
        try:
            await server.connect()
            await server.disconnect()
            break
        except (KafkaConnectionError, ConnectionRefusedError):
            await asyncio.sleep(0.1)
    else:
        raise Exception("Kafka did not start in time")

    # Connect admin client and list existing topics
    admin_client = AIOKafkaAdminClient(bootstrap_servers=dsn)
    for _ in range(10):
        try:
            await admin_client.start()
            existing_topics = await admin_client.list_topics()
            break
        except Exception:
            await asyncio.sleep(0.5)
    else:
        raise Exception("Failed to start admin client")

    # Create necessary topics
    try:
        topics_to_create = []
        for topic in [
            "default",
            "repid_default_dlq",
            "another",
            "test_reject_channel",
            "repid_test_reject_channel_dlq",
            "test_nack_channel",
            "repid_test_nack_channel_dlq",
            "test_reply_channel",
            "test_reply_dest_channel",
            "test_close_channel",
        ]:
            if topic not in existing_topics:
                topics_to_create.append(NewTopic(topic, num_partitions=1, replication_factor=1))

        if topics_to_create:
            await admin_client.create_topics(topics_to_create)
    finally:
        await admin_client.close()

    return server


@pytest.fixture
async def nats_connection(nats_container: "wrappers.Container") -> ServerT:

    port = None
    for _ in range(100):
        try:
            nats_container._container.reload()
            port = nats_container.ports["4222/tcp"][0]
            break
        except (KeyError, AttributeError):
            await asyncio.sleep(0.1)

    if port is None:
        raise RuntimeError("Could not get port from nats container")

    dsn = f"nats://127.0.0.1:{port}"
    server = NatsServer(dsn)

    for _ in range(100):
        nc = None
        try:
            nc = await nats.connect(dsn)
            js = nc.jetstream()

            async def ensure_stream(js_context: Any, name: str, subjects: list[str]) -> None:
                try:
                    await js_context.add_stream(name=name, subjects=subjects)
                except Exception as e:
                    if (
                        "stream name already in use" not in str(e).lower()
                        and "already exists" not in str(e).lower()
                    ):
                        raise

            await ensure_stream(
                js_context=js,
                name="repid_default",
                subjects=["default", "another", "another_edge_case"],
            )
            await ensure_stream(
                js_context=js,
                name="test_reject_channel_stream",
                subjects=["test_reject_channel"],
            )
            await ensure_stream(
                js_context=js,
                name="test_reply_channel_stream",
                subjects=["test_reply_channel"],
            )
            await ensure_stream(
                js_context=js,
                name="test_exc_channel_stream",
                subjects=["test_exc_channel"],
            )
            await ensure_stream(
                js_context=js,
                name="repid_test_exc_channel_dlq_stream",
                subjects=["repid_test_exc_channel_dlq"],
            )
            await ensure_stream(
                js_context=js,
                name="repid_default_dlq",
                subjects=["repid_default_dlq"],
            )
            await ensure_stream(
                js_context=js,
                name="test_nack_channel_stream",
                subjects=["test_nack_channel"],
            )
            await ensure_stream(
                js_context=js,
                name="test_nack_channel_stream_2",
                subjects=["test_nack_channel_2"],
            )
            await ensure_stream(
                js_context=js,
                name="repid_test_nack_channel_dlq_stream",
                subjects=["repid_test_nack_channel_dlq"],
            )
            break
        except Exception:
            await asyncio.sleep(0.1)
        finally:
            if nc is not None:
                with contextlib.suppress(Exception):
                    await nc.close()
    else:
        raise Exception("NATS did not start in time")

    return server


@pytest.fixture(scope="session")
async def sqs_connection(elasticmq_container: "wrappers.Container") -> ServerT:
    while "Started SQS rest server" not in elasticmq_container.logs():
        await asyncio.sleep(0.1)

    port = elasticmq_container.ports["9324/tcp"][0]
    endpoint_url = f"http://127.0.0.1:{port}"

    session = aiobotocore.session.get_session()
    async with session.create_client(
        "sqs",
        region_name="elasticmq",
        endpoint_url=endpoint_url,
        aws_access_key_id="x",
        aws_secret_access_key="x",
        aws_session_token="x",
    ) as client:
        # Create necessary queues for tests
        queues_to_create = [
            "default",
            "test_nack_channel",
            "repid_test_nack_channel_dlq",
            "test_reply_channel",
            "test_reply_dest_channel",
            "test_double_actions_channel",
            "test_exception_channel",
            "test_close_channel",
            "another",
            "other_channel",
            "wrong",
        ]
        for q in queues_to_create:
            await client.create_queue(QueueName=q)

    return SqsServer(
        endpoint_url=endpoint_url,
        region_name="elasticmq",
        aws_access_key_id="x",
        aws_secret_access_key="x",
        aws_session_token="x",
        receive_wait_time_seconds=0,
    )


@pytest.fixture(
    params=[
        lazy_fixture("rabbitmq_connection"),
        lazy_fixture("pubsub_connection"),
        lazy_fixture("redis_connection"),
        lazy_fixture("kafka_connection"),
        lazy_fixture("nats_connection"),
        lazy_fixture("sqs_connection"),
    ],
)
def autoconn(request: pytest.FixtureRequest) -> Repid:
    app = Repid()
    app.servers.register_server("default", request.param, is_default=True)
    return app
