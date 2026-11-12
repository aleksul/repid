import os
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from pathlib import Path

import pytest

from repid import Repid
from repid.connections.amqp import AmqpServer
from repid.connections.kafka import KafkaServer
from repid.connections.nats import NatsServer
from repid.connections.pubsub import PubsubServer
from repid.connections.redis import RedisServer
from repid.connections.sqs import SqsServer
from tests.integration.brokers import BROKERS, INTEGRATION_TARGETS, BrokerContainer, start_broker

BrokerFor = Callable[[str], BrokerContainer]


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    integration_dir = Path(__file__).parent
    for item in items:
        if item.path.is_relative_to(integration_dir):
            targets = [mark.kwargs.get("name") for mark in item.iter_markers("integration_target")]
            if len(targets) != 1 or targets[0] not in INTEGRATION_TARGETS:
                raise pytest.UsageError(f"{item.nodeid} must specify one valid integration target")
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def broker_for() -> Iterator[BrokerFor]:
    prepared_target = os.environ.get("REPID_TEST_BROKER")
    containers: dict[str, BrokerContainer] = {}
    with ExitStack() as stack:

        def get_broker(target: str) -> BrokerContainer:
            if prepared_target is not None:
                if prepared_target != target:
                    raise pytest.UsageError(
                        f"Prepared {prepared_target!r} but the test requested {target!r}",
                    )
                return BrokerContainer(
                    os.environ["REPID_TEST_CONTAINER"],
                    int(os.environ["REPID_TEST_PORT"]),
                )
            if target not in containers:
                broker = start_broker(target)
                stack.callback(broker.stop)
                containers[target] = broker
            return containers[target]

        yield get_broker


@pytest.fixture(scope="session")
def rabbitmq_container(broker_for: BrokerFor) -> BrokerContainer:
    return broker_for("rabbitmq")


@pytest.fixture(scope="session")
def rabbitmq_connection(rabbitmq_container: BrokerContainer) -> AmqpServer:
    return AmqpServer(f"amqp://user:testtest@{rabbitmq_container.address}")


@pytest.fixture(scope="session")
def activemq_connection(broker_for: BrokerFor) -> AmqpServer:
    return AmqpServer(
        dsn=f"amqp://user:testtest@{broker_for('activemq').address}",
        publish_naming_strategy=lambda channel, _: channel,
        subscribe_naming_strategy=lambda channel: channel,
    )


@pytest.fixture(scope="session")
def pubsub_connection(broker_for: BrokerFor) -> PubsubServer:
    return PubsubServer(f"http://{broker_for('pubsub').address}", default_project="my-project")


@pytest.fixture(scope="session")
def redis_connection(broker_for: BrokerFor) -> RedisServer:
    return RedisServer(f"redis://:test@{broker_for('redis').address}/0")


@pytest.fixture(scope="session")
def kafka_connection(broker_for: BrokerFor) -> KafkaServer:
    return KafkaServer(broker_for("kafka").address, client_id="repid-test-client")


@pytest.fixture
def nats_connection(broker_for: BrokerFor) -> NatsServer:
    return NatsServer(f"nats://{broker_for('nats').address}")


@pytest.fixture(scope="session")
def sqs_connection(broker_for: BrokerFor) -> SqsServer:
    return SqsServer(
        endpoint_url=f"http://{broker_for('sqs').address}",
        region_name="elasticmq",
        aws_access_key_id="x",
        aws_secret_access_key="x",
        aws_session_token="x",
        receive_wait_time_seconds=0,
    )


@pytest.fixture(
    params=[
        pytest.param(target, marks=pytest.mark.integration_target(name=target))
        for target in BROKERS
    ],
)
def autoconn(request: pytest.FixtureRequest) -> Repid:
    app = Repid()
    server = request.getfixturevalue(f"{request.param}_connection")
    app.servers.register_server("default", server, is_default=True)
    return app
