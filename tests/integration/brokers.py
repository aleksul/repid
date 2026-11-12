"""Start and initialize the same test brokers locally and in CI."""

import argparse
import asyncio
import shutil
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import aiobotocore.session
import grpc
import nats
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError

from tests.integration.pubsub_proto_helpers import Subscription, Topic


@dataclass(frozen=True)
class BrokerConfig:
    image: str
    port: int
    ready_message: str
    options: tuple[str, ...] = ()
    command: tuple[str, ...] = ()


BROKERS = {
    "rabbitmq": BrokerConfig(
        "rabbitmq:4.2-alpine",
        5672,
        "Server startup complete",
        options=(
            "--volume",
            f"{Path(__file__).with_name('rabbitmq_definitions.json')}:/etc/rabbitmq/definitions.json:ro",
        ),
    ),
    "activemq": BrokerConfig(
        "apache/artemis:2.54.0-alpine",
        61616,
        "AMQ221007",
        options=(
            "--env",
            "ARTEMIS_USER=user",
            "--env",
            "ARTEMIS_PASSWORD=testtest",
            # Keep the disposable journal independent of the Docker host's disk usage.
            "--tmpfs",
            "/var/lib/artemis-instance/data:rw,size=256m,mode=1777",
        ),
    ),
    "pubsub": BrokerConfig("ghcr.io/jeffijoe/deltio:0.7.0", 8085, "listening on"),
    "redis": BrokerConfig(
        "redis:8.4-alpine",
        6379,
        "Ready to accept connections tcp",
        command=("redis-server", "--requirepass", "test"),
    ),
    "kafka": BrokerConfig(
        "redpandadata/redpanda:v26.1.4",
        29092,
        "Started Kafka API server",
        options=("--entrypoint", "/bin/sh"),
        command=(
            "-c",
            (
                "while [ ! -s /tmp/port ]; do sleep 0.1; done; "
                "exec /usr/bin/rpk redpanda start --smp 1 --reserve-memory 0M --overprovisioned "
                "--node-id 0 --kafka-addr PLAINTEXT://0.0.0.0:29092 "
                '--advertise-kafka-addr "PLAINTEXT://127.0.0.1:$(cat /tmp/port)"'
            ),
        ),
    ),
    "nats": BrokerConfig("nats:2.12.6", 4222, "Server is ready", command=("-js",)),
    "sqs": BrokerConfig("softwaremill/elasticmq-native:1.7.1", 9324, "Started SQS rest server"),
}
INTEGRATION_TARGETS = (*BROKERS, "asyncapi")


def _docker(*args: str, timeout: int = 30, stderr: int | None = None) -> str:
    executable = shutil.which("docker")
    if executable is None:
        raise FileNotFoundError("Docker is required to start integration test brokers")
    return subprocess.check_output(
        [executable, *args],
        text=True,
        timeout=timeout,
        stderr=stderr,
    ).strip()


@dataclass(frozen=True)
class BrokerContainer:
    name: str
    port: int

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def exec(self, *args: str) -> str:
        return _docker("exec", self.name, *args)

    def logs(self, *, tail: str = "all") -> str:
        return _docker("logs", "--tail", tail, self.name, stderr=subprocess.STDOUT)

    def stop(self) -> None:
        _docker("rm", "--force", "--volumes", self.name)


async def _prepare_broker(target: str, broker: BrokerContainer) -> None:
    if target == "kafka":
        # Docker assigns the host port. Redpanda must advertise that port to clients.
        broker.exec("sh", "-c", 'echo "$1" > /tmp/port', "sh", str(broker.port))

    async with asyncio.timeout(120):
        while BROKERS[target].ready_message not in broker.logs():
            await asyncio.sleep(0.5)

        match target:
            case "rabbitmq":
                broker.exec("rabbitmqctl", "import_definitions", "/etc/rabbitmq/definitions.json")
            case "activemq":
                _prepare_activemq(broker)
            case "pubsub":
                await _prepare_pubsub(broker.address)
            case "kafka":
                await _prepare_kafka(broker.address)
            case "nats":
                await _prepare_nats(broker.address)
            case "sqs":
                await _prepare_sqs(broker.address)


def _prepare_activemq(broker: BrokerContainer) -> None:
    for queue in ("default", "another", "my_queue"):
        broker.exec(
            "/var/lib/artemis-instance/bin/artemis",
            "queue",
            "create",
            "--name",
            queue,
            "--address",
            queue,
            "--anycast",
            "--durable",
            "--auto-create-address",
            "--silent",
            "--user",
            "user",
            "--password",
            "testtest",
            "--url",
            "tcp://localhost:61616",
        )


async def _prepare_pubsub(address: str) -> None:
    async with grpc.aio.insecure_channel(address) as channel:
        await channel.channel_ready()
        create_topic: grpc.aio.UnaryUnaryMultiCallable[Topic, bytes] = channel.unary_unary(
            "/google.pubsub.v1.Publisher/CreateTopic",
            request_serializer=Topic.serialize,
        )
        create_subscription: grpc.aio.UnaryUnaryMultiCallable[Subscription, bytes] = (
            channel.unary_unary(
                "/google.pubsub.v1.Subscriber/CreateSubscription",
                request_serializer=Subscription.serialize,
            )
        )
        for channel_name in ("default", "another"):
            resource_name = f"projects/my-project/topics/{channel_name}"
            await create_topic(Topic(name=resource_name))
            await create_subscription(
                Subscription(
                    name=f"projects/my-project/subscriptions/{channel_name}",
                    topic=resource_name,
                    ack_deadline_seconds=10,
                ),
            )


async def _prepare_kafka(address: str) -> None:
    topics = (
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
    )
    while True:
        admin = AIOKafkaAdminClient(bootstrap_servers=address, request_timeout_ms=5000)
        try:
            await admin.start()
            existing = await admin.list_topics()
            missing = [NewTopic(name, 1, 1) for name in topics if name not in existing]
            if missing:
                await admin.create_topics(missing)
            return
        except KafkaConnectionError:
            await asyncio.sleep(0.5)
        finally:
            await admin.close()


async def _prepare_nats(address: str) -> None:
    streams = {
        "repid_default": ["default", "another", "another_edge_case"],
        "test_reject_channel_stream": ["test_reject_channel"],
        "test_reply_channel_stream": ["test_reply_channel"],
        "test_exc_channel_stream": ["test_exc_channel"],
        "repid_test_exc_channel_dlq_stream": ["repid_test_exc_channel_dlq"],
        "repid_default_dlq": ["repid_default_dlq"],
        "test_nack_channel_stream": ["test_nack_channel"],
        "test_nack_channel_stream_2": ["test_nack_channel_2"],
        "repid_test_nack_channel_dlq_stream": ["repid_test_nack_channel_dlq"],
    }
    client = await nats.connect(f"nats://{address}", allow_reconnect=False)
    try:
        js = client.jetstream()
        for name, subjects in streams.items():
            await js.add_stream(name=name, subjects=subjects)
    finally:
        await client.close()


async def _prepare_sqs(address: str) -> None:
    queues = (
        "default",
        "test_subscriber_state_channel",
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
    )
    session = aiobotocore.session.get_session()
    async with session.create_client(
        "sqs",
        region_name="elasticmq",
        endpoint_url=f"http://{address}",
        aws_access_key_id="x",
        aws_secret_access_key="x",
        aws_session_token="x",
    ) as client:
        for name in queues:
            await client.create_queue(QueueName=name)


def start_broker(target: str) -> BrokerContainer:
    config = BROKERS[target]
    name = f"repid-test-{target}-{uuid4().hex[:12]}"
    try:
        _docker(
            "run",
            "--detach",
            "--name",
            name,
            "--publish",
            f"127.0.0.1::{config.port}",
            *config.options,
            config.image,
            *config.command,
            timeout=300,
        )
        address = _docker("port", name, f"{config.port}/tcp")
        broker = BrokerContainer(name, int(address.rsplit(":", 1)[1]))
        asyncio.run(_prepare_broker(target, broker))
    except BaseException:
        with suppress(OSError, subprocess.SubprocessError):
            logs = _docker("logs", "--tail", "100", name, stderr=subprocess.STDOUT)
            sys.stderr.write(logs + "\n")
        with suppress(OSError, subprocess.SubprocessError):
            _docker("rm", "--force", "--volumes", name)
        raise
    return broker


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=BROKERS)
    parser.add_argument("--env-file", required=True, type=Path)
    args = parser.parse_args()
    with args.env_file.open("a", encoding="utf-8") as env_file:
        broker = start_broker(args.target)
        env_file.write(
            f"REPID_TEST_BROKER={args.target}\n"
            f"REPID_TEST_CONTAINER={broker.name}\n"
            f"REPID_TEST_PORT={broker.port}\n",
        )


if __name__ == "__main__":
    main()
