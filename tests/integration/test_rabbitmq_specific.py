import asyncio
from typing import TYPE_CHECKING, cast

import pytest

from repid import Job, Repid
from repid.connections.rabbitmq import RabbitMessageBroker

if TYPE_CHECKING:
    from pytest_docker_tools import wrappers


async def test_server_side_cancel(
    rabbitmq_connection: Repid,
    rabbitmq_container: "wrappers.Container",
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with rabbitmq_connection.magic(auto_disconnect=True) as conn:
        message_broker = cast(RabbitMessageBroker, conn.message_broker)
        await message_broker.queue_declare("default")

        consumer = message_broker.get_consumer("default")

        await consumer.start()

        await asyncio.sleep(0.5)

        consumers = rabbitmq_container.exec_run("rabbitmqctl list_consumers")
        ctag = consumers.output.decode().split("\n")[2].split("\t")[2]

        await message_broker._channel.basic_cancel(ctag)

        await Job("do_nothing").enqueue()

        r, _, _ = await asyncio.wait_for(consumer.consume(), timeout=5)
        assert r.topic == "do_nothing"

        assert any(
            (
                all(
                    (
                        "ERROR" in x,
                        "RabbitMQ has terminated consumer" in x,
                    ),
                )
                for x in caplog.text.splitlines()
            ),
        )

        await consumer.finish()
