from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from repid import Repid
from repid.connections.amqp import AmqpServer

if TYPE_CHECKING:
    from pytest_docker_tools import wrappers


async def test_server_side_cancel(
    rabbitmq_connection: AmqpServer,
    rabbitmq_container: wrappers.Container,
) -> None:
    callback_event = asyncio.Event()

    repid_app = Repid()
    repid_app.servers.register_server("default", rabbitmq_connection, is_default=True)

    async with repid_app.servers.default.connection() as conn:

        async def dummy_callback(_: Any) -> None:
            callback_event.set()

        subscriber = await conn.subscribe(channels_to_callbacks={"default": dummy_callback})

        rabbit_list_result = rabbitmq_container.exec_run("rabbitmqctl list_consumers")
        assert rabbit_list_result.exit_code == 0, (
            f"Failed to list consumers: {rabbit_list_result.output.decode()}"
        )
        pid: str = (
            rabbit_list_result.output.decode()
            .removeprefix("Listing consumers ...\n")
            .removeprefix("Listing consumers in vhost / ...\n")
            .splitlines()[1]
            .split("\t")[1]
        )

        rabbit_close_result = rabbitmq_container.exec_run(
            f'rabbitmqctl close_connection "{pid}" "Server side cancel test"',
        )
        assert rabbit_close_result.exit_code == 0, (
            f"Failed to close connection: {rabbit_close_result.output.decode()}"
        )

        assert not callback_event.is_set()

        # test that even after server closed the connection,
        # we still should be able to send and receive messages
        # because the client should reconnect automatically
        await repid_app.send_message(channel="default", payload=b"")

        await asyncio.wait_for(callback_event.wait(), timeout=30.0)

        await subscriber.close()
