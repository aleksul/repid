from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

from repid import Repid
from repid.connections.abc import ReceivedMessageT
from repid.connections.amqp import AmqpServer
from repid.connections.amqp._uamqp.message import Properties

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

        async def dummy_callback(msg: ReceivedMessageT) -> None:
            callback_event.set()
            await msg.ack()

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


async def test_message_id_is_set_to_uuid4(rabbitmq_connection: AmqpServer) -> None:
    received_message_id: str | None = None
    done = asyncio.Event()

    repid_app = Repid()
    repid_app.servers.register_server("default", rabbitmq_connection, is_default=True)

    async with repid_app.servers.default.connection() as conn:

        async def capture_callback(msg: ReceivedMessageT) -> None:
            nonlocal received_message_id
            received_message_id = msg.message_id
            done.set()
            await msg.ack()

        subscriber = await conn.subscribe(channels_to_callbacks={"default": capture_callback})

        await repid_app.send_message(channel="default", payload=b"")
        await asyncio.wait_for(done.wait(), timeout=10.0)
        await subscriber.close()

    assert received_message_id is not None
    UUID(received_message_id)  # raises ValueError if not a valid UUID


async def test_message_id_is_preserved_when_set_by_user(rabbitmq_connection: AmqpServer) -> None:
    custom_id = "my-custom-message-id"
    received_message_id: str | None = None
    done = asyncio.Event()

    repid_app = Repid()
    repid_app.servers.register_server("default", rabbitmq_connection, is_default=True)

    async with repid_app.servers.default.connection() as conn:

        async def capture_callback(msg: ReceivedMessageT) -> None:
            nonlocal received_message_id
            received_message_id = msg.message_id
            done.set()
            await msg.ack()

        subscriber = await conn.subscribe(channels_to_callbacks={"default": capture_callback})

        await repid_app.send_message(
            channel="default",
            payload=b"",
            server_specific_parameters={"properties": Properties(message_id=custom_id)},
        )
        await asyncio.wait_for(done.wait(), timeout=10.0)
        await subscriber.close()

    assert received_message_id == custom_id
