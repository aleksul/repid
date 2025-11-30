from typing import Annotated

import pytest

from repid import Header, Message, Repid, Router
from repid.test_client import TestClient


async def test_test_client_basic() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(arg1: str) -> None:
        nonlocal received
        received = arg1

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            payload=b'{"arg1": "hello"}',
            headers={"topic": "myactor"},
        )

    assert received == "hello"


async def test_test_client_with_router() -> None:
    router = Router()

    received = None

    @router.actor
    async def myactor(arg1: str) -> None:
        nonlocal received
        received = arg1

    app = Repid()
    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message(
            operation_id="myactor",
            payload=b'{"arg1": "world"}',
            headers={"topic": "myactor"},
        )

    assert received == "world"


async def test_test_client_json() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(key: str) -> None:
        nonlocal received
        received = key

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message_json(
            operation_id="myactor",
            payload={"key": "value"},
            headers={"topic": "myactor"},
        )

    assert received == "value"


async def test_test_client_with_headers() -> None:
    app = Repid()
    router = Router()

    received = None

    @router.actor
    async def myactor(
        arg1: str,  # noqa: ARG001
        x_custom: Annotated[str, Header(name="X-Custom")],
    ) -> None:
        nonlocal received
        received = x_custom

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "hello"},
            headers={"topic": "myactor", "X-Custom": "header"},
        )

    assert received == "header"


async def test_test_client_unknown_operation() -> None:
    app = Repid()

    async with TestClient(app) as client:
        with pytest.raises(ValueError, match="Operation 'unknown_actor' not found"):
            await client.send_message_json(operation_id="unknown_actor", payload={})


async def test_test_client_multiple_messages() -> None:
    app = Repid()
    router = Router()

    received_messages = []

    @router.actor
    async def myactor(arg1: str) -> None:
        nonlocal received_messages
        received_messages.append(arg1)

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    async with TestClient(app) as client:
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "first"},
            headers={"topic": "myactor"},
        )
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "second"},
            headers={"topic": "myactor"},
        )
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "third"},
            headers={"topic": "myactor"},
        )

    assert received_messages == ["first", "second", "third"]


async def test_test_client_process_next_and_clear() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def myactor(arg1: str) -> None:
        pass

    app.include_router(router)
    app.messages.register_operation(operation_id="myactor", channel="default")

    client = TestClient(app, auto_process=False)

    async with client:
        # send two messages and ensure they are queued
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "first"},
            headers={"topic": "myactor"},
        )
        await client.send_message_json(
            operation_id="myactor",
            payload={"arg1": "second"},
            headers={"topic": "myactor"},
        )

        # process next should process first queued message
        processed = await client.process_next()
        assert processed is not None
        assert processed.result is None or processed.success is True

        # process all should process remaining
        processed_messages = await client.process_all()
        assert isinstance(processed_messages, list)

        # clear should remove any queued messages and state
        client.clear()
        assert client.get_sent_messages() == []
        assert client.get_processed_messages() == []


async def test_test_client_reply_queues_new_message_and_error_handling() -> None:
    app = Repid()
    router = Router()

    processed = False

    @router.actor
    async def dummy_actor(message: Message) -> None:
        await message.reply(payload=b"", headers={"topic": "reply_actor"})

    @router.actor
    async def reply_actor() -> None:
        nonlocal processed
        processed = True

    app.include_router(router)

    async with TestClient(app) as client:
        await client.send_message(
            channel="default",
            payload=b"",
            headers={"topic": "dummy_actor"},
        )
        assert processed is False
        # processing of the original message has queued a reply message, process it now
        await client.process_next()
        assert processed is True
