import httpx
import pytest

from repid import Repid, Router
from repid.connections.in_memory import InMemoryServer


def test_get_asyncapi_schema() -> None:
    app = Repid()

    schema = app.generate_asyncapi_schema()
    assert schema is not None
    assert "asyncapi" in schema
    assert "info" in schema
    assert "channels" in schema


def test_get_asyncapi_schema_html() -> None:
    server = InMemoryServer()
    app = Repid(title="My title")
    app.servers.register_server("default", server, is_default=True)

    html_schema = app.asyncapi_html()
    assert "<!DOCTYPE html>" in html_schema
    assert "<title>My title AsyncAPI</title>" in html_schema


async def test_asyncapi_server_creation(fake_repid: Repid) -> None:
    asyncapi_server = fake_repid.asyncapi_server()
    async with asyncapi_server:
        assert asyncapi_server._server is not None
        assert asyncapi_server._server.is_serving()
        async with httpx.AsyncClient() as client:
            response = await client.get("http://localhost:8081/")
            response.raise_for_status()
            assert "<!DOCTYPE html>" in response.text


async def test_asyncapi_server_404(fake_repid: Repid) -> None:
    asyncapi_server = fake_repid.asyncapi_server()
    async with asyncapi_server, httpx.AsyncClient() as client:
        response = await client.get("http://localhost:8081/unknown/path")
        assert response.status_code == 404
        assert "404 Not Found" in response.text


async def test_send_message_basic(fake_repid: Repid) -> None:
    received = None
    router = Router()

    @router.actor
    async def test_actor(arg1: str) -> None:
        nonlocal received
        received = arg1

    fake_repid.include_router(router)

    async with fake_repid.servers.default.connection():
        await fake_repid.send_message(
            channel="default",
            payload=b'{"arg1": "hello"}',
            headers={"topic": "test_actor"},
        )
        await fake_repid.run_worker(messages_limit=1)

    assert received == "hello"


async def test_send_message_json(fake_repid: Repid) -> None:
    received = None
    router = Router()

    @router.actor
    async def test_actor(arg1: str) -> None:
        nonlocal received
        received = arg1

    fake_repid.include_router(router)

    async with fake_repid.servers.default.connection():
        await fake_repid.send_message_json(
            channel="default",
            payload={"arg1": "hello"},
            headers={"topic": "test_actor"},
        )
        await fake_repid.run_worker(messages_limit=1)

    assert received == "hello"


async def test_send_message_basic_via_operation(fake_repid: Repid) -> None:
    received = None
    router = Router()

    @router.actor
    async def test_actor(arg1: str) -> None:
        nonlocal received
        received = arg1

    fake_repid.include_router(router)
    fake_repid.messages.register_operation(operation_id="test_actor", channel="default")

    async with fake_repid.servers.default.connection():
        await fake_repid.send_message(
            operation_id="test_actor",
            payload=b'{"arg1": "hello"}',
            headers={"topic": "test_actor"},
        )
        await fake_repid.run_worker(messages_limit=1)

    assert received == "hello"


async def test_send_message_json_via_operation(fake_repid: Repid) -> None:
    received = None
    router = Router()

    @router.actor
    async def test_actor(arg1: str) -> None:
        nonlocal received
        received = arg1

    fake_repid.include_router(router)
    fake_repid.messages.register_operation(operation_id="test_actor", channel="default")

    async with fake_repid.servers.default.connection():
        await fake_repid.send_message_json(
            operation_id="test_actor",
            payload={"arg1": "hello"},
            headers={"topic": "test_actor"},
        )
        await fake_repid.run_worker(messages_limit=1)

    assert received == "hello"


def test_include_router() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def router_actor() -> None:
        pass

    app.include_router(router)

    assert any(actor.name == "router_actor" for actor in app._centralized_router.actors)


async def test_run_worker_no_server() -> None:
    app = Repid()

    with pytest.raises(ValueError, match="No default server configured"):
        await app.run_worker()


async def test_run_worker_server_not_found() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("default", server, is_default=True)

    with pytest.raises(ValueError, match="Server 'nonexistent' not found"):
        await app.run_worker(server_name="nonexistent")


async def test_send_message_no_server() -> None:
    app = Repid()

    with pytest.raises(ValueError, match="No default server configured"):
        await app.send_message(channel="default", payload=b"test")


async def test_send_message_server_not_found() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("default", server, is_default=True)

    async with server.connection():
        with pytest.raises(ValueError, match="Server 'nonexistent' not found"):
            await app.send_message(channel="default", payload=b"test", server_name="nonexistent")


async def test_send_message_operation_not_found() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("default", server, is_default=True)

    async with server.connection():
        with pytest.raises(ValueError, match="Operation 'nonexistent' not found"):
            await app.send_message(operation_id="nonexistent", payload=b"test")


async def test_send_message_neither_channel_nor_operation_id() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("default", server, is_default=True)

    async with server.connection():
        with pytest.raises(
            ValueError,
            match="Either 'channel' or 'operation_id' must be specified",
        ):
            await app.send_message(payload=b"test")  # type: ignore[call-overload]


async def test_send_message_both_channel_and_operation_id() -> None:
    app = Repid()
    server = InMemoryServer()
    app.servers.register_server("default", server, is_default=True)

    async with server.connection():
        with pytest.raises(
            ValueError,
            match="Specify either 'channel' or 'operation_id', not both",
        ):
            await app.send_message(
                channel="default",
                operation_id="some_op",
                payload=b"test",
            )  # type: ignore[call-overload]
