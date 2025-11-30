import httpx

from repid import Contact, ExternalDocs, License, Repid, Router, Tag
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


def test_repid_initialization_with_metadata() -> None:
    app = Repid(
        title="Test App",
        version="1.0.0",
        description="Test Description",
        terms_of_service="https://example.com/terms/",
        contact=Contact(
            name="Support",
            url="https://example.com/support",
            email="support@example.com",
        ),
        license=License(
            name="MIT",
            url="https://opensource.org/licenses/MIT",
        ),
        tags=[
            Tag(name="tag1", description="A test tag"),
        ],
        external_docs=ExternalDocs(
            url="https://example.com/docs",
            description="External documentation",
        ),
    )

    schema = app.generate_asyncapi_schema()
    assert schema["info"]["title"] == "Test App"
    assert schema["info"]["version"] == "1.0.0"
    assert schema["info"]["description"] == "Test Description"
    assert schema["info"]["termsOfService"] == "https://example.com/terms/"
    assert schema["info"]["contact"] == {
        "name": "Support",
        "url": "https://example.com/support",
        "email": "support@example.com",
    }
    assert schema["info"]["license"] == {
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    }
    assert schema["info"]["tags"] == [
        {"name": "tag1", "description": "A test tag"},
    ]
    assert schema["info"]["externalDocs"] == {
        "url": "https://example.com/docs",
        "description": "External documentation",
    }


async def test_asyncapi_server_creation(fake_repid: Repid) -> None:
    asyncapi_server = fake_repid.asyncapi_server()
    async with asyncapi_server:
        assert asyncapi_server._server is not None
        assert asyncapi_server._server.is_serving()
        async with httpx.AsyncClient() as client:
            response = await client.get("http://localhost:8081/")
            response.raise_for_status()
            assert "<!DOCTYPE html>" in response.text


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
