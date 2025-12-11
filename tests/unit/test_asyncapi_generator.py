from __future__ import annotations

from typing import Annotated, Any

import pytest
from pydantic import BaseModel

from repid import Contact, ExternalDocs, Header, License, Repid, Router, Tag
from repid.connections.amqp import AmqpServer
from repid.connections.in_memory import InMemoryServer
from repid.data.channel import Channel as ChannelData
from repid.data.message_schema import CorrelationId, MessageExample, MessageSchema


def test_message_example_raises_error_when_neither_headers_nor_payload_provided() -> None:
    with pytest.raises(ValueError, match="Either headers or payload must be provided"):
        MessageExample()


def test_message_example_with_headers_only() -> None:
    example = MessageExample(headers={"key": "value"})
    assert example.headers == {"key": "value"}
    assert example.payload is None


def test_message_example_with_payload_only() -> None:
    example = MessageExample(payload={"key": "value"})
    assert example.payload == {"key": "value"}
    assert example.headers is None


def test_asyncapi_generator_basic() -> None:
    app = Repid(
        title="Test API",
        version="2.0.0",
        description="Test description",
    )
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema["info"] == {
        "title": "Test API",
        "version": "2.0.0",
        "description": "Test description",
    }


def test_asyncapi_generator_server() -> None:
    app = Repid()
    app.servers.register_server(
        "default",
        InMemoryServer(
            title="In-Memory Server",
            description="A simple in-memory message broker that implements the ServerT protocol",
            summary="In-memory message broker for testing and development",
            tags=[Tag(name="in-memory")],
            external_docs=ExternalDocs(url="https://example.com/in-memory-docs"),
        ),
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {
            "default": {
                "host": "localhost",
                "protocol": "in-memory",
                "title": "In-Memory Server",
                "description": "A simple in-memory message broker that implements the ServerT protocol",
                "summary": "In-memory message broker for testing and development",
                "protocolVersion": "1.0.0",
                "tags": [{"$ref": "#/components/tags/in-memory"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_in_memory_docs",
                },
            },
        },
        "channels": {},
        "operations": {},
        "components": {
            "messages": {},
            "tags": {"in-memory": {"name": "in-memory"}},
            "externalDocs": {
                "external_docs_https_example_com_in_memory_docs": {
                    "url": "https://example.com/in-memory-docs",
                },
            },
        },
    }


def test_asyncapi_generator_amqp_server() -> None:
    app = Repid()
    app.servers.register_server(
        "default",
        AmqpServer(
            "localhost",
            title="AMQP Server",
            description="A simple AMQP message broker that implements the ServerT protocol",
            summary="AMQP message broker for testing and development",
            variables={
                "vhost": {"description": "The virtual host to connect to"},
            },
            security=[
                {
                    "type": "scramSha256",
                    "description": "Provide your username and password for SASL/SCRAM authentication",
                },
            ],
            tags=[Tag(name="amqp")],
            external_docs=ExternalDocs(url="https://example.com/amqp-docs"),
            bindings={
                "amqp1": {
                    "exchange": {"name": "test-exchange", "type": "topic"},
                },
            },
        ),
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {
            "default": {
                "host": "None",
                "protocol": "amqp",
                "title": "AMQP Server",
                "summary": "AMQP message broker for testing and development",
                "description": "A simple AMQP message broker that implements the ServerT protocol",
                "tags": [{"$ref": "#/components/tags/amqp"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_amqp_docs",
                },
                "bindings": {"$ref": "#/components/serverBindings/default_bindings"},
                "protocolVersion": "1.0.0",
                "pathname": "localhost",
                "variables": {"vhost": {"description": "The virtual host to connect to"}},
                "security": [
                    {"$ref": "#/components/securitySchemes/default_server-security-schema-0"},
                ],
            },
        },
        "channels": {},
        "operations": {},
        "components": {
            "messages": {},
            "securitySchemes": {
                "default_server-security-schema-0": {
                    "type": "scramSha256",
                    "description": "Provide your username and password for SASL/SCRAM authentication",
                },
            },
            "serverBindings": {
                "default_bindings": {
                    "amqp1": {"exchange": {"name": "test-exchange", "type": "topic"}},
                },
            },
            "tags": {"amqp": {"name": "amqp"}},
            "externalDocs": {
                "external_docs_https_example_com_amqp_docs": {
                    "url": "https://example.com/amqp-docs",
                },
            },
        },
    }


def test_asyncapi_generator_payload_references_model() -> None:
    app = Repid(
        title="Test API",
        version="2.0.0",
        description="Test description",
    )
    router = Router()

    class OtherModel(BaseModel):
        b: int

    @router.actor
    async def test_actor(a: OtherModel) -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Test API", "version": "2.0.0", "description": "Test description"},
        "servers": {},
        "channels": {
            "default": {"messages": {"test_actor": {"$ref": "#/components/messages/test_actor"}}},
        },
        "operations": {
            "receive_test_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Test Actor",
                "messages": [{"$ref": "#/channels/default/messages/test_actor"}],
            },
        },
        "components": {
            "messages": {
                "test_actor": {
                    "name": "test_actor",
                    "summary": "Test Actor",
                    "payload": {
                        "properties": {"a": {"$ref": "#/components/schemas/OtherModel"}},
                        "required": ["a"],
                        "title": "test_actor_payload",
                        "type": "object",
                    },
                    "contentType": "application/json",
                },
            },
            "schemas": {
                "OtherModel": {
                    "properties": {"b": {"title": "B", "type": "integer"}},
                    "required": ["b"],
                    "title": "OtherModel",
                    "type": "object",
                },
            },
        },
    }


def test_asyncapi_generator_empty_tag_has_no_effect() -> None:
    app = Repid(
        title="Test API",
        version="2.0.0",
        description="Test description",
        tags=[Tag(name="")],
    )
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema["info"] == {
        "title": "Test API",
        "version": "2.0.0",
        "description": "Test description",
    }


def test_asyncapi_generator_external_docs_with_weird_names() -> None:
    app = Repid(
        title="Test API",
        version="2.0.0",
        description="Test description",
        external_docs=ExternalDocs(url="~<><>__"),
    )
    router = Router(
        channel=ChannelData(
            address="default",
            external_docs=ExternalDocs(url="<>?>~"),
        ),
    )

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {
            "title": "Test API",
            "version": "2.0.0",
            "description": "Test description",
            "externalDocs": {"$ref": "#/components/externalDocs/external_docs"},
        },
        "servers": {},
        "channels": {
            "default": {
                "externalDocs": {"$ref": "#/components/externalDocs/external_docs_2"},
                "messages": {"test_actor": {"$ref": "#/components/messages/test_actor"}},
            },
        },
        "operations": {
            "receive_test_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Test Actor",
                "messages": [{"$ref": "#/channels/default/messages/test_actor"}],
            },
        },
        "components": {
            "messages": {
                "test_actor": {"name": "test_actor", "summary": "Test Actor", "contentType": ""},
            },
            "externalDocs": {
                "external_docs": {"url": "~<><>__"},
                "external_docs_2": {"url": "<>?>~"},
            },
        },
    }


def test_asyncapi_generator_tags_with_weird_names() -> None:
    app = Repid(
        title="Test API",
        version="2.0.0",
        description="Test description",
        tags=[
            Tag(name="~<><>__"),
            Tag(name="<>?>~"),
        ],
    )
    router = Router()

    @router.actor(tags=[Tag(name="~<><>__")])
    async def test_actor() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {
            "title": "Test API",
            "version": "2.0.0",
            "description": "Test description",
            "tags": [
                {"$ref": "#/components/tags/~<><>__"},
                {"$ref": "#/components/tags/<>?>~"},
            ],
        },
        "servers": {},
        "channels": {
            "default": {"messages": {"test_actor": {"$ref": "#/components/messages/test_actor"}}},
        },
        "operations": {
            "receive_test_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Test Actor",
                "tags": [{"$ref": "#/components/tags/~<><>__"}],
                "messages": [{"$ref": "#/channels/default/messages/test_actor"}],
            },
        },
        "components": {
            "messages": {
                "test_actor": {
                    "name": "test_actor",
                    "summary": "Test Actor",
                    "tags": [{"$ref": "#/components/tags/~<><>__"}],
                    "contentType": "",
                },
            },
            "tags": {
                "~<><>__": {"name": "~<><>__"},
                "<>?>~": {"name": "<>?>~"},
            },
        },
    }


def test_asyncapi_generator_populates_all_info_fields_in_schema() -> None:
    app = Repid(
        title="Full API",
        version="3.0.0",
        description="Full description",
        terms_of_service="https://example.com/terms",
        contact=Contact(
            name="Support",
            url="https://example.com/support",
            email="support@example.com",
        ),
        license=License(name="MIT", url="https://opensource.org/licenses/MIT"),
        tags=[
            Tag(
                name="tag1",
                description="Tag 1 description",
                external_docs=ExternalDocs(
                    url="https://example.com/tag1-docs",
                ),
            ),
            Tag(name="tag2"),
        ],
        external_docs=ExternalDocs(
            url="https://example.com/docs",
            description="External docs description",
        ),
    )
    router = Router()

    @router.actor
    async def test_actor(arg: str) -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema["info"] == {
        "title": "Full API",
        "version": "3.0.0",
        "description": "Full description",
        "termsOfService": "https://example.com/terms",
        "contact": {
            "name": "Support",
            "url": "https://example.com/support",
            "email": "support@example.com",
        },
        "license": {"name": "MIT", "url": "https://opensource.org/licenses/MIT"},
        "tags": [
            {"$ref": "#/components/tags/tag1"},
            {"$ref": "#/components/tags/tag2"},
        ],
        "externalDocs": {
            "$ref": "#/components/externalDocs/external_docs_https_example_com_docs",
        },
    }


@pytest.mark.parametrize(
    ("app_kwargs", "expected_key", "expected_value"),
    [
        pytest.param(
            {"contact": Contact(url="https://example.com")},
            "contact",
            {"url": "https://example.com"},
            id="contact_url_only",
        ),
        pytest.param(
            {"contact": Contact(email="test@example.com")},
            "contact",
            {"email": "test@example.com"},
            id="contact_email_only",
        ),
        pytest.param(
            {"license": License(name="Proprietary")},
            "license",
            {"name": "Proprietary"},
            id="license_name_only",
        ),
    ],
)
def test_asyncapi_generator_accepts_partial_info(
    app_kwargs: dict[str, Any],
    expected_key: str,
    expected_value: dict[str, Any],
) -> None:
    app = Repid(**app_kwargs)
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema["info"][expected_key] == expected_value  # type: ignore[literal-required]


def test_asyncapi_generator_message_registry_operation() -> None:
    app = Repid()

    app.messages.register_operation(
        operation_id="test_op",
        channel="test_channel",
        title="Test Operation",
        summary="Test summary",
        description="Test description",
        messages=[
            MessageSchema(
                name="test_message",
                title="Test Message",
                summary="Test message summary",
                description="Test message description",
                payload={"type": "object"},
                headers={"type": "object"},
                content_type="application/json",
                correlation_id=CorrelationId(
                    location="$message.header#/x-correlation-id",
                    description="Correlation ID",
                ),
                tags=(Tag(name="msg-tag", description="Message tag"),),
                external_docs=ExternalDocs(
                    url="https://example.com/msg-docs",
                    description="Message docs",
                ),
                deprecated=True,
                examples=(
                    MessageExample(
                        name="example1",
                        summary="Example summary",
                        headers={"X-Custom": "value"},
                        payload={"key": "value"},
                    ),
                ),
            ),
        ],
        tags=[Tag(name="op-tag")],
        external_docs=ExternalDocs(url="https://example.com/op-docs"),
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "test_channel": {
                "messages": {"test_message": {"$ref": "#/components/messages/test_message"}},
            },
        },
        "operations": {
            "test_op": {
                "action": "send",
                "channel": {"$ref": "#/channels/test_channel"},
                "title": "Test Operation",
                "summary": "Test summary",
                "description": "Test description",
                "tags": [{"$ref": "#/components/tags/op-tag"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_op_docs",
                },
                "messages": [{"$ref": "#/channels/test_channel/messages/test_message"}],
            },
        },
        "components": {
            "messages": {
                "test_message": {
                    "name": "test_message",
                    "title": "Test Message",
                    "summary": "Test message summary",
                    "description": "Test message description",
                    "tags": [{"$ref": "#/components/tags/msg-tag"}],
                    "externalDocs": {
                        "$ref": "#/components/externalDocs/external_docs_https_example_com_msg_docs",
                    },
                    "contentType": "application/json",
                    "headers": {"type": "object"},
                    "payload": {"type": "object"},
                    "correlationId": {
                        "location": "$message.header#/x-correlation-id",
                        "description": "Correlation ID",
                    },
                    "deprecated": True,
                    "examples": [
                        {
                            "name": "example1",
                            "summary": "Example summary",
                            "headers": {"X-Custom": "value"},
                            "payload": {"key": "value"},
                        },
                    ],
                },
            },
            "tags": {
                "msg-tag": {"name": "msg-tag", "description": "Message tag"},
                "op-tag": {"name": "op-tag"},
            },
            "externalDocs": {
                "external_docs_https_example_com_msg_docs": {
                    "url": "https://example.com/msg-docs",
                    "description": "Message docs",
                },
                "external_docs_https_example_com_op_docs": {"url": "https://example.com/op-docs"},
            },
        },
    }


def test_asyncapi_generator_actor_bindings() -> None:
    app = Repid()
    router = Router()

    @router.actor(
        bindings={
            "amqp": {
                "exchange": {"name": "test-exchange", "type": "topic"},
            },
        },
    )
    async def actor_with_bindings() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema["components"]["operationBindings"] == {
        "actor_with_bindings_bindings": {
            "amqp": {"exchange": {"name": "test-exchange", "type": "topic"}},
        },
    }
    assert schema["operations"] == {
        "receive_actor_with_bindings": {
            "action": "receive",
            "channel": {"$ref": "#/channels/default"},
            "summary": "Actor With Bindings",
            "messages": [{"$ref": "#/channels/default/messages/actor_with_bindings"}],
            "bindings": {
                "$ref": "#/components/operationBindings/actor_with_bindings_bindings",
            },
        },
    }


def test_asyncapi_generator_actor_metadata() -> None:
    app = Repid()
    router = Router()

    @router.actor(
        title="My Actor Title",
        summary="My actor summary",
        description="My actor description",
        tags=[Tag(name="actor-tag", description="Actor tag desc")],
        external_docs=ExternalDocs(
            url="https://example.com/actor-docs",
            description="Actor docs",
        ),
        security=[{"apiKey": []}],
        deprecated=True,
    )
    async def detailed_actor(x: int) -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "default": {
                "messages": {"detailed_actor": {"$ref": "#/components/messages/detailed_actor"}},
            },
        },
        "operations": {
            "receive_detailed_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "title": "My Actor Title",
                "summary": "My actor summary",
                "description": "My actor description",
                "messages": [{"$ref": "#/channels/default/messages/detailed_actor"}],
                "tags": [{"$ref": "#/components/tags/actor-tag"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_actor_docs",
                },
                "security": [
                    {
                        "$ref": "#/components/securitySchemes/receive_detailed_actor_operation-security-schema-0",
                    },
                ],
            },
        },
        "components": {
            "messages": {
                "detailed_actor": {
                    "name": "detailed_actor",
                    "title": "My Actor Title",
                    "summary": "My actor summary",
                    "description": "My actor description",
                    "payload": {
                        "properties": {"x": {"title": "X", "type": "integer"}},
                        "required": ["x"],
                        "title": "detailed_actor_payload",
                        "type": "object",
                    },
                    "contentType": "application/json",
                    "deprecated": True,
                    "tags": [{"$ref": "#/components/tags/actor-tag"}],
                    "externalDocs": {
                        "$ref": "#/components/externalDocs/external_docs_https_example_com_actor_docs",
                    },
                },
            },
            "tags": {"actor-tag": {"name": "actor-tag", "description": "Actor tag desc"}},
            "externalDocs": {
                "external_docs_https_example_com_actor_docs": {
                    "url": "https://example.com/actor-docs",
                    "description": "Actor docs",
                },
            },
            "securitySchemes": {
                "receive_detailed_actor_operation-security-schema-0": {
                    "apiKey": [],
                },
            },
        },
    }


def test_asyncapi_generator_channel_bindings() -> None:
    app = Repid()
    router = Router(
        channel=ChannelData(
            address="custom-channel",
            title="Custom Channel",
            summary="Channel summary",
            description="Channel description",
            bindings={
                "amqp": {
                    "queue": {"name": "test-queue"},
                },
            },
            external_docs=ExternalDocs(url="https://example.com/channel-docs"),
        ),
    )

    @router.actor
    async def actor_on_channel() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "custom-channel": {
                "title": "Custom Channel",
                "summary": "Channel summary",
                "description": "Channel description",
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_channel_docs",
                },
                "bindings": {"$ref": "#/components/channelBindings/custom-channel_bindings"},
                "messages": {
                    "actor_on_channel": {"$ref": "#/components/messages/actor_on_channel"},
                },
            },
        },
        "operations": {
            "receive_actor_on_channel": {
                "action": "receive",
                "channel": {"$ref": "#/channels/custom-channel"},
                "summary": "Actor On Channel",
                "messages": [{"$ref": "#/channels/custom-channel/messages/actor_on_channel"}],
            },
        },
        "components": {
            "messages": {
                "actor_on_channel": {
                    "name": "actor_on_channel",
                    "summary": "Actor On Channel",
                    "contentType": "",
                },
            },
            "channelBindings": {
                "custom-channel_bindings": {"amqp": {"queue": {"name": "test-queue"}}},
            },
            "externalDocs": {
                "external_docs_https_example_com_channel_docs": {
                    "url": "https://example.com/channel-docs",
                },
            },
        },
    }


def test_asyncapi_generator_channel_merge() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="minimal_op",
        channel="test-channel",
        messages=[MessageSchema(name="operation_message", payload={"type": "object"})],
    )

    @router.actor(channel="test-channel")
    async def test_actor() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "test-channel": {
            "messages": {
                "operation_message": {"$ref": "#/components/messages/operation_message"},
                "test_actor": {"$ref": "#/components/messages/test_actor"},
            },
        },
    }


def test_asyncapi_generator_channel_merge_actor_and_actor() -> None:
    app = Repid()
    router = Router()

    @router.actor(channel="shared-channel")
    async def actor1() -> None:
        pass

    @router.actor(
        channel=ChannelData(
            address="shared-channel",
            title="My Channel",
            summary="Shared summary",
            description="Shared description",
            bindings={
                "amqp": {
                    "queue": {"name": "test-queue"},
                },
            },
            external_docs=ExternalDocs(url="https://example.com/channel-docs"),
        ),
    )
    async def actor2() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "shared-channel": {
                "title": "My Channel",
                "summary": "Shared summary",
                "description": "Shared description",
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_channel_docs",
                },
                "bindings": {"$ref": "#/components/channelBindings/shared-channel_bindings"},
                "messages": {
                    "actor1": {"$ref": "#/components/messages/actor1"},
                    "actor2": {"$ref": "#/components/messages/actor2"},
                },
            },
        },
        "operations": {
            "receive_actor1": {
                "action": "receive",
                "channel": {"$ref": "#/channels/shared-channel"},
                "summary": "Actor1",
                "messages": [{"$ref": "#/channels/shared-channel/messages/actor1"}],
            },
            "receive_actor2": {
                "action": "receive",
                "channel": {"$ref": "#/channels/shared-channel"},
                "summary": "Actor2",
                "messages": [{"$ref": "#/channels/shared-channel/messages/actor2"}],
            },
        },
        "components": {
            "messages": {
                "actor1": {"name": "actor1", "summary": "Actor1", "contentType": ""},
                "actor2": {"name": "actor2", "summary": "Actor2", "contentType": ""},
            },
            "channelBindings": {
                "shared-channel_bindings": {"amqp": {"queue": {"name": "test-queue"}}},
            },
            "externalDocs": {
                "external_docs_https_example_com_channel_docs": {
                    "url": "https://example.com/channel-docs",
                },
            },
        },
    }


def test_asyncapi_generator_channel_merge_actor_and_operation() -> None:
    app = Repid()
    router = Router()

    @router.actor(channel="shared-channel")
    async def actor1() -> None:
        pass

    app.include_router(router)

    app.messages.register_operation(
        operation_id="op_on_shared",
        channel=ChannelData(
            address="shared-channel",
            title="My Channel",
        ),
        title="Operation title",
        summary="Operation summary",
        description="Operation description",
        messages=[MessageSchema(name="op_message", payload={"type": "object"})],
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "shared-channel": {
                "title": "My Channel",
                "messages": {
                    "actor1": {"$ref": "#/components/messages/actor1"},
                    "op_message": {"$ref": "#/components/messages/op_message"},
                },
            },
        },
        "operations": {
            "receive_actor1": {
                "action": "receive",
                "channel": {"$ref": "#/channels/shared-channel"},
                "summary": "Actor1",
                "messages": [{"$ref": "#/channels/shared-channel/messages/actor1"}],
            },
            "op_on_shared": {
                "action": "send",
                "channel": {"$ref": "#/channels/shared-channel"},
                "title": "Operation title",
                "summary": "Operation summary",
                "description": "Operation description",
                "messages": [
                    {"$ref": "#/channels/shared-channel/messages/op_message"},
                ],
            },
        },
        "components": {
            "messages": {
                "actor1": {"name": "actor1", "summary": "Actor1", "contentType": ""},
                "op_message": {
                    "name": "op_message",
                    "payload": {"type": "object"},
                },
            },
        },
    }


def test_asyncapi_generator_tag_deduplication() -> None:
    app = Repid(tags=[Tag(name="common-tag")])
    router = Router()

    @router.actor(tags=[Tag(name="common-tag")])
    async def actor1() -> None:
        pass

    @router.actor(tags=[Tag(name="common-tag")])
    async def actor2() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {
            "title": "Repid AsyncAPI",
            "version": "0.0.1",
            "tags": [{"$ref": "#/components/tags/common-tag"}],
        },
        "servers": {},
        "channels": {
            "default": {
                "messages": {
                    "actor1": {"$ref": "#/components/messages/actor1"},
                    "actor2": {"$ref": "#/components/messages/actor2"},
                },
            },
        },
        "operations": {
            "receive_actor1": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Actor1",
                "messages": [{"$ref": "#/channels/default/messages/actor1"}],
                "tags": [{"$ref": "#/components/tags/common-tag"}],
            },
            "receive_actor2": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Actor2",
                "messages": [{"$ref": "#/channels/default/messages/actor2"}],
                "tags": [{"$ref": "#/components/tags/common-tag"}],
            },
        },
        "components": {
            "messages": {
                "actor1": {
                    "name": "actor1",
                    "summary": "Actor1",
                    "contentType": "",
                    "tags": [{"$ref": "#/components/tags/common-tag"}],
                },
                "actor2": {
                    "name": "actor2",
                    "summary": "Actor2",
                    "contentType": "",
                    "tags": [{"$ref": "#/components/tags/common-tag"}],
                },
            },
            "tags": {"common-tag": {"name": "common-tag"}},
        },
    }


def test_asyncapi_generator_external_docs_deduplication() -> None:
    app = Repid(external_docs=ExternalDocs(url="https://example.com/docs"))
    router = Router()

    @router.actor(external_docs=ExternalDocs(url="https://example.com/docs"))
    async def actor1() -> None:
        pass

    @router.actor(external_docs=ExternalDocs(url="https://example.com/docs"))
    async def actor2() -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {
            "title": "Repid AsyncAPI",
            "version": "0.0.1",
            "externalDocs": {
                "$ref": "#/components/externalDocs/external_docs_https_example_com_docs",
            },
        },
        "servers": {},
        "channels": {
            "default": {
                "messages": {
                    "actor1": {"$ref": "#/components/messages/actor1"},
                    "actor2": {"$ref": "#/components/messages/actor2"},
                },
            },
        },
        "operations": {
            "receive_actor1": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Actor1",
                "messages": [{"$ref": "#/channels/default/messages/actor1"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_docs",
                },
            },
            "receive_actor2": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Actor2",
                "messages": [{"$ref": "#/channels/default/messages/actor2"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_docs",
                },
            },
        },
        "components": {
            "messages": {
                "actor1": {
                    "name": "actor1",
                    "summary": "Actor1",
                    "contentType": "",
                    "externalDocs": {
                        "$ref": "#/components/externalDocs/external_docs_https_example_com_docs",
                    },
                },
                "actor2": {
                    "name": "actor2",
                    "summary": "Actor2",
                    "contentType": "",
                    "externalDocs": {
                        "$ref": "#/components/externalDocs/external_docs_https_example_com_docs",
                    },
                },
            },
            "externalDocs": {
                "external_docs_https_example_com_docs": {"url": "https://example.com/docs"},
            },
        },
    }


def test_asyncapi_generator_external_docs_empty_url() -> None:
    app = Repid(external_docs=ExternalDocs(url=""))

    schema = app.generate_asyncapi_schema()

    assert schema["info"]["externalDocs"] == {"url": ""}


@pytest.mark.parametrize(
    "correlation_id",
    [
        pytest.param(
            CorrelationId(location="$message.header#/correlationId"),
            id="correlation_id",
        ),
        pytest.param(
            CorrelationId(
                location="$message.header#/correlationId",
                description="Request correlation ID",
            ),
            id="correlation_id_with_description",
        ),
    ],
)
def test_asyncapi_generator_actor_correlation_id(correlation_id: CorrelationId) -> None:
    app = Repid()
    router = Router()

    @router.actor(correlation_id=correlation_id)
    async def actor_with_correlation(x: int) -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    expected_correlation_id: dict[str, Any] = {"location": correlation_id.location}
    if correlation_id.description:
        expected_correlation_id["description"] = correlation_id.description

    assert schema["components"]["messages"]["actor_with_correlation"] == {
        "name": "actor_with_correlation",
        "summary": "Actor With Correlation",
        "payload": {
            "properties": {"x": {"title": "X", "type": "integer"}},
            "required": ["x"],
            "title": "actor_with_correlation_payload",
            "type": "object",
        },
        "contentType": "application/json",
        "correlationId": expected_correlation_id,
    }


def test_asyncapi_generator_operation_bindings() -> None:
    app = Repid()

    app.messages.register_operation(
        operation_id="op_with_bindings",
        channel="test_channel",
        bindings={
            "amqp": {"routingKey": "test-key"},
        },
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {"test_channel": {}},
        "operations": {
            "op_with_bindings": {
                "action": "send",
                "channel": {"$ref": "#/channels/test_channel"},
                "bindings": {"$ref": "#/components/operationBindings/op_with_bindings_bindings"},
            },
        },
        "components": {
            "messages": {},
            "operationBindings": {
                "op_with_bindings_bindings": {"amqp": {"routingKey": "test-key"}},
            },
        },
    }


def test_asyncapi_generator_operation_security() -> None:
    app = Repid()

    app.messages.register_operation(
        operation_id="secure_op",
        channel="secure_channel",
        security=[{"apiKey": []}],
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {"secure_channel": {}},
        "operations": {
            "secure_op": {
                "action": "send",
                "channel": {"$ref": "#/channels/secure_channel"},
                "security": [
                    {
                        "$ref": "#/components/securitySchemes/secure_op_operation-security-schema-0",
                    },
                ],
            },
        },
        "components": {
            "messages": {},
            "securitySchemes": {
                "secure_op_operation-security-schema-0": {
                    "apiKey": [],
                },
            },
        },
    }


def test_asyncapi_generator_actor_header() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def actor_with_header(
        arg1: str,
        x_custom: Annotated[str, Header(name="X-Custom")],
        x_optional: Annotated[int | None, Header(name="X-Optional")] = None,
    ) -> None:
        pass

    app.include_router(router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "default": {
                "messages": {
                    "actor_with_header": {"$ref": "#/components/messages/actor_with_header"},
                },
            },
        },
        "operations": {
            "receive_actor_with_header": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Actor With Header",
                "messages": [{"$ref": "#/channels/default/messages/actor_with_header"}],
            },
        },
        "components": {
            "messages": {
                "actor_with_header": {
                    "name": "actor_with_header",
                    "summary": "Actor With Header",
                    "payload": {
                        "properties": {"arg1": {"title": "Arg1", "type": "string"}},
                        "required": ["arg1"],
                        "title": "actor_with_header_payload",
                        "type": "object",
                    },
                    "contentType": "application/json",
                    "headers": {
                        "properties": {
                            "X-Custom": {"title": "X-Custom", "type": "string"},
                            "X-Optional": {
                                "title": "X-Optional",
                                "default": None,
                                "anyOf": [{"type": "integer"}, {"type": "null"}],
                            },
                        },
                        "required": ["X-Custom"],
                        "title": "actor_with_header_headers",
                        "type": "object",
                    },
                },
            },
        },
    }


def test_asyncapi_generator_operation() -> None:
    app = Repid()

    app.messages.register_operation(
        operation_id="standalone_op",
        channel="standalone-channel",
        title="Standalone Channel Title",
        description="Standalone channel description",
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "standalone-channel": {},
        },
        "operations": {
            "standalone_op": {
                "action": "send",
                "channel": {"$ref": "#/channels/standalone-channel"},
                "title": "Standalone Channel Title",
                "description": "Standalone channel description",
            },
        },
        "components": {"messages": {}},
    }


def test_asyncapi_generator_operation_complex() -> None:
    app = Repid()

    app.messages.register_operation(
        operation_id="manual_op",
        channel=ChannelData(
            address="manual-channel",
            title="Manual Channel",
            summary="A manually defined channel summary",
            description="A manually defined channel",
            bindings={"amqp": {"is": "queue"}},
            external_docs=ExternalDocs(
                url="https://example.com/manual-channel-docs",
                description="Manual channel docs",
            ),
        ),
        title="Manual Operation",
        summary="A manual operation summary",
        description="A manual operation description",
        messages=[
            MessageSchema(
                name="ManualMessage",
                title="Manual Message",
                payload={"type": "string"},
                correlation_id=CorrelationId(location="$message.header#/cid"),
                content_type="application/json",
                examples=(
                    MessageExample(
                        name="ex1",
                        payload="test",
                        headers={"cid": "123"},
                    ),
                ),
                bindings={
                    "amqp": {
                        "contentEncoding": "gzip",
                        "messageType": "user.signup",
                        "bindingVersion": "0.3.0",
                    },
                },
            ),
        ],
        security=[{"apiKey": []}],
        tags=[Tag(name="manual-op-tag", description="Manual operation tag")],
        external_docs=ExternalDocs(
            url="https://example.com/manual-op-docs",
            description="Manual operation docs",
        ),
        bindings={
            "amqp": {"priority": 5},
        },
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "manual-channel": {
                "title": "Manual Channel",
                "summary": "A manually defined channel summary",
                "description": "A manually defined channel",
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_manual_channel_docs",
                },
                "bindings": {"$ref": "#/components/channelBindings/manual-channel_bindings"},
                "messages": {"ManualMessage": {"$ref": "#/components/messages/ManualMessage"}},
            },
        },
        "operations": {
            "manual_op": {
                "action": "send",
                "channel": {"$ref": "#/channels/manual-channel"},
                "title": "Manual Operation",
                "summary": "A manual operation summary",
                "description": "A manual operation description",
                "tags": [{"$ref": "#/components/tags/manual-op-tag"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_manual_op_docs",
                },
                "messages": [{"$ref": "#/channels/manual-channel/messages/ManualMessage"}],
                "bindings": {"$ref": "#/components/operationBindings/manual_op_bindings"},
                "security": [
                    {"$ref": "#/components/securitySchemes/manual_op_operation-security-schema-0"},
                ],
            },
        },
        "components": {
            "messages": {
                "ManualMessage": {
                    "name": "ManualMessage",
                    "title": "Manual Message",
                    "contentType": "application/json",
                    "payload": {"type": "string"},
                    "correlationId": {"location": "$message.header#/cid"},
                    "bindings": {
                        "$ref": "#/components/messageBindings/ManualMessage-message_bindings",
                    },
                    "examples": [{"name": "ex1", "headers": {"cid": "123"}, "payload": "test"}],
                },
            },
            "securitySchemes": {"manual_op_operation-security-schema-0": {"apiKey": []}},
            "channelBindings": {"manual-channel_bindings": {"amqp": {"is": "queue"}}},
            "operationBindings": {"manual_op_bindings": {"amqp": {"priority": 5}}},
            "messageBindings": {
                "ManualMessage-message_bindings": {
                    "amqp": {
                        "contentEncoding": "gzip",
                        "messageType": "user.signup",
                        "bindingVersion": "0.3.0",
                    },
                },
            },
            "tags": {
                "manual-op-tag": {"name": "manual-op-tag", "description": "Manual operation tag"},
            },
            "externalDocs": {
                "external_docs_https_example_com_manual_channel_docs": {
                    "url": "https://example.com/manual-channel-docs",
                    "description": "Manual channel docs",
                },
                "external_docs_https_example_com_manual_op_docs": {
                    "url": "https://example.com/manual-op-docs",
                    "description": "Manual operation docs",
                },
            },
        },
    }


def test_asyncapi_generator_message_name_collision() -> None:
    app = Repid()
    router = Router(channel="test-channel")

    @router.actor
    async def my_actor(payload1: str) -> None:
        pass

    app.include_router(router)

    app.messages.register_operation(
        operation_id="same_name_op",
        channel="test-channel",
        messages=[
            MessageSchema(
                name="my_actor",
                payload={"type": "object"},
            ),
        ],
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "test-channel": {"messages": {"my_actor": {"$ref": "#/components/messages/my_actor"}}},
        },
        "operations": {
            "receive_my_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/test-channel"},
                "summary": "My Actor",
                "messages": [{"$ref": "#/channels/test-channel/messages/my_actor"}],
            },
            "same_name_op": {
                "action": "send",
                "channel": {"$ref": "#/channels/test-channel"},
                "messages": [{"$ref": "#/channels/test-channel/messages/my_actor_1"}],
            },
        },
        "components": {
            "messages": {
                "my_actor": {
                    "name": "my_actor",
                    "summary": "My Actor",
                    "payload": {
                        "properties": {"payload1": {"title": "Payload1", "type": "string"}},
                        "required": ["payload1"],
                        "title": "my_actor_payload",
                        "type": "object",
                    },
                    "contentType": "application/json",
                },
                "my_actor_1": {"name": "my_actor", "payload": {"type": "object"}},
            },
        },
    }


def test_asyncapi_generator_multiple_actors_on_same_channel() -> None:
    app = Repid()
    router = Router(channel="shared-channel")

    @router.actor
    async def actor1() -> None:
        pass

    @router.actor
    async def actor2() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "shared-channel": {
                "messages": {
                    "actor1": {"$ref": "#/components/messages/actor1"},
                    "actor2": {"$ref": "#/components/messages/actor2"},
                },
            },
        },
        "operations": {
            "receive_actor1": {
                "action": "receive",
                "channel": {"$ref": "#/channels/shared-channel"},
                "summary": "Actor1",
                "messages": [{"$ref": "#/channels/shared-channel/messages/actor1"}],
            },
            "receive_actor2": {
                "action": "receive",
                "channel": {"$ref": "#/channels/shared-channel"},
                "summary": "Actor2",
                "messages": [{"$ref": "#/channels/shared-channel/messages/actor2"}],
            },
        },
        "components": {
            "messages": {
                "actor1": {"name": "actor1", "summary": "Actor1", "contentType": ""},
                "actor2": {"name": "actor2", "summary": "Actor2", "contentType": ""},
            },
        },
    }


def test_asyncapi_generator_nested_router() -> None:
    app = Repid()
    parent_router = Router()
    child_router = Router()

    @child_router.actor
    async def child_actor() -> None:
        pass

    parent_router.include_router(child_router)
    app.include_router(parent_router)

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "default": {"messages": {"child_actor": {"$ref": "#/components/messages/child_actor"}}},
        },
        "operations": {
            "receive_child_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Child Actor",
                "messages": [{"$ref": "#/channels/default/messages/child_actor"}],
            },
        },
        "components": {
            "messages": {
                "child_actor": {"name": "child_actor", "summary": "Child Actor", "contentType": ""},
            },
        },
    }
