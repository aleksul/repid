from __future__ import annotations

from typing import Annotated

import pytest

from repid import Contact, ExternalDocs, Header, License, Repid, Router, Tag
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
            Tag(name="tag1", description="Tag 1 description"),
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


def test_asyncapi_generator_accepts_partial_contact_info_url_only() -> None:
    app = Repid(contact=Contact(url="https://example.com"))
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["info"]["contact"] == {"url": "https://example.com"}


def test_asyncapi_generator_accepts_partial_contact_info_email_only() -> None:
    app = Repid(contact=Contact(email="test@example.com"))
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["info"]["contact"] == {"email": "test@example.com"}


def test_asyncapi_generator_accepts_license_without_url() -> None:
    app = Repid(license=License(name="Proprietary"))
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["info"]["license"] == {"name": "Proprietary"}


def test_asyncapi_generator_includes_message_registry_operations_in_schema() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def my_actor(arg: str) -> None:
        pass

    app.include_router(router)

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
            "default": {"messages": {"my_actor": {"$ref": "#/components/messages/my_actor"}}},
            "test_channel": {
                "title": "Test Operation",
                "summary": "Test summary",
                "description": "Test description",
                "tags": [{"$ref": "#/components/tags/op-tag"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_op_docs",
                },
                "messages": {"test_message": {"$ref": "#/components/messages/test_message"}},
            },
        },
        "operations": {
            "receive_my_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "My Actor",
                "messages": [{"$ref": "#/channels/default/messages/my_actor"}],
            },
            "test_op": {
                "action": "send",
                "channel": {"$ref": "#/channels/test_channel"},
                "title": "Test Operation",
                "summary": "Test summary",
                "description": "Test description",
                "messages": [{"$ref": "#/channels/test_channel/messages/test_message"}],
                "tags": [{"$ref": "#/components/tags/op-tag"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_op_docs",
                },
            },
        },
        "components": {
            "messages": {
                "my_actor": {
                    "name": "my_actor",
                    "summary": "My Actor",
                    "payload": {
                        "properties": {"arg": {"title": "Arg", "type": "string"}},
                        "required": ["arg"],
                        "title": "my_actor_payload",
                        "type": "object",
                    },
                    "contentType": "application/json",
                },
                "test_message": {
                    "name": "test_message",
                    "title": "Test Message",
                    "summary": "Test message summary",
                    "description": "Test message description",
                    "contentType": "application/json",
                    "headers": {"type": "object"},
                    "payload": {"type": "object"},
                    "correlationId": {
                        "location": "$message.header#/x-correlation-id",
                        "description": "Correlation ID",
                    },
                    "tags": [{"$ref": "#/components/tags/msg-tag"}],
                    "externalDocs": {
                        "$ref": "#/components/externalDocs/external_docs_https_example_com_msg_docs",
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
                "external_docs_https_example_com_op_docs": {
                    "url": "https://example.com/op-docs",
                },
            },
        },
    }


def test_asyncapi_generator_collects_actor_bindings_in_components() -> None:
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


def test_asyncapi_generator_includes_registered_servers_in_schema() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)

    server = InMemoryServer()
    app.servers.register_server("default", server, is_default=True)

    schema = app.generate_asyncapi_schema()

    assert schema["servers"] == {
        "default": {
            "host": "localhost",
            "protocol": "in-memory",
            "title": "In-Memory Server",
            "description": "A simple in-memory message broker that implements the ServerT protocol",
            "summary": "In-memory message broker for testing and development",
            "protocolVersion": "1.0.0",
        },
    }


def test_asyncapi_generator_includes_actor_metadata_in_operation() -> None:
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
                "security": ({"apiKey": []},),
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
        },
    }


def test_asyncapi_generator_includes_channel_with_bindings() -> None:
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
                "bindings": {
                    "amqp": {
                        "queue": {"name": "test-queue"},
                    },
                },
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_example_com_channel_docs",
                },
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
        },
    }


def test_asyncapi_generator_merges_channel_fields_from_different_sources() -> None:
    app = Repid()
    router = Router()

    @router.actor(channel="shared-channel")
    async def actor1() -> None:
        pass

    app.include_router(router)

    app.messages.register_operation(
        operation_id="op_on_shared",
        channel="shared-channel",
        title="Shared Channel",
        summary="Shared summary",
        description="Shared description",
        messages=[MessageSchema(name="op_message", payload={"type": "object"})],
    )

    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "shared-channel": {
                "messages": {
                    "actor1": {"$ref": "#/components/messages/actor1"},
                    "op_message": {"$ref": "#/components/messages/op_message"},
                },
                "title": "Shared Channel",
                "summary": "Shared summary",
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
                "title": "Shared Channel",
                "summary": "Shared summary",
                "description": "Shared description",
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


def test_asyncapi_generator_deduplicates_tags_in_components() -> None:
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


def test_asyncapi_generator_deduplicates_external_docs_in_components() -> None:
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


def test_external_docs_with_empty_url_still_included_in_schema() -> None:
    app = Repid(external_docs=ExternalDocs(url=""))
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["info"]["externalDocs"] == {"url": ""}


def test_actor_with_correlation_id_generates_correlation_id_in_message() -> None:
    app = Repid()
    router = Router()

    @router.actor(correlation_id=CorrelationId(location="$message.header#/correlationId"))
    async def actor_with_correlation(x: int) -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["components"]["messages"]["actor_with_correlation"] == {
        "name": "actor_with_correlation",
        "summary": "Actor With Correlation",
        "payload": {
            "properties": {
                "x": {"title": "X", "type": "integer"},
            },
            "required": ["x"],
            "title": "actor_with_correlation_payload",
            "type": "object",
        },
        "contentType": "application/json",
        "correlationId": {"location": "$message.header#/correlationId"},
    }


def test_operation_with_bindings_includes_bindings_in_schema() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)

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
        "channels": {
            "default": {"messages": {"test_actor": {"$ref": "#/components/messages/test_actor"}}},
            "test_channel": {},
        },
        "operations": {
            "receive_test_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "summary": "Test Actor",
                "messages": [{"$ref": "#/channels/default/messages/test_actor"}],
            },
            "op_with_bindings": {
                "action": "send",
                "channel": {"$ref": "#/channels/test_channel"},
                "bindings": {"amqp": {"routingKey": "test-key"}},
            },
        },
        "components": {
            "messages": {
                "test_actor": {"name": "test_actor", "summary": "Test Actor", "contentType": ""},
            },
        },
    }


def test_operation_with_security_includes_security_in_schema() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="secure_op",
        channel="secure_channel",
        security=[{"apiKey": []}],
    )

    app.include_router(router)
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
                "security": ({"apiKey": []},),
            },
        },
        "components": {"messages": {}},
    }


def test_actor_with_header_dependency_generates_headers_schema() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def actor_with_header(
        arg1: str,
        x_custom: Annotated[str, Header(name="X-Custom")],
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
                        "properties": {"X-Custom": {"title": "X-Custom", "type": "string"}},
                        "required": ["X-Custom"],
                        "title": "actor_with_header_headers",
                        "type": "object",
                    },
                },
            },
        },
    }


def test_actor_with_correlation_id_includes_description_in_schema() -> None:
    app = Repid()
    router = Router()

    @router.actor(
        correlation_id=CorrelationId(
            location="$message.header#/correlationId",
            description="Request correlation ID",
        ),
    )
    async def actor_with_corr_desc(x: int) -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["components"]["messages"] == {
        "actor_with_corr_desc": {
            "name": "actor_with_corr_desc",
            "summary": "Actor With Corr Desc",
            "payload": {
                "properties": {"x": {"title": "X", "type": "integer"}},
                "required": ["x"],
                "title": "actor_with_corr_desc_payload",
                "type": "object",
            },
            "contentType": "application/json",
            "correlationId": {
                "location": "$message.header#/correlationId",
                "description": "Request correlation ID",
            },
        },
    }


def test_channel_from_operation_only_is_included_in_schema() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="standalone_op",
        channel="standalone-channel",
        title="Standalone Channel Title",
        description="Standalone channel description",
    )

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "standalone-channel": {
            "title": "Standalone Channel Title",
            "description": "Standalone channel description",
        },
    }


def test_merge_channel_when_extra_has_no_messages_field() -> None:
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


def test_merge_channel_messages_when_base_has_no_messages() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="op_with_msg",
        channel="msg-channel",
        messages=[
            MessageSchema(
                name="op_message",
                payload={"type": "object"},
            ),
        ],
    )

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "msg-channel": {
                "messages": {"op_message": {"$ref": "#/components/messages/op_message"}},
            },
        },
        "operations": {
            "op_with_msg": {
                "action": "send",
                "channel": {"$ref": "#/channels/msg-channel"},
                "messages": [{"$ref": "#/channels/msg-channel/messages/op_message"}],
            },
        },
        "components": {
            "messages": {"op_message": {"name": "op_message", "payload": {"type": "object"}}},
        },
    }


def test_operation_with_external_docs_creates_channel_in_schema() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="op_with_docs",
        channel="docs-channel",
        external_docs=ExternalDocs(
            url="https://example.com/docs",
            description="External documentation",
        ),
    )

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "docs-channel": {
            "externalDocs": {
                "$ref": "#/components/externalDocs/external_docs_https_example_com_docs",
            },
        },
    }


def test_operation_with_tags_creates_channel_in_schema() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="op_with_tags",
        channel="tags-channel",
        tags=[Tag(name="op-tag", description="Operation tag")],
    )

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "tags-channel": {
            "tags": [{"$ref": "#/components/tags/op-tag"}],
        },
    }


def test_operation_with_amqp_bindings_includes_bindings_in_schema() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="op_with_bindings",
        channel="bindings-channel",
        bindings={"amqp": {"ack": True}},
    )

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["operations"] == {
        "op_with_bindings": {
            "action": "send",
            "channel": {"$ref": "#/channels/bindings-channel"},
            "bindings": {"amqp": {"ack": True}},
        },
    }


def test_channel_with_bindings_in_channel_data() -> None:
    app = Repid()
    router = Router(
        channel=ChannelData(
            address="bound-channel",
            bindings={"amqp": {"is": "queue"}},
        ),
    )

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "bound-channel": {
            "bindings": {"amqp": {"is": "queue"}},
            "messages": {"test_actor": {"$ref": "#/components/messages/test_actor"}},
        },
    }


def test_router_channel_not_in_messages_registry_is_added_to_schema() -> None:
    app = Repid()
    router = Router(channel="router-channel")

    @router.actor
    async def my_actor() -> None:
        pass

    app.include_router(router)

    app.messages.register_operation(
        operation_id="other_op",
        channel="other-channel",
    )

    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "router-channel": {"messages": {"my_actor": {"$ref": "#/components/messages/my_actor"}}},
        "other-channel": {},
    }


def test_messages_added_to_existing_channel_without_messages_key() -> None:
    app = Repid()
    router = Router(channel="shared-channel")

    @router.actor
    async def actor1() -> None:
        pass

    app.include_router(router)

    app.messages.register_operation(
        operation_id="pre_op",
        channel="shared-channel",
        title="Pre-existing operation",
    )

    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "shared-channel": {
            "messages": {"actor1": {"$ref": "#/components/messages/actor1"}},
            "title": "Pre-existing operation",
        },
    }


def test_actor_message_not_duplicated_when_already_in_channel() -> None:
    app = Repid()
    router = Router(channel="test-channel")

    @router.actor
    async def my_actor() -> None:
        pass

    app.include_router(router)

    app.messages.register_operation(
        operation_id="same_name_op",
        channel="test-channel",
        messages=[MessageSchema(name="my_actor")],
    )

    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "test-channel": {"messages": {"my_actor": {"$ref": "#/components/messages/my_actor"}}},
    }


def test_channel_with_external_docs_via_channel_data() -> None:
    app = Repid()
    router = Router(
        channel=ChannelData(
            address="docs-channel",
            title="Documented Channel",
            external_docs=ExternalDocs(url="https://docs.example.com"),
        ),
    )

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "docs-channel": {
            "title": "Documented Channel",
            "externalDocs": {
                "$ref": "#/components/externalDocs/external_docs_https_docs_example_com",
            },
            "messages": {
                "test_actor": {"$ref": "#/components/messages/test_actor"},
            },
        },
    }


def test_actor_with_content_type_generates_message_content_type() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def actor_with_payload(data: dict) -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["components"] == {
        "messages": {
            "actor_with_payload": {
                "name": "actor_with_payload",
                "summary": "Actor With Payload",
                "payload": {
                    "properties": {
                        "data": {"additionalProperties": True, "title": "Data", "type": "object"},
                    },
                    "required": ["data"],
                    "title": "actor_with_payload_payload",
                    "type": "object",
                },
                "contentType": "application/json",
            },
        },
    }


def test_server_with_description() -> None:
    app = Repid()
    router = Router()

    @router.actor
    async def test_actor() -> None:
        pass

    app.include_router(router)

    server = InMemoryServer(
        title="Tagged Server",
        description="A server with description",
    )
    app.servers.register_server("tagged", server)

    schema = app.generate_asyncapi_schema()

    assert schema["servers"] == {
        "tagged": {
            "host": "localhost",
            "protocol": "in-memory",
            "title": "Tagged Server",
            "description": "A server with description",
            "summary": "In-memory message broker for testing and development",
            "protocolVersion": "1.0.0",
        },
    }


def test_operation_messages_included_in_channel_messages() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="op_with_msg",
        channel="my-channel",
        messages=[MessageSchema(name="OpMessage")],
    )

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["channels"] == {
        "my-channel": {"messages": {"OpMessage": {"$ref": "#/components/messages/OpMessage"}}},
    }


def test_multiple_actors_on_same_channel() -> None:
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

    assert schema["channels"] == {
        "shared-channel": {
            "messages": {
                "actor1": {"$ref": "#/components/messages/actor1"},
                "actor2": {"$ref": "#/components/messages/actor2"},
            },
        },
    }


def test_nested_router_actors_included_in_schema() -> None:
    app = Repid()
    parent_router = Router()
    child_router = Router()

    @child_router.actor
    async def child_actor() -> None:
        pass

    parent_router.include_router(child_router)
    app.include_router(parent_router)

    schema = app.generate_asyncapi_schema()

    # insert_assert(schema)
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


def test_actor_with_all_metadata() -> None:
    app = Repid()
    router = Router()

    @router.actor(
        title="Full Actor",
        summary="Actor summary",
        description="Actor description",
        tags=[Tag(name="full-tag")],
        external_docs=ExternalDocs(url="https://full-docs.example.com"),
        bindings={"amqp": {"ack": True}},
        correlation_id=CorrelationId(location="$message.header#/id"),
    )
    async def full_actor(x: int) -> None:
        pass

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema == {
        "asyncapi": "3.0.0",
        "info": {"title": "Repid AsyncAPI", "version": "0.0.1"},
        "servers": {},
        "channels": {
            "default": {"messages": {"full_actor": {"$ref": "#/components/messages/full_actor"}}},
        },
        "operations": {
            "receive_full_actor": {
                "action": "receive",
                "channel": {"$ref": "#/channels/default"},
                "title": "Full Actor",
                "summary": "Actor summary",
                "description": "Actor description",
                "messages": [{"$ref": "#/channels/default/messages/full_actor"}],
                "bindings": {
                    "$ref": "#/components/operationBindings/full_actor_bindings",
                },
                "tags": [{"$ref": "#/components/tags/full-tag"}],
                "externalDocs": {
                    "$ref": "#/components/externalDocs/external_docs_https_full_docs_example_com",
                },
            },
        },
        "components": {
            "messages": {
                "full_actor": {
                    "name": "full_actor",
                    "title": "Full Actor",
                    "summary": "Actor summary",
                    "description": "Actor description",
                    "payload": {
                        "properties": {"x": {"title": "X", "type": "integer"}},
                        "required": ["x"],
                        "title": "full_actor_payload",
                        "type": "object",
                    },
                    "contentType": "application/json",
                    "correlationId": {"location": "$message.header#/id"},
                    "tags": [{"$ref": "#/components/tags/full-tag"}],
                    "externalDocs": {
                        "$ref": "#/components/externalDocs/external_docs_https_full_docs_example_com",
                    },
                },
            },
            "operationBindings": {"full_actor_bindings": {"amqp": {"ack": True}}},
            "tags": {"full-tag": {"name": "full-tag"}},
            "externalDocs": {
                "external_docs_https_full_docs_example_com": {
                    "url": "https://full-docs.example.com",
                },
            },
        },
    }


def test_message_with_full_example() -> None:
    app = Repid()
    router = Router()

    app.messages.register_operation(
        operation_id="example_op",
        channel="example-channel",
        messages=[
            MessageSchema(
                name="ExampleMessage",
                examples=(
                    MessageExample(
                        name="full_example",
                        summary="Full example",
                        headers={"Content-Type": "application/json"},
                        payload={"id": 1, "name": "test"},
                    ),
                ),
            ),
        ],
    )

    app.include_router(router)
    schema = app.generate_asyncapi_schema()

    assert schema["components"]["messages"]["ExampleMessage"] == {
        "name": "ExampleMessage",
        "examples": [
            {
                "name": "full_example",
                "summary": "Full example",
                "headers": {"Content-Type": "application/json"},
                "payload": {"id": 1, "name": "test"},
            },
        ],
    }
