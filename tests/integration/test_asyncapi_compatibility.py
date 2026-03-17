import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Annotated

import pytest
from pydantic import BaseModel

from repid import (
    Channel,
    Contact,
    ExternalDocs,
    Header,
    License,
    Repid,
    Router,
    Tag,
)
from repid.connections.amqp import AmqpServer
from repid.connections.in_memory import InMemoryServer
from repid.data.message_schema import CorrelationId, MessageExample, MessageSchema


def is_npm_installed() -> bool:
    return shutil.which("npm") is not None


@pytest.mark.skipif(not is_npm_installed(), reason="npm is not installed")
def test_generated_schema_validity_with_official_parser() -> None:
    # 1. Construct a complex Repid app
    app = Repid(
        title="Complex API",
        version="1.0.0",
        description="A complex API for validation",
        terms_of_service="https://example.com/terms",
        contact=Contact(
            name="Support",
            url="https://example.com/support",
            email="support@example.com",
        ),
        license=License(name="MIT", url="https://opensource.org/licenses/MIT"),
        tags=[
            Tag(
                name="global-tag",
                description="Global tag",
                external_docs=ExternalDocs(
                    url="https://example.com/tag-docs",
                    description="Tag docs",
                ),
            ),
            Tag(name="another-tag", description="Another global tag"),
        ],
        external_docs=ExternalDocs(
            url="https://example.com/docs",
            description="Global docs",
        ),
    )

    # Add Servers
    app.servers.register_server(
        "in-memory",
        InMemoryServer(
            title="In-Memory",
            description="In-memory server",
            tags=[Tag(name="server-tag")],
        ),
        is_default=True,
    )
    app.servers.register_server(
        "amqp",
        AmqpServer(
            "amqp://localhost",
            title="AMQP",
            summary="AMQP server summary",
            description="AMQP server",
            variables={"vhost": {"default": "/", "description": "Virtual host"}},
            security=[{"type": "scramSha256"}],
            tags=[Tag(name="amqp-server-tag")],
            external_docs=ExternalDocs(url="https://example.com/amqp-docs"),
        ),
    )

    # Add Router with Actors
    router = Router()

    class MyModel(BaseModel):
        field: str

    @router.actor(
        title="My Actor",
        summary="Summary of actor",
        description="Description of actor",
        tags=[Tag(name="actor-tag")],
        external_docs=ExternalDocs(url="https://example.com/actor"),
        channel=Channel(
            address="my-queue",
            title="My Channel",
            summary="Channel summary",
            description="Channel description",
            bindings={
                "amqp": {
                    "is": "routingKey",
                    "exchange": {
                        "name": "myExchange",
                        "type": "topic",
                        "durable": True,
                        "autoDelete": False,
                        "vhost": "/",
                    },
                    "bindingVersion": "0.3.0",
                },
            },
            external_docs=ExternalDocs(
                url="https://example.com/channel-docs",
                description="Channel docs",
            ),
        ),
        bindings={
            "amqp": {
                "expiration": 100000,
                "userId": "guest",
                "cc": ["user.logs"],
                "priority": 10,
                "deliveryMode": 2,
                "mandatory": False,
                "bcc": ["external.audit"],
                "timestamp": True,
                "ack": False,
                "bindingVersion": "0.3.0",
            },
        },
        security=[{"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}],
    )
    async def my_actor(
        data: MyModel,
        header: Annotated[str, Header(name="X-Header")],
    ) -> None:
        pass

    app.include_router(router)

    # Add Manual Operations
    app.messages.register_operation(
        operation_id="manual_op",
        channel="manual-channel",
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
    )

    # 2. Generate Schema
    schema = app.generate_asyncapi_schema()

    # 3. Validate with @asyncapi/cli
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        json.dump(schema, tmp)
        tmp_path = tmp.name

    try:
        # We use npx to run the CLI without installing it globally
        # -y flag confirms installation if needed
        result = subprocess.run(
            ["npx", "-y", "@asyncapi/cli", "validate", tmp_path],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            pytest.fail(f"AsyncAPI validation failed:\n{result.stdout}\n{result.stderr}")

    finally:
        Path(tmp_path).unlink()
