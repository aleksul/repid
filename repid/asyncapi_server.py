from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from repid.logger import logger

if TYPE_CHECKING:
    from repid.asyncapi import AsyncAPI3Schema

ASYNCAPI_JS_DEFAULT_URL = (
    "https://unpkg.com/@asyncapi/react-component@2.6.5/browser/standalone/index.js"
)

ASYNCAPI_CSS_DEFAULT_URL = (
    "https://unpkg.com/@asyncapi/react-component@2.6.5/styles/default.min.css"
)


@dataclass(frozen=True, slots=True, kw_only=True)
class AsyncAPIServerSettings:
    address: str = "0.0.0.0"  # noqa: S104
    port: int = 8081
    endpoint_name: str = "/"
    sidebar: bool = True
    info: bool = True
    servers: bool = True
    operations: bool = True
    messages: bool = True
    schemas: bool = True
    errors: bool = True
    expand_message_examples: bool = True


class AsyncAPIServer:
    def __init__(
        self,
        schema: AsyncAPI3Schema,
        server_settings: AsyncAPIServerSettings | None = None,
    ) -> None:
        self.schema = schema
        self.server_settings = (
            server_settings if server_settings is not None else AsyncAPIServerSettings()
        )
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        if self._server is None or not self._server.is_serving():
            loop = asyncio.get_running_loop()
            self._server = await loop.create_server(
                lambda: _HttpServerProtocol(
                    schema=self.schema,
                    settings=self.server_settings,
                ),
                host=self.server_settings.address,
                port=self.server_settings.port,
                reuse_port=True,
            )
            await self._server.start_serving()
            logger.info("Started AsyncAPI server.", extra=asdict(self.server_settings))

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Stopped AsyncAPI server.")

    async def __aenter__(self) -> AsyncAPIServer:  # noqa: PYI034
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.stop()


class _HttpServerProtocol(asyncio.Protocol):
    def __init__(self, schema: AsyncAPI3Schema, settings: AsyncAPIServerSettings) -> None:
        super().__init__()
        self.schema = schema
        self.settings = settings

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport: asyncio.WriteTransport = transport  # type: ignore[assignment]

    def data_received(self, data: bytes) -> None:
        message = data.decode()

        headers, _ = message.split("\r\n\r\n", maxsplit=1)
        http_lines = headers.split("\r\n", maxsplit=1)
        method, path, _ = http_lines[0].split(" ", maxsplit=2)

        response = self.handle_request(method, path)

        self.transport.write(response.encode())
        self.transport.close()

    def handle_request(self, method: str, path: str) -> str:
        if method == "GET" and path == self.settings.endpoint_name:
            html_content = get_asyncapi_html(
                self.schema,
                sidebar=self.settings.sidebar,
                info=self.settings.info,
                servers=self.settings.servers,
                operations=self.settings.operations,
                messages=self.settings.messages,
                schemas=self.settings.schemas,
                errors=self.settings.errors,
                expand_message_examples=self.settings.expand_message_examples,
            )
            return (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: text/html\r\n"
                f"Content-Length: {len(html_content)}\r\n"
                f"Connection: close\r\n\r\n"
                f"{html_content}"
            )
        content = "404 Not Found"
        return (
            f"HTTP/1.1 404 Not Found\r\n"
            f"Content-Type: text/plain\r\n"
            f"Content-Length: {len(content)}\r\n"
            f"Connection: close\r\n\r\n"
            f"{content}"
        )


def get_asyncapi_html(
    schema: AsyncAPI3Schema,
    *,
    sidebar: bool = True,
    info: bool = True,
    servers: bool = True,
    operations: bool = True,
    messages: bool = True,
    schemas: bool = True,
    errors: bool = True,
    expand_message_examples: bool = True,
    asyncapi_js_url: str = ASYNCAPI_JS_DEFAULT_URL,
    asyncapi_css_url: str = ASYNCAPI_CSS_DEFAULT_URL,
) -> str:
    schema_json = schema

    config = {
        "schema": schema_json,
        "config": {
            "show": {
                "sidebar": sidebar,
                "info": info,
                "servers": servers,
                "operations": operations,
                "messages": messages,
                "schemas": schemas,
                "errors": errors,
            },
            "expand": {
                "messageExamples": expand_message_examples,
            },
            "sidebar": {
                "showServers": "byDefault",
                "showOperations": "byDefault",
            },
        },
    }

    return (
        """
    <!DOCTYPE html>
    <html>
        <head>
    """
        f"""
        <title>{schema["info"]["title"]} AsyncAPI</title>
    """
        """
        <link rel="icon" href="https://www.asyncapi.com/favicon.ico">
        <link rel="icon" type="image/png" sizes="16x16" href="https://www.asyncapi.com/favicon-16x16.png">
        <link rel="icon" type="image/png" sizes="32x32" href="https://www.asyncapi.com/favicon-32x32.png">
        <link rel="icon" type="image/png" sizes="194x194" href="https://www.asyncapi.com/favicon-194x194.png">
    """
        f"""
        <link rel="stylesheet" href="{asyncapi_css_url}">
    """
        """
        </head>

        <style>
        html {
            font-family: ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;
            line-height: 1.5;
        }
        </style>

        <body>
        <div id="asyncapi"></div>
    """
        f"""
        <script src="{asyncapi_js_url}"></script>
        <script>
    """
        f"""
        AsyncApiStandalone.render({json.dumps(config)}, document.getElementById('asyncapi'));
    """
        """
        </script>
        </body>
    </html>
    """
    )
