from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass
from enum import IntEnum
from wsgiref.handlers import format_date_time

from repid._utils import FROZEN_DATACLASS, SLOTS_DATACLASS
from repid.logger import logger


@dataclass(**FROZEN_DATACLASS, **SLOTS_DATACLASS)
class HealthCheckServerSettings:
    address: str = "0.0.0.0"  # noqa: S104
    port: int = 8080
    endpoint_name: str = "/healthz"


class HealthCheckStatus(IntEnum):
    OK = 200
    UNHEALTHY = 503


class HealthCheckServer:
    def __init__(self, server_settings: HealthCheckServerSettings | None = None) -> None:
        self.server_settings = (
            server_settings if server_settings is not None else HealthCheckServerSettings()
        )
        self._server_protocol: asyncio.AbstractServer | None = None
        self._server: asyncio.AbstractServer | None = None
        self._health_status = HealthCheckStatus.OK

    async def start(self) -> None:
        if self._server is None or not self._server.is_serving():
            loop = asyncio.get_running_loop()
            self._server = await loop.create_server(
                lambda: _HttpServerProtocol(
                    endpoint_name=self.server_settings.endpoint_name,
                    status=self.health_status,
                ),
                host=self.server_settings.address,
                port=self.server_settings.port,
                reuse_port=True,
            )
            await self._server.start_serving()
            logger.info("Started health check server.", extra=asdict(self.server_settings))

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Stopped health check server.")

    @property
    def health_status(self) -> HealthCheckStatus:
        return self._health_status

    @health_status.setter
    def health_status(self, new_health_status: HealthCheckStatus) -> HealthCheckStatus:
        self._health_status = new_health_status
        return new_health_status


class _HttpServerProtocol(asyncio.Protocol):
    def __init__(self, endpoint_name: str, status: HealthCheckStatus) -> None:
        super().__init__()
        self.endpoint_name = endpoint_name
        self.status = status

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
        if method == "GET" and path == self.endpoint_name:
            content = f"{self.status.value} {self.status.name}"
        else:
            content = "404 Not Found"
        return (
            f"HTTP/1.1 {content}\r\n"
            f"Date: {format_date_time(time.time())}\r\n"
            f"Content-Type: text/plain\r\n"
            f"Content-Length: {len(content)}\r\n"
            f"Connection: close\r\n\r\n"
            f"{content}"
        )
