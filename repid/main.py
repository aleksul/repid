from __future__ import annotations

import signal
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from repid._worker import _Worker
from repid.asyncapi import AsyncAPI3Schema, AsyncAPIGenerator
from repid.data import Message, RunnerInfo
from repid.message_registry import MessageRegistry
from repid.router import Router
from repid.serializer import default_serializer as repid_default_serializer
from repid.server_registry import ServerRegistry

if TYPE_CHECKING:
    from repid.data import Contact, ExternalDocs, License, Tag
    from repid.health_check_server import HealthCheckServerSettings
    from repid.serializer import SerializerT


class Repid:
    def __init__(
        self,
        *,
        title: str = "Repid AsyncAPI",
        version: str = "0.0.1",
        description: str | None = None,
        terms_of_service: str | None = None,
        contact: Contact | None = None,
        license: License | None = None,  # noqa: A002
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        default_serializer: SerializerT | None = None,
    ) -> None:
        self.title = title
        self.version = version
        self.description = description
        self.terms_of_service = terms_of_service
        self.contact = contact
        self.license = license
        self.tags = tags
        self.external_docs = external_docs
        self.default_serializer = (
            default_serializer if default_serializer is not None else repid_default_serializer
        )
        self._messages = MessageRegistry()
        self._servers = ServerRegistry()
        self._centralized_router = Router()

    @property
    def servers(self) -> ServerRegistry:
        return self._servers

    @property
    def messages(self) -> MessageRegistry:
        return self._messages

    def include_router(self, router: Router) -> None:
        self._centralized_router.include_router(router)

    async def run_worker(
        self,
        *,
        graceful_shutdown_time: float = 25.0,
        messages_limit: int = float("inf"),  # type: ignore[assignment]
        tasks_limit: int = 1000,
        register_signals: Iterable[signal.Signals] | None = None,
        health_check_server: HealthCheckServerSettings | None = None,
        server_name: str | None = None,
    ) -> RunnerInfo:
        server = self._servers.get_server(server_name)
        if server is None:
            raise ValueError(
                f"Server '{server_name}' not found."
                if server_name
                else "No default server configured.",
            )

        worker = _Worker(
            server=server,
            router=self._centralized_router,
            graceful_shutdown_time=graceful_shutdown_time,
            messages_limit=messages_limit,
            tasks_limit=tasks_limit,
            register_signals=register_signals,
            health_check_server=health_check_server,
        )
        runner = await worker.run()
        return RunnerInfo(processed=runner.processed)

    def generate_asyncapi_schema(self) -> AsyncAPI3Schema:
        return AsyncAPIGenerator(
            routers=[self._centralized_router],
            servers=self._servers,
            messages=self._messages,
            title=self.title,
            version=self.version,
            description=self.description,
            terms_of_service=self.terms_of_service,
            contact=self.contact,
            license=self.license,
            tags=list(self.tags) if self.tags is not None else None,
            external_docs=self.external_docs,
        ).generate_schema()

    async def send_message(
        self,
        operation_id: str,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        operation = self._messages.get_operation(operation_id)
        if operation is None:
            raise ValueError(f"Operation '{operation_id}' not found.")

        server = self._servers.get_server(server_name)
        if server is None:
            raise ValueError(
                f"Server '{server_name}' not found."
                if server_name
                else "No default server configured.",
            )

        await server.publish(
            channel=operation.channel.address,
            message=Message(
                payload=payload,
                headers=headers,
                content_type=content_type,
            ),
            server_specific_parameters=server_specific_parameters,
        )

    async def send_message_json(
        self,
        operation_id: str,
        payload: Any,
        *,
        headers: dict[str, str] | None = None,
        serializer: SerializerT | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        serializer = serializer if serializer is not None else self.default_serializer
        await self.send_message(
            operation_id=operation_id,
            payload=serializer(payload).encode(),
            headers=headers,
            content_type="application/json",
            server_name=server_name,
            server_specific_parameters=server_specific_parameters,
        )
