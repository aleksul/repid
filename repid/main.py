from __future__ import annotations

import signal
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, overload

from repid._worker import _Worker
from repid.asyncapi import AsyncAPI3Schema, AsyncAPIGenerator
from repid.asyncapi_server import AsyncAPIServer, get_asyncapi_html
from repid.data import MessageData, RunnerInfo
from repid.message_registry import MessageRegistry
from repid.middlewares import (
    ActorMiddlewareT,
    ProducerMiddlewareT,
    _compile_producer_middleware_pipeline,
)
from repid.router import Router
from repid.serializer import default_serializer as repid_default_serializer
from repid.server_registry import ServerRegistry

if TYPE_CHECKING:
    from repid.asyncapi_server import AsyncAPIServerSettings
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
        actor_middlewares: Sequence[ActorMiddlewareT] | None = None,
        producer_middlewares: Sequence[ProducerMiddlewareT] | None = None,
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
        self._centralized_router = Router(
            middlewares=actor_middlewares,
        )
        self._producer_middlewares = producer_middlewares
        self._producer_middleware_pipeline = _compile_producer_middleware_pipeline(
            producer_middlewares,
        )

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
        asyncapi_server: AsyncAPIServerSettings | None = None,
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
            asyncapi_server=asyncapi_server,
            asyncapi_schema=self.generate_asyncapi_schema() if asyncapi_server else None,
            default_serializer=self.default_serializer,
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

    def asyncapi_html(
        self,
        *,
        sidebar: bool = True,
        info: bool = True,
        servers: bool = True,
        operations: bool = True,
        messages: bool = True,
        schemas: bool = True,
        errors: bool = True,
        expand_message_examples: bool = True,
    ) -> str:
        return get_asyncapi_html(
            self.generate_asyncapi_schema(),
            sidebar=sidebar,
            info=info,
            servers=servers,
            operations=operations,
            messages=messages,
            schemas=schemas,
            errors=errors,
            expand_message_examples=expand_message_examples,
        )

    def asyncapi_server(
        self,
        server_settings: AsyncAPIServerSettings | None = None,
    ) -> AsyncAPIServer:
        return AsyncAPIServer(self.generate_asyncapi_schema(), server_settings)

    async def _send_message(
        self,
        *,
        channel: str | None = None,
        operation_id: str | None = None,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        if channel is None and operation_id is None:
            raise ValueError("Either 'channel' or 'operation_id' must be specified.")
        if channel is not None and operation_id is not None:
            raise ValueError("Specify either 'channel' or 'operation_id', not both.")

        if operation_id is not None:
            operation = self._messages.get_operation(operation_id)
            if operation is None:
                raise ValueError(f"Operation '{operation_id}' not found.")
            operation_channel = operation.channel.address

        server = self._servers.get_server(server_name)
        if server is None:
            raise ValueError(
                f"Server '{server_name}' not found."
                if server_name
                else "No default server configured.",
            )

        await self._producer_middleware_pipeline(server.publish)(
            channel if channel is not None else operation_channel,
            MessageData(
                payload=payload,
                headers=headers,
                content_type=content_type,
            ),
            server_specific_parameters,
        )

    @overload
    async def send_message(
        self,
        *,
        operation_id: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    @overload
    async def send_message(
        self,
        *,
        channel: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    async def send_message(
        self,
        *,
        channel: str | None = None,
        operation_id: str | None = None,
        payload: bytes,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        await self._send_message(
            channel=channel,
            operation_id=operation_id,
            payload=payload,
            headers=headers,
            content_type=content_type,
            server_name=server_name,
            server_specific_parameters=server_specific_parameters,
        )

    @overload
    async def send_message_json(
        self,
        *,
        operation_id: str,
        payload: Any,
        headers: dict[str, str] | None = None,
        serializer: SerializerT | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    @overload
    async def send_message_json(
        self,
        *,
        channel: str,
        payload: Any,
        headers: dict[str, str] | None = None,
        serializer: SerializerT | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None: ...

    async def send_message_json(
        self,
        *,
        operation_id: str | None = None,
        channel: str | None = None,
        payload: Any,
        headers: dict[str, str] | None = None,
        serializer: SerializerT | None = None,
        server_name: str | None = None,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        serializer = serializer if serializer is not None else self.default_serializer
        await self._send_message(
            channel=channel,
            operation_id=operation_id,
            payload=serializer(payload),
            headers=headers,
            content_type="application/json",
            server_name=server_name,
            server_specific_parameters=server_specific_parameters,
        )
