from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from repid.connections.abc import CapabilitiesT, SentMessageT, SubscriberT
from repid.connections.amqp.protocol.protocol import AmqpConnection, SenderLink, Session
from repid.connections.amqp.subscriber import AmqpSubscriber
from repid.logger import logger

if TYPE_CHECKING:
    from repid.asyncapi.models.common import (
        ExternalDocs,
        ServerBindingsObject,
        Tag,
    )
    from repid.asyncapi.models.servers import ServerVariable
    from repid.connections.abc import ReceivedMessageT


class AmqpServer:
    def __init__(
        self,
        dsn: str,
        *,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
    ) -> None:
        self.dsn = dsn
        self._connection: AmqpConnection | None = None

        self._session: Session | None = None
        self._publisher_links: dict[str, SenderLink] = {}

        # AsyncAPI metadata
        self._title = title
        self._summary = summary
        self._description = description
        self._protocol_version = "1.0.0"
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings

        # Parse DSN for server info
        parsed = urlparse(dsn)
        self._host = f"{parsed.hostname}:{parsed.port}" if parsed.port else str(parsed.hostname)
        self._pathname = parsed.path if parsed.path != "/" else None

        self._conn_host = parsed.hostname or "localhost"
        self._conn_port = parsed.port or 5672
        self._conn_username = parsed.username
        self._conn_password = parsed.password

        # Active subscribers
        self._active_subscribers: list[AmqpSubscriber] = []

    # ServerT properties
    @property
    def host(self) -> str:
        return self._host

    @property
    def protocol(self) -> str:
        return "amqp"

    @property
    def pathname(self) -> str | None:
        return self._pathname

    @property
    def title(self) -> str | None:
        return self._title

    @property
    def summary(self) -> str | None:
        return self._summary

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def protocol_version(self) -> str | None:
        return self._protocol_version

    @property
    def variables(self) -> Mapping[str, ServerVariable] | None:
        return self._variables

    @property
    def security(self) -> Sequence[Any] | None:
        return self._security

    @property
    def tags(self) -> Sequence[Tag] | None:
        return self._tags

    @property
    def external_docs(self) -> ExternalDocs | None:
        return self._external_docs

    @property
    def bindings(self) -> ServerBindingsObject | None:
        return self._bindings

    @property
    def capabilities(self) -> CapabilitiesT:
        return {
            "supports_acknowledgments": True,
            "supports_persistence": True,
            "supports_reply": True,
            "supports_lightweight_pause": False,
        }

    @property
    def session(self) -> Session | None:
        return self._session

    # Connection lifecycle management
    @property
    def is_connected(self) -> bool:
        return self._connection is not None and self._connection.is_connected

    async def connect(self) -> None:
        if self._connection is None or not self._connection.is_connected:
            self._connection = AmqpConnection(
                self._conn_host,
                self._conn_port,
                username=self._conn_username,
                password=self._conn_password,
            )
            await self._connection.connect()
            # Create session
            self._session = self._connection.create_session()
            await self._session.begin()

    async def disconnect(self) -> None:
        # Close all active subscribers
        for subscriber in self._active_subscribers:
            try:
                await subscriber.close()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error closing subscriber", exc_info=exc)

        self._active_subscribers.clear()
        self._publisher_links.clear()

        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._session = None

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[AmqpServer, None]:
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()

    # Message publishing
    async def publish(
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        del server_specific_parameters
        logger.debug("Publishing message to channel '{channel}'.", extra={"channel": channel})

        if not self.is_connected or self._session is None:
            raise ConnectionError("Not connected to AMQP server")

        # Get/Create Sender Link
        if channel not in self._publisher_links:
            link = await self._session.create_sender_link(channel, f"sender-{channel}")
            self._publisher_links[channel] = link

        link = self._publisher_links[channel]

        # Send message
        # We need to encode message payload if it's not bytes?
        # SentMessageT.payload is bytes.
        await link.send(message.payload, message.headers)

    # Message receiving
    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        logger.debug(
            "Subscribing to channels '{channels}'.",
            extra={"channels": list(channels_to_callbacks.keys())},
        )

        if not self.is_connected:
            raise ConnectionError("Not connected to AMQP server")

        # Create and store subscriber
        subscriber = await AmqpSubscriber.create(
            queues_to_callbacks=channels_to_callbacks,
            concurrency_limit=concurrency_limit,
            server=self,
        )
        self._active_subscribers.append(subscriber)

        return subscriber
