from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlparse

from repid.connections.abc import CapabilitiesT, SentMessageT, SubscriberT
from repid.connections.amqp.protocol import (
    AmqpConnection,
    ConnectionConfig,
    ManagedSession,
)
from repid.connections.amqp.subscriber import AmqpSubscriber
from repid.logger import logger

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.connections.abc import ReceivedMessageT
    from repid.data import ExternalDocs, Tag


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
        publish_naming_strategy: Callable[[str, str | None], str] | None = None,
        subscribe_naming_strategy: Callable[[str], str] | None = None,
    ) -> None:
        self.dsn = dsn
        self._connection: AmqpConnection | None = None
        self._managed_session: ManagedSession | None = None

        # AsyncAPI metadata
        self._title = title
        self._summary = summary
        self._description = description
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings

        self._publish_naming_strategy = (
            publish_naming_strategy or self._default_publish_naming_strategy
        )
        self._subscribe_naming_strategy = (
            subscribe_naming_strategy or self._default_subscribe_naming_strategy
        )

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

    @staticmethod
    def _default_publish_naming_strategy(
        channel: str,
        routing_key: str | None = None,  # noqa: ARG004
    ) -> str:
        # uses RabbitMQ Address v2 style - routes directly to a queue via amq.default exchange
        return f"/queues/{quote(channel, safe='')}"

    @staticmethod
    def _default_subscribe_naming_strategy(channel: str) -> str:
        # uses RabbitMQ Address v2 style - consumes from a queue
        return f"/queues/{quote(channel, safe='')}"

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
        return "1.0.0"

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
    def managed_session(self) -> ManagedSession | None:
        """Get the managed session (for advanced use cases)."""
        return self._managed_session

    # Connection lifecycle management
    @property
    def is_connected(self) -> bool:
        return self._connection is not None and self._connection.is_connected

    async def connect(self) -> None:
        if self._connection is None or not self._connection.is_connected:
            self._connection = AmqpConnection(
                ConnectionConfig(
                    host=self._conn_host,
                    port=self._conn_port,
                    username=self._conn_username,
                    password=self._conn_password,
                ),
            )
            await self._connection.connect()
            # Create managed session that handles reconnection automatically
            self._managed_session = ManagedSession(self._connection)

    async def disconnect(self) -> None:
        # Close all active subscribers
        for subscriber in self._active_subscribers:
            try:
                await subscriber.close()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error closing subscriber", exc_info=exc)

        self._active_subscribers.clear()

        # Close managed session
        if self._managed_session is not None:
            await self._managed_session.close()
            self._managed_session = None

        if self._connection is not None:
            await self._connection.close()
            self._connection = None

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
        """
        Publish a message to a channel.

        This method uses the SenderPool from the managed session, which automatically
        handles link creation, reconnection, and retry logic.

        Args:
            channel: Target channel name
            message: Message to send
            server_specific_parameters: Optional AMQP-specific parameters including:
                - header: AmqpHeader
                - properties: AmqpProperties
                - delivery_annotations: dict[str, Any]
                - message_annotations: dict[str, Any]
                - footers: dict[str, Any]
                - to: str - override address
                - routing_key: str - specify routing key for naming strategy
        """
        logger.debug("Publishing message to channel '{channel}'.", extra={"channel": channel})

        if self._managed_session is None:
            raise ConnectionError("Not connected to AMQP server")

        params = server_specific_parameters or {}

        # Get address
        if "to" in params:
            address = params["to"]
        else:
            address = self._publish_naming_strategy(channel, params.get("routing_key"))

        # Prepare server-specific parameters for uAMQP
        header = params.get("header")
        properties = params.get("properties")
        delivery_annotations = params.get("delivery_annotations")
        message_annotations = params.get("message_annotations")
        footers = params.get("footers")

        # Use SenderPool's send method with automatic retry
        await self._managed_session.sender_pool.send(
            address,
            message.payload,
            headers=message.headers,
            name=f"sender-{channel}",
            message_header=header,
            message_properties=properties,
            delivery_annotations=delivery_annotations,
            message_annotations=message_annotations,
            footer=footers,
        )

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

        if not self.is_connected or self._managed_session is None:
            raise ConnectionError("Not connected to AMQP server")

        # Create and store subscriber
        subscriber = await AmqpSubscriber.create(
            queues_to_callbacks=channels_to_callbacks,
            concurrency_limit=concurrency_limit,
            managed_session=self._managed_session,
            naming_strategy=self._subscribe_naming_strategy,
            publish_fn=self.publish,
        )
        self._active_subscribers.append(subscriber)

        return subscriber
