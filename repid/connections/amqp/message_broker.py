from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import aiormq
from aiormq.abc import Basic

from repid.connections.abc import CapabilitiesT, SentMessageT, SubscriberT
from repid.connections.amqp.helpers import AmqpSubscriber
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
        self._connection: aiormq.abc.AbstractConnection | None = None
        self._publisher_channel: aiormq.abc.AbstractChannel | None = None

        # AsyncAPI metadata
        self._title = title
        self._summary = summary
        self._description = description
        self._protocol_version = "0.9.1"
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings

        # Parse DSN for server info
        parsed = urlparse(dsn)
        self._host = f"{parsed.hostname}:{parsed.port}" if parsed.port else str(parsed.hostname)
        self._pathname = parsed.path if parsed.path != "/" else None

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

    # Connection lifecycle management
    @property
    def is_connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def connect(self) -> None:
        if self._connection is None or self._connection.is_closed:
            self._connection = await aiormq.connect(self.dsn)
            # Publisher channel will be created lazily when needed
            self._publisher_channel = None

    async def _get_publisher_channel(self) -> aiormq.abc.AbstractChannel:
        """Get or create the publisher channel (lazy initialization)"""
        if self._connection is None or self._connection.is_closed:
            raise ConnectionError("Not connected to RabbitMQ server")

        if self._publisher_channel is None or self._publisher_channel.is_closed:
            self._publisher_channel = await self._connection.channel()

        return self._publisher_channel

    async def disconnect(self) -> None:
        # Close all active subscribers (they manage their own channels)
        for subscriber in self._active_subscribers:
            try:
                await subscriber.close()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error closing subscriber", exc_info=exc)

        self._active_subscribers.clear()

        if self._publisher_channel is not None:
            await self._publisher_channel.close()
            self._publisher_channel = None
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
    async def publish(  # noqa: C901
        self,
        *,
        channel: str,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None = None,
    ) -> None:
        """
        Publish a message to a RabbitMQ channel.

        Args:
            channel: The queue/routing key to publish to
            message: The message to publish
            server_specific_parameters: RabbitMQ-specific options:
                Message Properties:
                - priority: Message priority (0-9)
                - expiration: Message TTL in milliseconds (e.g., 60000 for 60 seconds)
                - delivery_mode: 1 (non-persistent) or 2 (persistent, default: 2)
                - correlation_id: Correlation ID for request/reply patterns
                - reply_to: Queue name to reply to

                Publish Options:
                - mandatory: Whether message must be routable (default: True)
                - immediate: Whether message must be immediately consumable (default: False)

        Example:
            await server.publish(
                channel="tasks",
                message=message,
                server_specific_parameters={
                    "priority": 9,
                    "expiration": 300000,
                    "delivery_mode": 2,
                    "mandatory": True,
                },
            )
        """
        logger.debug("Publishing message to channel '{channel}'.", extra={"channel": channel})

        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ server")

        # Lazily get/create the publisher channel
        amqp_channel = await self._get_publisher_channel()

        # Get server-specific parameters or use defaults
        params = server_specific_parameters or {}

        # Build message properties
        properties_kwargs: dict[str, Any] = {
            "content_type": message.content_type,
            "headers": message.headers,
        }

        # Add RabbitMQ-specific properties from server_specific_parameters
        if "priority" in params:
            try:
                properties_kwargs["priority"] = int(params["priority"])
            except (ValueError, TypeError):
                logger.warning(f"Invalid priority value: {params['priority']}")

        if "expiration" in params:
            properties_kwargs["expiration"] = str(params["expiration"])

        if "delivery_mode" in params:
            try:
                delivery_mode = int(params["delivery_mode"])
                if delivery_mode in (1, 2):
                    properties_kwargs["delivery_mode"] = delivery_mode
                else:
                    logger.warning(f"Invalid delivery_mode: {delivery_mode}, must be 1 or 2")
            except (ValueError, TypeError):
                logger.warning(f"Invalid delivery_mode value: {params['delivery_mode']}")
        else:
            # Default to persistent messages
            properties_kwargs["delivery_mode"] = 2

        if "correlation_id" in params:
            properties_kwargs["correlation_id"] = str(params["correlation_id"])

        if "reply_to" in params:
            properties_kwargs["reply_to"] = str(params["reply_to"])

        properties = aiormq.spec.Basic.Properties(**properties_kwargs)

        # Extract publish options
        mandatory = params.get("mandatory", True)
        immediate = params.get("immediate", False)

        # Publish the message (no queue declaration)
        confirmation = await amqp_channel.basic_publish(
            body=message.payload,
            routing_key=channel,
            properties=properties,
            mandatory=mandatory,
            immediate=immediate,
        )
        if not isinstance(confirmation, Basic.Ack):  # pragma: no cover
            raise ConnectionError("Message wasn't published.")

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
            raise ConnectionError("Not connected to RabbitMQ server")

        # Create and store subscriber (it will create its own channel)
        subscriber = await AmqpSubscriber.create(
            queues_to_callbacks=channels_to_callbacks,
            concurrency_limit=concurrency_limit,
            server=self,
        )
        self._active_subscribers.append(subscriber)

        return subscriber
