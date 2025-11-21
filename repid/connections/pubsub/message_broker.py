from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import aiohttp
from gcloud.aio.pubsub import PublisherClient, SubscriberClient

from repid.connections.abc import CapabilitiesT, ReceivedMessageT, SentMessageT, SubscriberT
from repid.logger import logger

from .helpers import _ChannelConfig, _EncodablePubsubMessage
from .subscriber import PubsubSubscriber

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ExternalDocs, ServerBindingsObject, Tag
    from repid.asyncapi.models.servers import ServerVariable

    from .helpers import ChannelOverride


class PubsubServer:
    DEFAULT_API_ROOT = "https://pubsub.googleapis.com/v1"
    DEFAULT_PUBLISH_TIMEOUT = 10

    def __init__(
        self,
        dsn: str | None = None,
        *,
        default_project: str,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
        channel_overrides: Mapping[str, ChannelOverride] | None = None,
        pull_batch_size: int = 10,
        pull_idle_sleep: float = 0.5,
    ) -> None:
        self.dsn = dsn or PubsubServer.DEFAULT_API_ROOT

        # AsyncAPI metadata
        self._title = title
        self._summary = summary
        self._description = description
        self._protocol_version = "v1"
        self._variables = variables
        self._security = security
        self._tags = tags
        self._external_docs = external_docs
        self._bindings = bindings

        # Parse DSN for server info
        parsed = urlparse(self.dsn)
        self._host = f"{parsed.hostname}:{parsed.port}" if parsed.port else str(parsed.hostname)
        self._pathname = parsed.path if parsed.path != "/" else None

        # if dsn is default or not set, default it to None so that gcloud lib thinks it's in production mode
        self._api_root: str | None = (
            self.dsn if self.dsn and self.dsn != PubsubServer.DEFAULT_API_ROOT else None
        )

        self._default_project = default_project
        self._channel_overrides: dict[str, ChannelOverride] = dict(channel_overrides or {})
        self._pull_batch_size = max(1, min(pull_batch_size, 1000))
        self._pull_idle_sleep = max(0.05, pull_idle_sleep)

        self._session: aiohttp.ClientSession | None = None
        self._publisher_client: PublisherClient | None = None
        self._subscriber_client: SubscriberClient | None = None
        self._active_subscribers: list[PubsubSubscriber] = []

    # ServerT properties
    @property
    def host(self) -> str:
        return self._host

    @property
    def protocol(self) -> str:
        return "googlepubsub"

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
            "supports_reply": False,
            "supports_lightweight_pause": False,
        }

    # Connection lifecycle management
    @property
    def is_connected(self) -> bool:
        return (
            self._session is not None
            and not self._session.closed
            and self._publisher_client is not None
            and self._subscriber_client is not None
        )

    async def connect(self) -> None:
        if self.is_connected:
            return

        self._session = aiohttp.ClientSession()
        self._publisher_client = PublisherClient(session=self._session, api_root=self._api_root)
        self._subscriber_client = SubscriberClient(session=self._session, api_root=self._api_root)
        logger.debug("Connected to Pub/Sub API.")

    async def disconnect(self) -> None:
        while self._active_subscribers:
            subscriber = self._active_subscribers.pop()
            with suppress(Exception):
                await subscriber.close()

        if self._publisher_client is not None:
            await self._publisher_client.close()
            self._publisher_client = None

        if self._subscriber_client is not None:
            await self._subscriber_client.close()
            self._subscriber_client = None

        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

        logger.debug("Disconnected from Pub/Sub API.")

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[PubsubServer, None]:
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
        Publish a message to a PubSub topic.

        Args:
            channel: The topic to publish to
            message: The message to publish
            server_specific_parameters: PubSub-specific options:
                topic: Override topic name
                project: Override GCP project ID
                timeout: Publish timeout in seconds (int, float, or timedelta)
                ordering_key: Message ordering key (string)
                attributes: Additional message attributes (dict)
        """
        if not self.is_connected or self._publisher_client is None:
            raise ConnectionError("PubSub server is not connected.")

        topic_path = self._resolve_topic_path(channel, server_specific_parameters)
        timeout = self._extract_timeout(server_specific_parameters)
        ordering_key = self._extract_ordering_key(server_specific_parameters)
        attributes = self._build_attributes(message, server_specific_parameters)

        pubsub_message = _EncodablePubsubMessage(
            payload=message.payload,
            attributes=attributes,
            ordering_key=ordering_key,
        )

        logger.debug(
            "Publishing message to PubSub topic '{topic}'.",
            extra={"topic": topic_path, "channel": channel},
        )

        await self._publisher_client.publish(
            topic_path,
            [pubsub_message],
            timeout=timeout,
        )

    # Message receiving
    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        if not self.is_connected or self._subscriber_client is None:
            raise ConnectionError("PubSub server is not connected.")

        logger.debug(
            "Subscribing to PubSub channels '{channels}'.",
            extra={"channels": list(channels_to_callbacks.keys())},
        )

        channel_configs: list[_ChannelConfig] = []
        for channel, callback in channels_to_callbacks.items():
            subscription_path = self._resolve_subscription_path(channel)
            channel_configs.append(
                _ChannelConfig(
                    channel=channel,
                    subscription_path=subscription_path,
                    callback=callback,
                ),
            )

        subscriber = await PubsubSubscriber.create(
            subscriber_client=self._subscriber_client,
            channel_configs=channel_configs,
            concurrency_limit=concurrency_limit,
            pull_batch_size=self._pull_batch_size,
            pull_idle_sleep=self._pull_idle_sleep,
            server=self,
        )
        self._active_subscribers.append(subscriber)
        return subscriber

    def _resolve_topic_path(
        self,
        channel: str,
        server_specific_parameters: dict[str, Any] | None,
    ) -> str:
        topic = (
            (server_specific_parameters.get("topic") if server_specific_parameters else None)
            or (
                self._channel_overrides[channel]["topic"]
                if channel in self._channel_overrides
                and "topic" in self._channel_overrides[channel]
                else None
            )
            or channel
        )
        project = (
            (server_specific_parameters.get("project") if server_specific_parameters else None)
            or (
                self._channel_overrides[channel]["project"]
                if channel in self._channel_overrides
                and "project" in self._channel_overrides[channel]
                else None
            )
            or self._default_project
        )
        return f"projects/{project}/topics/{topic}"

    def _resolve_subscription_path(self, channel: str) -> str:
        subscription = (
            self._channel_overrides[channel]["subscription"]
            if channel in self._channel_overrides
            and "subscription" in self._channel_overrides[channel]
            else None
        ) or channel
        project = (
            self._channel_overrides[channel]["project"]
            if channel in self._channel_overrides and "project" in self._channel_overrides[channel]
            else None
        ) or self._default_project
        return f"projects/{project}/subscriptions/{subscription}"

    def _extract_timeout(self, server_specific_parameters: dict[str, Any] | None) -> int:
        if server_specific_parameters is None:
            return PubsubServer.DEFAULT_PUBLISH_TIMEOUT
        timeout_override = server_specific_parameters.get("timeout")
        if timeout_override is not None and isinstance(timeout_override, (int, float, timedelta)):
            if isinstance(timeout_override, timedelta):
                return int(timeout_override.total_seconds())
            return int(timeout_override)
        return PubsubServer.DEFAULT_PUBLISH_TIMEOUT

    def _extract_ordering_key(
        self,
        server_specific_parameters: dict[str, Any] | None,
    ) -> str | None:
        if server_specific_parameters is None:
            return None
        key = server_specific_parameters.get("ordering_key")
        return str(key) if key is not None else None

    def _build_attributes(
        self,
        message: SentMessageT,
        server_specific_parameters: dict[str, Any] | None,
    ) -> dict[str, str] | None:
        attributes: dict[str, str] = {}
        if message.headers is not None:
            attributes.update({str(k): str(v) for k, v in message.headers.items()})
        if message.content_type is not None:
            attributes.setdefault("content_type", message.content_type)

        if server_specific_parameters is not None:
            extra_attributes = server_specific_parameters.get("attributes")
            if isinstance(extra_attributes, Mapping):
                for key, value in extra_attributes.items():
                    attributes[str(key)] = str(value)

        return attributes or None
