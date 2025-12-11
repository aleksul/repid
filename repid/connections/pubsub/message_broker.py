from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import grpc.aio

from repid.connections.abc import CapabilitiesT, ReceivedMessageT, SentMessageT, SubscriberT
from repid.logger import logger

from .helpers import ChannelOverride
from .protocol import (
    ChannelConfig,
    CredentialsProvider,
    GoogleDefaultCredentials,
    GrpcChannelFactory,
    InsecureCredentials,
    PublishRequest,
    PublishResponse,
    PubsubMessage,
    PubsubSubscriber,
    ResilienceConfig,
    ResilienceState,
    with_retry,
)

if TYPE_CHECKING:
    from repid.asyncapi.models.common import ServerBindingsObject
    from repid.asyncapi.models.servers import ServerVariable
    from repid.data import ExternalDocs, Tag

    from .protocol import ChannelFactory


# gRPC method paths
PUBLISH_METHOD = "/google.pubsub.v1.Publisher/Publish"


class PubsubServer:
    """Pub/Sub server using async gRPC with resilience mechanisms.

    This implementation uses grpc.aio for async gRPC communication with
    Google Cloud Pub/Sub. It supports:
    - Custom DSN for connecting to any Pub/Sub-compatible endpoint
    - Automatic Google Default Credentials when no DSN is provided
    - Exponential backoff with jitter for retries
    - Stability-based reset of retry counters
    - Dependency injection for credentials and channel factories
    """

    DEFAULT_PUBLISH_TIMEOUT = 10

    def __init__(
        self,
        dsn: str | None = None,
        *,
        default_project: str,
        use_google_auth: bool | None = None,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        variables: Mapping[str, ServerVariable] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: ServerBindingsObject | None = None,
        channel_overrides: Mapping[str, ChannelOverride] | None = None,
        # Resilience configuration
        max_reconnect_attempts: int = 5,
        reconnect_base_delay: float = 1.0,
        reconnect_max_delay: float = 32.0,
        reconnect_jitter_factor: float = 0.25,
        stability_threshold: float = 60.0,
        # Subscriber configuration
        stream_ack_deadline_seconds: int = 300,
        # Dependency injection for testability
        credentials_provider: CredentialsProvider | None = None,
        channel_factory: ChannelFactory | None = None,
    ) -> None:
        """Initialize the Pub/Sub server.

        Args:
            dsn: Target endpoint URL (e.g., 'http://localhost:8681/v1' for emulator,
                'https://pubsub.googleapis.com' for production with custom URL).
                If None, uses Google Default Credentials with standard endpoint.
            default_project: Default GCP project ID for topics/subscriptions.
            use_google_auth: Controls authentication method:
                - None (default): Use Google auth only when DSN is not provided.
                - True: Always use Google Default Credentials.
                - False: Never use Google auth (insecure credentials).
                Has no effect if credentials_provider is explicitly set.
            title: AsyncAPI server title.
            summary: AsyncAPI server summary.
            description: AsyncAPI server description.
            variables: AsyncAPI server variables.
            security: AsyncAPI security requirements.
            tags: AsyncAPI server tags.
            external_docs: AsyncAPI external documentation link.
            bindings: AsyncAPI server bindings.
            channel_overrides: Per-channel configuration overrides.
            max_reconnect_attempts: Maximum retry/reconnection attempts.
            reconnect_base_delay: Initial retry delay in seconds.
            reconnect_max_delay: Maximum retry delay in seconds.
            reconnect_jitter_factor: Jitter factor (0.0-1.0) for retry delays.
            stability_threshold: Seconds of stability before resetting retry counter.
            stream_ack_deadline_seconds: Ack deadline for StreamingPull.
            max_outstanding_messages: Flow control for outstanding messages.
            max_outstanding_bytes: Flow control for outstanding bytes (0=unlimited).
            credentials_provider: Custom credentials provider for auth. Overrides
                automatic credential selection.
            channel_factory: Custom gRPC channel factory.
        """
        self._dsn = dsn
        self._default_project = default_project
        self._channel_overrides: dict[str, ChannelOverride] = dict(channel_overrides or {})

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

        # Resilience configuration
        self._resilience_config = ResilienceConfig(
            max_attempts=max_reconnect_attempts,
            base_delay=reconnect_base_delay,
            max_delay=reconnect_max_delay,
            jitter_factor=reconnect_jitter_factor,
            stability_threshold=stability_threshold,
        )
        self._resilience_state = ResilienceState(self._resilience_config)

        # Subscriber configuration
        self._stream_ack_deadline_seconds = stream_ack_deadline_seconds

        # Determine credentials provider
        # Priority: explicit credentials_provider > use_google_auth flag > DSN-based detection
        if credentials_provider is not None:
            self._credentials_provider = credentials_provider
        elif use_google_auth is True:
            # Explicitly enabled - always use Google auth
            self._credentials_provider = GoogleDefaultCredentials()
        elif use_google_auth is False:
            # Explicitly disabled - never use Google auth
            self._credentials_provider = InsecureCredentials()
        elif dsn is None:
            # Default behavior: no DSN means use Google auth
            self._credentials_provider = GoogleDefaultCredentials()
        else:
            # Default behavior: DSN provided means use insecure
            self._credentials_provider = InsecureCredentials()

        # Set up channel factory
        if channel_factory is not None:
            self._channel_factory = channel_factory
        else:
            self._channel_factory = GrpcChannelFactory(
                dsn=dsn,
                credentials_provider=self._credentials_provider,
            )

        # Parse target for AsyncAPI info
        target = self._channel_factory.target
        if ":" in target:
            self._host = target
            self._pathname = None
        else:
            self._host = f"{target}:443"
            self._pathname = None

        # Connection state
        self._channel: grpc.aio.Channel | None = None
        self._active_subscribers: list[PubsubSubscriber] = []
        self._client_id = str(uuid.uuid4())

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

    @property
    def resilience_config(self) -> ResilienceConfig:
        """Get the resilience configuration."""
        return self._resilience_config

    @property
    def resilience_state(self) -> ResilienceState:
        """Get the shared resilience state."""
        return self._resilience_state

    # Connection lifecycle management
    @property
    def is_connected(self) -> bool:
        return self._channel is not None

    async def connect(self) -> None:
        """Establish connection to Pub/Sub."""
        if self.is_connected:
            return

        # Ensure credentials are valid
        await self._credentials_provider.ensure_valid()

        # Create gRPC channel
        self._channel = await self._channel_factory.create()
        logger.debug("Connected to Pub/Sub via gRPC.")

    async def disconnect(self) -> None:
        """Close connection and clean up resources."""
        # Close all active subscribers
        while self._active_subscribers:
            subscriber = self._active_subscribers.pop()
            with suppress(Exception):
                await subscriber.close()

        # Close gRPC channel
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

        logger.debug("Disconnected from Pub/Sub.")

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[PubsubServer, None]:
        """Context manager for connection lifecycle."""
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
        """Publish a message to a Pub/Sub topic.

        Args:
            channel: The topic to publish to.
            message: The message to publish.
            server_specific_parameters: PubSub-specific options:
                topic: Override topic name
                project: Override GCP project ID
                timeout: Publish timeout in seconds (int, float, or timedelta)
                ordering_key: Message ordering key (string)
                attributes: Additional message attributes (dict)

        Raises:
            ConnectionError: If not connected.
            ReconnectionExhaustedError: If all retry attempts fail.
        """
        if not self.is_connected or self._channel is None:
            raise ConnectionError("PubSub server is not connected.")

        topic_path = self._resolve_topic_path(channel, server_specific_parameters)
        timeout = self._extract_timeout(server_specific_parameters)
        ordering_key = self._extract_ordering_key(server_specific_parameters)
        attributes = self._build_attributes(message, server_specific_parameters)

        # Build protobuf message
        pubsub_message = PubsubMessage(
            data=message.payload,
            attributes=attributes or {},
            ordering_key=ordering_key or "",
        )
        request = PublishRequest(
            topic=topic_path,
            messages=[pubsub_message],
        )

        logger.debug(
            "Publishing message to PubSub topic '{topic}'.",
            extra={"topic": topic_path, "channel": channel},
        )

        async def _do_publish() -> None:
            # Ensure credentials are still valid
            await self._credentials_provider.ensure_valid()

            # Make the gRPC call
            publish_call = self._channel.unary_unary(  # type: ignore[var-annotated, union-attr]
                PUBLISH_METHOD,
                request_serializer=lambda req: req.serialize(),
                response_deserializer=PublishResponse.deserialize,
            )
            await publish_call(request, timeout=timeout)

        await with_retry(
            self._resilience_state,
            _do_publish,
            operation_name="publish",
        )

    # Message receiving
    async def subscribe(
        self,
        *,
        channels_to_callbacks: dict[str, Callable[[ReceivedMessageT], Coroutine[None, None, None]]],
        concurrency_limit: int | None = None,
    ) -> SubscriberT:
        """Subscribe to Pub/Sub channels.

        Args:
            channels_to_callbacks: Mapping of channel names to callback functions.
            concurrency_limit: Maximum concurrent message processing (None=unlimited).

        Returns:
            A subscriber that can be used to manage the subscription.

        Raises:
            ConnectionError: If not connected.
        """
        if not self.is_connected or self._channel is None:
            raise ConnectionError("PubSub server is not connected.")

        logger.debug(
            "Subscribing to PubSub channels '{channels}'.",
            extra={"channels": list(channels_to_callbacks.keys())},
        )

        channel_configs: list[ChannelConfig] = []
        for channel, callback in channels_to_callbacks.items():
            subscription_path = self._resolve_subscription_path(channel)
            channel_configs.append(
                ChannelConfig(
                    channel=channel,
                    subscription_path=subscription_path,
                    callback=callback,
                ),
            )

        subscriber = await PubsubSubscriber.create(
            channel=self._channel,
            channel_configs=channel_configs,
            credentials_provider=self._credentials_provider,
            resilience_state=self._resilience_state,
            stream_ack_deadline_seconds=self._stream_ack_deadline_seconds,
            client_id=self._client_id,
            concurrency_limit=concurrency_limit,
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
