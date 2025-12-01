"""gRPC channel factory for Pub/Sub connections.

This module provides abstractions for creating gRPC channels with
support for custom endpoints and credential providers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable
from urllib.parse import urlparse

import grpc.aio

from .credentials import GoogleDefaultCredentials

if TYPE_CHECKING:
    from .credentials import CredentialsProvider


# Default Pub/Sub gRPC endpoint
DEFAULT_PUBSUB_TARGET = "pubsub.googleapis.com:443"


@runtime_checkable
class ChannelFactory(Protocol):
    """Protocol for gRPC channel factories."""

    @property
    def target(self) -> str:
        """The target endpoint for the channel."""
        ...

    @property
    def is_secure(self) -> bool:
        """Whether this factory creates secure channels."""
        ...

    async def create(self) -> grpc.aio.Channel:
        """Create a new gRPC channel."""
        ...


class GrpcChannelFactory:
    """Factory for creating gRPC channels.

    Supports both secure (with credentials) and insecure connections
    based on the credentials provider's is_secure property.
    """

    def __init__(
        self,
        *,
        dsn: str | None = None,
        credentials_provider: CredentialsProvider | None = None,
        options: list[tuple[str, str | int]] | None = None,
    ) -> None:
        """Initialize the channel factory.

        Args:
            dsn: The endpoint URL (e.g., 'http://localhost:8681/v1' or
                'https://pubsub.googleapis.com'). If None, uses the default
                Google Pub/Sub endpoint.
            credentials_provider: Provider for authentication credentials.
                Determines whether secure or insecure channel is created.
            options: Additional gRPC channel options.
        """
        self._target = self._parse_dsn(dsn)
        self._credentials_provider = credentials_provider
        self._options = options or []
        self._channel: grpc.aio.Channel | None = None

    @staticmethod
    def _parse_dsn(dsn: str | None) -> str:
        """Parse DSN URL into gRPC target.

        Args:
            dsn: URL like 'http://localhost:8681/v1' or 'https://pubsub.googleapis.com'

        Returns:
            gRPC target string like 'localhost:8681' or 'pubsub.googleapis.com:443'
        """
        if dsn is None:
            return DEFAULT_PUBSUB_TARGET

        parsed = urlparse(dsn)

        # Get host and port
        host = parsed.hostname or parsed.path
        if not host:
            return DEFAULT_PUBSUB_TARGET

        port = parsed.port
        if port is None:
            # Default ports based on scheme
            if parsed.scheme == "https":
                port = 443
            elif parsed.scheme == "http":
                port = 80
            else:
                port = 443

        return f"{host}:{port}"

    @property
    def target(self) -> str:
        """The target endpoint for the channel."""
        return self._target

    @property
    def is_secure(self) -> bool:
        """Whether this factory creates secure channels."""
        if self._credentials_provider is None:
            return True  # Default to secure
        return self._credentials_provider.is_secure

    async def create(self) -> grpc.aio.Channel:
        """Create a new gRPC channel.

        Creates an insecure channel if credentials are not secure,
        or a secure channel with call credentials otherwise.

        Returns:
            A configured gRPC async channel.
        """
        if not self.is_secure:
            return grpc.aio.insecure_channel(
                self._target,
                options=self._options or None,
            )

        # Ensure we have a credentials provider for secure connections
        if self._credentials_provider is None:
            self._credentials_provider = GoogleDefaultCredentials()

        # Ensure credentials are valid before creating channel
        await self._credentials_provider.ensure_valid()

        # Create a metadata plugin class for authentication
        credentials_provider = self._credentials_provider

        class AuthMetadataPlugin(grpc.AuthMetadataPlugin):
            """Plugin to add authorization header to gRPC calls."""

            def __call__(
                self,
                context: grpc.AuthMetadataContext,  # noqa: ARG002
                callback: grpc.AuthMetadataPluginCallback,
            ) -> None:
                token = credentials_provider.get_token()
                if token:
                    callback((("authorization", f"Bearer {token}"),), None)
                else:
                    callback((), None)

        auth_plugin = grpc.metadata_call_credentials(AuthMetadataPlugin())

        # Combine SSL and call credentials
        ssl_creds = grpc.ssl_channel_credentials()
        composite_creds = grpc.composite_channel_credentials(ssl_creds, auth_plugin)

        return grpc.aio.secure_channel(
            self._target,
            composite_creds,
            options=self._options or None,
        )

    async def close(self) -> None:
        """Close the channel if it exists."""
        if self._channel is not None:
            await self._channel.close()
            self._channel = None


class InsecureChannelFactory:
    """Factory for creating insecure gRPC channels.

    Useful for testing or connecting to local services.
    """

    def __init__(
        self,
        target: str,
        *,
        options: list[tuple[str, str | int]] | None = None,
    ) -> None:
        """Initialize the insecure channel factory.

        Args:
            target: The gRPC target endpoint.
            options: Additional gRPC channel options.
        """
        self._target = target
        self._options = options or []

    @property
    def target(self) -> str:
        """The target endpoint for the channel."""
        return self._target

    @property
    def is_secure(self) -> bool:
        """Always False for insecure channels."""
        return False

    async def create(self) -> grpc.aio.Channel:
        """Create a new insecure gRPC channel."""
        return grpc.aio.insecure_channel(
            self._target,
            options=self._options or None,
        )
