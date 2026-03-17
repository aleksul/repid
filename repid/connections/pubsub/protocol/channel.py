"""gRPC channel creation utilities for Pub/Sub connections.

This module provides functions for creating gRPC channels with
support for custom endpoints and credential providers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlparse

import grpc.aio

from .credentials import GoogleDefaultCredentials

if TYPE_CHECKING:
    from .credentials import CredentialsProvider


# Default Pub/Sub gRPC endpoint
DEFAULT_PUBSUB_TARGET = "pubsub.googleapis.com:443"


def parse_dsn(dsn: str | None) -> str:
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


async def create_channel(
    dsn: str | None = None,
    credentials_provider: CredentialsProvider | None = None,
    options: list[tuple[str, str | int]] | None = None,
) -> grpc.aio.Channel:
    """Create a new gRPC channel.

    Creates an insecure channel if credentials are not secure,
    or a secure channel with call credentials otherwise.

    Args:
        dsn: The endpoint URL (e.g., 'http://localhost:8681/v1' or
            'https://pubsub.googleapis.com'). If None, uses the default
            Google Pub/Sub endpoint.
        credentials_provider: Provider for authentication credentials.
            Determines whether secure or insecure channel is created.
        options: Additional gRPC channel options.

    Returns:
        A configured gRPC async channel.
    """
    target = parse_dsn(dsn)

    # Check if we should use secure connection
    is_secure = True
    if credentials_provider is not None:
        is_secure = credentials_provider.is_secure

    if not is_secure:
        return grpc.aio.insecure_channel(
            target,
            options=options,
        )

    # Ensure we have a credentials provider for secure connections
    if credentials_provider is None:
        credentials_provider = GoogleDefaultCredentials()

    # Ensure credentials are valid before creating channel
    await credentials_provider.ensure_valid()

    # Create a metadata plugin class for authentication
    provider = credentials_provider

    class AuthMetadataPlugin(grpc.AuthMetadataPlugin):
        """Plugin to add authorization header to gRPC calls."""

        def __call__(
            self,
            context: grpc.AuthMetadataContext,  # noqa: ARG002
            callback: grpc.AuthMetadataPluginCallback,
        ) -> None:
            token = provider.get_token()
            if token:
                callback((("authorization", f"Bearer {token}"),), None)
            else:
                callback((), None)

    auth_plugin = grpc.metadata_call_credentials(AuthMetadataPlugin())

    # Combine SSL and call credentials
    ssl_creds = grpc.ssl_channel_credentials()
    composite_creds = grpc.composite_channel_credentials(ssl_creds, auth_plugin)

    return grpc.aio.secure_channel(
        target,
        composite_creds,
        options=options,
    )
