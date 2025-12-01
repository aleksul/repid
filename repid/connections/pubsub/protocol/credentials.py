"""Credentials management for Pub/Sub gRPC authentication.

This module provides abstractions for credential providers, enabling
testability and flexibility in authentication approaches.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

import google.auth
import google.auth.transport.requests

if TYPE_CHECKING:
    import google.auth.credentials


@runtime_checkable
class CredentialsProvider(Protocol):
    """Protocol for credential providers.

    Implementations must provide token management for gRPC authentication.
    """

    @property
    def is_secure(self) -> bool:
        """Whether this provider uses secure (authenticated) credentials."""
        ...

    async def ensure_valid(self) -> None:
        """Ensure credentials are valid, refreshing if necessary.

        This should check credentials.expired and refresh via
        asyncio.to_thread if needed.
        """
        ...

    def get_token(self) -> str | None:
        """Get the current access token, or None for insecure connections."""
        ...


class GoogleDefaultCredentials:
    """Credentials provider using Google Application Default Credentials.

    Uses google.auth.default() to obtain credentials and checks
    credentials.expired before operations, refreshing via asyncio.to_thread.
    """

    SCOPES: ClassVar[list[str]] = ["https://www.googleapis.com/auth/pubsub"]

    def __init__(
        self,
        *,
        scopes: list[str] | None = None,
        credentials: google.auth.credentials.Credentials | None = None,
    ) -> None:
        """Initialize with optional custom scopes or pre-existing credentials.

        Args:
            scopes: OAuth scopes for the credentials. Defaults to Pub/Sub scope.
            credentials: Pre-existing credentials to use. If None, will use
                google.auth.default() on first access.
        """
        self._scopes = scopes or self.SCOPES
        self._credentials: google.auth.credentials.Credentials | None = credentials
        self._initialized = credentials is not None

    @property
    def is_secure(self) -> bool:
        """Always True for Google credentials."""
        return True

    def _get_default(self) -> google.auth.credentials.Credentials:
        creds, _ = google.auth.default(scopes=self._scopes)
        return creds  # type: ignore[no-any-return]

    async def _ensure_initialized(self) -> None:
        """Initialize credentials if not already done."""
        if self._initialized:
            return

        self._credentials = await asyncio.to_thread(self._get_default)
        self._initialized = True

    def _refresh(self) -> None:
        if self._credentials is not None:
            request = google.auth.transport.requests.Request()
            self._credentials.refresh(request)

    async def ensure_valid(self) -> None:
        """Check if credentials are expired and refresh if needed."""
        await self._ensure_initialized()

        if self._credentials is None:
            raise RuntimeError("Credentials not initialized")

        if self._credentials.expired or not self._credentials.valid:
            await asyncio.to_thread(self._refresh)

    def get_token(self) -> str | None:
        """Get the current access token."""
        if self._credentials is None:
            return None
        return self._credentials.token


class InsecureCredentials:
    """Credentials provider for insecure connections (e.g., emulator).

    Use this when connecting to the Pub/Sub emulator which doesn't
    require authentication.
    """

    @property
    def is_secure(self) -> bool:
        """Always False for insecure connections."""
        return False

    async def ensure_valid(self) -> None:
        """No-op for insecure credentials."""

    def get_token(self) -> str | None:
        """Always returns None for insecure connections."""
        return None


class StaticTokenCredentials:
    """Credentials provider with a static token for testing.

    Useful for unit tests where you want to provide a known token
    without actual Google authentication.
    """

    def __init__(self, token: str) -> None:
        """Initialize with a static token.

        Args:
            token: The static token to return.
        """
        self._token = token

    @property
    def is_secure(self) -> bool:
        """Always True as we're providing a token."""
        return True

    async def ensure_valid(self) -> None:
        """No-op for static credentials."""

    def get_token(self) -> str | None:
        """Return the static token."""
        return self._token
