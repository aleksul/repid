"""
AMQP Managed Resources - High-level session and link management with auto-reconnection.

This module provides:
- ManagedSession: A session that auto-recreates on reconnection
- SenderPool: A pool of sender links with auto-recreation
- ReceiverPool: A pool of receiver links with auto-recreation

These abstractions hide the complexity of session/link lifecycle and reconnection
from the message broker, providing a clean, high-level API.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .events import ConnectionEvent
from .links import LinkError, ReceiverLink, SenderLink

if TYPE_CHECKING:
    from .connection import AmqpConnection
    from .session import Session

logger = logging.getLogger(__name__)


class ManagedSessionError(Exception):
    """Raised when a managed session operation fails."""


class ManagedSession:
    """
    A managed session that auto-recreates on connection loss.

    This provides a stable session reference that automatically handles:
    - Session creation on first access
    - Session recreation after reconnection
    - Tracking of links for recreation

    Example:
        managed = ManagedSession(connection)
        session = await managed.get_session()  # Always returns a valid session
    """

    def __init__(self, connection: AmqpConnection) -> None:
        """
        Initialize managed session.

        Args:
            connection: The AMQP connection to use
        """
        self._connection = connection
        self._session: Session | None = None
        self._lock = asyncio.Lock()

        # Track sender and receiver pools
        self._sender_pool: SenderPool | None = None
        self._receiver_pool: ReceiverPool | None = None

        # Register for reconnection events
        self._connection.events.on(ConnectionEvent.RECONNECTED, self._on_reconnected)
        self._connection.events.on(ConnectionEvent.DISCONNECTED, self._on_disconnected)

    async def get_session(self) -> Session:
        """
        Get a valid session, creating one if needed.

        This method is safe to call concurrently - it uses a lock to prevent
        multiple sessions from being created.

        Returns:
            A valid, usable Session

        Raises:
            ConnectionError: If not connected
            ManagedSessionError: If session creation fails
        """
        async with self._lock:
            if self._session is not None and self._session.is_usable:
                return self._session

            if not self._connection.is_connected:
                raise ConnectionError("Not connected to AMQP server")

            # Create new session
            try:
                self._session = await self._connection.create_session()
                return self._session
            except Exception as e:
                raise ManagedSessionError(f"Failed to create session: {e}") from e

    @property
    def sender_pool(self) -> SenderPool:
        """Get the sender pool for this managed session."""
        if self._sender_pool is None:
            self._sender_pool = SenderPool(self)
        return self._sender_pool

    @property
    def receiver_pool(self) -> ReceiverPool:
        """Get the receiver pool for this managed session."""
        if self._receiver_pool is None:
            self._receiver_pool = ReceiverPool(self)
        return self._receiver_pool

    def invalidate(self) -> None:
        """Invalidate the current session."""
        self._session = None
        if self._sender_pool:
            self._sender_pool.invalidate()
        if self._receiver_pool:
            self._receiver_pool.invalidate()

    async def _on_reconnected(self, _data: Any = None) -> None:
        """Handle reconnection event."""
        logger.debug(
            "ManagedSession: connection reconnected, session will be recreated on next use",
        )
        # Session will be recreated on next get_session() call
        # Pools will recreate links on next get() call

    async def _on_disconnected(self, _data: Any = None) -> None:
        """Handle disconnection event."""
        self.invalidate()

    async def close(self) -> None:
        """Close the managed session and release resources."""
        self._connection.events.off(ConnectionEvent.RECONNECTED, self._on_reconnected)
        self._connection.events.off(ConnectionEvent.DISCONNECTED, self._on_disconnected)

        if self._sender_pool:
            await self._sender_pool.close()
        if self._receiver_pool:
            await self._receiver_pool.close()

        if self._session:
            with contextlib.suppress(Exception):
                await self._session.end()
            self._session = None


class SenderPool:
    """
    A pool of sender links that auto-recreates links on reconnection.

    This provides a stable way to get sender links without worrying about
    connection state or link lifecycle.

    Example:
        pool = SenderPool(managed_session)
        link = await pool.get("/queues/my-queue")  # Always returns a valid link
        await link.send(b"Hello")
    """

    def __init__(self, managed_session: ManagedSession) -> None:
        """
        Initialize sender pool.

        Args:
            managed_session: The managed session to use
        """
        self._managed_session = managed_session
        self._links: dict[str, SenderLink] = {}
        self._link_names: dict[str, str] = {}  # address -> link name
        self._lock = asyncio.Lock()

    async def get(self, address: str, name: str | None = None) -> SenderLink:
        """
        Get a sender link for the given address, creating one if needed.

        Args:
            address: The target address
            name: Optional link name (auto-generated if not provided)

        Returns:
            A valid, usable SenderLink
        """
        async with self._lock:
            # Check if we have a usable link
            if address in self._links:
                link = self._links[address]
                if link.is_usable:
                    return link
                # Link is stale, remove it
                del self._links[address]

            # Get or create session
            session = await self._managed_session.get_session()

            # Create new link
            link_name = (
                name or self._link_names.get(address) or f"sender-{address}-{len(self._links)}"
            )
            self._link_names[address] = link_name

            link = await session.create_sender(address, link_name)
            self._links[address] = link
            return link

    async def send(
        self,
        address: str,
        payload: bytes,
        *,
        headers: dict[str, Any] | None = None,
        name: str | None = None,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> None:
        """
        Send a message to the given address with automatic retry.

        This is a convenience method that handles link acquisition and retry logic.

        Args:
            address: Target address
            payload: Message payload
            headers: Optional message headers
            name: Optional link name
            max_retries: Maximum retry attempts
            **kwargs: Additional arguments passed to link.send()
        """
        for attempt in range(max_retries):
            try:
                link = await self.get(address, name)
                await link.send(payload, headers=headers, **kwargs)
                return

            except LinkError as e:
                logger.warning(
                    "SenderPool: link error during send (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    e,
                )
                # Invalidate the stale link
                if address in self._links:
                    del self._links[address]

                if attempt < max_retries - 1:
                    # Wait for connection to be ready
                    await self._wait_for_connection(timeout=5.0)
                else:
                    raise

    async def _wait_for_connection(self, timeout: float = 5.0) -> None:
        """Wait for the connection to be available."""
        connection = self._managed_session._connection
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < timeout:
            if connection.is_connected:
                return
            await asyncio.sleep(0.1)
        raise ConnectionError("Connection not available after wait")

    def invalidate(self) -> None:
        """Invalidate all links in the pool."""
        self._links.clear()

    async def close(self) -> None:
        """Close all links in the pool."""
        for link in list(self._links.values()):
            with contextlib.suppress(Exception):
                await link.detach()
        self._links.clear()


class ReceiverPool:
    """
    A pool of receiver links that auto-recreates links on reconnection.

    Unlike SenderPool, ReceiverPool also manages callback registration and
    automatically re-subscribes after reconnection.

    Example:
        pool = ReceiverPool(managed_session)

        async def on_message(payload, headers, delivery_id, delivery_tag, link):
            print(f"Received: {payload}")

        link = await pool.subscribe("/queues/my-queue", on_message)
    """

    def __init__(self, managed_session: ManagedSession) -> None:
        """
        Initialize receiver pool.

        Args:
            managed_session: The managed session to use
        """
        self._managed_session = managed_session
        self._links: dict[str, ReceiverLink] = {}
        self._callbacks: dict[
            str,
            Callable[[bytes, dict[str, Any] | None, int, bytes, ReceiverLink], Any],
        ] = {}
        self._link_names: dict[str, str] = {}  # address -> link name
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

        # Register for reconnection to re-subscribe
        self._managed_session._connection.events.on(
            ConnectionEvent.RECONNECTED,
            self._on_reconnected,
        )

    async def subscribe(
        self,
        address: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, ReceiverLink], Any],
        name: str | None = None,
    ) -> ReceiverLink:
        """
        Subscribe to messages from the given address.

        Args:
            address: Source address
            callback: Function called when message received
            name: Optional link name

        Returns:
            A ReceiverLink for the subscription
        """
        async with self._lock:
            # Store callback for re-subscription
            self._callbacks[address] = callback
            link_name = name or f"receiver-{address}-{len(self._links)}"
            self._link_names[address] = link_name

            return await self._create_link(address, callback, link_name)

    async def _create_link(
        self,
        address: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, ReceiverLink], Any],
        name: str,
    ) -> ReceiverLink:
        """Create a new receiver link."""
        session = await self._managed_session.get_session()
        link = await session.create_receiver(address, callback, name)
        self._links[address] = link
        return link

    async def _on_reconnected(self, _data: Any = None) -> None:
        """Handle reconnection by re-subscribing all receivers."""
        if self._stop_event.is_set():
            return

        async with self._lock:
            logger.debug(
                "ReceiverPool: reconnected, re-subscribing %d receivers",
                len(self._callbacks),
            )

            # Clear old links
            self._links.clear()

            # Re-create all subscriptions
            for address, callback in self._callbacks.items():
                try:
                    link_name = self._link_names.get(address, f"receiver-{address}")
                    await self._create_link(address, callback, link_name)
                    logger.debug("ReceiverPool: re-subscribed to %s", address)
                except Exception:
                    logger.exception("ReceiverPool: failed to re-subscribe to %s", address)

    def invalidate(self) -> None:
        """Invalidate all links in the pool."""
        self._links.clear()

    async def unsubscribe(self, address: str) -> None:
        """Unsubscribe from the given address."""
        async with self._lock:
            if address in self._callbacks:
                del self._callbacks[address]
            if address in self._link_names:
                del self._link_names[address]
            if address in self._links:
                link = self._links.pop(address)
                with contextlib.suppress(Exception):
                    await link.detach()

    async def close(self) -> None:
        """Close all subscriptions."""
        self._stop_event.set()
        self._managed_session._connection.events.off(
            ConnectionEvent.RECONNECTED,
            self._on_reconnected,
        )

        for link in list(self._links.values()):
            with contextlib.suppress(Exception):
                await link.detach()

        self._links.clear()
        self._callbacks.clear()
