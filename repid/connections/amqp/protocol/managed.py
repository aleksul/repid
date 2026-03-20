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
    from repid.connections.amqp._uamqp.message import Properties

    from .connection import AmqpConnection
    from .session import Session

logger = logging.getLogger("repid.connections.amqp.protocol")


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
        logger.debug("managed_session.reconnected")
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
        self._link_counter = 0
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
            self._link_counter += 1
            base_name = name or self._link_names.get(address) or f"sender-{address}"
            link_name = f"{base_name}-{self._link_counter}"
            self._link_names[address] = base_name

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
                    "sender_pool.send.link_error",
                    extra={"attempt": attempt + 1, "max": max_retries, "error": str(e)},
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
        async with self._lock:
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
            Callable[
                [bytes, dict[str, Any] | None, int, bytes, ReceiverLink, Properties | None],
                Any,
            ],
        ] = {}
        self._link_names: dict[str, str] = {}  # address -> link name
        self._prefetches: dict[str, int] = {}  # address -> prefetch count
        self._link_counter = 0
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
        callback: Callable[
            [bytes, dict[str, Any] | None, int, bytes, ReceiverLink, Properties | None],
            Any,
        ],
        name: str | None = None,
        prefetch: int = 100,
    ) -> ReceiverLink:
        """
        Subscribe to messages from the given address.

        Args:
            address: Source address
            callback: Function called when message received
            name: Optional link name
            prefetch: Number of messages to pre-fetch (AMQP link credit).
                Set to the concurrency limit to apply protocol-level back-pressure.

        Returns:
            A ReceiverLink for the subscription
        """
        async with self._lock:
            # Store callback and prefetch for re-subscription
            self._callbacks[address] = callback
            self._prefetches[address] = prefetch
            self._link_counter += 1
            base_name = name or f"receiver-{address}"
            link_name = f"{base_name}-{self._link_counter}"
            self._link_names[address] = base_name

            return await self._create_link(address, callback, link_name, prefetch)

    async def _create_link(
        self,
        address: str,
        callback: Callable[
            [bytes, dict[str, Any] | None, int, bytes, ReceiverLink, Properties | None],
            Any,
        ],
        name: str,
        prefetch: int = 100,
    ) -> ReceiverLink:
        """Create a new receiver link."""
        session = await self._managed_session.get_session()
        link = await session.create_receiver(address, callback, name, prefetch=prefetch)
        self._links[address] = link
        return link

    async def _on_reconnected(self, _data: Any = None) -> None:
        """Handle reconnection by re-subscribing all receivers."""
        if self._stop_event.is_set():
            return

        async with self._lock:
            logger.debug(
                "receiver_pool.reconnected",
                extra={"count": len(self._callbacks)},
            )

            # Clear old links
            self._links.clear()

            # Re-create all subscriptions
            for address, callback in list(self._callbacks.items()):
                try:
                    self._link_counter += 1
                    base_name = self._link_names.get(address, f"receiver-{address}")
                    link_name = f"{base_name}-{self._link_counter}"
                    prefetch = self._prefetches.get(address, 100)
                    await self._create_link(address, callback, link_name, prefetch)
                    logger.debug("receiver_pool.resubscribed", extra={"address": address})
                except Exception:
                    logger.exception("receiver_pool.resubscribe.error", extra={"address": address})

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
            if address in self._prefetches:
                del self._prefetches[address]
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

        async with self._lock:
            for link in list(self._links.values()):
                with contextlib.suppress(Exception):
                    await link.detach()

            self._links.clear()
            self._callbacks.clear()
