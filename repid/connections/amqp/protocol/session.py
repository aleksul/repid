"""
AMQP Session - Session management with state machine.

This module provides:
- Session lifecycle management
- Link management within sessions
- Flow control
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from repid.connections.amqp._uamqp.performatives import (
    AttachFrame,
    BeginFrame,
    DetachFrame,
    DispositionFrame,
    EndFrame,
    FlowFrame,
    TransferFrame,
)

from .states import SessionState, SessionStateMachine

if TYPE_CHECKING:
    from .connection import AmqpConnection
    from .links import ReceiverLink, SenderLink

logger = logging.getLogger(__name__)


class SessionError(Exception):
    """Raised when a session operation fails."""


class Session:
    """
    AMQP 1.0 Session with state machine management.

    A session multiplexes multiple links over a single connection channel.
    It maintains flow control windows and manages link lifecycles.

    Example:
        session = await connection.create_session()
        sender = await session.create_sender("my-queue")
        await sender.send(b"Hello, World!")
    """

    def __init__(self, connection: AmqpConnection, channel: int) -> None:
        """
        Initialize session.

        Args:
            connection: Parent connection
            channel: Channel number for this session
        """
        self._connection = connection
        self._channel = channel

        # State machine
        self._state_machine = SessionStateMachine()

        # Links
        self._links: dict[str, SenderLink | ReceiverLink] = {}  # by name
        self._links_by_handle: dict[int, SenderLink | ReceiverLink] = {}  # by local handle
        self._links_by_remote_handle: dict[int, SenderLink | ReceiverLink] = {}  # by remote handle
        self._next_handle = 0

        # Flow control
        self._next_outgoing_id = 0
        self._next_incoming_id = 0
        self._incoming_window = 100
        self._outgoing_window = 100

        # Remote flow control
        self._remote_incoming_window = 0
        self._remote_outgoing_window = 0

        # Ready event (set when BEGIN response received)
        self._ready = asyncio.Event()

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def connection(self) -> AmqpConnection:
        """Get parent connection."""
        return self._connection

    @property
    def channel(self) -> int:
        """Get channel number."""
        return self._channel

    @property
    def state(self) -> SessionState:
        """Get current session state."""
        return self._state_machine.state

    @property
    def is_mapped(self) -> bool:
        """Check if session is in MAPPED state."""
        return self._state_machine.is_mapped()

    @property
    def is_usable(self) -> bool:
        """Check if session can be used for operations."""
        return self._state_machine.is_usable()

    # -------------------------------------------------------------------------
    # Session Lifecycle
    # -------------------------------------------------------------------------

    async def begin(self) -> None:
        """
        Begin the session by sending BEGIN frame.

        This is called automatically by connection.create_session().
        """
        begin_frame = BeginFrame(
            next_outgoing_id=self._next_outgoing_id,
            incoming_window=self._incoming_window,
            outgoing_window=self._outgoing_window,
        )

        await self._connection.send_performative(self._channel, begin_frame)
        await self._state_machine.transition("send_begin")

        logger.debug("Session %d: BEGIN sent", self._channel)

    async def wait_ready(self, timeout: float = 10.0) -> None:
        """
        Wait for session to be ready (BEGIN response received).

        Args:
            timeout: Maximum time to wait

        Raises:
            asyncio.TimeoutError: If timeout expires
        """
        await asyncio.wait_for(self._ready.wait(), timeout)

    def invalidate(self) -> None:
        """
        Invalidate the session due to connection loss.

        This resets the session state to UNMAPPED and invalidates all links.
        The session cannot be reused after this - a new session must be created.
        """
        # Reset session state to UNMAPPED (not usable)
        self._state_machine.reset(SessionState.UNMAPPED)
        self._ready.clear()

        # Invalidate all links
        for link in self._links.values():
            link.invalidate()

        logger.debug("Session %d: invalidated due to connection loss", self._channel)

    async def end(self, _error: Exception | None = None) -> None:
        """
        End the session gracefully.

        Args:
            _error: Optional error that caused the end (reserved for future use)
        """
        if self._state_machine.is_terminal():
            return

        # Detach all links first
        for link in list(self._links.values()):
            try:
                await link.detach()
            except (OSError, asyncio.TimeoutError):
                logger.debug("Error detaching link %s", link.name, exc_info=True)

        # Send END if possible
        if self._state_machine.is_usable():
            try:
                end_frame = EndFrame()
                await self._connection.send_performative(self._channel, end_frame)
                await self._state_machine.transition("send_end")
            except (OSError, asyncio.TimeoutError):
                logger.debug("Error sending END frame", exc_info=True)

        # Remove from connection
        self._connection._remove_session(self._channel)

        logger.debug("Session %d: ended", self._channel)

    # -------------------------------------------------------------------------
    # Frame Handling
    # -------------------------------------------------------------------------

    async def handle_performative(self, performative: Any) -> None:
        """
        Handle an incoming performative for this session.

        Args:
            performative: The performative to handle
        """
        logger.debug(
            "Session %d handling: %s",
            self._channel,
            type(performative).__name__,
        )

        if isinstance(performative, BeginFrame):
            await self._handle_begin(performative)
        elif isinstance(performative, EndFrame):
            await self._handle_end(performative)
        elif isinstance(performative, AttachFrame):
            await self._handle_attach(performative)
        elif isinstance(performative, DetachFrame):
            await self._handle_detach(performative)
        elif isinstance(performative, FlowFrame):
            await self._handle_flow(performative)
        elif isinstance(performative, TransferFrame):
            await self._handle_transfer(performative)
        elif isinstance(performative, DispositionFrame):
            await self._handle_disposition(performative)
        else:
            logger.warning(
                "Session %d: unhandled performative %s",
                self._channel,
                type(performative).__name__,
            )

    async def _handle_begin(self, begin: BeginFrame) -> None:
        """Handle BEGIN frame (session mapped)."""
        self._remote_incoming_window = begin.incoming_window or 0
        self._remote_outgoing_window = begin.outgoing_window or 0

        await self._state_machine.transition("recv_begin")
        self._ready.set()

        logger.debug(
            "Session %d: MAPPED (remote_incoming=%d, remote_outgoing=%d)",
            self._channel,
            self._remote_incoming_window,
            self._remote_outgoing_window,
        )

    async def _handle_end(self, _end: EndFrame) -> None:
        """Handle END frame."""
        logger.debug("Session %d: received END", self._channel)

        with contextlib.suppress(ValueError):
            await self._state_machine.transition("recv_end")

        # Send END response if we haven't already
        if self._state_machine.state == SessionState.END_RCVD:
            with contextlib.suppress(OSError, asyncio.TimeoutError, ValueError):
                await self._connection.send_performative(self._channel, EndFrame())
                await self._state_machine.transition("send_end")

        self._connection._remove_session(self._channel)

    async def _handle_attach(self, attach: AttachFrame) -> None:
        """Handle ATTACH frame (link attached)."""
        name = attach.name
        if isinstance(name, bytes):
            name = name.decode()

        remote_handle = attach.handle
        if remote_handle is None:
            logger.error("Session %d: ATTACH missing handle", self._channel)
            return

        if name in self._links:
            link = self._links[name]
            link._remote_handle = remote_handle
            self._links_by_remote_handle[remote_handle] = link
            await link._handle_attach(attach)
        else:
            logger.warning(
                "Session %d: ATTACH for unknown link %s",
                self._channel,
                name,
            )

    async def _handle_detach(self, detach: DetachFrame) -> None:
        """Handle DETACH frame."""
        handle = detach.handle
        if handle is None:
            return

        if handle in self._links_by_remote_handle:
            link = self._links_by_remote_handle[handle]
            await link._handle_detach(detach)
        else:
            logger.warning(
                "Session %d: DETACH for unknown handle %d",
                self._channel,
                handle,
            )

    async def _handle_flow(self, flow: FlowFrame) -> None:
        """Handle FLOW frame."""
        # Update session-level flow control
        if flow.next_incoming_id is not None:
            self._remote_incoming_window = (
                (flow.next_incoming_id or 0) + (flow.incoming_window or 0) - self._next_outgoing_id
            )

        # Dispatch to link if handle specified
        if flow.handle is not None and flow.handle in self._links_by_remote_handle:
            link = self._links_by_remote_handle[flow.handle]
            await link._handle_flow(flow)

    async def _handle_transfer(self, transfer: TransferFrame) -> None:
        """Handle TRANSFER frame (incoming message)."""
        handle = transfer.handle
        if handle is None:
            logger.error("Session %d: TRANSFER missing handle", self._channel)
            return

        if handle in self._links_by_remote_handle:
            link = self._links_by_remote_handle[handle]
            # Import here to avoid circular import
            from .links import ReceiverLink  # noqa: PLC0415

            if isinstance(link, ReceiverLink):
                await link._handle_transfer(transfer)
            else:
                logger.warning(
                    "Session %d: TRANSFER on sender link %d",
                    self._channel,
                    handle,
                )
        else:
            logger.warning(
                "Session %d: TRANSFER for unknown handle %d. Known handles: %s",
                self._channel,
                handle,
                list(self._links_by_remote_handle.keys()),
            )

    async def _handle_disposition(self, disposition: DispositionFrame) -> None:
        """Handle DISPOSITION frame."""
        # For now, just log
        logger.debug(
            "Session %d: DISPOSITION first=%s last=%s settled=%s",
            self._channel,
            disposition.first,
            disposition.last,
            disposition.settled,
        )

    # -------------------------------------------------------------------------
    # Link Management
    # -------------------------------------------------------------------------

    def _allocate_handle(self) -> int:
        """Allocate a new link handle."""
        handle = self._next_handle
        self._next_handle += 1
        return handle

    async def create_sender(
        self,
        address: str,
        name: str | None = None,
    ) -> SenderLink:
        """
        Create a sender link.

        Args:
            address: Target address (queue/topic name)
            name: Link name (auto-generated if not provided)

        Returns:
            A new SenderLink

        Raises:
            SessionError: If session is not usable
        """
        if not self.is_mapped:
            await self.wait_ready()

        if not self.is_usable:
            raise SessionError("Session is not usable")

        # Import here to avoid circular import
        from .links import SenderLink  # noqa: PLC0415

        if name is None:
            name = f"sender-{address}-{len(self._links)}"

        handle = self._allocate_handle()
        link = SenderLink(self, name, address, handle)
        self._links[name] = link
        self._links_by_handle[handle] = link

        await link.attach()
        await link.wait_ready()  # Wait for ATTACH response
        return link

    async def create_receiver(
        self,
        address: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, ReceiverLink], Any],
        name: str | None = None,
    ) -> ReceiverLink:
        """
        Create a receiver link.

        Args:
            address: Source address (queue/topic name)
            callback: Function to call when message received
            name: Link name (auto-generated if not provided)

        Returns:
            A new ReceiverLink

        Raises:
            SessionError: If session is not usable
        """
        if not self.is_mapped:
            await self.wait_ready()

        if not self.is_usable:
            raise SessionError("Session is not usable")

        # Import here to avoid circular import
        from .links import ReceiverLink  # noqa: PLC0415

        if name is None:
            name = f"receiver-{address}-{len(self._links)}"

        handle = self._allocate_handle()
        link = ReceiverLink(self, name, address, handle, callback)
        self._links[name] = link
        self._links_by_handle[handle] = link

        await link.attach()
        await link.wait_ready()  # Wait for ATTACH response
        return link

    # Backward-compatible aliases
    async def create_sender_link(
        self,
        address: str,
        name: str,
    ) -> SenderLink:
        """Create a sender link (alias for create_sender)."""
        return await self.create_sender(address, name)

    async def create_receiver_link(
        self,
        address: str,
        name: str,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, ReceiverLink], Any],
    ) -> ReceiverLink:
        """Create a receiver link (alias for create_receiver)."""
        return await self.create_receiver(address, callback, name)

    def _remove_link(self, link: SenderLink | ReceiverLink) -> None:
        """Remove a link from the session."""
        if link.name in self._links:
            del self._links[link.name]
        if link._handle in self._links_by_handle:
            del self._links_by_handle[link._handle]
        if link._remote_handle is not None and link._remote_handle in self._links_by_remote_handle:
            del self._links_by_remote_handle[link._remote_handle]
