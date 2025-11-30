"""
AMQP Links - Sender and Receiver link implementations.

This module provides:
- SenderLink for sending messages
- ReceiverLink for receiving messages
- Link lifecycle management with state machine
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from repid.connections.amqp._uamqp._decode import transfer_frames_to_message
from repid.connections.amqp._uamqp._encode import message_to_transfer_frames
from repid.connections.amqp._uamqp.endpoints import Source, Target
from repid.connections.amqp._uamqp.message import Header, Message, Properties
from repid.connections.amqp._uamqp.performatives import (
    AttachFrame,
    DetachFrame,
    FlowFrame,
    TransferFrame,
)

from .states import LinkState, LinkStateMachine

if TYPE_CHECKING:
    from .session import Session

logger = logging.getLogger(__name__)


class LinkError(Exception):
    """Raised when a link operation fails."""


# =============================================================================
# Base Link
# =============================================================================


class Link(ABC):
    """
    Base class for AMQP links.

    A link is a unidirectional route between a source and target.
    Sender links send messages, receiver links receive messages.
    """

    def __init__(
        self,
        session: Session,
        name: str,
        address: str,
        handle: int,
        role: bool,  # True = receiver, False = sender  # noqa: FBT001
    ) -> None:
        """
        Initialize link.

        Args:
            session: Parent session
            name: Link name (unique within session)
            address: Source or target address
            handle: Local handle number
            role: True for receiver, False for sender
        """
        self._session = session
        self._name = name
        self._address = address
        self._handle = handle
        self._role = role

        # State machine
        self._state_machine = LinkStateMachine()

        # Remote parameters
        self._remote_handle: int | None = None

        # Ready event (set when ATTACH response received)
        self._ready = asyncio.Event()

        # Invalidation flag
        self._invalidated = False

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def session(self) -> Session:
        """Get parent session."""
        return self._session

    @property
    def name(self) -> str:
        """Get link name."""
        return self._name

    @property
    def address(self) -> str:
        """Get link address."""
        return self._address

    @property
    def handle(self) -> int:
        """Get local handle number."""
        return self._handle

    @property
    def state(self) -> LinkState:
        """Get current link state."""
        return self._state_machine.state

    @property
    def is_attached(self) -> bool:
        """Check if link is in ATTACHED state."""
        return self._state_machine.is_attached()

    @property
    def is_usable(self) -> bool:
        """Check if link can be used for operations."""
        return self._state_machine.is_usable()

    @property
    def closed(self) -> bool:
        """Check if link is detached."""
        return self._state_machine.is_terminal()

    # -------------------------------------------------------------------------
    # Link Lifecycle
    # -------------------------------------------------------------------------

    @abstractmethod
    async def attach(self) -> None:
        """Attach the link by sending ATTACH frame."""
        raise NotImplementedError

    async def wait_ready(self, timeout: float = 10.0) -> None:
        """
        Wait for link to be ready (ATTACH response received).

        Args:
            timeout: Maximum time to wait

        Raises:
            asyncio.TimeoutError: If timeout expires
            LinkError: If link was invalidated (connection lost)
        """
        await asyncio.wait_for(self._ready.wait(), timeout)
        if self._invalidated:
            raise LinkError(f"Link {self._name} was invalidated due to connection loss")

    async def detach(self, _error: Exception | None = None) -> None:
        """
        Detach the link gracefully.

        Args:
            _error: Optional error that caused the detach (reserved for future use)
        """
        if self._state_machine.is_terminal():
            return

        # Send DETACH if possible
        if self._state_machine.is_usable():
            try:
                detach_frame = DetachFrame(handle=self._handle, closed=True)
                await self._session.connection.send_performative(
                    self._session.channel,
                    detach_frame,
                )
                await self._state_machine.transition("send_detach")
            except (OSError, asyncio.TimeoutError):
                logger.debug("Error sending DETACH frame", exc_info=True)

        # Remove from session
        self._session._remove_link(self)  # type: ignore[arg-type]

        logger.debug(
            "Link %s on session %d: detached",
            self._name,
            self._session.channel,
        )

    def invalidate(self) -> None:
        """
        Invalidate the link due to session/connection loss.

        This resets the link state to DETACHED. The link cannot be reused
        after this - a new link must be created.
        """
        self._state_machine.reset(LinkState.DETACHED)
        self._invalidated = True
        # Set the ready event to unblock any waiters (they'll check _invalidated)
        self._ready.set()

        logger.debug(
            "Link %s on session %d: invalidated due to connection loss",
            self._name,
            self._session.channel,
        )

    # -------------------------------------------------------------------------
    # Frame Handling
    # -------------------------------------------------------------------------

    async def _handle_attach(self, _attach: AttachFrame) -> None:
        """Handle ATTACH response."""
        await self._state_machine.transition("recv_attach")
        self._ready.set()

        logger.debug(
            "Link %s: ATTACHED (remote_handle=%d)",
            self._name,
            self._remote_handle,
        )

    async def _handle_detach(self, _detach: DetachFrame) -> None:
        """Handle DETACH from remote."""
        logger.debug("Link %s: received DETACH", self._name)

        with contextlib.suppress(ValueError):
            await self._state_machine.transition("recv_detach")

        # Send DETACH response if we haven't already
        if self._state_machine.state == LinkState.DETACH_RCVD:
            with contextlib.suppress(OSError, asyncio.TimeoutError, ValueError):
                await self._session.connection.send_performative(
                    self._session.channel,
                    DetachFrame(handle=self._handle, closed=True),
                )
                await self._state_machine.transition("send_detach")

        self._session._remove_link(self)  # type: ignore[arg-type]

    @abstractmethod
    async def _handle_flow(self, flow: FlowFrame) -> None:
        """Handle FLOW frame. Override in subclasses."""


# =============================================================================
# Sender Link
# =============================================================================


class SenderLink(Link):
    """
    AMQP Sender Link for sending messages.

    Example:
        sender = await session.create_sender("my-queue")
        await sender.send(b"Hello, World!")
    """

    def __init__(
        self,
        session: Session,
        name: str,
        address: str,
        handle: int,
    ) -> None:
        super().__init__(session, name, address, handle, role=False)

        # Sender-specific state
        self._delivery_count = 0
        self._link_credit = 0
        self._available = 0

        # Flow control wait event
        self._credit_available = asyncio.Event()
        self._credit_available.set()  # Initially set (assume we can send)

    @property
    def link_credit(self) -> int:
        """Get current link credit."""
        return self._link_credit

    async def attach(self) -> None:
        """Attach the sender link."""
        target = Target(address=self._address)
        source = Source(address=f"client-{self._name}")

        attach_frame = AttachFrame(
            name=self._name,
            handle=self._handle,
            role=self._role,
            source=source,
            target=target,
            initial_delivery_count=0,
        )

        await self._session.connection.send_performative(
            self._session.channel,
            attach_frame,
        )
        await self._state_machine.transition("send_attach")

        logger.debug(
            "SenderLink %s: ATTACH sent to %s",
            self._name,
            self._address,
        )

    async def send(
        self,
        payload: bytes,
        headers: dict[str, Any] | None = None,
        *,
        message_header: Header | None = None,
        message_properties: Properties | None = None,
        delivery_annotations: dict[str, Any] | None = None,
        message_annotations: dict[str, Any] | None = None,
        footer: dict[str, Any] | None = None,
        settled: bool = True,
        _timeout: float = 30.0,  # Reserved for credit-based flow control
    ) -> None:
        """
        Send a message.

        Args:
            payload: Message body as bytes
            headers: Application properties (key-value headers)
            message_header: AMQP message header
            message_properties: AMQP message properties
            delivery_annotations: Delivery annotations
            message_annotations: Message annotations
            footer: Message footer
            settled: Whether to pre-settle the message
            timeout: Timeout for waiting for credit
        """
        if not self.is_usable:
            raise LinkError("Link is not usable")

        # Wait for credit if needed
        # TODO: implement proper credit-based flow control
        # For now, we just send

        # Build message
        msg = Message(
            data=payload,
            application_properties=headers,
            header=message_header,
            properties=message_properties,
            delivery_annotations=delivery_annotations,
            message_annotations=message_annotations,
            footer=footer,
        )

        # Calculate max frame size
        max_frame_size = self._session.connection.max_frame_size

        # Encode message into transfer frames
        frames = list(
            message_to_transfer_frames(
                message=msg,
                handle=self._handle,
                delivery_id=self._delivery_count,
                delivery_tag=str(self._delivery_count).encode(),
                settled=settled,
                max_frame_size=max_frame_size,
            ),
        )

        # Send all frames
        for frame in frames:
            await self._session.connection.send_performative(
                self._session.channel,
                frame,
            )

        self._delivery_count += 1

        logger.debug(
            "SenderLink %s: sent message (delivery=%d, frames=%d)",
            self._name,
            self._delivery_count - 1,
            len(frames),
        )

    async def _handle_flow(self, flow: FlowFrame) -> None:
        """Handle FLOW frame (credit update)."""
        if flow.link_credit is not None:
            old_credit = self._link_credit
            self._link_credit = flow.link_credit
            if flow.delivery_count is not None:
                # Adjust for already consumed credit
                consumed = flow.delivery_count - self._delivery_count
                if consumed > 0:
                    self._link_credit = max(0, self._link_credit - consumed)

            logger.debug(
                "SenderLink %s: credit updated %d -> %d",
                self._name,
                old_credit,
                self._link_credit,
            )

            if self._link_credit > 0:
                self._credit_available.set()
            else:
                self._credit_available.clear()


# =============================================================================
# Receiver Link
# =============================================================================


class ReceiverLink(Link):
    """
    AMQP Receiver Link for receiving messages.

    Example:
        async def on_message(body, headers, delivery_id, delivery_tag, link):
            print(f"Received: {body}")

        receiver = await session.create_receiver("my-queue", on_message)
    """

    def __init__(
        self,
        session: Session,
        name: str,
        address: str,
        handle: int,
        callback: Callable[[bytes, dict[str, Any] | None, int, bytes, ReceiverLink], Any],
    ) -> None:
        super().__init__(session, name, address, handle, role=True)

        self._callback = callback

        # Receiver-specific state
        self._delivery_count = 0
        self._link_credit = 100  # Default credit
        self._prefetch = 100

        # Multi-frame transfer handling
        self._incoming_transfers: list[TransferFrame] = []

    @property
    def link_credit(self) -> int:
        """Get current link credit."""
        return self._link_credit

    async def attach(self) -> None:
        """Attach the receiver link."""
        source = Source(address=self._address)
        target = Target(address=f"client-{self._name}")

        attach_frame = AttachFrame(
            name=self._name,
            handle=self._handle,
            role=self._role,
            source=source,
            target=target,
        )

        await self._session.connection.send_performative(
            self._session.channel,
            attach_frame,
        )
        await self._state_machine.transition("send_attach")

        logger.debug(
            "ReceiverLink %s: ATTACH sent from %s",
            self._name,
            self._address,
        )

    async def _handle_attach(self, _attach: AttachFrame) -> None:
        """Handle ATTACH response and send initial flow."""
        await super()._handle_attach(_attach)

        # Send initial FLOW to grant credit
        await self._send_flow()

    async def _handle_flow(self, flow: FlowFrame) -> None:
        """Handle FLOW frame (echo request)."""
        # Receivers typically only respond to drain/echo requests
        if flow.echo:
            await self._send_flow()

    async def _send_flow(self) -> None:
        """Send FLOW frame to grant credit."""
        flow_frame = FlowFrame(
            next_incoming_id=0,
            incoming_window=100,
            next_outgoing_id=0,
            outgoing_window=100,
            handle=self._handle,
            delivery_count=self._delivery_count,
            link_credit=self._link_credit,
            available=0,
            drain=False,
            echo=False,
            properties=None,
        )

        await self._session.connection.send_performative(
            self._session.channel,
            flow_frame,
        )

        logger.debug(
            "ReceiverLink %s: FLOW sent (credit=%d)",
            self._name,
            self._link_credit,
        )

    async def _handle_transfer(self, transfer: TransferFrame) -> None:
        """Handle TRANSFER frame (incoming message)."""
        self._incoming_transfers.append(transfer)

        # Check if this is the last frame of the message
        if not transfer.more:
            await self._process_message()

    async def _process_message(self) -> None:
        """Process a complete message from accumulated transfer frames."""
        try:
            # Decode message from transfer frames
            msg = transfer_frames_to_message(self._incoming_transfers)

            # Get the last transfer for delivery info
            last_transfer = self._incoming_transfers[-1]
            delivery_id = last_transfer.delivery_id or 0
            delivery_tag = last_transfer.delivery_tag or b""

            # Extract body
            body = msg.body
            if isinstance(body, str):
                body = body.encode()
            elif not isinstance(body, bytes) and body is not None:
                body = json.dumps(body).encode()
            body = body or b""

            # Get headers
            headers = msg.application_properties
            if headers is not None:
                # Ensure it's a dict
                headers = dict(headers)

            # Call the callback
            result = self._callback(body, headers, delivery_id, delivery_tag, self)
            if asyncio.iscoroutine(result):
                await result

            # Update delivery count
            self._delivery_count += 1

            # Check if we need to replenish credit
            if self._link_credit > 0:
                self._link_credit -= 1
                if self._link_credit < self._prefetch // 2:
                    # Replenish credit
                    self._link_credit = self._prefetch
                    await self._send_flow()

        except Exception:
            logger.exception("Error processing message on link %s", self._name)
        finally:
            self._incoming_transfers.clear()

    async def set_credit(self, credit: int) -> None:
        """
        Set link credit and send FLOW.

        Args:
            credit: New credit value
        """
        self._link_credit = credit
        await self._send_flow()


# =============================================================================
# Subscription (convenience wrapper)
# =============================================================================


class Subscription:
    """
    Convenience wrapper for a receiver link.

    Provides a simple interface for managing message subscriptions.
    """

    def __init__(self, link: ReceiverLink) -> None:
        self._link = link

    @property
    def is_active(self) -> bool:
        """Check if subscription is active."""
        return self._link.is_usable and self._link.session.connection.is_connected

    @property
    def link(self) -> ReceiverLink:
        """Get the underlying receiver link."""
        return self._link

    async def cancel(self) -> None:
        """Cancel the subscription (detach the link)."""
        await self._link.detach()
