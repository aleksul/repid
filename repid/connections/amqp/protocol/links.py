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
import inspect
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from repid.connections.amqp._uamqp._decode import transfer_frames_to_message
from repid.connections.amqp._uamqp._encode import message_to_transfer_frames
from repid.connections.amqp._uamqp.endpoints import Source, Target
from repid.connections.amqp._uamqp.message import Header, Message, Properties
from repid.connections.amqp._uamqp.outcomes import Accepted, DeliveryState, Rejected, Released
from repid.connections.amqp._uamqp.performatives import (
    AttachFrame,
    DetachFrame,
    DispositionFrame,
    FlowFrame,
    TransferFrame,
)

from .states import LinkState, LinkStateMachine

if TYPE_CHECKING:
    from .session import Session

logger = logging.getLogger("repid.connections.amqp.protocol")

ReceiverSettlementState = Accepted | Rejected | Released


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
                logger.warning("link.detach_frame.error", exc_info=True)

        # Remove from session
        self._session._remove_link(self)  # type: ignore[arg-type]

        logger.debug(
            "link.detached",
            extra={"link": self._name, "session": self._session.channel},
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
            "link.invalidated",
            extra={"link": self._name, "session": self._session.channel},
        )

    # -------------------------------------------------------------------------
    # Frame Handling
    # -------------------------------------------------------------------------

    async def _handle_attach(self, _attach: AttachFrame) -> None:
        """Handle ATTACH response."""
        await self._state_machine.transition("recv_attach")
        self._ready.set()

        logger.debug(
            "link.attached",
            extra={"link": self._name, "remote_handle": self._remote_handle},
        )

    async def _handle_detach(self, _detach: DetachFrame) -> None:
        """Handle DETACH from remote."""
        logger.debug(
            "link.detach.received",
            extra={"link": self._name, "session": self._session.channel},
        )

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
        self._send_lock = asyncio.Lock()
        self._unsettled: dict[int, asyncio.Future[DeliveryState | None]] = {}

        # Flow control wait event
        self._credit_available = asyncio.Event()
        self._credit_available.clear()  # Initially cleared (wait for credit)

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
            "sender_link.attach.sent",
            extra={"link": self._name, "address": self._address},
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

        settlement_future: asyncio.Future[DeliveryState | None] | None = None
        async with self._send_lock:
            await self._wait_for_credit(_timeout)
            delivery_id = self._session.allocate_outgoing_delivery_id()
            frames = self._build_transfer_frames(
                payload=payload,
                headers=headers,
                message_header=message_header,
                message_properties=message_properties,
                delivery_annotations=delivery_annotations,
                message_annotations=message_annotations,
                footer=footer,
                delivery_id=delivery_id,
                settled=settled,
            )

            if not settled:
                settlement_future = asyncio.get_running_loop().create_future()
                self._unsettled[delivery_id] = settlement_future

            try:
                await self._send_transfer_frames(frames, _timeout)
            except Exception:
                self._discard_unsettled_delivery(delivery_id, settlement_future)
                raise

        logger.debug(
            "sender_link.message.sent",
            extra={"link": self._name, "delivery": delivery_id, "frames": len(frames)},
        )

        if settlement_future is not None:
            await self._wait_for_delivery_settlement(delivery_id, settlement_future, _timeout)

    async def _wait_for_credit(self, timeout: float) -> None:
        while self._link_credit <= 0:
            try:
                await asyncio.wait_for(self._credit_available.wait(), timeout=timeout)
            except asyncio.TimeoutError as e:
                raise LinkError("Timeout waiting for link credit") from e

    def _build_transfer_frames(
        self,
        *,
        payload: bytes,
        headers: dict[str, Any] | None,
        message_header: Header | None,
        message_properties: Properties | None,
        delivery_annotations: dict[str, Any] | None,
        message_annotations: dict[str, Any] | None,
        footer: dict[str, Any] | None,
        delivery_id: int,
        settled: bool,
    ) -> list[TransferFrame]:
        msg = Message(
            data=[payload],
            application_properties=headers,
            header=message_header,
            properties=message_properties,
            delivery_annotations=delivery_annotations,
            message_annotations=message_annotations,
            footer=footer,
        )
        return list(
            message_to_transfer_frames(
                message=msg,
                handle=self._handle,
                delivery_id=delivery_id,
                delivery_tag=str(delivery_id).encode(),
                settled=settled,
                max_frame_size=self._session.connection.max_frame_size,
            ),
        )

    async def _send_transfer_frames(self, frames: list[TransferFrame], timeout: float) -> None:
        # Session flow control is per transfer frame, while link credit is per delivery.
        credit_consumed = False
        for frame in frames:
            try:
                await self._session.wait_for_remote_incoming_window(timeout)
            except asyncio.TimeoutError as e:
                raise LinkError("Timeout waiting for session window") from e

            if not credit_consumed:
                self._consume_send_credit()
                credit_consumed = True

            await self._session.connection.send_performative(
                self._session.channel,
                frame,
            )
            self._session._next_outgoing_id = (self._session._next_outgoing_id + 1) & 0xFFFFFFFF
            self._session.consume_remote_incoming_window()

    def _consume_send_credit(self) -> None:
        self._link_credit -= 1
        if self._link_credit <= 0:
            self._credit_available.clear()
        self._delivery_count = (self._delivery_count + 1) & 0xFFFFFFFF

    def _discard_unsettled_delivery(
        self,
        delivery_id: int,
        settlement_future: asyncio.Future[DeliveryState | None] | None,
    ) -> None:
        if settlement_future is None:
            return
        self._unsettled.pop(delivery_id, None)
        settlement_future.cancel()

    async def _wait_for_delivery_settlement(
        self,
        delivery_id: int,
        settlement_future: asyncio.Future[DeliveryState | None],
        timeout: float,
    ) -> None:
        try:
            state = await asyncio.wait_for(settlement_future, timeout=timeout)
        except asyncio.TimeoutError as e:
            self._unsettled.pop(delivery_id, None)
            raise LinkError("Timeout waiting for delivery settlement") from e
        if not isinstance(state, Accepted):
            raise LinkError(f"Delivery was not accepted: {state!r}")

    async def _handle_flow(self, flow: FlowFrame) -> None:
        """Handle FLOW frame (credit update)."""
        if flow.link_credit is not None:
            old_credit = self._link_credit

            if flow.delivery_count is not None:
                # AMQP 1.0 spec: link-credit_snd := delivery-count_rcv + link-credit_rcv - delivery-count_snd
                in_flight = (self._delivery_count - flow.delivery_count) & 0xFFFFFFFF
                self._link_credit = flow.link_credit - in_flight
                self._link_credit = max(self._link_credit, 0)
            else:
                self._link_credit = flow.link_credit

            logger.debug(
                "sender_link.credit.updated",
                extra={"link": self._name, "old": old_credit, "new": self._link_credit},
            )

            if self._link_credit > 0:
                self._credit_available.set()
            else:
                self._credit_available.clear()

    async def _handle_disposition(self, disposition: DispositionFrame) -> None:
        last = disposition.last if disposition.last is not None else disposition.first
        for delivery_id in range(disposition.first, last + 1):
            future = self._unsettled.pop(delivery_id, None)
            if future is not None and not future.done():
                future.set_result(disposition.state)


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
        callback: Callable[
            [bytes, dict[str, Any] | None, int, bytes, ReceiverLink, Properties | None],
            Any,
        ],
        prefetch: int = 100,
    ) -> None:
        super().__init__(session, name, address, handle, role=True)

        self._callback = callback

        # Receiver-specific state
        self._delivery_count = 0
        self._link_credit = prefetch
        self._prefetch = prefetch

        # Multi-frame transfer handling
        self._incoming_transfers: list[TransferFrame] = []
        self._delivery_settled_by_callback = False
        self._credit_lock = asyncio.Lock()
        self._credit_pending_delivery_ids: set[int] = set()
        self._deferred_credit_delivery_ids: set[int] = set()
        self._settled_delivery_ids: set[int] = set()

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
            "receiver_link.attach.sent",
            extra={"link": self._name, "address": self._address},
        )

    async def _handle_attach(self, _attach: AttachFrame) -> None:
        """Handle ATTACH response and send initial flow."""
        await super()._handle_attach(_attach)

        if _attach.initial_delivery_count is not None:
            self._delivery_count = _attach.initial_delivery_count

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
            next_incoming_id=self._session._next_incoming_id,
            incoming_window=self._session._incoming_window,
            next_outgoing_id=self._session._next_outgoing_id,
            outgoing_window=self._session._outgoing_window,
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
            "receiver_link.flow.sent",
            extra={"link": self._name, "credit": self._link_credit},
        )

    async def _handle_transfer(self, transfer: TransferFrame) -> None:
        """Handle TRANSFER frame (incoming message)."""
        is_first_transfer = not self._incoming_transfers
        delivery_id = self._delivery_id_from_transfer(transfer)

        if is_first_transfer:
            await self._accept_delivery(transfer, delivery_id)
        elif transfer.settled:
            self._settled_delivery_ids.add(
                self._delivery_id_from_transfer(self._incoming_transfers[0]),
            )

        if transfer.aborted:
            if not is_first_transfer:
                delivery_id = self._delivery_id_from_transfer(self._incoming_transfers[0])
            self._incoming_transfers.clear()
            await self.release_delivery_credit(delivery_id)
            return

        self._incoming_transfers.append(transfer)

        # Check if this is the last frame of the message
        if not transfer.more:
            await self._process_message()

    async def _process_message(self) -> None:
        """Process a complete message from accumulated transfer frames."""
        delivery_id: int | None = None
        try:
            # Continuation transfers may omit delivery metadata.
            first_transfer = self._incoming_transfers[0]
            delivery_id = self._delivery_id_from_transfer(first_transfer)

            # Decode message from transfer frames
            msg = transfer_frames_to_message(self._incoming_transfers)

            delivery_tag = first_transfer.delivery_tag or b""

            body = self._body_to_bytes(msg.body)
            headers = (
                dict(msg.application_properties) if msg.application_properties is not None else None
            )

            # Call the callback
            self._delivery_settled_by_callback = False
            result = self._callback(body, headers, delivery_id, delivery_tag, self, msg.properties)
            if inspect.iscoroutine(result):
                await result

        except Exception:
            logger.exception("receiver_link.message.error", extra={"link": self._name})
        finally:
            if delivery_id is not None and delivery_id not in self._deferred_credit_delivery_ids:
                await self.release_delivery_credit(delivery_id)
            self._delivery_settled_by_callback = False
            self._incoming_transfers.clear()

    async def _send_disposition(self, delivery_id: int, state: ReceiverSettlementState) -> None:
        disp = DispositionFrame(
            role=True,
            first=delivery_id,
            last=delivery_id,
            settled=True,
            state=state,
        )
        await self._session.connection.send_performative(self._session.channel, disp)
        self._delivery_settled_by_callback = True

    async def settle_delivery(self, delivery_id: int, state: ReceiverSettlementState) -> None:
        if not self.is_delivery_settled(delivery_id):
            await self._send_disposition(delivery_id, state)
        await self.release_delivery_credit(delivery_id)

    @staticmethod
    def _body_to_bytes(body: Any) -> bytes:
        if isinstance(body, list):
            if body and isinstance(body[0], bytes):
                return b"".join(cast(list[bytes], body))
            if body:
                return json.dumps(body).encode()
        elif isinstance(body, str):
            return body.encode()
        elif isinstance(body, bytes):
            return body
        elif body is not None:
            return json.dumps(body).encode()
        return b""

    @staticmethod
    def _delivery_id_from_transfer(transfer: TransferFrame) -> int:
        return transfer.delivery_id if transfer.delivery_id is not None else 0

    async def _accept_delivery(self, transfer: TransferFrame, delivery_id: int) -> None:
        async with self._credit_lock:
            if self._link_credit > 0:
                self._credit_pending_delivery_ids.add(delivery_id)
                if transfer.settled:
                    self._settled_delivery_ids.add(delivery_id)
                self._link_credit -= 1
            else:
                logger.warning(
                    "receiver_link.credit.overdrawn",
                    extra={"link": self._name, "delivery_id": delivery_id},
                )
            self._delivery_count = (self._delivery_count + 1) & 0xFFFFFFFF

    def defer_delivery_credit(self, delivery_id: int) -> None:
        if delivery_id in self._credit_pending_delivery_ids:
            self._deferred_credit_delivery_ids.add(delivery_id)

    def is_delivery_settled(self, delivery_id: int) -> bool:
        return delivery_id in self._settled_delivery_ids

    async def release_delivery_credit(self, delivery_id: int) -> None:
        async with self._credit_lock:
            if delivery_id not in self._credit_pending_delivery_ids:
                return

            self._credit_pending_delivery_ids.remove(delivery_id)
            self._deferred_credit_delivery_ids.discard(delivery_id)
            self._settled_delivery_ids.discard(delivery_id)

            if self._link_credit < self._prefetch:
                self._link_credit += 1
                if self.is_usable:
                    await self._send_flow()

    async def set_credit(self, credit: int) -> None:
        """
        Set link credit and send FLOW.

        Args:
            credit: New credit value
        """
        if credit < 0:
            raise ValueError("credit must be non-negative")
        self._link_credit = credit
        self._prefetch = credit
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
