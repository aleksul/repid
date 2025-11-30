"""
AMQP Connection - High-level connection management with state machine.

This module provides:
- Connection lifecycle management
- SASL authentication
- Automatic reconnection
- Session management
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from repid.connections.amqp._uamqp.constants import MAX_FRAME_SIZE_BYTES, MIN_MAX_FRAME_SIZE
from repid.connections.amqp._uamqp.performatives import (
    CloseFrame,
    OpenFrame,
    SASLChallenge,
    SASLInit,
    SASLMechanism,
    SASLOutcome,
)

from .events import ConnectionEvent, EventData, EventEmitter, MetricsCollector
from .reconnect import ReconnectConfig, ReconnectStrategy
from .states import ConnectionState, ConnectionStateMachine
from .transport import (
    SASL_HEADER,
    AmqpTransport,
    ConnectionClosedError,
    TransportConfig,
)

if TYPE_CHECKING:
    from .session import Session

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class AmqpError(Exception):
    """Base exception for AMQP protocol errors."""


class AuthenticationError(AmqpError):
    """Raised when SASL authentication fails."""


class ProtocolError(AmqpError):
    """Raised when protocol negotiation fails."""


class AmqpConnectionError(AmqpError):
    """Raised when connection fails."""


# =============================================================================
# Configuration
# =============================================================================


@dataclass(slots=True)
class ConnectionConfig:
    """Configuration for AMQP connection."""

    host: str
    port: int = 5672
    username: str | None = None
    password: str | None = None
    virtual_host: str = "/"
    ssl_context: Any = None

    # Connection settings
    connect_timeout: float = 10.0
    tcp_keepalive: bool = True

    # Frame settings
    max_frame_size: int = MAX_FRAME_SIZE_BYTES
    channel_max: int = 65535

    # Reconnection settings
    reconnect: ReconnectConfig = field(default_factory=ReconnectConfig)

    # Container ID (unique identifier for this connection)
    container_id: str = field(default_factory=lambda: f"repid-{uuid.uuid4().hex[:8]}")


# =============================================================================
# AMQP Connection
# =============================================================================


class AmqpConnection:
    """
    High-level AMQP 1.0 connection with state machine management.

    Features:
    - Explicit state management via ConnectionStateMachine
    - SASL authentication (PLAIN, ANONYMOUS, EXTERNAL)
    - Automatic reconnection with configurable backoff
    - Event-driven monitoring
    - Session management

    Example:
        async with AmqpConnection(config) as conn:
            session = await conn.create_session()
            # Use session...

    Or for manual control:
        conn = AmqpConnection(config)
        await conn.connect()
        try:
            # Use connection...
        finally:
            await conn.close()
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """
        Initialize connection with configuration.

        Args:
            config: Connection configuration
        """
        self._config = config
        self._connection_id = f"{config.host}:{config.port}/{config.container_id}"

        # Transport layer
        self._transport = AmqpTransport(
            TransportConfig(
                host=config.host,
                port=config.port,
                ssl_context=config.ssl_context,
                connect_timeout=config.connect_timeout,
                tcp_keepalive=config.tcp_keepalive,
            ),
        )

        # State machine
        self._state_machine = ConnectionStateMachine()

        # Events and metrics
        self._events = EventEmitter()
        self._metrics = MetricsCollector()
        self._events.on_all(self._metrics.handle_event)

        # Reconnection strategy
        self._reconnect = ReconnectStrategy(config.reconnect)

        # Sessions
        self._sessions: dict[int, Session] = {}
        self._next_channel = 0

        # Remote parameters
        self._remote_max_frame_size = MIN_MAX_FRAME_SIZE
        self._remote_channel_max = 65535
        self._remote_container_id: str | None = None
        self._remote_idle_timeout: int | None = None

        # Background tasks
        self._read_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

        # Waiters for responses
        self._waiters: list[tuple[Any, asyncio.Future[Any]]] = []

        # State change listener
        self._state_machine.add_listener(self._on_state_change)

    @classmethod
    def create(
        cls,
        host: str,
        port: int = 5672,
        *,
        username: str | None = None,
        password: str | None = None,
        ssl_context: Any = None,
        **kwargs: Any,
    ) -> Self:
        """
        Create connection with individual parameters (convenience factory).

        Args:
            host: Server hostname
            port: Server port
            username: SASL username
            password: SASL password
            ssl_context: SSL context for TLS
            **kwargs: Additional ConnectionConfig parameters

        Returns:
            AmqpConnection instance
        """
        config = ConnectionConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            ssl_context=ssl_context,
            **kwargs,
        )
        return cls(config)

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def connection_id(self) -> str:
        """Get connection identifier."""
        return self._connection_id

    @property
    def state(self) -> ConnectionState:
        """Get current connection state."""
        return self._state_machine.state

    @property
    def is_connected(self) -> bool:
        """Check if connection is in OPENED state."""
        return self._state_machine.is_open()

    @property
    def is_usable(self) -> bool:
        """Check if connection can be used for operations."""
        return self._state_machine.is_usable()

    @property
    def _incoming_task(self) -> asyncio.Task[None] | None:
        """Get the background read task (for backward compatibility)."""
        return self._read_task

    @property
    def events(self) -> EventEmitter:
        """Get event emitter for monitoring."""
        return self._events

    @property
    def metrics(self) -> MetricsCollector:
        """Get metrics collector."""
        return self._metrics

    @property
    def max_frame_size(self) -> int:
        """Get negotiated max frame size."""
        return min(self._config.max_frame_size, self._remote_max_frame_size)

    # -------------------------------------------------------------------------
    # Context Manager
    # -------------------------------------------------------------------------

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    # -------------------------------------------------------------------------
    # Connection Lifecycle
    # -------------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Establish connection to AMQP server.

        Handles:
        - TCP connection
        - SASL authentication (if credentials provided)
        - AMQP protocol negotiation
        - Automatic reconnection on failure

        Raises:
            ConnectionError: If connection fails after all retries
            AuthenticationError: If authentication fails
        """
        await self._emit(ConnectionEvent.CONNECTING)

        while not self._stop_event.is_set():
            try:
                await self._do_connect()
                self._reconnect.reset()
                await self._emit(ConnectionEvent.CONNECTED)
                return

            except Exception as exc:
                logger.error("Connection failed: %s", exc)
                await self._emit(
                    ConnectionEvent.ERROR,
                    {"error": str(exc), "error_type": type(exc).__name__},
                )

                # Close transport on failure
                await self._transport.close()
                self._state_machine.reset(ConnectionState.START)

                # Check if we should retry
                if not self._reconnect.should_retry():
                    raise ConnectionError(
                        f"Connection failed after {self._reconnect.attempts} attempts: {exc}",
                    ) from exc

                await self._emit(
                    ConnectionEvent.RECONNECTING,
                    {"attempt": self._reconnect.attempts + 1},
                )
                await self._reconnect.wait()

    async def _do_connect(self) -> None:
        """Perform the actual connection sequence."""
        # 1. TCP Connect
        await self._transport.connect()

        # 2. SASL if credentials provided
        if self._config.username is not None:
            await self._do_sasl_auth()

        # 3. AMQP protocol negotiation
        await self._do_amqp_open()

        # 4. Start read loop
        self._read_task = asyncio.create_task(
            self._read_loop(),
            name=f"amqp-read-{self._connection_id}",
        )

    async def _do_sasl_auth(self) -> None:
        """Perform SASL authentication."""
        await self._emit(ConnectionEvent.SASL_START)

        # Send SASL header
        await self._transport.send_sasl_header()
        await self._state_machine.transition("send_header")

        # Receive SASL header
        response = await self._transport.read_protocol_header()
        if not self._transport.validate_sasl_header(response):
            raise ProtocolError(f"Unexpected SASL header response: {response!r}")
        await self._state_machine.transition("recv_header")

        # Receive SASL mechanisms
        _, performative = await self._transport.read_frame()
        if not isinstance(performative, SASLMechanism):
            raise ProtocolError(f"Expected SASL-MECHANISMS, got {type(performative)}")

        # Choose mechanism and authenticate
        await self._sasl_authenticate(performative.sasl_server_mechanisms)

        # Send AMQP header after SASL
        await self._transport.send_amqp_header()

        # Receive AMQP header
        response = await self._transport.read_protocol_header()
        if not self._transport.validate_amqp_header(response):
            raise ProtocolError(f"Unexpected AMQP header after SASL: {response!r}")

        # Reset state for AMQP protocol phase
        self._state_machine.reset(ConnectionState.HDR_EXCH)

    async def _sasl_authenticate(self, mechanisms: list[Any]) -> None:
        """Perform SASL authentication with server."""
        mech_names = [m.decode() if isinstance(m, bytes) else m for m in mechanisms]

        # Choose mechanism
        chosen_mech: str
        initial_response: bytes | None = None

        if "EXTERNAL" in mech_names and self._config.ssl_context:
            chosen_mech = "EXTERNAL"
            initial_response = b""
        elif "PLAIN" in mech_names and self._config.username and self._config.password:
            chosen_mech = "PLAIN"
            # SASL PLAIN: authorization-id \0 authentication-id \0 password
            initial_response = f"\0{self._config.username}\0{self._config.password}".encode()
        elif "ANONYMOUS" in mech_names:
            chosen_mech = "ANONYMOUS"
            initial_response = None
        else:
            raise AuthenticationError(f"No supported SASL mechanism. Server offered: {mech_names}")

        # Send SASL-INIT
        sasl_init = SASLInit(
            mechanism=chosen_mech,
            initial_response=initial_response,
            hostname=self._config.host,
        )
        await self._transport.send_performative(0, sasl_init)

        # Wait for outcome
        while True:
            _, performative = await self._transport.read_frame()

            if isinstance(performative, SASLChallenge):
                raise AuthenticationError("SASL Challenge received but not supported")

            if isinstance(performative, SASLOutcome):
                if performative.code != 0:
                    await self._emit(ConnectionEvent.SASL_FAILED)
                    raise AuthenticationError(
                        f"SASL authentication failed with code {performative.code}",
                    )
                await self._emit(ConnectionEvent.SASL_SUCCESS)
                return

            raise ProtocolError(f"Unexpected SASL frame: {type(performative)}")

    async def _do_amqp_open(self) -> None:
        """Perform AMQP OPEN handshake."""
        # Send AMQP header if not already sent (no SASL case)
        if self._state_machine.state == ConnectionState.START:
            await self._transport.send_amqp_header()
            await self._state_machine.transition("send_header")

            response = await self._transport.read_protocol_header()
            if response == SASL_HEADER:
                raise ProtocolError("Server requires SASL but no credentials provided")
            if not self._transport.validate_amqp_header(response):
                raise ProtocolError(f"Unexpected protocol header: {response!r}")
            await self._state_machine.transition("recv_header")

        # Send OPEN frame
        open_frame = OpenFrame(
            container_id=self._config.container_id,
            hostname=self._config.host,
            max_frame_size=self._config.max_frame_size,
            channel_max=self._config.channel_max,
        )
        await self._transport.send_performative(0, open_frame)
        await self._state_machine.transition("send_open")

        # Wait for OPEN response
        _, response_frame = await self._transport.read_frame()
        if not isinstance(response_frame, OpenFrame):
            raise ProtocolError(f"Expected OPEN, got {type(response_frame)}")

        # Store remote parameters
        if response_frame.max_frame_size:
            self._remote_max_frame_size = response_frame.max_frame_size
        if response_frame.channel_max:
            self._remote_channel_max = response_frame.channel_max
        self._remote_container_id = response_frame.container_id
        self._remote_idle_timeout = response_frame.idle_timeout

        await self._state_machine.transition("recv_open")

        logger.info(
            "Connection opened: container=%s, max_frame=%d",
            self._remote_container_id,
            self._remote_max_frame_size,
        )

    async def close(self, error: Exception | None = None) -> None:
        """
        Close the connection gracefully.

        Args:
            error: Optional error that caused the close
        """
        if self._state_machine.is_terminal():
            return

        await self._emit(ConnectionEvent.DISCONNECTING)
        self._stop_event.set()

        # Close all sessions first
        for session in list(self._sessions.values()):
            with contextlib.suppress(Exception):
                await session.end()

        # Send CLOSE if possible
        if self._state_machine.is_usable():
            try:
                close_frame = CloseFrame()
                await self._transport.send_performative(0, close_frame)
                await self._state_machine.transition("send_close")
            except (OSError, asyncio.TimeoutError):
                logger.debug("Error sending CLOSE frame", exc_info=True)

        # Cancel read task
        if self._read_task:
            self._read_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._read_task
            self._read_task = None

        # Close transport
        await self._transport.close()

        # Update state
        if not self._state_machine.is_terminal():
            with contextlib.suppress(ValueError):
                await self._state_machine.transition("fatal_error")

        await self._emit(
            ConnectionEvent.DISCONNECTED,
            {"error": str(error) if error else None},
        )

    # -------------------------------------------------------------------------
    # Read Loop
    # -------------------------------------------------------------------------

    async def _read_loop(self) -> None:
        """Background task to read and dispatch frames."""
        try:
            while not self._stop_event.is_set() and self._state_machine.is_usable():
                try:
                    channel, performative = await self._transport.read_frame()
                    await self._handle_performative(channel, performative)
                except ConnectionClosedError:
                    logger.info("Connection closed by server")
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Error in read loop")
                    break

        except asyncio.CancelledError:
            pass
        finally:
            if not self._stop_event.is_set() and not self._state_machine.is_open():
                # Connection lost unexpectedly - only handle if not already reconnected
                task = asyncio.create_task(self._handle_unexpected_close())
                task.add_done_callback(lambda _t: None)  # Prevent GC

    async def _handle_unexpected_close(self) -> None:
        """Handle unexpected connection loss."""
        # Skip if already connected (might have been called twice)
        if self._state_machine.is_open():
            return

        await self._emit(ConnectionEvent.DISCONNECTED, {"unexpected": True})

        # Invalidate all sessions (they can't be reused after reconnection)
        for session in list(self._sessions.values()):
            session.invalidate()
        self._sessions.clear()

        # Try to reconnect if enabled
        if self._reconnect.config.enabled and not self._stop_event.is_set():
            self._state_machine.reset(ConnectionState.START)
            try:
                await self.connect()
                await self._emit(ConnectionEvent.RECONNECTED)
            except ConnectionError as e:
                logger.error("Reconnection failed: %s", e)
            except Exception:
                logger.exception("Unexpected error during reconnection")

    async def _handle_performative(self, channel: int, performative: Any) -> None:
        """Handle an incoming performative."""
        logger.debug("Received on channel %d: %s", channel, type(performative).__name__)

        await self._emit(
            ConnectionEvent.FRAME_RECEIVED,
            {"channel": channel, "type": type(performative).__name__},
        )

        # Check waiters
        for predicate, future in self._waiters[:]:
            with contextlib.suppress(Exception):
                if predicate(channel, performative):
                    if not future.done():
                        future.set_result(performative)
                    self._waiters.remove((predicate, future))

        # Handle connection-level performatives
        if isinstance(performative, OpenFrame):
            # Unexpected OPEN
            pass
        elif isinstance(performative, CloseFrame):
            await self._handle_close(performative)
        elif channel in self._sessions:
            # Dispatch to session
            await self._sessions[channel].handle_performative(performative)
        else:
            logger.warning("No session for channel %d", channel)

    async def _handle_close(self, close_frame: CloseFrame) -> None:
        """Handle CLOSE from server."""
        error = getattr(close_frame, "error", None)
        logger.info(
            "Received CLOSE: %s",
            error if error else "no error",
        )

        with contextlib.suppress(ValueError):
            await self._state_machine.transition("recv_close")

        # Send CLOSE response if we haven't already
        if self._state_machine.state == ConnectionState.CLOSE_RCVD:
            with contextlib.suppress(OSError, asyncio.TimeoutError, ValueError):
                await self._transport.send_performative(0, CloseFrame())
                await self._state_machine.transition("send_close")

        # If server closed with an error, treat as unexpected and reconnect
        if error and self._reconnect.config.enabled:
            # Call _handle_unexpected_close directly to reconnect before returning
            # This ensures the connection is restored before any waiters can proceed
            await self._handle_unexpected_close()
        else:
            await self.close()

    # -------------------------------------------------------------------------
    # Session Management
    # -------------------------------------------------------------------------

    async def create_session(self) -> Session:
        """
        Create a new session on this connection.

        Returns:
            A new Session instance

        Raises:
            ConnectionError: If connection is not open
        """
        if not self.is_connected:
            raise ConnectionError("Connection is not open")

        # Find free channel
        channel = self._next_channel
        while channel in self._sessions:
            channel = (channel + 1) % (self._remote_channel_max + 1)
            if channel == self._next_channel:
                raise ConnectionError("No free channels available")

        self._next_channel = (channel + 1) % (self._remote_channel_max + 1)

        # Import here to avoid circular import
        from .session import Session  # noqa: PLC0415

        session = Session(self, channel)
        self._sessions[channel] = session

        await session.begin()
        return session

    def _remove_session(self, channel: int) -> None:
        """Remove a session from the connection."""
        if channel in self._sessions:
            del self._sessions[channel]

    # -------------------------------------------------------------------------
    # Frame Sending
    # -------------------------------------------------------------------------

    async def send_performative(
        self,
        channel: int,
        performative: Any,
        wait_for_response: Any | None = None,
        timeout: float = 10.0,
    ) -> Any | None:
        """
        Send a performative frame.

        Args:
            channel: Channel number
            performative: The performative to send
            wait_for_response: Optional predicate to wait for response
            timeout: Timeout for waiting

        Returns:
            Response performative if wait_for_response provided
        """
        if not self.is_usable:
            raise ConnectionError("Connection is not usable")

        await self._transport.send_performative(channel, performative)

        await self._emit(
            ConnectionEvent.FRAME_SENT,
            {"channel": channel, "type": type(performative).__name__},
        )

        if wait_for_response:
            future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            waiter = (wait_for_response, future)
            self._waiters.append(waiter)

            try:
                return await asyncio.wait_for(future, timeout)
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)

        return None

    # -------------------------------------------------------------------------
    # Event Helpers
    # -------------------------------------------------------------------------

    def _on_state_change(self, transition: Any) -> None:
        """Handle state machine transitions."""
        self._events.emit_sync(
            EventData(
                event=ConnectionEvent.STATE_CHANGED,
                connection_id=self._connection_id,
                details={
                    "from_state": transition.from_state,
                    "to_state": transition.to_state,
                    "trigger": transition.trigger,
                },
            ),
        )

    async def _emit(
        self,
        event: ConnectionEvent,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Emit an event."""
        await self._events.emit(
            EventData(
                event=event,
                connection_id=self._connection_id,
                details=details or {},
            ),
        )
