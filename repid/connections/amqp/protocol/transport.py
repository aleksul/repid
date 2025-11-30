"""
AMQP Transport Layer - Low-level TCP/SSL connection handling.

This module handles the raw TCP transport with:
- Connection establishment with timeout
- TCP keepalive for connection health
- Frame reading/writing
- SSL/TLS support
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import ssl
import struct
import sys
import time
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING

from typing_extensions import Self

from repid.connections.amqp._uamqp._decode import bytes_to_performative
from repid.connections.amqp._uamqp._encode import performative_to_bytes
from repid.connections.amqp._uamqp.performatives import Performative

if TYPE_CHECKING:
    from .events import EventEmitter

logger = logging.getLogger(__name__)

# Protocol headers
AMQP_HEADER = b"AMQP\x00\x01\x00\x00"
SASL_HEADER = b"AMQP\x03\x01\x00\x00"
TLS_HEADER = b"AMQP\x02\x01\x00\x00"

# Frame constants
FRAME_HEADER_SIZE = 8
MIN_FRAME_SIZE = 512


# =============================================================================
# Exceptions
# =============================================================================


class TransportError(Exception):
    """Base exception for transport errors."""


class ConnectionClosedError(TransportError):
    """Raised when the connection is closed unexpectedly."""


class ConnectionTimeoutError(TransportError):
    """Raised when connection times out."""


class FrameError(TransportError):
    """Raised when frame parsing fails."""


# =============================================================================
# Configuration
# =============================================================================


@dataclass(slots=True)
class TransportConfig:
    """Configuration for the transport layer."""

    host: str
    port: int
    ssl_context: ssl.SSLContext | None = None
    connect_timeout: float = 10.0
    tcp_keepalive: bool = True
    tcp_keepalive_idle: int = 60  # Seconds before sending keepalive probes
    tcp_keepalive_interval: int = 10  # Seconds between probes
    tcp_keepalive_count: int = 6  # Number of failed probes before disconnect
    read_buffer_size: int = 65536


# =============================================================================
# Frame Buffer
# =============================================================================


class FrameBuffer:
    """
    Efficient buffer for accumulating and parsing AMQP frames.

    Pattern: Buffer/Accumulator
    - Handles partial reads efficiently
    - Minimizes memory copies
    - Thread-safe via external synchronization
    """

    def __init__(self, max_size: int = 1024 * 1024):
        self._buffer = bytearray()
        self._max_size = max_size

    def __len__(self) -> int:
        return len(self._buffer)

    def append(self, data: bytes) -> None:
        """Append data to the buffer."""
        if len(self._buffer) + len(data) > self._max_size:
            raise FrameError(
                f"Buffer overflow: {len(self._buffer) + len(data)} > {self._max_size}",
            )
        self._buffer.extend(data)

    def peek(self, n: int) -> bytes:
        """Peek at the first n bytes without consuming."""
        return bytes(self._buffer[:n])

    def consume(self, n: int) -> bytes:
        """Consume and return the first n bytes."""
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data

    def clear(self) -> None:
        """Clear the buffer."""
        self._buffer.clear()

    def try_read_frame(self) -> bytes | None:
        """
        Try to read a complete AMQP frame.

        AMQP frame format:
        - Bytes 0-3: Frame size (uint32, big-endian)
        - Bytes 4-7: DOFF, type, channel
        - Bytes 8+: Frame body

        Returns complete frame bytes or None if incomplete.
        """
        if len(self._buffer) < FRAME_HEADER_SIZE:
            return None

        # Read frame size from first 4 bytes (big-endian)
        frame_size = int.from_bytes(self._buffer[:4], "big")

        if frame_size < FRAME_HEADER_SIZE:
            raise FrameError(f"Invalid frame size: {frame_size}")

        if frame_size > self._max_size:
            raise FrameError(f"Frame too large: {frame_size} > {self._max_size}")

        if len(self._buffer) < frame_size:
            return None  # Incomplete frame

        return self.consume(frame_size)


# =============================================================================
# Transport Metrics
# =============================================================================


@dataclass
class TransportMetrics:
    """Metrics for the transport layer."""

    bytes_sent: int = 0
    bytes_received: int = 0
    frames_sent: int = 0
    frames_received: int = 0
    connect_time_ms: float = 0.0
    last_read_time: float = 0.0
    last_write_time: float = 0.0


# =============================================================================
# AMQP Transport
# =============================================================================


class AmqpTransport:
    """
    Low-level AMQP transport handling TCP/SSL connections.

    Responsibilities:
    - TCP connection management
    - TCP keepalive configuration
    - Frame reading/writing
    - Protocol header exchange

    This class is intentionally simple and focused on I/O.
    State management and protocol logic belong in higher layers.
    """

    def __init__(self, config: TransportConfig):
        """
        Initialize transport with configuration.

        Args:
            config: Transport configuration
        """
        self._config = config
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._buffer = FrameBuffer()
        self._metrics = TransportMetrics()
        self._write_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._events: EventEmitter | None = None

    @property
    def is_connected(self) -> bool:
        """Check if transport is connected."""
        return self._writer is not None and not self._writer.is_closing()

    @property
    def metrics(self) -> TransportMetrics:
        """Get transport metrics."""
        return self._metrics

    @property
    def host(self) -> str:
        """Get the host."""
        return self._config.host

    @property
    def port(self) -> int:
        """Get the port."""
        return self._config.port

    def set_events(self, events: EventEmitter) -> None:
        """Set event emitter for monitoring."""
        self._events = events

    async def connect(self) -> None:
        """
        Establish TCP connection.

        Raises:
            ConnectionTimeoutError: If connection times out
            TransportError: If connection fails
        """
        start_time = time.monotonic()

        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self._config.host,
                    self._config.port,
                    ssl=self._config.ssl_context,
                ),
                timeout=self._config.connect_timeout,
            )
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(
                f"Connection timed out after {self._config.connect_timeout}s",
            ) from None
        except OSError as exc:
            raise TransportError(
                f"Could not connect to {self._config.host}:{self._config.port}: {exc}",
            ) from exc

        # Configure TCP keepalive
        if self._config.tcp_keepalive and self._writer:
            self._configure_keepalive()

        self._metrics.connect_time_ms = (time.monotonic() - start_time) * 1000
        logger.debug(
            "Connected to %s:%d in %.2fms",
            self._config.host,
            self._config.port,
            self._metrics.connect_time_ms,
        )

    def _configure_keepalive(self) -> None:
        """Configure TCP keepalive on the socket."""
        if self._writer is None:
            return

        sock = self._writer.get_extra_info("socket")
        if sock is None:
            return

        # Enable keepalive
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        # Platform-specific keepalive settings
        if sys.platform == "linux":
            # TCP_KEEPIDLE: Seconds before sending keepalive probes
            sock.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_KEEPIDLE,
                self._config.tcp_keepalive_idle,
            )
            # TCP_KEEPINTVL: Seconds between probes
            sock.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_KEEPINTVL,
                self._config.tcp_keepalive_interval,
            )
            # TCP_KEEPCNT: Number of failed probes before disconnect
            sock.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_KEEPCNT,
                self._config.tcp_keepalive_count,
            )
        elif sys.platform == "darwin":
            # macOS uses TCP_KEEPALIVE for idle time
            sock.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_KEEPALIVE,
                self._config.tcp_keepalive_idle,
            )
        # Windows uses different socket options handled automatically

        logger.debug(
            "TCP keepalive configured: idle=%ds, interval=%ds, count=%d",
            self._config.tcp_keepalive_idle,
            self._config.tcp_keepalive_interval,
            self._config.tcp_keepalive_count,
        )

    async def close(self) -> None:
        """Close the transport connection."""
        if self._writer:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        self._buffer.clear()
        logger.debug("Transport closed")

    # -------------------------------------------------------------------------
    # Raw I/O
    # -------------------------------------------------------------------------

    async def read_exactly(self, n: int) -> bytes:
        """
        Read exactly n bytes from the connection.

        Raises:
            ConnectionClosedError: If connection is closed
        """
        if self._reader is None:
            raise ConnectionClosedError("Not connected")

        try:
            data = await self._reader.readexactly(n)
            self._metrics.bytes_received += len(data)
            return data
        except asyncio.IncompleteReadError as exc:
            raise ConnectionClosedError(
                f"Connection closed by server (read {len(exc.partial)} of {n} bytes)",
            ) from None

    async def write(self, data: bytes) -> None:
        """
        Write data to the connection.

        Thread-safe via write lock.

        Raises:
            ConnectionClosedError: If not connected
        """
        if self._writer is None:
            raise ConnectionClosedError("Not connected")

        async with self._write_lock:
            self._writer.write(data)
            await self._writer.drain()
            self._metrics.bytes_sent += len(data)

    # -------------------------------------------------------------------------
    # Protocol Headers
    # -------------------------------------------------------------------------

    async def send_amqp_header(self) -> None:
        """Send AMQP protocol header."""
        await self.write(AMQP_HEADER)

    async def send_sasl_header(self) -> None:
        """Send SASL protocol header."""
        await self.write(SASL_HEADER)

    async def read_protocol_header(self) -> bytes:
        """Read and return protocol header (8 bytes)."""
        return await self.read_exactly(8)

    def validate_amqp_header(self, header: bytes) -> bool:
        """Validate AMQP protocol header."""
        return header == AMQP_HEADER

    def validate_sasl_header(self, header: bytes) -> bool:
        """Validate SASL protocol header."""
        return header == SASL_HEADER

    # -------------------------------------------------------------------------
    # Frame I/O
    # -------------------------------------------------------------------------

    async def read_frame(self) -> tuple[int, Performative]:
        """
        Read a complete AMQP frame.

        Returns:
            Tuple of (channel, performative)

        Raises:
            ConnectionClosedError: If connection is closed
            FrameError: If frame is invalid
        """
        async with self._read_lock:
            if self._reader is None:
                raise ConnectionClosedError("Not connected")

            # Read header first
            try:
                header_data = await self._reader.readexactly(8)
            except asyncio.IncompleteReadError:
                raise ConnectionClosedError("Connection closed by server") from None

            self._metrics.bytes_received += 8

            # Parse header
            size, _doff, _frame_type, channel = struct.unpack(">IBBH", header_data)

            if size < FRAME_HEADER_SIZE:
                raise FrameError(f"Invalid frame size: {size}")

            # Read body
            body_size = size - FRAME_HEADER_SIZE
            if body_size > 0:
                try:
                    payload = await self._reader.readexactly(body_size)
                except asyncio.IncompleteReadError:
                    raise ConnectionClosedError("Connection closed mid-frame") from None

                self._metrics.bytes_received += body_size
            else:
                payload = b""

            # Decode performative
            full_frame = header_data + payload
            performative = bytes_to_performative(full_frame)

            self._metrics.frames_received += 1
            self._metrics.last_read_time = time.monotonic()

            return channel, performative

    async def send_performative(
        self,
        channel: int,
        performative: Performative,
    ) -> None:
        """
        Send a performative frame.

        Args:
            channel: AMQP channel number
            performative: The performative to send
        """
        frame_bytes = performative_to_bytes(performative, channel)

        async with self._write_lock:
            if self._writer is None:
                raise ConnectionClosedError("Not connected")

            self._writer.write(frame_bytes)
            await self._writer.drain()

            self._metrics.bytes_sent += len(frame_bytes)
            self._metrics.frames_sent += 1
            self._metrics.last_write_time = time.monotonic()

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
