"""
AMQP Events and Metrics - Observer pattern for monitoring and observability.

This module provides:
- Event types for connection lifecycle
- Event emitter with async support
- Metrics collection for monitoring
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

from .states import ConnectionState

logger = logging.getLogger(__name__)


# =============================================================================
# Event Types
# =============================================================================


class ConnectionEvent(Enum):
    """Events that can be observed on a connection."""

    # Connection lifecycle
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTING = auto()
    DISCONNECTED = auto()
    RECONNECTING = auto()
    RECONNECTED = auto()

    # State changes
    STATE_CHANGED = auto()
    SESSION_STATE_CHANGED = auto()
    LINK_STATE_CHANGED = auto()

    # I/O events
    FRAME_SENT = auto()
    FRAME_RECEIVED = auto()

    # Errors
    ERROR = auto()
    TRANSPORT_ERROR = auto()
    PROTOCOL_ERROR = auto()

    # SASL
    SASL_START = auto()
    SASL_SUCCESS = auto()
    SASL_FAILED = auto()


# =============================================================================
# Event Data
# =============================================================================


@dataclass
class EventData:
    """Data associated with a connection event."""

    event: ConnectionEvent
    timestamp: float = field(default_factory=time.time)
    connection_id: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def with_details(self, **kwargs: Any) -> EventData:
        """Create a copy with additional details."""
        new_details = {**self.details, **kwargs}
        return EventData(
            event=self.event,
            timestamp=self.timestamp,
            connection_id=self.connection_id,
            details=new_details,
        )


# Type alias for event callbacks
EventCallback = Callable[[EventData], Awaitable[None] | None]


# =============================================================================
# Event Emitter
# =============================================================================


class EventEmitter:
    """
    Async-aware event emitter for connection monitoring.

    Pattern: Observer
    - Decouples monitoring from connection logic
    - Supports both sync and async handlers
    - Enables metrics collection, logging, alerting

    Example:
        emitter = EventEmitter()

        async def log_events(event: EventData) -> None:
            print(f"Event: {event.event.name}")

        emitter.on(ConnectionEvent.CONNECTED, log_events)
        await emitter.emit(EventData(ConnectionEvent.CONNECTED))
    """

    def __init__(self) -> None:
        self._handlers: dict[ConnectionEvent, list[EventCallback]] = {
            event: [] for event in ConnectionEvent
        }
        self._global_handlers: list[EventCallback] = []

    def on(self, event: ConnectionEvent, handler: EventCallback) -> Callable[[], None]:
        """
        Subscribe to a specific event.

        Args:
            event: The event type to listen for
            handler: Callback function (sync or async)

        Returns:
            Unsubscribe function
        """
        self._handlers[event].append(handler)
        return lambda: self._handlers[event].remove(handler)

    def on_all(self, handler: EventCallback) -> Callable[[], None]:
        """
        Subscribe to all events.

        Args:
            handler: Callback function (sync or async)

        Returns:
            Unsubscribe function
        """
        self._global_handlers.append(handler)
        return lambda: self._global_handlers.remove(handler)

    def off(self, event: ConnectionEvent, handler: EventCallback) -> None:
        """
        Unsubscribe from an event.

        Args:
            event: The event type
            handler: The handler to remove
        """
        if handler in self._handlers[event]:
            self._handlers[event].remove(handler)

    def off_all(self, handler: EventCallback) -> None:
        """
        Unsubscribe from all events.

        Args:
            handler: The handler to remove
        """
        if handler in self._global_handlers:
            self._global_handlers.remove(handler)

    async def emit(self, event_data: EventData) -> None:
        """
        Emit an event to all subscribers.

        Calls both specific event handlers and global handlers.
        Handles both sync and async callbacks.
        Errors in handlers are logged but don't stop other handlers.

        Args:
            event_data: The event data to emit
        """
        handlers = self._handlers[event_data.event] + self._global_handlers

        for handler in handlers:
            try:
                result = handler(event_data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception(
                    "Error in event handler for %s",
                    event_data.event.name,
                )

    def emit_sync(self, event_data: EventData) -> None:
        """
        Emit an event synchronously (only sync handlers).

        Useful when called from non-async context.
        Async handlers will be skipped with a warning.

        Args:
            event_data: The event data to emit
        """
        handlers = self._handlers[event_data.event] + self._global_handlers

        for handler in handlers:
            try:
                result = handler(event_data)
                if asyncio.iscoroutine(result):
                    # Can't await in sync context, log warning
                    logger.warning(
                        "Async handler skipped in sync emit: %s",
                        event_data.event.name,
                    )
                    # Create task if running in async context
                    try:
                        loop = asyncio.get_running_loop()
                        task = loop.create_task(result)
                        # Store reference to prevent GC
                        task.add_done_callback(lambda _t: None)
                    except RuntimeError:
                        # No running loop, just close the coroutine
                        result.close()
            except Exception:
                logger.exception(
                    "Error in event handler for %s",
                    event_data.event.name,
                )

    def clear(self) -> None:
        """Remove all handlers."""
        for event in ConnectionEvent:
            self._handlers[event].clear()
        self._global_handlers.clear()


# =============================================================================
# Connection Metrics
# =============================================================================


@dataclass
class ConnectionMetrics:
    """Metrics for connection monitoring."""

    # Counters
    frames_sent: int = 0
    frames_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: int = 0
    reconnections: int = 0

    # Current state
    connection_state: ConnectionState = ConnectionState.START

    # Timestamps
    connected_at: float | None = None
    last_frame_sent_at: float | None = None
    last_frame_received_at: float | None = None
    last_error_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Export metrics as dictionary for monitoring systems."""
        uptime = 0.0
        if self.connected_at is not None:
            uptime = time.time() - self.connected_at

        return {
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "errors": self.errors,
            "reconnections": self.reconnections,
            "connection_state": self.connection_state.name,
            "connected_at": self.connected_at,
            "uptime_seconds": uptime,
            "last_frame_sent_at": self.last_frame_sent_at,
            "last_frame_received_at": self.last_frame_received_at,
            "last_error_at": self.last_error_at,
        }


# =============================================================================
# Metrics Collector
# =============================================================================


class MetricsCollector:
    """
    Collects and exposes connection metrics.

    Pattern: Observer (listener to events)
    - Automatically updates on events
    - Thread-safe metric updates
    - Export-ready for Prometheus, StatsD, etc.

    Example:
        collector = MetricsCollector()
        emitter.on_all(collector.handle_event)

        # Later...
        metrics_dict = collector.metrics.to_dict()
    """

    def __init__(self) -> None:
        self._metrics = ConnectionMetrics()
        self._lock = asyncio.Lock()

    @property
    def metrics(self) -> ConnectionMetrics:
        """Get current metrics (copy for thread safety)."""
        return self._metrics

    async def handle_event(self, event_data: EventData) -> None:
        """
        Event handler that updates metrics.

        Call this from event emitter to automatically track metrics.
        """
        async with self._lock:
            match event_data.event:
                case ConnectionEvent.CONNECTED:
                    self._metrics.connected_at = event_data.timestamp
                case ConnectionEvent.RECONNECTED:
                    self._metrics.reconnections += 1
                    self._metrics.connected_at = event_data.timestamp
                case ConnectionEvent.FRAME_SENT:
                    self._metrics.frames_sent += 1
                    self._metrics.bytes_sent += event_data.details.get("bytes", 0)
                    self._metrics.last_frame_sent_at = event_data.timestamp
                case ConnectionEvent.FRAME_RECEIVED:
                    self._metrics.frames_received += 1
                    self._metrics.bytes_received += event_data.details.get("bytes", 0)
                    self._metrics.last_frame_received_at = event_data.timestamp
                case (
                    ConnectionEvent.ERROR
                    | ConnectionEvent.TRANSPORT_ERROR
                    | ConnectionEvent.PROTOCOL_ERROR
                ):
                    self._metrics.errors += 1
                    self._metrics.last_error_at = event_data.timestamp
                case ConnectionEvent.STATE_CHANGED:
                    state = event_data.details.get("state")
                    if isinstance(state, ConnectionState):
                        self._metrics.connection_state = state
                case ConnectionEvent.DISCONNECTED:
                    self._metrics.connected_at = None

    def reset(self) -> None:
        """Reset all metrics."""
        self._metrics = ConnectionMetrics()


# =============================================================================
# Logging Event Handler
# =============================================================================


class LoggingEventHandler:
    """
    Event handler that logs events.

    Useful for debugging and audit trails.
    """

    def __init__(
        self,
        logger_name: str = "amqp.events",
        level: int = logging.DEBUG,
    ) -> None:
        self._logger = logging.getLogger(logger_name)
        self._level = level

    def __call__(self, event_data: EventData) -> None:
        """Log the event."""
        self._logger.log(
            self._level,
            "[%s] %s: %s",
            event_data.connection_id or "unknown",
            event_data.event.name,
            event_data.details or {},
        )
