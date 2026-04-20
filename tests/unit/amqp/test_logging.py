from __future__ import annotations

from repid.connections.amqp.protocol.events import (
    ConnectionEvent,
    EventData,
    LoggingEventHandler,
)


async def test_logging_event_handler() -> None:
    handler = LoggingEventHandler("test.logger")
    event_data = EventData(ConnectionEvent.CONNECTED, connection_id="conn-1")

    # Should not raise
    handler(event_data)
