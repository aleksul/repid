from __future__ import annotations

import logging

import pytest

from repid.connections.amqp.protocol.events import (
    ConnectionEvent,
    EventData,
    LoggingEventHandler,
)


def test_logging_event_handler_formats_connection_event(caplog: pytest.LogCaptureFixture) -> None:
    handler = LoggingEventHandler("test.logger")
    event_data = EventData(ConnectionEvent.CONNECTED, connection_id="conn-1")

    with caplog.at_level(logging.DEBUG, logger="test.logger"):
        handler(event_data)

    assert caplog.record_tuples == [("test.logger", logging.DEBUG, "[conn-1] CONNECTED: {}")]
