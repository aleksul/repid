from __future__ import annotations

from repid.connections.amqp.protocol.events import (
    ConnectionEvent,
    EventData,
    MetricsCollector,
)


async def test_metrics_collector() -> None:
    collector = MetricsCollector()

    await collector.handle_event(EventData(ConnectionEvent.CONNECTED, timestamp=100.0))
    await collector.handle_event(EventData(ConnectionEvent.FRAME_SENT).with_details(bytes=100))
    await collector.handle_event(EventData(ConnectionEvent.FRAME_SENT).with_details(bytes=50))
    await collector.handle_event(EventData(ConnectionEvent.FRAME_RECEIVED).with_details(bytes=200))
    await collector.handle_event(EventData(ConnectionEvent.ERROR))
    await collector.handle_event(EventData(ConnectionEvent.RECONNECTED, timestamp=200.0))

    metrics = collector.metrics
    assert metrics.frames_sent == 2
    assert metrics.bytes_sent == 150
    assert metrics.frames_received == 1
    assert metrics.bytes_received == 200
    assert metrics.errors == 1
    assert metrics.reconnections == 1
    assert metrics.connected_at == 200.0

    # Test to_dict
    metrics_dict = metrics.to_dict()
    assert metrics_dict["frames_sent"] == 2
    assert metrics_dict["connection_state"] == "START"


async def test_metrics_collector_reset() -> None:
    collector = MetricsCollector()

    await collector.handle_event(EventData(ConnectionEvent.FRAME_SENT).with_details(bytes=100))
    assert collector.metrics.frames_sent == 1

    collector.reset()
    assert collector.metrics.frames_sent == 0


async def test_metrics_collector_disconnected() -> None:
    collector = MetricsCollector()

    await collector.handle_event(EventData(ConnectionEvent.CONNECTED, timestamp=100.0))
    assert collector.metrics.connected_at == 100.0

    await collector.handle_event(EventData(ConnectionEvent.DISCONNECTED))
    assert collector.metrics.connected_at is None
