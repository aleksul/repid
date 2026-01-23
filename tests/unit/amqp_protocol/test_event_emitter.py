from __future__ import annotations

import threading

from repid.connections.amqp.protocol.events import (
    ConnectionEvent,
    EventData,
    EventEmitter,
    MetricsCollector,
)


async def test_event_emitter_off_all() -> None:
    emitter = EventEmitter()
    called = []

    def handler(data: EventData) -> None:
        called.append(data)

    emitter.on_all(handler)
    emitter.emit_sync(EventData(ConnectionEvent.CONNECTED))
    assert len(called) == 1

    emitter.off_all(handler)
    emitter.emit_sync(EventData(ConnectionEvent.CONNECTED))
    # Should still be 1 since handler was removed
    assert len(called) == 1


async def test_event_emitter_async_handler_error() -> None:
    emitter = EventEmitter()

    async def bad_handler(_data: EventData) -> None:
        raise ValueError("Test error")

    emitter.on(ConnectionEvent.CONNECTED, bad_handler)

    # Should not raise despite handler error
    await emitter.emit(EventData(ConnectionEvent.CONNECTED))


async def test_event_emitter_sync_handler_error() -> None:
    emitter = EventEmitter()

    def bad_handler(_data: EventData) -> None:
        raise ValueError("Test error")

    emitter.on(ConnectionEvent.CONNECTED, bad_handler)

    # Should not raise despite handler error
    emitter.emit_sync(EventData(ConnectionEvent.CONNECTED))


async def test_event_emitter_clear() -> None:
    emitter = EventEmitter()
    called = []

    def handler(data: EventData) -> None:
        called.append(data)

    emitter.on(ConnectionEvent.CONNECTED, handler)
    emitter.on_all(handler)

    emitter.clear()

    emitter.emit_sync(EventData(ConnectionEvent.CONNECTED))
    # Should be empty since all handlers were cleared
    assert len(called) == 0


async def test_event_emitter_sync_with_no_running_loop() -> None:
    emitter = EventEmitter()
    called = []

    async def async_handler(data: EventData) -> None:
        called.append(data)

    emitter.on(ConnectionEvent.CONNECTED, async_handler)

    # emit_sync should handle async handler (closes coroutine)
    emitter.emit_sync(EventData(ConnectionEvent.CONNECTED))
    # Called should be empty since coroutine was closed
    assert len(called) == 0


async def test_event_emitter_coroutine_without_loop() -> None:
    emitter = EventEmitter()

    async def async_handler(_event_data: EventData) -> None:
        pass

    emitter.on(ConnectionEvent.CONNECTED, async_handler)

    # emit_sync should close coroutine when no loop is running
    # Run in a thread to avoid having an event loop
    result = []

    def run_in_thread() -> None:
        try:
            emitter.emit_sync(EventData(ConnectionEvent.CONNECTED))
            result.append("success")
        except Exception as e:
            result.append(f"error: {e}")

    thread = threading.Thread(target=run_in_thread)
    thread.start()
    thread.join()

    assert result == ["success"]


async def test_event_emitter_and_metrics() -> None:
    emitter = EventEmitter()
    collector = MetricsCollector()
    emitter.on_all(collector.handle_event)

    called: list[str] = []

    def sync_handler(_event: EventData) -> None:
        called.append("sync")

    emitter.on(ConnectionEvent.CONNECTED, sync_handler)

    await emitter.emit(EventData(event=ConnectionEvent.CONNECTED))
    await emitter.emit(EventData(event=ConnectionEvent.FRAME_SENT, details={"bytes": 10}))
    await emitter.emit(EventData(event=ConnectionEvent.FRAME_RECEIVED, details={"bytes": 8}))
    await emitter.emit(EventData(event=ConnectionEvent.ERROR))
    await emitter.emit(EventData(event=ConnectionEvent.DISCONNECTED))

    assert called == ["sync"]
    assert collector.metrics.frames_sent == 1
    assert collector.metrics.frames_received == 1
    assert collector.metrics.bytes_sent == 10
    assert collector.metrics.bytes_received == 8
    assert collector.metrics.errors == 1


async def test_event_data_with_details() -> None:
    event_data = EventData(ConnectionEvent.CONNECTED)
    new_data = event_data.with_details(host="localhost", port=5672)

    assert new_data.details["host"] == "localhost"
    assert new_data.details["port"] == 5672
    assert new_data.event == ConnectionEvent.CONNECTED
