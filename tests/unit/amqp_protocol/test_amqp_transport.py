from __future__ import annotations

import asyncio
import struct
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from repid.connections.amqp._uamqp._encode import performative_to_bytes
from repid.connections.amqp._uamqp.performatives import (
    CloseFrame,
    EmptyFrame,
    OpenFrame,
)
from repid.connections.amqp.protocol.events import (
    EventEmitter,
)
from repid.connections.amqp.protocol.transport import (
    AMQP_HEADER,
    SASL_HEADER,
    AmqpTransport,
    ConnectionClosedError,
    ConnectionTimeoutError,
    FrameError,
    TransportConfig,
    TransportError,
)

from .utils import FakeStreamReader, FakeStreamWriter


async def test_amqp_transport_read_exactly_not_connected() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    with pytest.raises(ConnectionClosedError, match="Not connected"):
        await transport.read_exactly(8)


async def test_amqp_transport_write_not_connected() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    with pytest.raises(ConnectionClosedError, match="Not connected"):
        await transport.write(b"data")


async def test_amqp_transport_read_frame_not_connected() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    with pytest.raises(ConnectionClosedError, match="Not connected"):
        await transport.read_frame()


async def test_amqp_transport_send_performative_not_connected() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    with pytest.raises(ConnectionClosedError, match="Not connected"):
        await transport.send_performative(channel=0, performative=OpenFrame(container_id="test"))


def test_amqp_transport_validate_amqp_header() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Valid AMQP header
    assert transport.validate_amqp_header(AMQP_HEADER) is True

    # Invalid header
    assert transport.validate_amqp_header(b"INVALID\x00") is False


def test_amqp_transport_validate_sasl_header() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Valid SASL header
    assert transport.validate_sasl_header(SASL_HEADER) is True

    # Invalid header
    assert transport.validate_sasl_header(b"INVALID\x00") is False


async def test_amqp_transport_context_manager() -> None:
    config = TransportConfig(host="localhost", port=5672, connect_timeout=0.01)

    # Test __aenter__ and __aexit__ paths
    # Since we can't actually connect to localhost:5672, this will raise TransportError
    # but it will still exercise the context manager code
    with pytest.raises((ConnectionTimeoutError, TransportError, OSError)):
        async with AmqpTransport(config):
            pass


async def test_amqp_transport_context_manager_success(monkeypatch: Any) -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Mock the reader and writer to simulate successful connection
    mock_reader = AsyncMock()
    mock_writer = MagicMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()

    # Mock connect to set reader/writer
    async def mock_connect() -> None:
        transport._reader = mock_reader
        transport._writer = mock_writer

    monkeypatch.setattr(transport, "connect", mock_connect)

    # Test context manager
    async with transport as t:
        assert t is transport
        assert transport._reader is mock_reader
        assert transport._writer is mock_writer

    # After exit, close should have been called
    mock_writer.close.assert_called_once()
    mock_writer.wait_closed.assert_called_once()


async def test_amqp_transport_read_frame_incomplete_header() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Mock reader that raises IncompleteReadError
    mock_reader = AsyncMock()
    mock_reader.readexactly.side_effect = asyncio.IncompleteReadError(b"", 8)

    transport._reader = mock_reader

    with pytest.raises(ConnectionClosedError, match="Connection closed by server"):
        await transport.read_frame()


async def test_amqp_transport_read_frame_invalid_size() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Mock reader that returns header with invalid size (less than 8)
    mock_reader = AsyncMock()
    # Frame size = 7 (invalid, must be >= 8)
    mock_reader.readexactly.return_value = struct.pack(">IBBH", 7, 2, 0, 0)

    transport._reader = mock_reader

    with pytest.raises(FrameError, match="Invalid frame size"):
        await transport.read_frame()


async def test_amqp_transport_read_frame_incomplete_body() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Mock reader
    mock_reader = AsyncMock()
    # First call returns valid header (frame size = 16)
    # Second call raises IncompleteReadError when reading body
    mock_reader.readexactly.side_effect = [
        struct.pack(">IBBH", 16, 2, 0, 0),  # Header
        asyncio.IncompleteReadError(b"", 8),  # Body incomplete
    ]

    transport._reader = mock_reader

    with pytest.raises(ConnectionClosedError, match="Connection closed mid-frame"):
        await transport.read_frame()


async def test_amqp_transport_read_frame_empty_body() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Create a valid OPEN frame
    open_frame = OpenFrame(container_id="test")
    frame_bytes = performative_to_bytes(open_frame, channel=0)

    # Mock reader to return the frame
    mock_reader = AsyncMock()
    header = frame_bytes[:8]
    body = frame_bytes[8:]

    mock_reader.readexactly.side_effect = [
        header,
        body,
    ]

    transport._reader = mock_reader

    # Should successfully read the frame
    channel, performative = await transport.read_frame()
    assert channel == 0
    assert isinstance(performative, OpenFrame)


async def test_amqp_transport_read_frame_header_only() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)

    # Create header indicating size=8 (header only, no body)
    # This will trigger the else branch where payload = b""
    mock_reader = AsyncMock()
    mock_reader.readexactly.return_value = struct.pack(">IBBH", 8, 2, 0, 0)

    transport._reader = mock_reader

    # Should return EmptyFrame
    channel, performative = await transport.read_frame()
    assert channel == 0
    assert isinstance(performative, EmptyFrame)


async def test_transport_properties() -> None:
    config = TransportConfig(host="example.com", port=5672)
    transport = AmqpTransport(config)

    assert transport.host == "example.com"
    assert transport.port == 5672
    assert transport.metrics is not None


async def test_transport_set_events() -> None:
    config = TransportConfig(host="localhost", port=5672)
    transport = AmqpTransport(config)
    emitter = EventEmitter()

    transport.set_events(emitter)
    assert transport._events == emitter


async def test_transport_connect_timeout() -> None:
    config = TransportConfig(host="192.0.2.1", port=5672, connect_timeout=0.01)
    transport = AmqpTransport(config)

    with pytest.raises(ConnectionTimeoutError, match="Connection timed out"):
        await transport.connect()


async def test_transport_connect_error() -> None:
    # Use invalid host
    config = TransportConfig(
        host="invalid.host.that.does.not.exist",
        port=5672,
        connect_timeout=1.0,
    )
    transport = AmqpTransport(config)

    with pytest.raises(TransportError, match="Could not connect"):
        await transport.connect()


async def test_transport_connect_read_write_and_decode(monkeypatch: Any) -> None:
    writer = FakeStreamWriter()
    frame = performative_to_bytes(OpenFrame(container_id="x"), 1)
    # read_protocol_header reads 8 bytes, then read_frame reads the full frame
    # So we need: 8 bytes for protocol header + full frame
    reader = FakeStreamReader(bytearray(AMQP_HEADER + frame))

    async def fake_open_connection(*_args: Any, **_kwargs: Any) -> tuple[Any, Any]:
        return reader, writer

    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)

    transport = AmqpTransport(TransportConfig(host="localhost", port=5672))
    await transport.connect()

    await transport.send_amqp_header()
    assert writer.data == AMQP_HEADER
    await transport.send_sasl_header()
    assert writer.data.endswith(SASL_HEADER)

    header = await transport.read_protocol_header()
    assert header == AMQP_HEADER

    channel, performative = await transport.read_frame()
    assert channel == 1
    assert isinstance(performative, OpenFrame)

    await transport.send_performative(1, CloseFrame())
    assert transport.metrics.frames_sent == 1

    await transport.close()
    assert transport.is_connected is False


async def test_transport_errors(monkeypatch: Any) -> None:
    async def fake_timeout(*_args: Any, **_kwargs: Any) -> tuple[Any, Any]:
        raise asyncio.TimeoutError

    monkeypatch.setattr(asyncio, "open_connection", fake_timeout)

    transport = AmqpTransport(TransportConfig(host="localhost", port=5672, connect_timeout=0.01))
    with pytest.raises(ConnectionTimeoutError):
        await transport.connect()

    with (
        patch.object(transport, "_reader", FakeStreamReader(bytearray(b"abc"))),
        pytest.raises(ConnectionClosedError),
    ):
        await transport.read_exactly(10)


def test_amqp_transport_keepalive_linux() -> None:
    config = TransportConfig(
        host="localhost",
        port=5672,
        tcp_keepalive_idle=60,
        tcp_keepalive_interval=10,
        tcp_keepalive_count=3,
    )
    transport = AmqpTransport(config)

    mock_socket = Mock()
    mock_writer = FakeStreamWriter()

    with (
        patch.object(transport, "_writer", mock_writer),
        patch.object(mock_writer, "get_extra_info", Mock(return_value=mock_socket)),
        patch("sys.platform", "linux"),
        # We need to ensure socket constants exist if they don't on the running platform
        patch("socket.SOL_SOCKET", 1, create=True),
        patch("socket.SO_KEEPALIVE", 2, create=True),
        patch("socket.IPPROTO_TCP", 6, create=True),
        patch("socket.TCP_KEEPIDLE", 4, create=True),
        patch("socket.TCP_KEEPINTVL", 5, create=True),
        patch("socket.TCP_KEEPCNT", 6, create=True),
    ):
        transport._configure_keepalive()

        assert mock_socket.setsockopt.call_count >= 4


def test_amqp_transport_keepalive_darwin() -> None:
    config = TransportConfig(
        host="localhost",
        port=5672,
        tcp_keepalive_idle=60,
    )
    transport = AmqpTransport(config)

    mock_socket = Mock()
    mock_writer = FakeStreamWriter()

    with (
        patch.object(transport, "_writer", mock_writer),
        patch.object(mock_writer, "get_extra_info", Mock(return_value=mock_socket)),
        patch("sys.platform", "darwin"),
        patch("socket.SOL_SOCKET", 1, create=True),
        patch("socket.SO_KEEPALIVE", 2, create=True),
        patch("socket.IPPROTO_TCP", 6, create=True),
        patch("socket.TCP_KEEPALIVE", 16, create=True),
    ):
        transport._configure_keepalive()

        assert mock_socket.setsockopt.call_count >= 2


def test_amqp_transport_keepalive_no_socket() -> None:
    transport = AmqpTransport(TransportConfig(host="localhost", port=5672))
    mock_writer = FakeStreamWriter()

    with (
        patch.object(transport, "_writer", mock_writer),
        patch.object(mock_writer, "get_extra_info", Mock(return_value=None)),
    ):
        transport._configure_keepalive()
