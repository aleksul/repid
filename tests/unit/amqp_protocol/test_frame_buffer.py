from __future__ import annotations

import struct

import pytest

from repid.connections.amqp.protocol.transport import (
    FrameBuffer,
    FrameError,
)

from .utils import _build_frame_bytes


async def test_frame_buffer_peek() -> None:
    buffer = FrameBuffer()
    buffer.append(b"hello world")

    peeked = buffer.peek(5)
    assert peeked == b"hello"
    # Should not consume
    assert len(buffer) == 11


async def test_frame_buffer_invalid_frame_size() -> None:
    buffer = FrameBuffer(max_size=1024)

    # Frame too small (less than header size of 8)
    # Need at least 8 bytes to read the header
    buffer.append(struct.pack(">I", 4) + b"\x00" * 4)
    with pytest.raises(FrameError, match="Invalid frame size"):
        buffer.try_read_frame()


async def test_frame_buffer_frame_too_large() -> None:
    buffer = FrameBuffer(max_size=1024)

    # Frame too large - need at least 8 bytes to check
    buffer.append(struct.pack(">I", 2048) + b"\x00" * 4)
    with pytest.raises(FrameError, match="Frame too large"):
        buffer.try_read_frame()


def test_frame_buffer_reads_partial_and_complete_frames() -> None:
    buffer = FrameBuffer(max_size=1024)
    frame = _build_frame_bytes()

    buffer.append(frame[:4])
    assert buffer.try_read_frame() is None

    buffer.append(frame[4:])
    read_frame = buffer.try_read_frame()
    assert read_frame == frame
    assert len(buffer) == 0


def test_frame_buffer_rejects_invalid_frame_size() -> None:
    buffer = FrameBuffer(max_size=1024)
    invalid_header = struct.pack(">I B B H", 4, 2, 0, 0)
    buffer.append(invalid_header)

    with pytest.raises(FrameError, match="Invalid frame size"):
        buffer.try_read_frame()


def test_frame_buffer_rejects_overflow() -> None:
    buffer = FrameBuffer(max_size=8)
    with pytest.raises(FrameError, match="Buffer overflow"):
        buffer.append(b"123456789")


async def test_frame_buffer_incomplete_frame() -> None:
    buffer = FrameBuffer(max_size=4096)

    # Add frame header saying frame is 16 bytes, but only provide 12 bytes total
    # Frame size (4 bytes): 0x00000010 = 16
    # DOFF (1 byte): 0x02
    # Type (1 byte): 0x00
    # Channel (2 bytes): 0x0000
    # Then 4 more bytes of payload (but need 8 more to complete 16-byte frame)
    buffer.append(b"\x00\x00\x00\x10\x02\x00\x00\x00\x01\x02\x03\x04")

    # Should return None (incomplete frame)
    assert buffer.try_read_frame() is None
