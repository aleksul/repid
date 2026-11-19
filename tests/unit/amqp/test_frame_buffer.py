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
    assert len(buffer) == 11


async def test_frame_buffer_invalid_frame_size() -> None:
    buffer = FrameBuffer(max_size=1024)

    # Declared size 4 is below the 8-byte header; padding lets the header be read at all
    buffer.append(struct.pack(">I", 4) + b"\x00" * 4)
    with pytest.raises(FrameError, match="Invalid frame size"):
        buffer.try_read_frame()


async def test_frame_buffer_frame_too_large() -> None:
    buffer = FrameBuffer(max_size=1024)

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

    # Header declares a 16-byte frame (size=0x10, doff=2, type=0, channel=0), but only
    # 4 of the 8 body bytes have arrived.
    buffer.append(b"\x00\x00\x00\x10\x02\x00\x00\x00\x01\x02\x03\x04")

    assert buffer.try_read_frame() is None
