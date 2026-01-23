from __future__ import annotations

import datetime
import struct
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any
from unittest.mock import patch

import pytest

from repid.connections.amqp._uamqp import _encode as encode
from repid.connections.amqp._uamqp import performatives
from repid.connections.amqp._uamqp._decode import decode_frame, transfer_frames_to_message
from repid.connections.amqp._uamqp._encode import message_to_transfer_frames, performative_to_bytes
from repid.connections.amqp._uamqp.amqptypes import (
    TYPE,
    VALUE,
    AMQPTypes,
    FieldDefinition,
    ObjDefinition,
)
from repid.connections.amqp._uamqp.endpoints import Source
from repid.connections.amqp._uamqp.message import Header, Message, Properties
from repid.connections.amqp._uamqp.performatives import (
    AMQPTAnnotation,
    AttachFrame,
    BeginFrame,
    CloseFrame,
    DetachFrame,
    DispositionFrame,
    EndFrame,
    FlowFrame,
    OpenFrame,
    TransferFrame,
)


def test_performative_encoding_various_types() -> None:
    # Test encoding various performative types
    open_frame = OpenFrame(
        container_id="test-container",
        hostname="localhost",
        max_frame_size=65536,
    )
    encoded = performative_to_bytes(open_frame, 0)
    assert len(encoded) > 0

    begin_frame = BeginFrame(next_outgoing_id=1, incoming_window=5000, outgoing_window=5000)
    encoded = performative_to_bytes(begin_frame, 1)
    assert len(encoded) > 0

    attach_frame = AttachFrame(name="test-link", handle=5, role=True, source=None, target=None)
    encoded = performative_to_bytes(attach_frame, 1)
    assert len(encoded) > 0

    flow_frame = FlowFrame(
        next_incoming_id=10,
        incoming_window=1000,
        next_outgoing_id=5,
        outgoing_window=1000,
        handle=5,
        delivery_count=0,
        link_credit=100,
    )
    encoded = performative_to_bytes(flow_frame, 1)
    assert len(encoded) > 0

    disposition_frame = DispositionFrame(
        role=True,
        first=1,
        last=10,
        settled=True,
        state=None,
        batchable=False,
    )
    encoded = performative_to_bytes(disposition_frame, 1)
    assert len(encoded) > 0

    detach_frame = DetachFrame(handle=5, closed=True, error=None)
    encoded = performative_to_bytes(detach_frame, 1)
    assert len(encoded) > 0

    end_frame = EndFrame(error=None)
    encoded = performative_to_bytes(end_frame, 1)
    assert len(encoded) > 0

    close_frame = CloseFrame(error=None)
    encoded = performative_to_bytes(close_frame, 0)
    assert len(encoded) > 0


def test_decode_frame_round_trip() -> None:
    payload = b"payload"
    msg = Message(data=payload)
    frames = list(
        message_to_transfer_frames(
            message=msg,
            handle=0,
            delivery_id=1,
            delivery_tag=b"1",
            settled=True,
            max_frame_size=512,
        ),
    )

    encoded = performative_to_bytes(frames[0], 0)
    channel, performative = decode_frame(encoded)
    assert channel == 0
    assert isinstance(performative, TransferFrame)

    decoded_msg = transfer_frames_to_message(frames)
    assert decoded_msg.data == payload


def test_encode_null() -> None:
    out = bytearray()
    encode._encode_null(out)
    assert out == b"\x40"


def test_encode_boolean() -> None:
    out = bytearray()
    encode._encode_boolean(out, True)
    assert out == b"\x56\x01"  # 0x56 is boolean, 0x41 is true, 0x42 is false.
    # Wait, _encode_boolean implementation chooses encoding based on with_constructor=True -> 0x56

    out = bytearray()
    encode._encode_boolean(out, True, with_constructor=False)
    assert out == b"\x41"

    out = bytearray()
    encode._encode_boolean(out, False, with_constructor=False)
    assert out == b"\x42"


def test_encode_ubyte() -> None:
    out = bytearray()
    encode._encode_ubyte(out, 255)
    assert out == b"\x50\xff"

    # Error case
    with pytest.raises(ValueError, match="Unsigned byte value"):
        encode._encode_ubyte(bytearray(), 256)

    # Bytes input coverage
    out = bytearray()
    encode._encode_ubyte(out, b"a")  # ord('a') = 97
    assert out == b"\x50\x61"


def test_encode_ushort() -> None:
    out = bytearray()
    encode._encode_ushort(out, 65535)
    assert out == b"\x60\xff\xff"

    with pytest.raises(ValueError, match="Unsigned byte value"):
        encode._encode_ushort(out, 70000)


def test_encode_uint() -> None:
    out = bytearray()
    encode._encode_uint(out, 0)
    assert out == b"\x43"  # uint0

    out = bytearray()
    encode._encode_uint(out, 10, use_smallest=True)
    assert out == b"\x52\x0a"  # smalluint

    out = bytearray()
    encode._encode_uint(out, 10, use_smallest=False)
    assert out == b"\x70\x00\x00\x00\x0a"  # uint

    out = bytearray()
    encode._encode_uint(out, 300)
    assert out == b"\x70\x00\x00\x01\x2c"

    with pytest.raises(ValueError, match="Value supplied for unsigned int invalid"):
        encode._encode_uint(bytearray(), 2**32)


def test_encode_ulong() -> None:
    out = bytearray()
    encode._encode_ulong(out, 0)
    assert out == b"\x44"  # ulong0

    out = bytearray()
    encode._encode_ulong(out, 10, use_smallest=True)
    assert out == b"\x53\x0a"

    out = bytearray()
    encode._encode_ulong(out, 10, use_smallest=False)
    assert out == b"\x80\x00\x00\x00\x00\x00\x00\x00\x0a"

    with pytest.raises(ValueError, match="unsigned long invalid"):
        encode._encode_ulong(bytearray(), 2**64)


def test_encode_byte() -> None:
    out = bytearray()
    encode._encode_byte(out, -1)
    assert out == b"\x51\xff"

    with pytest.raises(ValueError, match="Byte value must be -128-127"):
        encode._encode_byte(bytearray(), 128)


def test_encode_short() -> None:
    out = bytearray()
    encode._encode_short(out, -1)
    assert out == b"\x61\xff\xff"

    with pytest.raises(ValueError, match="Short value must be -32768-32767"):
        encode._encode_short(bytearray(), 40000)


def test_encode_int() -> None:
    out = bytearray()
    encode._encode_int(out, 10, use_smallest=True)
    assert out == b"\x54\x0a"  # smallint

    out = bytearray()
    encode._encode_int(out, 10, use_smallest=False)
    assert out == b"\x71\x00\x00\x00\x0a"

    with pytest.raises(ValueError, match="int invalid"):
        encode._encode_int(bytearray(), 2**31)


def test_encode_long() -> None:
    out = bytearray()
    encode._encode_long(out, 10, use_smallest=True)
    assert out == b"\x55\x0a"

    out = bytearray()
    dt = datetime.datetime.fromtimestamp(100, datetime.timezone.utc)
    # 100 * 1000 = 100000 ms
    encode._encode_long(out, dt, use_smallest=False)
    # Should convert dt to int ms
    assert len(out) > 0

    with pytest.raises(ValueError, match="long invalid"):
        encode._encode_long(bytearray(), 2**63)


def test_encode_float() -> None:
    out = bytearray()
    encode._encode_float(out, 1.0)
    assert out == b"\x72\x3f\x80\x00\x00"


def test_encode_double() -> None:
    out = bytearray()
    encode._encode_double(out, 1.0)
    assert out == b"\x82\x3f\xf0\x00\x00\x00\x00\x00\x00"


def test_encode_timestamp() -> None:
    out = bytearray()
    encode._encode_timestamp(out, 1000)
    assert out == b"\x83\x00\x00\x00\x00\x00\x00\x03\xe8"

    out = bytearray()
    dt = datetime.datetime.fromtimestamp(100, datetime.timezone.utc)
    encode._encode_timestamp(out, dt)
    assert len(out) == 9


def test_encode_uuid() -> None:
    u = uuid.uuid4()
    out = bytearray()
    encode._encode_uuid(out, u)
    assert out[0] == 0x98
    assert len(out) == 17

    # str
    out = bytearray()
    encode._encode_uuid(out, str(u))
    assert out[0] == 0x98

    # bytes
    out = bytearray()
    encode._encode_uuid(out, u.bytes)
    assert out[0] == 0x98

    with pytest.raises(TypeError):
        encode._encode_uuid(bytearray(), 123)  # type: ignore[arg-type]


def test_encode_binary() -> None:
    out = bytearray()
    encode._encode_binary(out, b"123", use_smallest=True)
    assert out == b"\xa0\x03123"

    out = bytearray()
    encode._encode_binary(out, b"123", use_smallest=False)
    assert out == b"\xb0\x00\x00\x00\x03123"

    # Mock large binary failure
    with (
        patch("struct.pack", side_effect=struct.error),
        pytest.raises(
            ValueError,
            match="Binary data to long to encode",
        ),
    ):
        # Force large path
        encode._encode_binary(bytearray(), b"x" * 300)


def test_encode_string() -> None:
    out = bytearray()
    encode._encode_string(out, "abc", use_smallest=True)
    assert out == b"\xa1\x03abc"

    out = bytearray()
    encode._encode_string(out, "abc", use_smallest=False)
    assert out == b"\xb1\x00\x00\x00\x03abc"

    with (
        patch("struct.pack", side_effect=struct.error),
        pytest.raises(
            ValueError,
            match=r"String value too long to encode\.",
        ),
    ):
        encode._encode_string(bytearray(), "x" * 300)


def test_encode_symbol() -> None:
    out = bytearray()
    encode._encode_symbol(out, "abc", use_smallest=True)
    assert out == b"\xa3\x03abc"

    out = bytearray()
    encode._encode_symbol(out, "abc", use_smallest=False)
    assert out == b"\xb3\x00\x00\x00\x03abc"

    with (
        patch("struct.pack", side_effect=struct.error),
        pytest.raises(
            ValueError,
            match=r"Symbol value too long to encode\.",
        ),
    ):
        encode._encode_symbol(bytearray(), "x" * 300)


def test_encode_list() -> None:
    out = bytearray()
    encode._encode_list(out, [], use_smallest=True)
    assert out == b"\x45"

    items = [1, 2]
    out = bytearray()
    encode._encode_list(out, items, use_smallest=True)
    assert out[0] == 0xC0

    out = bytearray()
    encode._encode_list(out, items, use_smallest=False)
    assert out[0] == 0xD0

    with (
        patch(
            "repid.connections.amqp._uamqp._encode.struct.pack",
            side_effect=struct.error,
        ),
        pytest.raises(ValueError, match=r".+"),
    ):
        # Force large path then fail
        encode._encode_list(bytearray(), [1] * 300)


def test_encode_map() -> None:
    m = {"a": 1}
    out = bytearray()
    encode._encode_map(out, m, use_smallest=True)
    assert out[0] == 0xC1

    out = bytearray()
    encode._encode_map(out, m, use_smallest=False)
    assert out[0] == 0xD1

    # Iterable input
    out = bytearray()
    encode._encode_map(out, [("a", 1)], use_smallest=True)
    assert out[0] == 0xC1

    original_pack = struct.pack

    with patch("repid.connections.amqp._uamqp._encode.struct.pack") as mock_pack:

        def side_effect(fmt: str, *args: Any) -> bytes:
            if fmt == ">L" and len(args) == 1 and args[0] >= 4:  # Map header packing
                raise struct.error("Map too big")
            return original_pack(fmt, *args)

        mock_pack.side_effect = side_effect

        with pytest.raises(ValueError, match="Map is too large"):
            # Force large path
            encode._encode_map(bytearray(), {"a": 1}, use_smallest=False)


def test_encode_array() -> None:
    items = [1, 2]
    out = bytearray()
    encode._encode_array(out, items, use_smallest=True)
    assert out[0] == 0xE0

    out = bytearray()
    encode._encode_array(out, items, use_smallest=False)
    assert out[0] == 0xF0

    with pytest.raises(TypeError, match="All elements in an array must be the same type"):
        encode._encode_array(bytearray(), [1, "a"])

    # Test mixed types where both have "TYPE" (hit line 560)
    # Type check logic: item1=1 (int), item2 (dict).
    # item2["TYPE"] mismatch int -> raise TypeErr.
    # Caught. isinstance(item2, int) -> False.
    item1 = 1
    item2 = {"TYPE": "FLOAT", "value": 2.0}
    with pytest.raises(TypeError, match="All elements in an array must be the same type"):
        encode._encode_array(bytearray(), [item1, item2])

    with (
        patch(
            "repid.connections.amqp._uamqp._encode.struct.pack",
            side_effect=struct.error,
        ),
        pytest.raises(ValueError, match=r".+"),
    ):
        encode._encode_array(bytearray(), [1], use_smallest=False)

    out = bytearray()
    encode._encode_array(out, [None])
    assert out[0] == 0xE0


def test_encode_unknown() -> None:
    out = bytearray()
    encode._encode_unknown(out, None)
    assert out == b"\x40"

    with pytest.raises(TypeError, match="Unable to encode unknown value"):
        encode._encode_unknown(bytearray(), object())

    # Branch coverage for int logic
    out = bytearray()
    encode._encode_unknown(out, 123)  # int
    assert out[0] == 0x54  # smallint

    out = bytearray()
    encode._encode_unknown(out, 2**40)  # large int -> long
    assert out[0] == 0x81  # long


def test_encode_performative_body() -> None:
    perf = performatives.TransferFrame(handle=1)
    out = encode._encode_performative_body(perf)
    assert len(out) > 0


def test_encode_array_with_type_hint() -> None:
    # Test _check_element_type with item having TYPE

    # Item with TYPE
    item1 = {TYPE: AMQPTypes.int.value, VALUE: 1}
    item2 = {TYPE: AMQPTypes.int.value, VALUE: 2}

    arr = [item1, item2]
    out = bytearray()
    encode._encode_array(out, arr)
    assert len(out) > 0
    assert out[0] in (0xE0, 0xF0)


def test_encode_performative_body_large() -> None:
    # Trigger list_large in performative body

    @dataclass
    class LargePerformative:
        CODE: int = 0
        FRAME_TYPE: bytes = b"\x00"
        FRAME_OFFSET: bytes = b"\x00"

        f1: Annotated[bytes, AMQPTAnnotation(AMQPTypes.binary)] = b""

    # 300 bytes > 255 -> list_large
    p = LargePerformative(f1=b"x" * 300)
    out = encode._encode_performative_body(p)
    assert len(out) > 300
    assert out[2] == 0xD0


def test_encode_unknown_coverage() -> None:
    # Ensure we hit branches in _encode_unknown
    # bytes
    out = bytearray()
    encode._encode_unknown(out, b"123")
    assert out[0] in (0xA0, 0xB0)

    # tuple
    out = bytearray()
    encode._encode_unknown(out, (0x00, "val"))
    assert out[0] == 0x00

    # float
    out = bytearray()
    encode._encode_unknown(out, 1.23)
    assert out[0] == 0x82


class DummyEnum(Enum):
    Unknown = "unknown"


def test_encode_performative_body_complex() -> None:
    @dataclass
    class FakePerformative:
        CODE: int = 0
        FRAME_TYPE: bytes = b"\x00"
        FRAME_OFFSET: bytes = b"\x00"

        # Use FieldDefinition
        f1: Annotated[int, AMQPTAnnotation(FieldDefinition.handle)] = 1
        # Use ObjDefinition
        f2: Annotated[Source | None, AMQPTAnnotation(ObjDefinition.source)] = None

    p = FakePerformative(f1=1, f2=Source(address="addr"))
    out = encode._encode_performative_body(p)
    assert len(out) > 0

    # Now test failure with unknown type
    @dataclass
    class BadPerformative:
        CODE: int = 0
        FRAME_TYPE: bytes = b"\x00"
        FRAME_OFFSET: bytes = b"\x00"

        f1: Annotated[int, AMQPTAnnotation(DummyEnum.Unknown)] = 1

    p_bad = BadPerformative()
    with pytest.raises(ValueError, match="No encoder for type"):
        encode._encode_performative_body(p_bad)


def test_message_to_transfer_frames_full() -> None:
    msg = Message()
    msg.header = Header(durable=True)
    msg.delivery_annotations = {"da": 1}
    msg.message_annotations = {"ma": 1}
    msg.properties = Properties(message_id="1")
    msg.application_properties = {"ap": 1}
    msg.footer = {"footer": 1}
    msg.data = b"data"

    frames = list(encode.message_to_transfer_frames(msg, max_frame_size=4096, handle=1))
    assert len(frames) >= 1


def test_message_to_transfer_frames_split() -> None:
    # Test splitting message into multiple frames
    msg = Message(data=b"x" * 1000)
    frames = list(encode.message_to_transfer_frames(msg, max_frame_size=200, handle=1))
    assert len(frames) > 1
    assert frames[0].more is True
    assert frames[-1].more is False


def test_message_to_transfer_frames() -> None:
    msg = Message()
    msg.data = b"data"

    frames = list(encode.message_to_transfer_frames(msg, max_frame_size=1024, handle=1))
    assert len(frames) >= 1

    msg = Message(sequence=[1, 2])
    frames = list(encode.message_to_transfer_frames(msg, max_frame_size=1024, handle=1))

    msg = Message(value="val")
    frames = list(encode.message_to_transfer_frames(msg, max_frame_size=1024, handle=1))

    # Test small frame size exception
    with pytest.raises(ValueError, match="Frame size too small"):
        list(encode.message_to_transfer_frames(msg, max_frame_size=10, handle=1))


def test_performative_to_bytes() -> None:
    perf = performatives.OpenFrame(container_id="test")
    data = encode.performative_to_bytes(perf)
    assert len(data) > 8
