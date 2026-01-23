from __future__ import annotations

import datetime
import struct
import uuid
from typing import Any
from unittest.mock import patch

import pytest

from repid.connections.amqp._uamqp._decode import (
    _DECODE_MAP,
    _construct_message,
    bytes_to_performative,
    decode_frame,
    transfer_frames_to_message,
)
from repid.connections.amqp._uamqp._encode import message_to_transfer_frames
from repid.connections.amqp._uamqp.message import Message, MessageBodyType
from repid.connections.amqp._uamqp.performatives import OpenFrame, TransferFrame


def test_message_body_types() -> None:
    # 1. Sequence (Line 237, 247)
    msg_seq = Message(sequence=[1, 2, 3])
    assert msg_seq.body == [1, 2, 3]
    assert msg_seq.body_type == MessageBodyType.SEQUENCE

    # 2. None (Line 240)
    msg_empty = Message()
    assert msg_empty.body is None
    assert msg_empty.body_type == MessageBodyType.EMPTY


def test_decode_primitives() -> None:
    # Helper to decode
    def decode(code: int, payload: bytes) -> Any:
        decoder = _DECODE_MAP[code]
        buffer = memoryview(payload)
        _, value = decoder(buffer)
        return value

    # Null
    assert decode(0x40, b"") is None
    # True
    assert decode(0x41, b"") is True
    # False
    assert decode(0x42, b"") is False

    # Ubyte (0x50)
    assert decode(0x50, b"\x05") == 5
    # Ushort (0x60)
    assert decode(0x60, b"\x00\x05") == 5
    # Uint (0x70)
    assert decode(0x70, b"\x00\x00\x00\x05") == 5
    # Uint Small (0x52)
    assert decode(0x52, b"\x05") == 5
    # Ulong (0x80)
    assert decode(0x80, b"\x00\x00\x00\x00\x00\x00\x00\x05") == 5
    # Ulong Small (0x53)
    assert decode(0x53, b"\x05") == 5

    # Byte (0x51) signed
    assert decode(0x51, b"\xff") == -1
    # Short (0x61)
    assert decode(0x61, b"\xff\xff") == -1


def test_decode_compounds() -> None:
    # Helper to decode
    def decode(code: int, payload: bytes) -> Any:
        decoder = _DECODE_MAP[code]
        buffer = memoryview(payload)
        _, value = decoder(buffer)
        return value

    # List Small (0xC0)
    # size (1) | count (1) | elements...
    # Empty list
    assert decode(0xC0, b"\x01\x00") == []
    # List with one element (null 0x40)
    # size = 1(count)+1(elt) = 2
    assert decode(0xC0, b"\x02\x01\x40") == [None]

    # List Large (0xD0)
    # size (4) | count (4) | elements...
    assert decode(0xD0, b"\x00\x00\x00\x04\x00\x00\x00\x00") == []

    # Map Small (0xC1)
    # size (1) | count (1) | elements (key, value)
    # Empty
    assert decode(0xC1, b"\x01\x00") == {}
    # One item: invalid key 0x40 (null) -> value 0x41 (true)
    # size = 1(count) + 1(k) + 1(v) = 3?
    # Count is "number of elements", i.e. 2 * items
    # So 1 item = count 2.
    assert decode(0xC1, b"\x03\x02\x40\x41") == {None: True}

    # Map Large (0xD1)
    assert decode(0xD1, b"\x00\x00\x00\x04\x00\x00\x00\x00") == {}

    # Array Small (0xE0)
    # size (1) | count (1) | constructor (1) | elements...
    # Empty (count0)
    assert decode(0xE0, b"\x01\x00") == []
    # One item. item type null (0x40).
    # count=1. constructor=0x40.
    # size = 1(count) + 1(cons) + 0(value of null is empty) = 2
    assert decode(0xE0, b"\x02\x01\x40") == [None]

    # Array Large (0xF0)
    # size (4) | count (4) | constructor (1) | elements...
    # Empty
    assert decode(0xF0, b"\x00\x00\x00\x04\x00\x00\x00\x00") == []
    # One item
    assert decode(0xF0, b"\x00\x00\x00\x05\x00\x00\x00\x01\x40") == [None]
    # Int (0x71)
    assert decode(0x71, b"\xff\xff\xff\xff") == -1
    # Int Small (0x54)
    assert decode(0x54, b"\xff") == -1
    # Long (0x81)
    assert decode(0x81, b"\xff" * 8) == -1
    # Long Small (0x55)
    assert decode(0x55, b"\xff") == -1

    # Float (0x72) 32-bit
    assert decode(0x72, struct.pack(">f", 1.23)) == pytest.approx(1.23)
    # Double (0x82) 64-bit
    assert decode(0x82, struct.pack(">d", 1.23)) == pytest.approx(1.23)

    # Timestamp (0x83) 64-bit int ms
    assert decode(0x83, b"\x00\x00\x00\x00\x00\x00\x00\x01") == 1

    # UUID (0x98)
    u = uuid.uuid4()
    assert decode(0x98, u.bytes) == u

    # Binary Small (0xa0)
    assert decode(0xA0, b"\x03ABC") == b"ABC"
    # Binary Large (0xb0)
    assert decode(0xB0, b"\x00\x00\x00\x03ABC") == b"ABC"

    # String Small (0xa1)
    assert decode(0xA1, b"\x03ABC") == "ABC"
    # String Large (0xb1)
    assert decode(0xB1, b"\x00\x00\x00\x03ABC") == "ABC"

    # Symbol Small (0xa3)
    assert decode(0xA3, b"\x03ABC") == "ABC"
    # Symbol Large (0xb3)
    assert decode(0xB3, b"\x00\x00\x00\x03ABC") == "ABC"

    # List Small (0xc0)
    assert decode(0xC0, b"\x04\x01\xa1\x01A") == ["A"]

    # List Large (0xd0)
    assert decode(0xD0, b"\x00\x00\x00\x04" + b"\x00\x00\x00\x01" + b"\xa1\x01A") == ["A"]

    # Map Small (0xc1)
    assert decode(0xC1, b"\x07\x02" + b"\xa1\x01K" + b"\xa1\x01V") == {"K": "V"}

    # Map Large (0xd1)
    assert decode(
        0xD1,
        b"\x00\x00\x00\x0a" + b"\x00\x00\x00\x02" + b"\xa1\x01K" + b"\xa1\x01V",
    ) == {"K": "V"}

    # Array Small (0xe0)
    assert decode(0xE0, b"\x04\x01\xa1\x01A") == ["A"]

    # Array Large (0xf0)
    assert decode(0xF0, b"\x00\x00\x00\x04" + b"\x00\x00\x00\x01" + b"\xa1\x01A") == ["A"]


def test_decode_errors() -> None:
    # Invalid frame body (not 0x00)
    header = b"\x00\x00\x00\x09\x02\x00\x00\x00"
    with pytest.raises(ValueError, match="Invalid frame body"):
        bytes_to_performative(header + b"\x01")

    # Unknown constructor for descriptor (0x00 then 0xFF)
    header_bad_desc = b"\x00\x00\x00\x0a\x02\x00\x00\x00"

    # Mock _DECODE_MAP to return None for 255
    with (
        patch.dict(_DECODE_MAP, {255: None}),
        pytest.raises(
            ValueError,
            match="Unknown constructor: 255",
        ),
    ):
        bytes_to_performative(header_bad_desc + b"\x00\xff")

    # Unknown performative descriptor
    header_bad_perf = b"\x00\x00\x00\x0b\x02\x00\x00\x00"
    with pytest.raises(ValueError, match="Unknown performative"):
        bytes_to_performative(header_bad_perf + b"\x00\x53\x99")

    # Valid performative (OpenFrame 0x10) but unknown list constructor (0xFF)
    header_bad_list = b"\x00\x00\x00\x0c\x02\x00\x00\x00"
    with (
        patch.dict(_DECODE_MAP, {255: None}),
        pytest.raises(
            ValueError,
            match="Unknown constructor: 255",
        ),
    ):
        bytes_to_performative(header_bad_list + b"\x00\x53\x10\xff")

    # Test line 361 in _decode.py (_construct_message value unknown)
    data_bad_val = b"\x00\x53\x75\xff"
    with (
        patch.dict(_DECODE_MAP, {255: None}),
        pytest.raises(
            ValueError,
            match="Unknown constructor: 255",
        ),
    ):
        _construct_message(data_bad_val)


def test_construct_message_coverage() -> None:
    # Invalid message section (not 0x00)
    with pytest.raises(ValueError, match="Invalid message section"):
        _construct_message(b"\x01")

    # Invalid constructor
    with (
        patch.dict(_DECODE_MAP, {255: None}),
        pytest.raises(
            ValueError,
            match="Unknown constructor: 255",
        ),
    ):
        _construct_message(b"\x00\xff")

    # Helper to build section
    def build_section(code: int, value_bytes: bytes) -> bytes:
        return b"\x00" + b"\x53" + struct.pack("B", code) + value_bytes

    # Data (0x75)
    data = build_section(0x75, b"\xa0\x01A")
    msg = _construct_message(data)
    assert msg.data == b"A"

    # Sequence (0x76) - List - Empty \x45
    seq = build_section(0x76, b"\x45")
    msg = _construct_message(seq)
    assert msg.sequence == []

    # Value (0x77) - Any value - ubyte 1
    val = build_section(0x77, b"\x50\x01")
    msg = _construct_message(val)
    assert msg.value == 1

    # Header (0x70) - List
    hdr = build_section(0x70, b"\x45")
    msg = _construct_message(hdr)
    assert msg.header is not None

    # Delivery Annotations (0x71) - Map
    da = build_section(0x71, b"\xc1\x01\x00")
    msg = _construct_message(da)
    assert msg.delivery_annotations == {}

    # Message Annotations (0x72) - Map
    ma = build_section(0x72, b"\xc1\x01\x00")
    msg = _construct_message(ma)
    assert msg.message_annotations == {}

    # Properties (0x73) - List
    prop = build_section(0x73, b"\x45")
    msg = _construct_message(prop)
    assert msg.properties is not None

    # Application Properties (0x74) - Map
    ap = build_section(0x74, b"\xc1\x01\x00")
    msg = _construct_message(ap)
    assert msg.application_properties == {}

    # Footer (0x78) - Map
    ft = build_section(0x78, b"\xc1\x01\x00")
    msg = _construct_message(ft)
    assert msg.footer == {}


def test_decode_described_composite() -> None:
    # Test _decode_described (0x00) logic
    # Helper to decode
    def decode(code: int, payload: bytes) -> Any:
        decoder = _DECODE_MAP[code]
        buffer = memoryview(payload)
        _, value = decoder(buffer)
        return value

    # Case 1: Descriptor NOT in _COMPOSITES (unknown) -> returns value
    # Descriptor: ulong small (0x53) value 100 (0x64)
    # Value: Null (0x40)
    # Payload: 0x53 \x64 0x40
    res = decode(0x00, b"\x53\x64\x40")
    assert res is None

    # Case 2: Descriptor IN _COMPOSITES (known) -> returns {name: value}
    # Descriptor: 36 (accepted) -> 0x24. ulong small (0x53) \x24
    # Value: Null (0x40)
    res = decode(0x00, b"\x53\x24\x40")
    assert res == {"accepted": None}


def test_transfer_frames_to_message_coverage() -> None:
    # 1. Frames with payload
    # Payload must be a valid message section. e.g. Data (0x75) + string "A"
    # Data section: 0x00 (Desc) | 0x53 0x75 (Desc Data) | 0xA0 ... (Binary)
    # Let's just execute it with any payload, as _construct_message will parse it.
    # But _construct_message expects valid sections.
    # Section: 0x00 | 0x53 0x77 (Value) | 0x50 0x01 (ubyte 1)
    payload_chunk = b"\x00\x53\x77\x50\x01"

    frame1 = TransferFrame(handle=0)
    frame1.payload = payload_chunk

    frame2 = TransferFrame(handle=1)  # No payload

    msg = transfer_frames_to_message([frame1, frame2])
    assert msg.value == 1


def test_decode_frame_coverage() -> None:
    # Construct a frame
    # Header: Size(4) Doff(1) Type(1) Channel(2)
    # Doff=2 (8 bytes header)
    # Type=0
    # Channel=1 (0x0001)

    # Body: OpenFrame (16) 0x10.
    # Descriptor: 0x00 (Desc) | 0x53 0x10 (Data: ulong-small 16)
    # Value: List... containing container_id (String).
    # Container ID "cid" (3 chars).

    # List (0xc0): Size(1) Count(1) ...
    # Items: container_id, hostname...
    # We only need container_id for minimal OpenFrame

    # Fields: container_id(0), hostname(1), max_frame_size(2)...
    # encode "cid" -> 0xa1 0x03 c i d

    # List: size=1+1+5 = 7. Count=1.
    # List bytes: 0xc0 0x07 0x01 \xa1\x03cid

    # Descriptor + Value: \x00 \x53 \x10 \xc0 \x07 \x01 \xa1 \x03 c i d

    body = b"\x00\x53\x10\xc0\x07\x01\xa1\x03cid"

    header = struct.pack(">LBBH", len(body) + 8, 2, 0, 1)

    frame_bytes = header + body

    channel, frame = decode_frame(frame_bytes)

    assert channel == 1
    assert isinstance(frame, OpenFrame)
    assert frame.container_id == "cid"


def round_trip(value: Any) -> Any:
    msg = Message(value=value)

    if value is not None:
        # Ensure body_type is VALUE
        assert msg.body_type == MessageBodyType.VALUE
    else:
        # Check empty if none
        assert msg.body_type == MessageBodyType.EMPTY

    frames = list(message_to_transfer_frames(msg, 4096, 0, b"0", True))
    decoded_msg = transfer_frames_to_message(frames)

    if value is not None:
        assert decoded_msg.body_type == MessageBodyType.VALUE

    return decoded_msg.value


def test_amqp_types_none() -> None:
    assert round_trip(None) is None


def test_amqp_types_bool() -> None:
    assert round_trip(True) is True
    assert round_trip(False) is False


def test_amqp_types_ubyte() -> None:
    # Python ints usually encode to smallest fitting type or long
    # We verify that they come back equal
    assert round_trip(10) == 10
    assert round_trip(255) == 255


def test_amqp_types_int_various() -> None:
    values = [
        0,
        1,
        -1,
        127,
        -128,  # Byte
        255,  # UByte
        32767,
        -32768,  # Short
        65535,  # UShort
        2147483647,
        -2147483648,  # Int
        4294967295,  # UInt
        9223372036854775807,
        -9223372036854775808,  # Long
    ]
    for v in values:
        assert round_trip(v) == v


def test_amqp_types_float() -> None:
    assert round_trip(1.5) == 1.5
    assert round_trip(0.0) == 0.0
    assert round_trip(-123.456) == -123.456


def test_amqp_types_string() -> None:
    assert round_trip("hello") == "hello"
    assert round_trip("") == ""
    assert round_trip("🚀") == "🚀"

    # Long string
    long_str = "a" * 1000
    assert round_trip(long_str) == long_str


def test_amqp_types_binary() -> None:
    assert round_trip(b"hello") == b"hello"
    assert round_trip(b"") == b""
    assert round_trip(b"\x00\x01\xff") == b"\x00\x01\xff"


def test_amqp_types_uuid() -> None:
    u = uuid.uuid4()
    assert round_trip(u) == u


def test_amqp_types_timestamp() -> None:
    # Timestamps in AMQP are milliseconds since epoch
    # _encode converts datetime to timestamp
    ts = 1600000000.0
    now = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

    # decode returns the raw integer value (ms since epoch)
    # it does not automatically convert back to datetime
    expected = int(ts * 1000)
    assert round_trip(now) == expected


def test_amqp_types_list() -> None:
    values = [1, "two", 3.0, [4], {"five": 6}]
    assert round_trip(values) == values


def test_amqp_types_map() -> None:
    m = {"key": "value", "int": 1, "list": [1, 2]}
    assert round_trip(m) == m


def test_amqp_types_symbol() -> None:
    # Symbols are not native python type, but might be supported via wrapped types or simple strings
    # _encode_unknown might not infer symbol from string.
    # Usually one needs a wrapper type like amqp.Symbol('name').
    # But current implementation uses native types mostly.
    pass


def test_amqp_types_arrays() -> None:
    # Arrays are lists of same type.
    # Python lists are heterogeneous.
    # _encode_list vs _encode_array.
    # _encode_unknown will likely choose List.
    pass
