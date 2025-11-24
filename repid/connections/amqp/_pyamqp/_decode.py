from __future__ import annotations

import struct
import uuid
from collections.abc import Callable
from dataclasses import fields
from typing import Any, Literal, TypeVar, cast

from . import performatives
from .message import Header, Message, Properties

_COMPOSITES = {
    35: "received",
    36: "accepted",
    37: "rejected",
    38: "released",
    39: "modified",
}

c_unsigned_char = struct.Struct(">B")
c_signed_char = struct.Struct(">b")
c_unsigned_short = struct.Struct(">H")
c_signed_short = struct.Struct(">h")
c_unsigned_int = struct.Struct(">I")
c_signed_int = struct.Struct(">i")
c_unsigned_long = struct.Struct(">L")
c_unsigned_long_long = struct.Struct(">Q")
c_signed_long_long = struct.Struct(">q")
c_float = struct.Struct(">f")
c_double = struct.Struct(">d")


def _decode_null(buffer: memoryview) -> tuple[memoryview, None]:
    return buffer, None


def _decode_true(buffer: memoryview) -> tuple[memoryview, Literal[True]]:
    return buffer, True


def _decode_false(buffer: memoryview) -> tuple[memoryview, Literal[False]]:
    return buffer, False


def _decode_zero(buffer: memoryview) -> tuple[memoryview, Literal[0]]:
    return buffer, 0


def _decode_empty(buffer: memoryview) -> tuple[memoryview, list[Any]]:
    return buffer, []


def _decode_boolean(buffer: memoryview) -> tuple[memoryview, bool]:
    return buffer[1:], buffer[:1] == b"\x01"


def _decode_ubyte(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[1:], buffer[0]


def _decode_ushort(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[2:], c_unsigned_short.unpack(buffer[:2])[0]


def _decode_uint_small(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[1:], buffer[0]


def _decode_uint_large(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[4:], c_unsigned_int.unpack(buffer[:4])[0]


def _decode_ulong_small(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[1:], buffer[0]


def _decode_ulong_large(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[8:], c_unsigned_long_long.unpack(buffer[:8])[0]


def _decode_byte(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[1:], c_signed_char.unpack(buffer[:1])[0]


def _decode_short(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[2:], c_signed_short.unpack(buffer[:2])[0]


def _decode_int_small(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[1:], c_signed_char.unpack(buffer[:1])[0]


def _decode_int_large(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[4:], c_signed_int.unpack(buffer[:4])[0]


def _decode_long_small(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[1:], c_signed_char.unpack(buffer[:1])[0]


def _decode_long_large(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[8:], c_signed_long_long.unpack(buffer[:8])[0]


def _decode_float(buffer: memoryview) -> tuple[memoryview, float]:
    return buffer[4:], c_float.unpack(buffer[:4])[0]


def _decode_double(buffer: memoryview) -> tuple[memoryview, float]:
    return buffer[8:], c_double.unpack(buffer[:8])[0]


def _decode_timestamp(buffer: memoryview) -> tuple[memoryview, int]:
    return buffer[8:], c_signed_long_long.unpack(buffer[:8])[0]


def _decode_uuid(buffer: memoryview) -> tuple[memoryview, uuid.UUID]:
    return buffer[16:], uuid.UUID(bytes=buffer[:16].tobytes())


def _decode_binary_small(buffer: memoryview) -> tuple[memoryview, bytes]:
    length_index = buffer[0] + 1
    return buffer[length_index:], buffer[1:length_index].tobytes()


def _decode_binary_large(buffer: memoryview) -> tuple[memoryview, bytes]:
    length_index = c_unsigned_long.unpack(buffer[:4])[0] + 4
    return buffer[length_index:], buffer[4:length_index].tobytes()


def _decode_list_small(buffer: memoryview) -> tuple[memoryview, list[Any]]:
    count = buffer[1]
    buffer = buffer[2:]
    values = [None] * count
    for i in range(count):
        buffer, values[i] = _DECODE_MAP[buffer[0]](buffer[1:])
    return buffer, values


def _decode_list_large(buffer: memoryview) -> tuple[memoryview, list[Any]]:
    count = c_unsigned_long.unpack(buffer[4:8])[0]
    buffer = buffer[8:]
    values = [None] * count
    for i in range(count):
        buffer, values[i] = _DECODE_MAP[buffer[0]](buffer[1:])
    return buffer, values


def _decode_map_small(buffer: memoryview) -> tuple[memoryview, dict[Any, Any]]:
    count = int(buffer[1] / 2)
    buffer = buffer[2:]
    values = {}
    for _ in range(count):
        buffer, key = _DECODE_MAP[buffer[0]](buffer[1:])
        buffer, value = _DECODE_MAP[buffer[0]](buffer[1:])
        values[key] = value
    return buffer, values


def _decode_map_large(buffer: memoryview) -> tuple[memoryview, dict[Any, Any]]:
    count = int(c_unsigned_long.unpack(buffer[4:8])[0] / 2)
    buffer = buffer[8:]
    values = {}
    for _ in range(count):
        buffer, key = _DECODE_MAP[buffer[0]](buffer[1:])
        buffer, value = _DECODE_MAP[buffer[0]](buffer[1:])
        values[key] = value
    return buffer, values


def _decode_array_small(buffer: memoryview) -> tuple[memoryview, list[Any]]:
    count = buffer[1]  # Ignore first byte (size) and just rely on count
    if count:
        subconstructor = buffer[2]
        buffer = buffer[3:]
        values = [None] * count
        for i in range(count):
            buffer, values[i] = _DECODE_MAP[subconstructor](buffer)
        return buffer, values
    return buffer[2:], []


def _decode_array_large(buffer: memoryview) -> tuple[memoryview, list[Any]]:
    count = c_unsigned_long.unpack(buffer[4:8])[0]
    if count:
        subconstructor = buffer[8]
        buffer = buffer[9:]
        values = [None] * count
        for i in range(count):
            buffer, values[i] = _DECODE_MAP[subconstructor](buffer)
        return buffer, values
    return buffer[8:], []


def _decode_described(buffer: memoryview) -> tuple[memoryview, object]:
    # TODO: to move the cursor of the buffer to the described value based on size of the
    #  descriptor without decoding descriptor value
    composite_type = buffer[0]
    buffer, descriptor = _DECODE_MAP[composite_type](buffer[1:])
    buffer, value = _DECODE_MAP[buffer[0]](buffer[1:])
    try:
        composite_type = cast(int, _COMPOSITES[descriptor])
        return buffer, {composite_type: value}
    except KeyError:
        return buffer, value


def _decode_string_small(buffer: memoryview) -> tuple[memoryview, str]:
    length = buffer[0]
    value = buffer[1 : 1 + length].tobytes().decode("utf-8")
    return buffer[1 + length :], value


def _decode_string_large(buffer: memoryview) -> tuple[memoryview, str]:
    length = c_unsigned_int.unpack(buffer[:4])[0]
    value = buffer[4 : 4 + length].tobytes().decode("utf-8")
    return buffer[4 + length :], value


def _decode_symbol_small(buffer: memoryview) -> tuple[memoryview, str]:
    length = buffer[0]
    value = buffer[1 : 1 + length].tobytes().decode("ascii")
    return buffer[1 + length :], value


def _decode_symbol_large(buffer: memoryview) -> tuple[memoryview, str]:
    length = c_unsigned_int.unpack(buffer[:4])[0]
    value = buffer[4 : 4 + length].tobytes().decode("ascii")
    return buffer[4 + length :], value


_DECODE_MAP: dict[int, Callable] = {
    0x00000000: _decode_described,
    0x00000040: _decode_null,
    0x00000041: _decode_true,
    0x00000042: _decode_false,
    0x00000043: _decode_zero,
    0x00000044: _decode_zero,
    0x00000045: _decode_empty,
    0x00000050: _decode_ubyte,
    0x00000051: _decode_byte,
    0x00000052: _decode_uint_small,
    0x00000053: _decode_ulong_small,
    0x00000054: _decode_int_small,
    0x00000055: _decode_long_small,
    0x00000056: _decode_boolean,
    0x00000060: _decode_ushort,
    0x00000061: _decode_short,
    0x00000070: _decode_uint_large,
    0x00000071: _decode_int_large,
    0x00000072: _decode_float,
    0x00000080: _decode_ulong_large,
    0x00000081: _decode_long_large,
    0x00000082: _decode_double,
    0x00000083: _decode_timestamp,
    0x00000098: _decode_uuid,
    0x000000A0: _decode_binary_small,
    0x000000A1: _decode_string_small,
    0x000000A3: _decode_symbol_small,
    0x000000B0: _decode_binary_large,
    0x000000B1: _decode_string_large,
    0x000000B3: _decode_symbol_large,
    0x000000C0: _decode_list_small,
    0x000000C1: _decode_map_small,
    0x000000D0: _decode_list_large,
    0x000000D1: _decode_map_large,
    0x000000E0: _decode_array_small,
    0x000000F0: _decode_array_large,
}

ListToDataclassT = TypeVar("ListToDataclassT")


def _list_to_dataclass(cls: type[ListToDataclassT], fields_list: list[Any]) -> ListToDataclassT:
    kwargs = {}
    cls_fields = fields(cls)  # type: ignore[arg-type]
    target_fields = [f for f in cls_fields if f.name != "payload"]

    for i, f in enumerate(target_fields):
        if i < len(fields_list):
            kwargs[f.name] = fields_list[i]

    return cls(**kwargs)


def bytes_to_performative(data: bytes) -> performatives.Performative:
    buffer = memoryview(data)
    # Header
    size = struct.unpack(">I", buffer[:4])[0]
    doff = buffer[4]
    # type_ = buffer[5]
    # channel = struct.unpack(">H", buffer[6:8])[0]

    # Body
    body_start = doff * 4
    body_buffer = buffer[body_start:size]

    # Decode Described Type
    # Expect 0x00
    if body_buffer[0] != 0x00:
        raise ValueError("Invalid frame body")

    body_buffer = body_buffer[1:]

    # Descriptor
    # We need to decode the descriptor. It's usually a ulong.
    # _decode_by_constructor uses the first byte to dispatch.
    constructor = body_buffer[0]
    decoder = _DECODE_MAP[constructor]
    if decoder is None:
        raise ValueError(f"Unknown constructor: {constructor}")
    body_buffer, descriptor = decoder(body_buffer[1:])

    # Find class
    cls = performatives.PERFORMATIVES_MAP.get(descriptor)
    if not cls:
        raise ValueError(f"Unknown performative: {descriptor}")

    # List
    # The value of the described type is a list.
    # We decode it as a python list using existing decoders.
    constructor = body_buffer[0]
    decoder = _DECODE_MAP[constructor]
    if decoder is None:
        raise ValueError(f"Unknown constructor: {constructor}")
    body_buffer, fields_list = decoder(body_buffer[1:])

    performative = _list_to_dataclass(cls, fields_list)

    # Payload
    # If TransferFrame, remaining body_buffer is payload.
    if isinstance(performative, performatives.TransferFrame) and len(body_buffer) > 0:
        performative.payload = body_buffer.tobytes()

    return performative


def _construct_message(payload: bytes) -> Message:  # noqa: C901, PLR0912
    buffer = memoryview(payload)
    message = Message()

    while len(buffer) > 0:
        # Decode Described Type
        # Expect 0x00
        if buffer[0] != 0x00:
            raise ValueError("Invalid message section")

        buffer = buffer[1:]

        # Descriptor
        constructor = buffer[0]
        decoder = _DECODE_MAP[constructor]
        if decoder is None:
            raise ValueError(f"Unknown constructor: {constructor}")
        buffer, descriptor = decoder(buffer[1:])

        # Value
        constructor = buffer[0]
        decoder = _DECODE_MAP[constructor]
        if decoder is None:
            raise ValueError(f"Unknown constructor: {constructor}")
        buffer, value = decoder(buffer[1:])

        # Map descriptor to section
        if descriptor == 0x00000070:  # Header  # noqa: PLR2004
            message.header = _list_to_dataclass(Header, value)
        elif descriptor == 0x00000071:  # Delivery Annotations  # noqa: PLR2004
            message.delivery_annotations = value
        elif descriptor == 0x00000072:  # Message Annotations  # noqa: PLR2004
            message.message_annotations = value
        elif descriptor == 0x00000073:  # Properties  # noqa: PLR2004
            message.properties = _list_to_dataclass(Properties, value)
        elif descriptor == 0x00000074:  # Application Properties  # noqa: PLR2004
            message.application_properties = value
        elif descriptor == 0x00000075:  # Data  # noqa: PLR2004
            message.data = value
        elif descriptor == 0x00000076:  # Sequence  # noqa: PLR2004
            message.sequence = value
        elif descriptor == 0x00000077:  # Value  # noqa: PLR2004
            message.value = value
        elif descriptor == 0x00000078:  # Footer  # noqa: PLR2004
            message.footer = value

    return message


def transfer_frames_to_message(frames: list[performatives.TransferFrame]) -> Message:
    payload = bytearray()
    for frame in frames:
        if frame.payload:
            payload.extend(frame.payload)
    return _construct_message(payload)


def decode_frame(data: bytes) -> tuple[int, performatives.Performative]:
    buffer = memoryview(data)
    channel = struct.unpack(">H", buffer[6:8])[0]
    performative = bytes_to_performative(data)
    return channel, performative
