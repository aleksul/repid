from __future__ import annotations

import calendar
import struct
import uuid
from collections.abc import Callable, Generator, Iterable, Sequence, Sized
from dataclasses import fields
from datetime import datetime
from functools import cache
from typing import Any, Protocol, cast, get_type_hints

from . import performatives
from .amqptypes import (
    TYPE,
    VALUE,
    AMQPTypes,
    ConstructorBytes,
    FieldDefinition,
    ObjDefinition,
)
from .constants import INT32_MAX, INT32_MIN, MIN_MAX_LIST_SIZE
from .message import Message, MessageBodyType


class EncodableT(Protocol):
    @property
    def CODE(self) -> int: ...  # noqa: N802

    @property
    def FRAME_TYPE(self) -> bytes: ...  # noqa: N802

    @property
    def FRAME_OFFSET(self) -> bytes: ...  # noqa: N802


AQMPSimpleType = bool | float | str | bytes | uuid.UUID | None


def _construct(byte: bytes, construct: bool) -> bytes:
    return byte if construct else b""


def _encode_null(
    output: bytearray,
    *args: Any,  # noqa: ARG001
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    encoding code="0x40" category="fixed" width="0" label="the null value"

    :param bytearray output: The output buffer to write to.
    :param any args: Ignored.
    """
    output.extend(ConstructorBytes.null)


def _encode_boolean(
    output: bytearray,
    value: bool,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding name="true" code="0x41" category="fixed" width="0" label="the boolean value true"/>
    <encoding name="false" code="0x42" category="fixed" width="0" label="the boolean value false"/>
    <encoding code="0x56" category="fixed" width="1"
        label="boolean with the octet 0x00 being false and octet 0x01 being true"/>

    :param bytearray output: The output buffer to write to.
    :param bool value: The boolean to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    value = bool(value)
    if with_constructor:
        output.extend(_construct(ConstructorBytes.bool, with_constructor))
        output.extend(b"\x01" if value else b"\x00")
        return

    output.extend(ConstructorBytes.bool_true if value else ConstructorBytes.bool_false)


def _encode_ubyte(
    output: bytearray,
    value: int | bytes,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding code="0x50" category="fixed" width="1" label="8-bit unsigned integer"/>

    :param bytearray output: The output buffer to write to.
    :param int or bytes value: The ubyte to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    try:
        value = int(value)
    except ValueError:
        value = cast(bytes, value)
        value = ord(value)
    try:
        output.extend(_construct(ConstructorBytes.ubyte, with_constructor))
        output.extend(struct.pack(">B", abs(value)))
    except struct.error as exc:
        raise ValueError("Unsigned byte value must be 0-255") from exc


def _encode_ushort(
    output: bytearray,
    value: int,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding code="0x60" category="fixed" width="2" label="16-bit unsigned integer in network byte order"/>

    :param bytearray output: The output buffer to write to.
    :param int value: The ushort to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    value = int(value)
    try:
        output.extend(_construct(ConstructorBytes.ushort, with_constructor))
        output.extend(struct.pack(">H", abs(value)))
    except struct.error as exc:
        raise ValueError("Unsigned byte value must be 0-65535") from exc


def _encode_uint(
    output: bytearray,
    value: int,
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="uint0" code="0x43" category="fixed" width="0" label="the uint value 0"/>
    <encoding name="smalluint" code="0x52" category="fixed" width="1"
        label="unsigned integer value in the range 0 to 255 inclusive"/>
    <encoding code="0x70" category="fixed" width="4" label="32-bit unsigned integer in network byte order"/>

    :param bytearray output: The output buffer to write to.
    :param int value: The uint to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    value = int(value)
    if value == 0:
        output.extend(ConstructorBytes.uint_0)
        return
    try:
        if use_smallest and value <= 255:  # noqa: PLR2004
            output.extend(_construct(ConstructorBytes.uint_small, with_constructor))
            output.extend(struct.pack(">B", abs(value)))
            return
        output.extend(_construct(ConstructorBytes.uint_large, with_constructor))
        output.extend(struct.pack(">I", abs(value)))
    except struct.error as exc:
        raise ValueError(f"Value supplied for unsigned int invalid: {value}") from exc


def _encode_ulong(
    output: bytearray,
    value: int,
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="ulong0" code="0x44" category="fixed" width="0" label="the ulong value 0"/>
    <encoding name="smallulong" code="0x53" category="fixed" width="1"
        label="unsigned long value in the range 0 to 255 inclusive"/>
    <encoding code="0x80" category="fixed" width="8" label="64-bit unsigned integer in network byte order"/>

    :param bytearray output: The output buffer to write to.
    :param int value: The ulong to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    value = int(value)
    if value == 0:
        output.extend(ConstructorBytes.ulong_0)
        return
    try:
        if use_smallest and value <= 255:  # noqa: PLR2004
            output.extend(_construct(ConstructorBytes.ulong_small, with_constructor))
            output.extend(struct.pack(">B", abs(value)))
            return
        output.extend(_construct(ConstructorBytes.ulong_large, with_constructor))
        output.extend(struct.pack(">Q", abs(value)))
    except struct.error as exc:
        raise ValueError(f"Value supplied for unsigned long invalid: {value}") from exc


def _encode_byte(
    output: bytearray,
    value: int,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding code="0x51" category="fixed" width="1" label="8-bit two's-complement integer"/>

    :param bytearray output: The output buffer to write to.
    :param byte value: The byte to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    value = int(value)
    try:
        output.extend(_construct(ConstructorBytes.byte, with_constructor))
        output.extend(struct.pack(">b", value))
    except struct.error as exc:
        raise ValueError("Byte value must be -128-127") from exc


def _encode_short(
    output: bytearray,
    value: int,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding code="0x61" category="fixed" width="2" label="16-bit two's-complement integer in network byte order"/>

    :param bytearray output: The output buffer to write to.
    :param int value: The short to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    value = int(value)
    try:
        output.extend(_construct(ConstructorBytes.short, with_constructor))
        output.extend(struct.pack(">h", value))
    except struct.error as exc:
        raise ValueError("Short value must be -32768-32767") from exc


def _encode_int(
    output: bytearray,
    value: int,
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="smallint" code="0x54" category="fixed" width="1" label="8-bit two's-complement integer"/>
    <encoding code="0x71" category="fixed" width="4" label="32-bit two's-complement integer in network byte order"/>

    :param bytearray output: The output buffer to write to.
    :param int value: The int to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    value = int(value)
    try:
        if use_smallest and (-128 <= value <= 127):  # noqa: PLR2004
            output.extend(_construct(ConstructorBytes.int_small, with_constructor))
            output.extend(struct.pack(">b", value))
            return
        output.extend(_construct(ConstructorBytes.int_large, with_constructor))
        output.extend(struct.pack(">i", value))
    except struct.error as exc:
        raise ValueError(f"Value supplied for int invalid: {value}") from exc


def _encode_long(
    output: bytearray,
    value: int | datetime,
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="smalllong" code="0x55" category="fixed" width="1" label="8-bit two's-complement integer"/>
    <encoding code="0x81" category="fixed" width="8" label="64-bit two's-complement integer in network byte order"/>

    :param bytearray output: The output buffer to write to.
    :param int or datetime value: The UUID to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    if isinstance(value, datetime):
        value = int((calendar.timegm(value.utctimetuple()) * 1000) + (value.microsecond / 1000))
    try:
        if use_smallest and (-128 <= value <= 127):  # noqa: PLR2004
            output.extend(_construct(ConstructorBytes.long_small, with_constructor))
            output.extend(struct.pack(">b", value))
            return
        output.extend(_construct(ConstructorBytes.long_large, with_constructor))
        output.extend(struct.pack(">q", value))
    except struct.error as exc:
        raise ValueError(f"Value supplied for long invalid: {value}") from exc


def _encode_float(
    output: bytearray,
    value: float,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding name="ieee-754" code="0x72" category="fixed" width="4" label="IEEE 754-2008 binary32"/>

    :param bytearray output: The output buffer to write to.
    :param float value: The value to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    value = float(value)
    output.extend(_construct(ConstructorBytes.float, with_constructor))
    output.extend(struct.pack(">f", value))


def _encode_double(
    output: bytearray,
    value: float,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding name="ieee-754" code="0x82" category="fixed" width="8" label="IEEE 754-2008 binary64"/>

    :param bytearray output: The output buffer to write to.
    :param float value: The double to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    value = float(value)
    output.extend(_construct(ConstructorBytes.double, with_constructor))
    output.extend(struct.pack(">d", value))


def _encode_timestamp(
    output: bytearray,
    value: int | datetime,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding name="ms64" code="0x83" category="fixed" width="8"
        label="64-bit two's-complement integer representing milliseconds since the unix epoch"/>

    :param bytearray output: The output buffer to write to.
    :param int or datetime value: The timestamp to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    if isinstance(value, datetime):
        value = int((calendar.timegm(value.utctimetuple()) * 1000) + (value.microsecond / 1000))

    value = int(value)
    output.extend(_construct(ConstructorBytes.timestamp, with_constructor))
    output.extend(struct.pack(">q", value))


def _encode_uuid(
    output: bytearray,
    value: uuid.UUID | str | bytes,
    with_constructor: bool = True,
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """
    <encoding code="0x98" category="fixed" width="16" label="UUID as defined in section 4.1.2 of RFC-4122"/>

    :param bytearray output: The output buffer to write to.
    :param uuid.UUID or str or bytes value: The UUID to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    """
    if isinstance(value, str):
        value = uuid.UUID(value).bytes
    elif isinstance(value, uuid.UUID):
        value = value.bytes
    elif isinstance(value, bytes):
        value = uuid.UUID(bytes=value).bytes
    else:
        raise TypeError(f"Invalid UUID type: {type(value)}")
    output.extend(_construct(ConstructorBytes.uuid, with_constructor))
    output.extend(value)


def _encode_binary(
    output: bytearray,
    value: bytes | bytearray,
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="vbin8" code="0xa0" category="variable" width="1" label="up to 2^8 - 1 octets of binary data"/>
    <encoding name="vbin32" code="0xb0" category="variable" width="4" label="up to 2^32 - 1 octets of binary data"/>

    :param bytearray output: The output buffer to write to.
    :param bytes or bytearray value: The value to encode.
    :param bool with_constructor: Whether to include the constructor in the output.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    length = len(value)
    if use_smallest and length <= 255:  # noqa: PLR2004
        output.extend(_construct(ConstructorBytes.binary_small, with_constructor))
        output.extend(struct.pack(">B", length))
        output.extend(value)
        return
    try:
        output.extend(_construct(ConstructorBytes.binary_large, with_constructor))
        output.extend(struct.pack(">L", length))
        output.extend(value)
    except struct.error as exc:
        raise ValueError("Binary data to long to encode") from exc


def _encode_string(
    output: bytearray,
    value: bytes | str,
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="str8-utf8" code="0xa1" category="variable" width="1"
        label="up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark)"/>
    <encoding name="str32-utf8" code="0xb1" category="variable" width="4"
        label="up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark)"/>

    :param bytearray output: The output buffer to write to.
    :param str value: The string to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    if isinstance(value, str):
        value = value.encode("utf-8")
    length = len(value)
    if use_smallest and length <= 255:  # noqa: PLR2004
        output.extend(_construct(ConstructorBytes.string_small, with_constructor))
        output.extend(struct.pack(">B", length))
        output.extend(value)
        return
    try:
        output.extend(_construct(ConstructorBytes.string_large, with_constructor))
        output.extend(struct.pack(">L", length))
        output.extend(value)
    except struct.error as exc:
        raise ValueError("String value too long to encode.") from exc


def _encode_symbol(
    output: bytearray,
    value: bytes | str,
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="sym8" code="0xa3" category="variable" width="1"
        label="up to 2^8 - 1 seven bit ASCII characters representing a symbolic value"/>
    <encoding name="sym32" code="0xb3" category="variable" width="4"
        label="up to 2^32 - 1 seven bit ASCII characters representing a symbolic value"/>

    :param bytearray output: The output buffer to write to.
    :param bytes or str value: The value to encode.
    :param bool with_constructor: Whether to include the constructor byte.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    if isinstance(value, str):
        value = value.encode("utf-8")
    length = len(value)
    if use_smallest and length <= 255:  # noqa: PLR2004
        output.extend(_construct(ConstructorBytes.symbol_small, with_constructor))
        output.extend(struct.pack(">B", length))
        output.extend(value)
        return
    try:
        output.extend(_construct(ConstructorBytes.symbol_large, with_constructor))
        output.extend(struct.pack(">L", length))
        output.extend(value)
    except struct.error as exc:
        raise ValueError("Symbol value too long to encode.") from exc


def _encode_list(
    output: bytearray,
    value: Sequence[Any],
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="list0" code="0x45" category="fixed" width="0"
        label="the empty list (i.e. the list with no elements)"/>
    <encoding name="list8" code="0xc0" category="compound" width="1"
        label="up to 2^8 - 1 list elements with total size less than 2^8 octets"/>
    <encoding name="list32" code="0xd0" category="compound" width="4"
        label="up to 2^32 - 1 list elements with total size less than 2^32 octets"/>

    :param bytearray output: The output buffer to write to.
    :param sequence value: The list to encode.
    :param bool with_constructor: Whether to include the constructor in the output.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    count = len(cast(Sized, value))
    if use_smallest and count == 0:
        output.extend(ConstructorBytes.list_0)
        return
    encoded_size = 0
    encoded_values = bytearray()
    for item in value:
        _encode_value(encoded_values, item, with_constructor=True)
    encoded_size += len(encoded_values)
    if use_smallest and count <= 255 and encoded_size < 255:  # noqa: PLR2004
        output.extend(_construct(ConstructorBytes.list_small, with_constructor))
        output.extend(struct.pack(">B", encoded_size + 1))
        output.extend(struct.pack(">B", count))
    else:
        try:
            output.extend(_construct(ConstructorBytes.list_large, with_constructor))
            output.extend(struct.pack(">L", encoded_size + 4))
            output.extend(struct.pack(">L", count))
        except struct.error as exc:
            raise ValueError("List is too large or too long to be encoded.") from exc
    output.extend(encoded_values)


def _encode_map(
    output: bytearray,
    value: dict[Any, Any] | Iterable[tuple[Any, Any]],
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="map8" code="0xc1" category="compound" width="1"
        label="up to 2^8 - 1 octets of encoded map data"/>
    <encoding name="map32" code="0xd1" category="compound" width="4"
        label="up to 2^32 - 1 octets of encoded map data"/>

    :param bytearray output: The output buffer to write to.
    :param dict value: The value to encode.
    :param bool with_constructor: Whether to include the constructor in the output.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    count = len(cast(Sized, value)) * 2
    encoded_size = 0
    encoded_values = bytearray()
    if isinstance(value, dict):
        items: Iterable[Any] = value.items()
    elif isinstance(value, Iterable):
        items = value

    for key, data in items:
        _encode_value(encoded_values, key, with_constructor=True)
        _encode_value(encoded_values, data, with_constructor=True)
    encoded_size = len(encoded_values)
    if use_smallest and count <= 255 and encoded_size < 255:  # noqa: PLR2004
        output.extend(_construct(ConstructorBytes.map_small, with_constructor))
        output.extend(struct.pack(">B", encoded_size + 1))
        output.extend(struct.pack(">B", count))
    else:
        try:
            output.extend(_construct(ConstructorBytes.map_large, with_constructor))
            output.extend(struct.pack(">L", encoded_size + 4))
            output.extend(struct.pack(">L", count))
        except struct.error as exc:
            raise ValueError("Map is too large or too long to be encoded.") from exc
    output.extend(encoded_values)


def _check_element_type(item: dict[str, Any], element_type: Any) -> Any:
    if not element_type:
        try:
            return item["TYPE"]
        except (KeyError, TypeError):
            return type(item)
    try:
        if item["TYPE"] != element_type:
            raise TypeError("All elements in an array must be the same type.")
    except (KeyError, TypeError) as exc:
        if not isinstance(item, element_type):
            raise TypeError("All elements in an array must be the same type.") from exc
    return element_type


def _encode_array(
    output: bytearray,
    value: Sequence[Any],
    with_constructor: bool = True,
    use_smallest: bool = True,
) -> None:
    """
    <encoding name="array8" code="0xe0" category="array" width="1"
        label="up to 2^8 - 1 array elements with total size less than 2^8 octets"/>
    <encoding name="array32" code="0xf0" category="array" width="4"
        label="up to 2^32 - 1 array elements with total size less than 2^32 octets"/>

    :param bytearray output: The output buffer to write to.
    :param sequence value: The array to encode.
    :param bool with_constructor: Whether to include the constructor in the output.
    :param bool use_smallest: Whether to use the smallest possible encoding.
    """
    count = len(cast(Sized, value))
    encoded_size = 0
    encoded_values = bytearray()
    first_item = True
    element_type = None
    for item in value:
        element_type = _check_element_type(item, element_type)
        _encode_value(encoded_values, item, with_constructor=first_item, use_smallest=False)
        first_item = False
        if item is None:
            encoded_size -= 1
            break
    encoded_size += len(encoded_values)
    if use_smallest and count <= 255 and encoded_size < 255:  # noqa: PLR2004
        output.extend(_construct(ConstructorBytes.array_small, with_constructor))
        output.extend(struct.pack(">B", encoded_size + 1))
        output.extend(struct.pack(">B", count))
    else:
        try:
            output.extend(_construct(ConstructorBytes.array_large, with_constructor))
            output.extend(struct.pack(">L", encoded_size + 4))
            output.extend(struct.pack(">L", count))
        except struct.error as exc:
            raise ValueError("Array is too large or too long to be encoded.") from exc
    output.extend(encoded_values)


def _encode_described(
    output: bytearray,
    value: tuple[Any, Any],
    _: bool | None = None,
    **kwargs: Any,
) -> None:
    output.extend(ConstructorBytes.descriptor)
    _encode_value(output, value[0], **kwargs)
    _encode_value(output, value[1], **kwargs)


def _encode_unknown(  # noqa: C901, PLR0912
    output: bytearray,
    value: object | None,
    **kwargs: Any,
) -> None:
    """
    Dynamic encoding according to the type of `value`.
    :param bytearray output: The output buffer.
    :param any value: The value to encode.
    """
    if value is None:
        _encode_null(output, **kwargs)
    elif isinstance(value, bool):
        _encode_boolean(output, value, **kwargs)
    elif isinstance(value, str):
        _encode_string(output, value, **kwargs)
    elif isinstance(value, uuid.UUID):
        _encode_uuid(output, value, **kwargs)
    elif isinstance(value, (bytearray, bytes)):
        _encode_binary(output, value, **kwargs)
    elif isinstance(value, float):
        _encode_double(output, value, **kwargs)
    elif isinstance(value, int):
        # if the value fits within AMQP 1.0 32-bit signed integer bounds, encode as an int
        if INT32_MIN <= value <= INT32_MAX:
            _encode_int(output, value, **kwargs)
        else:  # otherwise, we'll assume it fits in a 64-bit long
            _encode_long(output, value, **kwargs)
    elif isinstance(value, datetime):
        _encode_timestamp(output, value, **kwargs)
    elif isinstance(value, list):
        _encode_list(output, value, **kwargs)
    elif isinstance(value, tuple):
        _encode_described(output, cast(tuple[Any, Any], value), **kwargs)
    elif isinstance(value, dict):
        _encode_map(output, value, **kwargs)
    else:
        raise TypeError(f"Unable to encode unknown value: {value}")


_ENCODE_MAP: dict[str | None, Callable] = {
    None: _encode_unknown,
    AMQPTypes.null.value: _encode_null,
    AMQPTypes.boolean.value: _encode_boolean,
    AMQPTypes.ubyte.value: _encode_ubyte,
    AMQPTypes.byte.value: _encode_byte,
    AMQPTypes.ushort.value: _encode_ushort,
    AMQPTypes.short.value: _encode_short,
    AMQPTypes.uint.value: _encode_uint,
    AMQPTypes.int.value: _encode_int,
    AMQPTypes.ulong.value: _encode_ulong,
    AMQPTypes.long.value: _encode_long,
    AMQPTypes.float.value: _encode_float,
    AMQPTypes.double.value: _encode_double,
    AMQPTypes.timestamp.value: _encode_timestamp,
    AMQPTypes.uuid.value: _encode_uuid,
    AMQPTypes.binary.value: _encode_binary,
    AMQPTypes.string.value: _encode_string,
    AMQPTypes.symbol.value: _encode_symbol,
    AMQPTypes.list.value: _encode_list,
    AMQPTypes.map.value: _encode_map,
    AMQPTypes.array.value: _encode_array,
    AMQPTypes.described.value: _encode_described,
}


_FIELD_DEFINITION_TO_AMQP_TYPE = {
    FieldDefinition.role: AMQPTypes.boolean,
    FieldDefinition.sender_settle_mode: AMQPTypes.ubyte,
    FieldDefinition.receiver_settle_mode: AMQPTypes.ubyte,
    FieldDefinition.handle: AMQPTypes.uint,
    FieldDefinition.seconds: AMQPTypes.uint,
    FieldDefinition.milliseconds: AMQPTypes.uint,
    FieldDefinition.delivery_tag: AMQPTypes.binary,
    FieldDefinition.delivery_number: AMQPTypes.uint,
    FieldDefinition.transfer_number: AMQPTypes.uint,
    FieldDefinition.sequence_no: AMQPTypes.uint,
    FieldDefinition.message_format: AMQPTypes.uint,
    FieldDefinition.ietf_language_tag: AMQPTypes.symbol,
    FieldDefinition.sasl_code: AMQPTypes.ubyte,
    FieldDefinition.terminus_durability: AMQPTypes.uint,
    FieldDefinition.expiry_policy: AMQPTypes.symbol,
    FieldDefinition.distribution_mode: AMQPTypes.symbol,
    FieldDefinition.fields: AMQPTypes.map,
    FieldDefinition.annotations: AMQPTypes.map,
    FieldDefinition.app_properties: AMQPTypes.map,
    FieldDefinition.node_properties: AMQPTypes.map,
    FieldDefinition.filter_set: AMQPTypes.map,
}


def _encode_value(output: bytearray, value: Any, **kwargs: Any) -> None:
    try:
        cast(Callable, _ENCODE_MAP[value[TYPE]])(output, value[VALUE], **kwargs)
    except (KeyError, TypeError):
        _encode_unknown(output, value, **kwargs)


@cache
def _get_performative_fields(
    performative_type: type[EncodableT],
) -> list[tuple[str, AMQPTypes | FieldDefinition | ObjDefinition | None]]:
    cached_fields = []
    type_hints = get_type_hints(performative_type, include_extras=True)
    current_fields = fields(performative_type)  # type: ignore[arg-type]
    for f in current_fields:
        if f.name == "payload":
            continue

        # Get annotation
        hint = type_hints[f.name]
        # Extract AMQPTAnnotation
        amqp_type = None
        if hasattr(hint, "__metadata__"):
            for meta in hint.__metadata__:
                if isinstance(meta, performatives.AMQPTAnnotation):
                    amqp_type = meta.type_
                    break
        cached_fields.append((f.name, amqp_type))
    return cached_fields


def _encode_performative_field(output: bytearray, value: Any) -> None:
    output.extend(_encode_performative_body(value))


def _encode_performative_body(performative: EncodableT) -> bytes:
    output = bytearray()

    # Body
    # Described type: 0x00
    output.extend(b"\x00")
    # Descriptor: ulong (code)
    _encode_ulong(output, cast(int, performative.CODE))

    # List of fields
    cached_fields = _get_performative_fields(performative.__class__)

    field_values = []
    for name, amqp_type in cached_fields:
        val = getattr(performative, name)
        field_values.append((val, amqp_type))

    # Trim trailing Nones
    while field_values and field_values[-1][0] is None:
        field_values.pop()

    list_body = bytearray()
    for val, type_ in field_values:
        if val is None:
            _encode_null(list_body)
        else:
            encoder = _ENCODE_MAP.get(type_.value if type_ is not None else None)
            if not encoder:
                # Try FieldDefinition mapping
                if isinstance(type_, FieldDefinition):
                    field_definition_type = _FIELD_DEFINITION_TO_AMQP_TYPE.get(type_)
                    encoder = _ENCODE_MAP.get(
                        field_definition_type.value if field_definition_type is not None else None,
                    )
                elif isinstance(type_, ObjDefinition):
                    encoder = _encode_performative_field

            if encoder:
                encoder(list_body, val)
            else:
                raise ValueError(f"No encoder for type {amqp_type} value {val}")

    if len(list_body) + 1 < MIN_MAX_LIST_SIZE and len(field_values) < MIN_MAX_LIST_SIZE:
        output.extend(ConstructorBytes.list_small)
        output.append(len(list_body) + 1)
        output.append(len(field_values))
    else:
        output.extend(ConstructorBytes.list_large)
        output.extend((len(list_body) + 4).to_bytes(4, "big"))
        output.extend(len(field_values).to_bytes(4, "big"))

    output.extend(list_body)
    return output


def performative_to_bytes(performative: EncodableT, channel: int = 0) -> bytes:
    output = bytearray()
    output.extend(_encode_performative_body(performative))

    if hasattr(performative, "payload") and performative.payload:
        output.extend(performative.payload)

    # Header
    # SIZE (4) DOFF (1) TYPE (1) CHANNEL (2)
    # DOFF = 2 (8 bytes)
    # TYPE = performative.FRAME_TYPE (0x00 or 0x01)
    # CHANNEL = channel

    frame_size = len(output) + 8
    header = bytearray()
    header.extend(frame_size.to_bytes(4, "big"))
    header.append(2)  # DOFF
    header.extend(performative.FRAME_TYPE)
    header.extend(channel.to_bytes(2, "big"))

    return header + output


def message_to_transfer_frames(  # noqa: C901, PLR0912, PLR0915
    message: Message,
    max_frame_size: int,
    handle: int,
    delivery_tag: bytes | None = None,
    delivery_id: int | None = None,
    message_format: int = 0,
    settled: bool | None = None,
    more: bool = False,
    rcv_settle_mode: Any | None = None,
    state: Any | None = None,
    resume: bool = False,
    aborted: bool = False,
    batchable: bool = False,
) -> Generator[performatives.TransferFrame, None, None]:
    payload = bytearray()

    # Header
    if message.header:
        # header follows EncodableT protocol
        payload.extend(_encode_performative_body(message.header))

    # Delivery Annotations
    if message.delivery_annotations:
        payload.extend(b"\x00")
        _encode_ulong(payload, 0x00000071)
        _encode_map(payload, message.delivery_annotations)

    # Message Annotations
    if message.message_annotations:
        payload.extend(b"\x00")
        _encode_ulong(payload, 0x00000072)
        _encode_map(payload, message.message_annotations)

    # Properties
    if message.properties:
        # properties follow EncodableT protocol
        payload.extend(_encode_performative_body(message.properties))

    # Application Properties
    if message.application_properties:
        payload.extend(b"\x00")
        _encode_ulong(payload, 0x00000074)
        _encode_map(payload, message.application_properties)

    # Body
    if message.body_type == MessageBodyType.DATA:
        payload.extend(b"\x00")
        _encode_ulong(payload, 0x00000075)
        _encode_binary(payload, cast(bytes, message.data))
    elif message.body_type == MessageBodyType.SEQUENCE:
        payload.extend(b"\x00")
        _encode_ulong(payload, 0x00000076)
        _encode_list(payload, cast(list, message.sequence))
    elif message.body_type == MessageBodyType.VALUE:
        payload.extend(b"\x00")
        _encode_ulong(payload, 0x00000077)
        _encode_unknown(payload, message.value)

    # Footer
    if isinstance(message, Message) and message.footer:
        payload.extend(b"\x00")
        _encode_ulong(payload, 0x00000078)
        _encode_map(payload, message.footer)

    # Split into frames
    remaining_payload = payload
    first = True

    while remaining_payload or first:
        # Construct Transfer
        transfer = performatives.TransferFrame(
            handle=handle,
            delivery_id=delivery_id if first else None,
            delivery_tag=delivery_tag if first else None,
            message_format=message_format if first else None,
            settled=settled,
            more=True,  # Placeholder
            rcv_settle_mode=rcv_settle_mode if first else None,
            state=state if first else None,
            resume=resume if first else False,
            aborted=aborted,
            batchable=batchable,
        )

        # Calculate overhead
        # We use a temporary transfer with empty payload to calculate header size
        transfer.more = True  # Assume more for overhead calculation
        transfer_bytes = _encode_performative_body(transfer)
        overhead = 8 + len(transfer_bytes)

        available = max_frame_size - overhead
        if available <= 0:
            raise ValueError("Frame size too small for Transfer frame overhead")

        chunk = remaining_payload[:available]
        remaining_payload = remaining_payload[available:]

        if not remaining_payload:
            transfer.more = more
        else:
            transfer.more = True

        transfer.payload = chunk
        yield transfer

        first = False
        if not remaining_payload:
            break
