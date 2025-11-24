from __future__ import annotations

import calendar
import struct
import uuid
from collections.abc import Callable, Collection, Generator, Iterable, Sequence, Sized
from dataclasses import fields
from datetime import datetime
from typing import Any, TypeAlias, cast, get_type_hints

from typing_extensions import Buffer

from . import performatives
from .amqptypes import (
    TYPE,
    VALUE,
    AMQPTypes,
    ConstructorBytes,
    FieldDefinition,
    ObjDefinition,
)
from .constants import INT32_MAX, INT32_MIN
from .message import AnnotatedMessage as Message
from .message import BareMessage, Header, MessageBodyType, Properties

Performative: TypeAlias = performatives.Performative | Message | Header | Properties

_FRAME_OFFSET = b"\x02"
_FRAME_TYPE = b"\x00"
AQMPSimpleType = bool | float | str | bytes | uuid.UUID | None


def _construct(byte: bytes, construct: bool) -> bytes:
    return byte if construct else b""


def encode_null(
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


def encode_boolean(
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


def encode_ubyte(
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


def encode_ushort(
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


def encode_uint(
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


def encode_ulong(
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


def encode_byte(
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


def encode_short(
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


def encode_int(
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


def encode_long(
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


def encode_float(
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


def encode_double(
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


def encode_timestamp(
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


def encode_uuid(
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


def encode_binary(
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


def encode_string(
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


def encode_symbol(
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


def encode_list(
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
        encode_value(encoded_values, item, with_constructor=True)
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


def encode_map(
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
        encode_value(encoded_values, key, with_constructor=True)
        encode_value(encoded_values, data, with_constructor=True)
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


def encode_array(
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
        encode_value(encoded_values, item, with_constructor=first_item, use_smallest=False)
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


def encode_described(
    output: bytearray,
    value: tuple[Any, Any],
    _: bool | None = None,
    **kwargs: Any,
) -> None:
    output.extend(ConstructorBytes.descriptor)
    encode_value(output, value[0], **kwargs)
    encode_value(output, value[1], **kwargs)


def encode_fields(value: dict[bytes, Any] | None) -> dict[str, Any]:
    """A mapping from field name to value.

    The fields type is a map where the keys are restricted to be of type symbol (this excludes the possibility
    of a null key).  There is no further restriction implied by the fields type on the allowed values for the
    entries or the set of allowed keys.

    <type name="fields" class="restricted" source="map"/>

    :param dict or None value: The field values to encode.
    :return: The encoded field values.
    :rtype: dict
    """
    if not value:
        return {TYPE: AMQPTypes.null, VALUE: None}
    fields = {TYPE: AMQPTypes.map, VALUE: []}
    for key, data in value.items():
        cast(list, fields[VALUE]).append(
            (
                {
                    TYPE: AMQPTypes.symbol,
                    VALUE: key.encode("utf-8") if isinstance(key, str) else key,
                },
                data,
            ),
        )
    return fields


def encode_annotations(value: dict[str | bytes, Any] | None) -> dict[str, Any]:
    """The annotations type is a map where the keys are restricted to be of type symbol or of type ulong.

    All ulong keys, and all symbolic keys except those beginning with "x-" are reserved.
    On receiving an annotations map containing keys or values which it does not recognize, and for which the
    key does not begin with the string 'x-opt-' an AMQP container MUST detach the link with the not-implemented
    amqp-error.

    <type name="annotations" class="restricted" source="map"/>

    :param dict or None value: The annotations to encode.
    :return: The encoded annotations.
    :rtype: dict
    """
    if not value:
        return {TYPE: AMQPTypes.null, VALUE: None}
    fields = {TYPE: AMQPTypes.map, VALUE: []}
    for key, data in value.items():
        if isinstance(key, int):
            field_key = {TYPE: AMQPTypes.ulong, VALUE: key}
        else:
            field_key = {TYPE: AMQPTypes.symbol, VALUE: key}
        try:
            cast(list, fields[VALUE]).append((field_key, {TYPE: data[TYPE], VALUE: data[VALUE]}))
        except (KeyError, TypeError):
            cast(list, fields[VALUE]).append((field_key, {TYPE: None, VALUE: data}))
    return fields


def encode_application_properties(
    value: dict[str | bytes, AQMPSimpleType] | None,
) -> dict[str | bytes, Any]:
    """The application-properties section is a part of the bare message used for structured application data.

    <type name="application-properties" class="restricted" source="map" provides="section">
        <descriptor name="amqp:application-properties:map" code="0x00000000:0x00000074"/>
    </type>

    Intermediaries may use the data within this structure for the purposes of filtering or routing.
    The keys of this map are restricted to be of type string (which excludes the possibility of a null key)
    and the values are restricted to be of simple types only, that is (excluding map, list, and array types).

    :param dict value: The application properties to encode.
    :return: The encoded application properties.
    :rtype: dict
    """
    if not value:
        return {TYPE: AMQPTypes.null, VALUE: None}
    fields: dict[str | bytes, Any] = {TYPE: AMQPTypes.map, VALUE: cast(list, [])}
    for key, data in value.items():
        cast(list, fields[VALUE]).append(({TYPE: AMQPTypes.string, VALUE: key}, data))
    return fields


def encode_message_id(
    value: int | uuid.UUID | bytes | str,
) -> dict[str, int | uuid.UUID | bytes | str]:
    """
    <type name="message-id-ulong" class="restricted" source="ulong" provides="message-id"/>
    <type name="message-id-uuid" class="restricted" source="uuid" provides="message-id"/>
    <type name="message-id-binary" class="restricted" source="binary" provides="message-id"/>
    <type name="message-id-string" class="restricted" source="string" provides="message-id"/>

    :param any value: The message ID to encode.
    :return: The encoded message ID.
    :rtype: dict
    """
    if isinstance(value, int):
        return {TYPE: AMQPTypes.ulong, VALUE: value}
    if isinstance(value, uuid.UUID):
        return {TYPE: AMQPTypes.uuid, VALUE: value}
    if isinstance(value, bytes):
        return {TYPE: AMQPTypes.binary, VALUE: value}
    if isinstance(value, str):
        return {TYPE: AMQPTypes.string, VALUE: value}
    raise TypeError("Unsupported Message ID type.")


def encode_node_properties(value: dict[str, Any] | None) -> dict[str, Any]:
    """Properties of a node.

    <type name="node-properties" class="restricted" source="fields"/>

    A symbol-keyed map containing properties of a node used when requesting creation or reporting
    the creation of a dynamic node. The following common properties are defined::

        - `lifetime-policy`: The lifetime of a dynamically generated node. Definitionally, the lifetime will
          never be less than the lifetime of the link which caused its creation, however it is possible to extend
          the lifetime of dynamically created node using a lifetime policy. The value of this entry MUST be of a type
          which provides the lifetime-policy archetype. The following standard lifetime-policies are defined below:
          delete-on-close, delete-on-no-links, delete-on-no-messages or delete-on-no-links-or-messages.

        - `supported-dist-modes`: The distribution modes that the node supports. The value of this entry MUST be one or
          more symbols which are valid distribution-modes. That is, the value MUST be of the same type as would be valid
          in a field defined with the following attributes:
          type="symbol" multiple="true" requires="distribution-mode"

    :param dict value: The node properties.
    :return: The encoded node properties.
    :rtype: dict
    """
    if not value:
        return {TYPE: AMQPTypes.null, VALUE: None}
    # TODO
    fields = {TYPE: AMQPTypes.map, VALUE: []}
    # fields[{TYPE: AMQPTypes.symbol, VALUE: b'lifetime-policy'}] = {
    #     TYPE: AMQPTypes.described,
    #     VALUE: (
    #         {TYPE: AMQPTypes.ulong, VALUE: value['lifetime_policy']},
    #         {TYPE: AMQPTypes.list, VALUE: []}
    #     )
    # }
    # fields[{TYPE: AMQPTypes.symbol, VALUE: b'supported-dist-modes'}] = {}
    return fields  # noqa: RET504


def encode_filter_set(value: dict[str, Any] | None) -> dict[str, Any]:
    """A set of predicates to filter the Messages admitted onto the Link.

    <type name="filter-set" class="restricted" source="map"/>

    A set of named filters. Every key in the map MUST be of type symbol, every value MUST be either null or of a
    described type which provides the archetype filter. A filter acts as a function on a message which returns a
    boolean result indicating whether the message can pass through that filter or not. A message will pass
    through a filter-set if and only if it passes through each of the named filters. If the value for a given key is
    null, this acts as if there were no such key present (i.e., all messages pass through the null filter).

    Filter types are a defined extension point. The filter types that a given source supports will be indicated
    by the capabilities of the source.

    :param dict value: A set of named filters.
    :return: A set of encoded named filters.
    :rtype: dict
    """
    if not value:
        return {TYPE: AMQPTypes.null, VALUE: None}
    fields = {TYPE: AMQPTypes.map, VALUE: cast(list, [])}
    for name, data in value.items():
        described_filter: dict[str, tuple[dict[str, Any], Any] | (str | None)] | str | bytes
        if data is None:
            described_filter = {TYPE: AMQPTypes.null, VALUE: None}
        elif isinstance(data, (str, bytes)):
            described_filter = data
        # handle the situation when data is a tuple or list of length 2
        else:
            try:
                descriptor, filter_value = data
                described_filter = {
                    TYPE: AMQPTypes.described,
                    VALUE: ({TYPE: AMQPTypes.symbol, VALUE: descriptor}, filter_value),
                }
            # if its not a type that is known, raise the error from the server
            except (ValueError, TypeError):
                described_filter = data

        cast(list, fields[VALUE]).append(
            (
                {
                    TYPE: AMQPTypes.symbol,
                    VALUE: name.encode("utf-8") if isinstance(name, str) else name,
                },
                described_filter,
            ),
        )
    return fields


def encode_unknown(  # noqa: C901, PLR0912
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
        encode_null(output, **kwargs)
    elif isinstance(value, bool):
        encode_boolean(output, value, **kwargs)
    elif isinstance(value, str):
        encode_string(output, value, **kwargs)
    elif isinstance(value, uuid.UUID):
        encode_uuid(output, value, **kwargs)
    elif isinstance(value, (bytearray, bytes)):
        encode_binary(output, value, **kwargs)
    elif isinstance(value, float):
        encode_double(output, value, **kwargs)
    elif isinstance(value, int):
        # if the value fits within AMQP 1.0 32-bit signed integer bounds, encode as an int
        if INT32_MIN <= value <= INT32_MAX:
            encode_int(output, value, **kwargs)
        else:  # otherwise, we'll assume it fits in a 64-bit long
            encode_long(output, value, **kwargs)
    elif isinstance(value, datetime):
        encode_timestamp(output, value, **kwargs)
    elif isinstance(value, list):
        encode_list(output, value, **kwargs)
    elif isinstance(value, tuple):
        encode_described(output, cast(tuple[Any, Any], value), **kwargs)
    elif isinstance(value, dict):
        encode_map(output, value, **kwargs)
    else:
        raise TypeError(f"Unable to encode unknown value: {value}")


def _wrap_simple(type_: str) -> Callable[[Any], dict[str, Any]]:
    def wrapper(value: Any) -> dict[str, Any]:
        return {TYPE: type_, VALUE: value}

    return wrapper


_FIELD_DEFINITIONS: dict[FieldDefinition, Callable[[Any], Any]] = {
    FieldDefinition.fields: encode_fields,
    FieldDefinition.annotations: encode_annotations,
    FieldDefinition.message_id: encode_message_id,
    FieldDefinition.app_properties: encode_application_properties,
    FieldDefinition.node_properties: encode_node_properties,
    FieldDefinition.filter_set: encode_filter_set,
    FieldDefinition.role: _wrap_simple(AMQPTypes.boolean),
    FieldDefinition.sender_settle_mode: _wrap_simple(AMQPTypes.ubyte),
    FieldDefinition.receiver_settle_mode: _wrap_simple(AMQPTypes.ubyte),
    FieldDefinition.handle: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.seconds: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.milliseconds: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.delivery_tag: _wrap_simple(AMQPTypes.binary),
    FieldDefinition.delivery_number: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.transfer_number: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.sequence_no: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.message_format: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.ietf_language_tag: _wrap_simple(AMQPTypes.symbol),
    FieldDefinition.sasl_code: _wrap_simple(AMQPTypes.ubyte),
    FieldDefinition.terminus_durability: _wrap_simple(AMQPTypes.uint),
    FieldDefinition.expiry_policy: _wrap_simple(AMQPTypes.symbol),
    FieldDefinition.distribution_mode: _wrap_simple(AMQPTypes.symbol),
}

_ENCODE_MAP = {
    None: encode_unknown,
    AMQPTypes.null: encode_null,
    AMQPTypes.boolean: encode_boolean,
    AMQPTypes.ubyte: encode_ubyte,
    AMQPTypes.byte: encode_byte,
    AMQPTypes.ushort: encode_ushort,
    AMQPTypes.short: encode_short,
    AMQPTypes.uint: encode_uint,
    AMQPTypes.int: encode_int,
    AMQPTypes.ulong: encode_ulong,
    AMQPTypes.long: encode_long,
    AMQPTypes.float: encode_float,
    AMQPTypes.double: encode_double,
    AMQPTypes.timestamp: encode_timestamp,
    AMQPTypes.uuid: encode_uuid,
    AMQPTypes.binary: encode_binary,
    AMQPTypes.string: encode_string,
    AMQPTypes.symbol: encode_symbol,
    AMQPTypes.list: encode_list,
    AMQPTypes.map: encode_map,
    AMQPTypes.array: encode_array,
    AMQPTypes.described: encode_described,
}


def encode_value(output: bytearray, value: Any, **kwargs: Any) -> None:
    try:
        cast(Callable, _ENCODE_MAP[value[TYPE]])(output, value[VALUE], **kwargs)
    except (KeyError, TypeError):
        encode_unknown(output, value, **kwargs)


def describe_performative(performative: Performative) -> dict[str, Sequence[Collection[str]]]:
    body: list[dict[str, Any]] = []
    if isinstance(performative, Message):
        raise TypeError("Message performative should not be described here.")
    for field_def in performative.DEFINITION:
        if field_def is None:
            continue
        value = getattr(performative, field_def.name, None)
        if value is None:
            body.append({TYPE: AMQPTypes.null, VALUE: None})
        elif isinstance(field_def.type, FieldDefinition):
            if field_def.multiple:
                body.append(
                    {
                        TYPE: AMQPTypes.array,
                        VALUE: [_FIELD_DEFINITIONS[field_def.type](v) for v in value],
                    },
                )
            else:
                body.append(_FIELD_DEFINITIONS[field_def.type](value))
        elif isinstance(field_def.type, ObjDefinition):
            body.append(describe_performative(value))
        elif field_def.multiple:
            body.append(
                {
                    TYPE: AMQPTypes.array,
                    VALUE: [{TYPE: field_def.type, VALUE: v} for v in value],
                },
            )
        else:
            body.append({TYPE: field_def.type, VALUE: value})

    return {
        TYPE: AMQPTypes.described,
        VALUE: (
            {TYPE: AMQPTypes.ulong, VALUE: performative.CODE},
            {TYPE: AMQPTypes.list, VALUE: body},
        ),
    }


def encode_payload(output: bytearray, payload: Message) -> bytes:  # noqa: C901
    if payload.header:
        # TODO: Header and Properties encoding can be optimized to
        #  1. not encoding trailing None fields
        #  Possible fix 1:
        #  header = payload[0]
        #  header = header[0:max(i for i, v in enumerate(header) if v is not None) + 1]
        #  Possible fix 2:
        #  itertools.dropwhile(lambda x: x is None, header[::-1]))[::-1]
        #  2. encoding bool without constructor
        #  Possible fix 3:
        #  header = list(payload[0])
        #  while header[-1] is None:
        #      del header[-1]
        encode_value(output, describe_performative(payload.header))

    if payload.message_annotations:
        encode_value(
            output,
            {
                TYPE: AMQPTypes.described,
                VALUE: (
                    {TYPE: AMQPTypes.ulong, VALUE: 0x00000072},
                    encode_annotations(payload.message_annotations),
                ),
            },
        )

    if payload.properties:
        # TODO: Header and Properties encoding can be optimized to
        #  1. not encoding trailing None fields
        #  2. encoding bool without constructor
        encode_value(output, describe_performative(payload.properties))

    if payload.application_properties:
        encode_value(
            output,
            {
                TYPE: AMQPTypes.described,
                VALUE: (
                    {TYPE: AMQPTypes.ulong, VALUE: 0x00000074},
                    encode_application_properties(payload.application_properties),
                ),
            },
        )

    if payload.body_type == MessageBodyType.DATA:
        encode_value(
            output,
            {
                TYPE: AMQPTypes.described,
                VALUE: (
                    {TYPE: AMQPTypes.ulong, VALUE: 0x00000075},
                    {TYPE: AMQPTypes.binary, VALUE: payload._data_body},
                ),
            },
        )
    elif payload.body_type == MessageBodyType.SEQUENCE:
        for item_value in payload._sequence_body or []:
            encode_value(
                output,
                {
                    TYPE: AMQPTypes.described,
                    VALUE: (
                        {TYPE: AMQPTypes.ulong, VALUE: 0x00000076},
                        {TYPE: None, VALUE: item_value},
                    ),
                },
            )
    elif payload.body_type == MessageBodyType.VALUE:
        encode_value(
            output,
            {
                TYPE: AMQPTypes.described,
                VALUE: (
                    {TYPE: AMQPTypes.ulong, VALUE: 0x00000077},
                    {TYPE: None, VALUE: payload._value_body},
                ),
            },
        )

    if payload.footer:
        encode_value(
            output,
            {
                TYPE: AMQPTypes.described,
                VALUE: (
                    {TYPE: AMQPTypes.ulong, VALUE: 0x00000078},
                    encode_annotations(payload.footer),
                ),
            },
        )

    # TODO:
    #  currently the delivery annotations must be finally encoded instead of being encoded at the 2nd position
    #  otherwise the event hubs service would ignore the delivery annotations
    #  -- received message doesn't have it populated
    #  check with service team?
    if payload.delivery_annotations:
        encode_value(
            output,
            {
                TYPE: AMQPTypes.described,
                VALUE: (
                    {TYPE: AMQPTypes.ulong, VALUE: 0x00000071},
                    encode_annotations(payload.delivery_annotations),
                ),
            },
        )

    return output


def encode_frame(
    frame: Performative | None,
    frame_type: bytes = _FRAME_TYPE,
) -> tuple[bytes, bytes | None]:
    # TODO: allow passing type specific bytes manually, e.g. Empty Frame needs padding
    if frame is None:
        size = 8
        header = size.to_bytes(4, "big") + _FRAME_OFFSET + frame_type
        return header, None

    frame_description = describe_performative(frame)
    frame_data = bytearray()
    encode_value(frame_data, frame_description)
    if isinstance(frame, performatives.TransferFrame):
        # casting from Optional[Buffer] since payload will not be None at this point
        frame_data += cast(Buffer, frame.payload)  # type: ignore[attr-defined]

    size = len(frame_data) + 8
    header = size.to_bytes(4, "big") + _FRAME_OFFSET + frame_type
    return header, frame_data


def encode_performative_field(output: bytearray, value: Any) -> None:
    output.extend(_encode_performative_body(value))


def _encode_performative_body(performative: performatives.Performative) -> bytes:
    output = bytearray()

    # Body
    # Described type: 0x00
    output.extend(b"\x00")
    # Descriptor: ulong (code)
    encode_ulong(output, cast(int, performative.CODE))

    # List of fields
    field_values = []
    type_hints = get_type_hints(performative.__class__, include_extras=True)

    current_fields = fields(performative)
    for f in current_fields:
        if f.name == "payload":
            continue

        val = getattr(performative, f.name)

        # Get annotation
        hint = type_hints[f.name]
        # Extract AMQPTAnnotation
        amqp_type = None
        if hasattr(hint, "__metadata__"):
            for meta in hint.__metadata__:
                if isinstance(meta, performatives.AMQPTAnnotation):
                    amqp_type = meta.type
                    break

        field_values.append((val, amqp_type))

    # Trim trailing Nones
    while field_values and field_values[-1][0] is None:
        field_values.pop()

    list_body = bytearray()
    for val, amqp_type in field_values:
        if val is None:
            encode_null(list_body)
        else:
            encoder = _ENCODE_MAP.get(amqp_type)
            if not encoder:
                # Try FieldDefinition mapping
                if isinstance(amqp_type, FieldDefinition):
                    amqp_type = _FIELD_DEFINITION_TO_AMQP_TYPE.get(amqp_type)
                    encoder = _ENCODE_MAP.get(amqp_type)
                elif isinstance(amqp_type, ObjDefinition):
                    encoder = encode_performative_field

            if encoder:
                encoder(list_body, val)
            else:
                raise ValueError(f"No encoder for type {amqp_type} value {val}")

    if len(list_body) + 1 < 255 and len(field_values) < 255:
        output.extend(ConstructorBytes.list_small)
        output.append(len(list_body) + 1)
        output.append(len(field_values))
    else:
        output.extend(ConstructorBytes.list_large)
        output.extend((len(list_body) + 4).to_bytes(4, "big"))
        output.extend(len(field_values).to_bytes(4, "big"))

    output.extend(list_body)
    return output


def performative_to_bytes(performative: performatives.Performative, channel: int = 0) -> bytes:
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


def message_to_transfer_frames(
    message: Message | BareMessage,
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
    if isinstance(message, Message) and message.header:
        payload.extend(_encode_performative_body(message.header))

    # Delivery Annotations
    if isinstance(message, Message) and message.delivery_annotations:
        payload.extend(b"\x00")
        encode_ulong(payload, 0x00000071)
        encode_map(payload, message.delivery_annotations)

    # Message Annotations
    if isinstance(message, Message) and message.message_annotations:
        payload.extend(b"\x00")
        encode_ulong(payload, 0x00000072)
        encode_map(payload, message.message_annotations)

    # Properties
    if message.properties:
        payload.extend(_encode_performative_body(message.properties))

    # Application Properties
    if message.application_properties:
        payload.extend(b"\x00")
        encode_ulong(payload, 0x00000074)
        encode_map(payload, message.application_properties)

    # Body
    if message.body_type == MessageBodyType.DATA:
        payload.extend(b"\x00")
        encode_ulong(payload, 0x00000075)
        encode_binary(payload, message.data)  # type: ignore
    elif message.body_type == MessageBodyType.SEQUENCE:
        payload.extend(b"\x00")
        encode_ulong(payload, 0x00000076)
        encode_list(payload, message.sequence)  # type: ignore
    elif message.body_type == MessageBodyType.VALUE:
        payload.extend(b"\x00")
        encode_ulong(payload, 0x00000077)
        encode_unknown(payload, message.value)

    # Footer
    if isinstance(message, Message) and message.footer:
        payload.extend(b"\x00")
        encode_ulong(payload, 0x00000078)
        encode_map(payload, message.footer)

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
