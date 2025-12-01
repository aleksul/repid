"""Manual protobuf serialization for Pub/Sub messages.

This module provides dataclasses that serialize to/from protobuf wire format
without requiring generated stubs or the protobuf library.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import Self


# Wire type constants
WIRE_TYPE_VARINT = 0
WIRE_TYPE_FIXED64 = 1
WIRE_TYPE_LENGTH_DELIMITED = 2
WIRE_TYPE_FIXED32 = 5


def _encode_varint(value: int) -> bytes:
    """Encode an integer as a varint."""
    if value < 0:
        # Handle negative numbers as unsigned 64-bit
        value = value & 0xFFFFFFFFFFFFFFFF
    parts = []
    while value > 0x7F:  # noqa: PLR2004
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value)
    return bytes(parts) if parts else b"\x00"


def _decode_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Decode a varint from bytes, returning (value, new_position)."""
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError("Truncated varint")
        byte = data[pos]
        result |= (byte & 0x7F) << shift
        pos += 1
        if not (byte & 0x80):
            break
        shift += 7
    return result, pos


def _encode_tag(field_number: int, wire_type: int) -> bytes:
    """Encode a field tag."""
    return _encode_varint((field_number << 3) | wire_type)


def _decode_tag(data: bytes, pos: int) -> tuple[int, int, int]:
    """Decode a field tag, returning (field_number, wire_type, new_position)."""
    tag, pos = _decode_varint(data, pos)
    return tag >> 3, tag & 0x07, pos


def _encode_string(value: str) -> bytes:
    """Encode a string as length-delimited bytes."""
    encoded = value.encode("utf-8")
    return _encode_varint(len(encoded)) + encoded


def _encode_bytes(value: bytes) -> bytes:
    """Encode bytes as length-delimited."""
    return _encode_varint(len(value)) + value


def _decode_length_delimited(data: bytes, pos: int) -> tuple[bytes, int]:
    """Decode length-delimited bytes."""
    length, pos = _decode_varint(data, pos)
    if pos + length > len(data):
        raise ValueError("Truncated length-delimited field")
    return data[pos : pos + length], pos + length


def _skip_field(data: bytes, pos: int, wire_type: int) -> int:
    """Skip an unknown field."""
    if wire_type == WIRE_TYPE_VARINT:
        _, pos = _decode_varint(data, pos)
    elif wire_type == WIRE_TYPE_FIXED64:
        pos += 8
    elif wire_type == WIRE_TYPE_LENGTH_DELIMITED:
        length, pos = _decode_varint(data, pos)
        pos += length
    elif wire_type == WIRE_TYPE_FIXED32:
        pos += 4
    else:
        raise ValueError(f"Unknown wire type: {wire_type}")
    return pos


def _encode_timestamp(seconds: int, nanos: int) -> bytes:
    """Encode a google.protobuf.Timestamp message."""
    parts = []
    if seconds != 0:
        parts.append(_encode_tag(1, WIRE_TYPE_VARINT))
        parts.append(_encode_varint(seconds))
    if nanos != 0:
        parts.append(_encode_tag(2, WIRE_TYPE_VARINT))
        parts.append(_encode_varint(nanos))
    return b"".join(parts)


def _decode_timestamp(data: bytes) -> tuple[int, int]:
    """Decode a google.protobuf.Timestamp message, returning (seconds, nanos)."""
    seconds = 0
    nanos = 0
    pos = 0
    while pos < len(data):
        field_num, wire_type, pos = _decode_tag(data, pos)
        if field_num == 1 and wire_type == WIRE_TYPE_VARINT:
            seconds, pos = _decode_varint(data, pos)
        elif field_num == 2 and wire_type == WIRE_TYPE_VARINT:  # noqa: PLR2004
            nanos, pos = _decode_varint(data, pos)
        else:
            pos = _skip_field(data, pos, wire_type)
    return seconds, nanos


@dataclass(slots=True)
class PubsubMessage:
    """A Pub/Sub message for publishing or receiving."""

    data: bytes = b""
    attributes: dict[str, str] = field(default_factory=dict)
    message_id: str = ""
    publish_time_seconds: int = 0
    publish_time_nanos: int = 0
    ordering_key: str = ""

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: data (bytes)
        if self.data:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_bytes(self.data))

        # Field 2: attributes (map<string, string>)
        for key, value in self.attributes.items():
            entry = (
                _encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED)
                + _encode_string(key)
                + _encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED)
                + _encode_string(value)
            )
            parts.append(_encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_bytes(entry))

        # Field 3: message_id (string)
        if self.message_id:
            parts.append(_encode_tag(3, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.message_id))

        # Field 4: publish_time (Timestamp)
        if self.publish_time_seconds or self.publish_time_nanos:
            ts_data = _encode_timestamp(self.publish_time_seconds, self.publish_time_nanos)
            parts.append(_encode_tag(4, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_bytes(ts_data))

        # Field 5: ordering_key (string)
        if self.ordering_key:
            parts.append(_encode_tag(5, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.ordering_key))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        msg = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                msg.data, pos = _decode_length_delimited(data, pos)
            elif field_num == 2 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                entry_data, pos = _decode_length_delimited(data, pos)
                key, value = _decode_map_entry(entry_data)
                msg.attributes[key] = value
            elif field_num == 3 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                raw, pos = _decode_length_delimited(data, pos)
                msg.message_id = raw.decode("utf-8")
            elif field_num == 4 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                ts_data, pos = _decode_length_delimited(data, pos)
                msg.publish_time_seconds, msg.publish_time_nanos = _decode_timestamp(ts_data)
            elif field_num == 5 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                raw, pos = _decode_length_delimited(data, pos)
                msg.ordering_key = raw.decode("utf-8")
            else:
                pos = _skip_field(data, pos, wire_type)

        return msg


def _decode_map_entry(data: bytes) -> tuple[str, str]:
    """Decode a map entry with string key and value."""
    key = ""
    value = ""
    pos = 0
    while pos < len(data):
        field_num, wire_type, pos = _decode_tag(data, pos)
        if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
            raw, pos = _decode_length_delimited(data, pos)
            key = raw.decode("utf-8")
        elif field_num == 2 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
            raw, pos = _decode_length_delimited(data, pos)
            value = raw.decode("utf-8")
        else:
            pos = _skip_field(data, pos, wire_type)
    return key, value


@dataclass(slots=True)
class PublishRequest:
    """Request for the Publish method."""

    topic: str = ""
    messages: list[PubsubMessage] = field(default_factory=list)

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: topic (string)
        if self.topic:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.topic))

        # Field 2: messages (repeated PubsubMessage)
        for msg in self.messages:
            msg_data = msg.serialize()
            parts.append(_encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_bytes(msg_data))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        req = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                raw, pos = _decode_length_delimited(data, pos)
                req.topic = raw.decode("utf-8")
            elif field_num == 2 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                msg_data, pos = _decode_length_delimited(data, pos)
                req.messages.append(PubsubMessage.deserialize(msg_data))
            else:
                pos = _skip_field(data, pos, wire_type)

        return req


@dataclass(slots=True)
class PublishResponse:
    """Response for the Publish method."""

    message_ids: list[str] = field(default_factory=list)

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: message_ids (repeated string)
        for msg_id in self.message_ids:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(msg_id))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        resp = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                raw, pos = _decode_length_delimited(data, pos)
                resp.message_ids.append(raw.decode("utf-8"))
            else:
                pos = _skip_field(data, pos, wire_type)

        return resp


@dataclass(slots=True)
class ReceivedMessage:
    """A message received from a subscription."""

    ack_id: str = ""
    message: PubsubMessage | None = None
    delivery_attempt: int = 0

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: ack_id (string)
        if self.ack_id:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.ack_id))

        # Field 2: message (PubsubMessage)
        if self.message is not None:
            msg_data = self.message.serialize()
            parts.append(_encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_bytes(msg_data))

        # Field 3: delivery_attempt (int32)
        if self.delivery_attempt:
            parts.append(_encode_tag(3, WIRE_TYPE_VARINT))
            parts.append(_encode_varint(self.delivery_attempt))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        msg = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                raw, pos = _decode_length_delimited(data, pos)
                msg.ack_id = raw.decode("utf-8")
            elif field_num == 2 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                msg_data, pos = _decode_length_delimited(data, pos)
                msg.message = PubsubMessage.deserialize(msg_data)
            elif field_num == 3 and wire_type == WIRE_TYPE_VARINT:  # noqa: PLR2004
                msg.delivery_attempt, pos = _decode_varint(data, pos)
            else:
                pos = _skip_field(data, pos, wire_type)

        return msg


@dataclass(slots=True)
class AcknowledgeRequest:
    """Request for the Acknowledge method."""

    subscription: str = ""
    ack_ids: list[str] = field(default_factory=list)

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: subscription (string)
        if self.subscription:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.subscription))

        # Field 2: ack_ids (repeated string)
        for ack_id in self.ack_ids:
            parts.append(_encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(ack_id))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        req = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                raw, pos = _decode_length_delimited(data, pos)
                req.subscription = raw.decode("utf-8")
            elif field_num == 2 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                raw, pos = _decode_length_delimited(data, pos)
                req.ack_ids.append(raw.decode("utf-8"))
            else:
                pos = _skip_field(data, pos, wire_type)

        return req


@dataclass(slots=True)
class ModifyAckDeadlineRequest:
    """Request for the ModifyAckDeadline method."""

    subscription: str = ""
    ack_ids: list[str] = field(default_factory=list)
    ack_deadline_seconds: int = 0

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: subscription (string)
        if self.subscription:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.subscription))

        # Field 3: ack_deadline_seconds (int32) - Note: field 3, not 2
        if self.ack_deadline_seconds:
            parts.append(_encode_tag(3, WIRE_TYPE_VARINT))
            parts.append(_encode_varint(self.ack_deadline_seconds))

        # Field 4: ack_ids (repeated string) - Note: field 4, not 2
        for ack_id in self.ack_ids:
            parts.append(_encode_tag(4, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(ack_id))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        req = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                raw, pos = _decode_length_delimited(data, pos)
                req.subscription = raw.decode("utf-8")
            elif field_num == 3 and wire_type == WIRE_TYPE_VARINT:  # noqa: PLR2004
                req.ack_deadline_seconds, pos = _decode_varint(data, pos)
            elif field_num == 4 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                raw, pos = _decode_length_delimited(data, pos)
                req.ack_ids.append(raw.decode("utf-8"))
            else:
                pos = _skip_field(data, pos, wire_type)

        return req


@dataclass(slots=True)
class StreamingPullRequest:
    """Request for the StreamingPull streaming RPC method."""

    subscription: str = ""
    ack_ids: list[str] = field(default_factory=list)
    modify_deadline_seconds: list[int] = field(default_factory=list)
    modify_deadline_ack_ids: list[str] = field(default_factory=list)
    stream_ack_deadline_seconds: int = 0
    client_id: str = ""
    max_outstanding_messages: int = 0
    max_outstanding_bytes: int = 0

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: subscription (string)
        if self.subscription:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.subscription))

        # Field 2: ack_ids (repeated string)
        for ack_id in self.ack_ids:
            parts.append(_encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(ack_id))

        # Field 3: modify_deadline_seconds (repeated int32)
        for seconds in self.modify_deadline_seconds:
            parts.append(_encode_tag(3, WIRE_TYPE_VARINT))
            parts.append(_encode_varint(seconds))

        # Field 4: modify_deadline_ack_ids (repeated string)
        for ack_id in self.modify_deadline_ack_ids:
            parts.append(_encode_tag(4, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(ack_id))

        # Field 5: stream_ack_deadline_seconds (int32)
        if self.stream_ack_deadline_seconds:
            parts.append(_encode_tag(5, WIRE_TYPE_VARINT))
            parts.append(_encode_varint(self.stream_ack_deadline_seconds))

        # Field 6: client_id (string)
        if self.client_id:
            parts.append(_encode_tag(6, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.client_id))

        # Field 7: max_outstanding_messages (int64)
        if self.max_outstanding_messages:
            parts.append(_encode_tag(7, WIRE_TYPE_VARINT))
            parts.append(_encode_varint(self.max_outstanding_messages))

        # Field 8: max_outstanding_bytes (int64)
        if self.max_outstanding_bytes:
            parts.append(_encode_tag(8, WIRE_TYPE_VARINT))
            parts.append(_encode_varint(self.max_outstanding_bytes))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        req = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                raw, pos = _decode_length_delimited(data, pos)
                req.subscription = raw.decode("utf-8")
            elif field_num == 2 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                raw, pos = _decode_length_delimited(data, pos)
                req.ack_ids.append(raw.decode("utf-8"))
            elif field_num == 3 and wire_type == WIRE_TYPE_VARINT:  # noqa: PLR2004
                val, pos = _decode_varint(data, pos)
                req.modify_deadline_seconds.append(val)
            elif field_num == 4 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                raw, pos = _decode_length_delimited(data, pos)
                req.modify_deadline_ack_ids.append(raw.decode("utf-8"))
            elif field_num == 5 and wire_type == WIRE_TYPE_VARINT:  # noqa: PLR2004
                req.stream_ack_deadline_seconds, pos = _decode_varint(data, pos)
            elif field_num == 6 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:  # noqa: PLR2004
                raw, pos = _decode_length_delimited(data, pos)
                req.client_id = raw.decode("utf-8")
            elif field_num == 7 and wire_type == WIRE_TYPE_VARINT:  # noqa: PLR2004
                req.max_outstanding_messages, pos = _decode_varint(data, pos)
            elif field_num == 8 and wire_type == WIRE_TYPE_VARINT:  # noqa: PLR2004
                req.max_outstanding_bytes, pos = _decode_varint(data, pos)
            else:
                pos = _skip_field(data, pos, wire_type)

        return req


@dataclass(slots=True)
class StreamingPullResponse:
    """Response for the StreamingPull method."""

    received_messages: list[ReceivedMessage] = field(default_factory=list)
    # Note: We skip acknowledge_confirmation, modify_ack_deadline_confirmation,
    # and subscription_properties for simplicity - add if needed

    def serialize(self) -> bytes:
        """Serialize to protobuf wire format."""
        parts: list[bytes] = []

        # Field 1: received_messages (repeated ReceivedMessage)
        for msg in self.received_messages:
            msg_data = msg.serialize()
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_bytes(msg_data))

        return b"".join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize from protobuf wire format."""
        resp = cls()
        pos = 0
        while pos < len(data):
            field_num, wire_type, pos = _decode_tag(data, pos)

            if field_num == 1 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                msg_data, pos = _decode_length_delimited(data, pos)
                resp.received_messages.append(ReceivedMessage.deserialize(msg_data))
            else:
                pos = _skip_field(data, pos, wire_type)

        return resp
