from dataclasses import dataclass

from repid.connections.pubsub.protocol import proto

WIRE_TYPE_VARINT = 0
WIRE_TYPE_LENGTH_DELIMITED = 2


def _encode_tag(field_number: int, wire_type: int) -> bytes:
    return proto._encode_varint((field_number << 3) | wire_type)


def _encode_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return proto._encode_varint(len(encoded)) + encoded


@dataclass(slots=True)
class Topic:
    name: str = ""

    def serialize(self) -> bytes:
        parts: list[bytes] = []
        if self.name:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.name))
        return b"".join(parts)


@dataclass(slots=True)
class Subscription:
    name: str = ""
    topic: str = ""
    ack_deadline_seconds: int = 0

    def serialize(self) -> bytes:
        parts: list[bytes] = []
        if self.name:
            parts.append(_encode_tag(1, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.name))
        if self.topic:
            parts.append(_encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            parts.append(_encode_string(self.topic))
        if self.ack_deadline_seconds:
            parts.append(_encode_tag(5, WIRE_TYPE_VARINT))
            parts.append(proto._encode_varint(self.ack_deadline_seconds))
        return b"".join(parts)
