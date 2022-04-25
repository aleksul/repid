from enum import Enum
from typing import Any, Optional, Union

import msgspec


class PrioritiesT(Enum):
    HIGH = "hi"
    MEDIUM = "me"
    LOW = "lo"


class StructWithParams(msgspec.Struct, tag=True, omit_defaults=True):  # type: ignore[call-arg]
    pass


class Timestamp(msgspec.Struct):
    timestamp: int
    ttl: Optional[int] = None


class Message(StructWithParams, Timestamp):
    id_: str
    actor_name: str
    queue: str
    priority: PrioritiesT
    retries_left: int = 1
    actor_timeout: int = 600


class DeferredMessage(Message):
    delay_until: int
    defer_by: Optional[int] = None


AnyMessageT = Union[Message, DeferredMessage]


class Bucket(StructWithParams, Timestamp):
    id_: str
    data: Any


class ResultBucket(Bucket):
    success: bool
    started_when: int
    finished_when: int
    exception: Optional[str] = None


AnyBucketT = Union[Bucket, ResultBucket]

AnySerializableT = Union[Message, DeferredMessage, Bucket, ResultBucket]


class Serializer:
    decoder = msgspec.msgpack.Decoder(type=AnySerializableT)  # type: ignore[call-overload]
    encoder = msgspec.msgpack.Encoder()

    @classmethod
    def encode(cls, obj: Any) -> bytes:
        return cls.encoder.encode(obj)

    @classmethod
    def decode(cls, data: bytes) -> AnySerializableT:
        return cls.decoder.decode(data)
