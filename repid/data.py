from enum import Enum
from typing import Any, Dict, Optional, Tuple, Union

import msgspec


class PrioritiesT(Enum):
    HIGH = "hi"
    MEDIUM = "me"
    LOW = "lo"


class StructWithParams(msgspec.Struct, tag=True, array_like=True):
    pass


class Timestamp(msgspec.Struct):
    timestamp: int
    ttl: Optional[int] = None


class Message(StructWithParams, Timestamp):
    id_: str
    topic: str  # the same as actor's & job's name
    queue: str = "default"
    priority: PrioritiesT = PrioritiesT.MEDIUM
    retries_left: int = 1
    bucket: Union[str, "ArgsBucket", None] = None  # bucket_id or bucket itself
    result_id: Optional[str] = None
    result_ttl: Optional[int] = None
    timeout: int = 600


class DeferredMessage(Message):
    delay_until: int


class DeferredByMessage(DeferredMessage):
    defer_by: int


class DeferredCronMessage(DeferredMessage):
    cron: str


AnyMessageT = Union[Message, DeferredMessage, DeferredByMessage, DeferredCronMessage]


class Bucket(StructWithParams, Timestamp):
    id_: str


class ArgsBucket(Bucket):
    args: Optional[Tuple] = None
    kwargs: Optional[Dict] = None


class ResultBucket(Bucket):
    data: Any
    success: bool
    started_when: int
    finished_when: int
    exception: Optional[str] = None


AnyBucketT = Union[ArgsBucket, ResultBucket]

AnySerializableT = Union[
    Message, DeferredMessage, DeferredByMessage, DeferredCronMessage, ArgsBucket, ResultBucket
]


class Serializer:
    decoder = msgspec.msgpack.Decoder(type=AnySerializableT)  # type: ignore[call-overload]
    encoder = msgspec.msgpack.Encoder()

    @classmethod
    def encode(cls, obj: Any) -> bytes:
        return cls.encoder.encode(obj)

    @classmethod
    def decode(cls, data: bytes) -> AnySerializableT:
        return cls.decoder.decode(data)  # type: ignore[no-any-return]
