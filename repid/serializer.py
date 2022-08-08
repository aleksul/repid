from typing import Any, Tuple

from orjson import dumps, loads

from repid.data import (
    AnyBucketT,
    ArgsBucket,
    ArgsBucketMetadata,
    Message,
    PrioritiesT,
    ResultBucket,
    ResultBucketMetadata,
    SimpleArgsBucket,
)


class MessageSerializer:
    @classmethod
    def encode(cls, obj: Message) -> bytes:
        return dumps(obj)

    @staticmethod
    def __transform_values(kv: Tuple[Any, Any]) -> Tuple[Any, Any]:
        if isinstance(kv[0], str):
            if kv[0] == "args_bucket" and isinstance(kv[1], dict):
                try:
                    return (kv[0], ArgsBucketMetadata(**kv[1]))
                except TypeError:
                    pass
                try:
                    return (kv[0], SimpleArgsBucket(**kv[1]))
                except TypeError:
                    pass
                return kv
            if kv[0] == "result_bucket" and isinstance(kv[1], dict):
                try:
                    return (kv[0], ResultBucketMetadata(**kv[1]))
                except TypeError:
                    pass
                return kv
            if kv[0] == "priority" and isinstance(kv[1], int):
                return (kv[0], PrioritiesT(kv[1]))
        return kv

    @classmethod
    def decode(cls, data: bytes) -> Message:
        decoded = loads(data)
        if not isinstance(decoded, dict):
            raise ValueError(f"Can't decode data to Message: {data!r}.")
        decoded = dict(map(cls.__transform_values, decoded.items()))
        return Message(**decoded)


class BucketSerializer:
    @staticmethod
    def encode(obj: AnyBucketT) -> bytes:
        return dumps(obj)

    @staticmethod
    def decode(data: bytes) -> AnyBucketT:
        decoded = loads(data)
        if not isinstance(decoded, dict):
            raise ValueError(f"Can't decode data to Bucket: {data!r}.")
        try:
            return ArgsBucket(**decoded)
        except TypeError:
            pass
        try:
            return ResultBucket(**decoded)
        except TypeError:
            pass
        raise ValueError(f"Can't decode data to Bucket: {data!r}.")
