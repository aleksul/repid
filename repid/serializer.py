from orjson import dumps, loads

from repid.data import AnyBucketT, ArgsBucket, Message, ResultBucket


class MessageSerializer:
    @classmethod
    def encode(cls, obj: Message) -> bytes:
        return dumps(obj)

    @classmethod
    def decode(cls, data: bytes) -> Message:
        return Message(**loads(data))


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
