import json

from repid._utils.json_encoder import JSON_ENCODER


class _ArgsBucketInMessageId:
    KEY = "__repid_payload_id"

    @classmethod
    def construct(cls, id_: str) -> str:
        return JSON_ENCODER.encode({cls.KEY: id_})

    @classmethod
    def check(cls, string: str) -> bool:
        return string.find(cls.KEY, 0, len(cls.KEY) + 3) != -1

    @classmethod
    def deconstruct(cls, string: str) -> str:
        return json.loads(string).get(cls.KEY)  # type: ignore[no-any-return]
