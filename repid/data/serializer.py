from typing import Any

import msgspec


class Serializer:
    decoder = msgspec.msgpack.Decoder(type=AnySerializableT)  # type: ignore[call-overload]
    encoder = msgspec.msgpack.Encoder()

    @classmethod
    def encode(cls, obj: Any) -> bytes:
        return cls.encoder.encode(obj)

    @classmethod
    def decode(cls, data: bytes) -> AnySerializableT:
        return cls.decoder.decode(data)  # type: ignore[no-any-return]
