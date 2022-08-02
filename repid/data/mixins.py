from typing import Optional

import msgspec


class StructWithParams(msgspec.Struct, tag=True, array_like=True):
    pass


class Timestamp(msgspec.Struct):
    timestamp: int
    ttl: Optional[int] = None
