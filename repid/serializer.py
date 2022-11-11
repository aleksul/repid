from typing import Any, Protocol

import orjson

PYDANTIC_IMPORTED = True
try:
    import pydantic
except ImportError:  # pragma: no cover
    PYDANTIC_IMPORTED = False


class SerializerT(Protocol):
    def __call__(self, data: Any) -> str:
        ...


def default_serializer(data: Any) -> str:
    if PYDANTIC_IMPORTED:
        if isinstance(data, pydantic.BaseModel):
            return data.json()
    return orjson.dumps(data).decode()
