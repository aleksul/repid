from dataclasses import asdict, is_dataclass
from typing import Any, Protocol

from repid.utils import JSON_ENCODER, is_installed

if PYDANTIC_IMPORTED := is_installed("pydantic"):
    import pydantic


class SerializerT(Protocol):
    def __call__(self, data: Any) -> str:
        ...


def default_serializer(data: Any) -> str:
    if PYDANTIC_IMPORTED and isinstance(data, pydantic.BaseModel):
        return data.json()
    if is_dataclass(data) and not isinstance(data, type):
        return JSON_ENCODER.encode(asdict(data))
    return JSON_ENCODER.encode(data)
