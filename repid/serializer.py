from dataclasses import asdict, is_dataclass
from typing import Any, Protocol

from repid.utils import JSON_ENCODER

PYDANTIC_IMPORTED = True
try:
    import pydantic
except ImportError:  # pragma: no cover
    PYDANTIC_IMPORTED = False


class SerializerT(Protocol):
    def __call__(self, data: Any) -> str:
        ...


def default_serializer(data: Any) -> str:
    if PYDANTIC_IMPORTED and isinstance(data, pydantic.BaseModel):
        return data.json()
    if is_dataclass(data) and not isinstance(data, type):
        return JSON_ENCODER.encode(asdict(data))
    return JSON_ENCODER.encode(data)
