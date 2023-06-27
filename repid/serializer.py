from dataclasses import asdict, is_dataclass
from typing import Any, Protocol

from repid._utils import JSON_ENCODER, is_installed

if PYDANTIC_IMPORTED := is_installed("pydantic"):
    import pydantic

    PYDANTIC_V2 = is_installed("pydantic", ">=2.0.0a1,<3.0.0")


class SerializerT(Protocol):
    def __call__(self, data: Any) -> str:
        ...


def default_serializer(data: Any) -> str:
    if PYDANTIC_IMPORTED and isinstance(data, pydantic.BaseModel):
        if PYDANTIC_V2:
            return data.model_dump_json()
        return data.json()  # pragma: no cover
    if is_dataclass(data) and not isinstance(data, type):
        return JSON_ENCODER.encode(asdict(data))
    return JSON_ENCODER.encode(data)
