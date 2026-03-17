from typing import Any, Protocol

from repid._utils import JSON_ENCODER, is_installed

if is_installed("pydantic"):
    import pydantic


class SerializerT(Protocol):
    def __call__(self, data: Any) -> bytes: ...


def default_serializer(data: Any) -> bytes:
    if is_installed("pydantic") and isinstance(data, pydantic.BaseModel):
        if is_installed("pydantic", ">=2.0.0,<3.0.0"):
            return data.model_dump_json().encode()
        return data.json().encode()  # pragma: no cover
    return JSON_ENCODER.encode(data).encode()
