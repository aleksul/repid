from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class Message:
    payload: bytes
    headers: dict[str, str] | None = None
    content_type: str | None = None
