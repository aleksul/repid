from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class MessageData:
    payload: bytes
    headers: dict[str, str] | None = None
    content_type: str | None = None
