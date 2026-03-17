from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class License:
    name: str
    url: str | None = None
