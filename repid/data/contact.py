from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class Contact:
    name: str | None = None
    url: str | None = None
    email: str | None = None
