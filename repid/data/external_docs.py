from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class ExternalDocs:
    url: str
    description: str | None = None
