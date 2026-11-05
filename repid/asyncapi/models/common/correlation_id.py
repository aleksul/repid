from typing import Required, TypedDict


class CorrelationId(TypedDict, total=False):
    location: Required[str]
    description: str
