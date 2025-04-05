from typing import TypedDict

from typing_extensions import Required


class CorrelationId(TypedDict, total=False):
    location: Required[str]
    description: str
