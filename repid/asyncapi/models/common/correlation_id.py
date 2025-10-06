import sys
from typing import TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required


class CorrelationId(TypedDict, total=False):
    location: Required[str]
    description: str
