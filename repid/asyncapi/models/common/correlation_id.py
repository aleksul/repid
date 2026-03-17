import sys
from typing import TypedDict

if sys.version_info >= (3, 11):  # pragma: no cover
    from typing import Required
else:  # pragma: no cover
    from typing_extensions import Required


class CorrelationId(TypedDict, total=False):
    location: Required[str]
    description: str
