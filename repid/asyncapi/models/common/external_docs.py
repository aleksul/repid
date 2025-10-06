from __future__ import annotations

import sys
from typing import TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required


class ExternalDocs(TypedDict, total=False):
    description: str
    url: Required[str]
