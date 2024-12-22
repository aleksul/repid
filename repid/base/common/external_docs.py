from __future__ import annotations

from typing import TypedDict

from typing_extensions import Required


class ExternalDocs(TypedDict, total=False):
    description: str
    url: Required[str]
