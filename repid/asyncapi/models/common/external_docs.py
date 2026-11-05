from __future__ import annotations

from typing import Required, TypedDict


class ExternalDocs(TypedDict, total=False):
    description: str
    url: Required[str]
