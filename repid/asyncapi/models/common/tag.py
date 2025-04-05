from __future__ import annotations

from typing import TypedDict

from typing_extensions import Required

from .external_docs import ExternalDocs
from .reference import ReferenceModel


class Tag(TypedDict, total=False):
    name: Required[str]
    description: str
    external_docs: ReferenceModel | ExternalDocs
