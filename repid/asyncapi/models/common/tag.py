from __future__ import annotations

import sys
from typing import TYPE_CHECKING, TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required

if TYPE_CHECKING:
    from .external_docs import ExternalDocs
    from .reference import ReferenceModel


class Tag(TypedDict, total=False):
    name: Required[str]
    description: str
    externalDocs: ReferenceModel | ExternalDocs
