from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required

if TYPE_CHECKING:
    from .common import ExternalDocs, ReferenceModel, ServerBindingsObject, Tag


class ServerVariable(TypedDict, total=False):
    enum: Sequence[str]
    default: str
    description: str
    examples: Sequence[str]


class Server(TypedDict, total=False):
    host: Required[str]
    pathname: str
    title: str
    summary: str
    description: str
    protocol: Required[str]
    protocolVersion: str
    variables: Mapping[str, ReferenceModel | ServerVariable] | None
    security: Sequence[ReferenceModel | Any]
    tags: Sequence[ReferenceModel | Tag]
    externalDocs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | ServerBindingsObject
