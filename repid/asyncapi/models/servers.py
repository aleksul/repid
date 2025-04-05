from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypedDict

from typing_extensions import Required

from .common import ExternalDocs, ReferenceModel, SecurityScheme, ServerBindingsObject, Tag


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
    protocol_version: str
    variables: Mapping[str, ReferenceModel | ServerVariable] | None
    security: Sequence[ReferenceModel | SecurityScheme]
    tags: Sequence[ReferenceModel | Tag]
    external_docs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | ServerBindingsObject
