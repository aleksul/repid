from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from typing_extensions import Required

from .bindings import MessageBindingsObject
from .correlation_id import CorrelationId
from .external_docs import ExternalDocs
from .reference import ReferenceModel
from .tag import Tag

AnySchema = Any


class MessageExampleObject1(TypedDict, total=False):
    name: str
    summary: str
    headers: Mapping[str, Any]
    payload: Required[Any]


class MessageExampleObject2(TypedDict, total=False):
    name: str
    summary: str
    headers: Required[Mapping[str, Any]]
    payload: Any


MessageExampleObject = MessageExampleObject1 | MessageExampleObject2


class MessageTrait(TypedDict, total=False):
    content_type: str
    headers: AnySchema
    correlation_id: ReferenceModel | CorrelationId
    tags: Sequence[ReferenceModel | Tag]
    summary: str
    name: str
    title: str
    description: str
    external_docs: ReferenceModel | ExternalDocs
    deprecated: bool
    examples: Sequence[MessageExampleObject]
    bindings: ReferenceModel | MessageBindingsObject


class MessageObject(TypedDict, total=False):
    content_type: str
    headers: AnySchema
    payload: AnySchema
    correlation_id: ReferenceModel | CorrelationId
    tags: Sequence[ReferenceModel | Tag]
    summary: str
    name: str
    title: str
    description: str
    external_docs: ReferenceModel | ExternalDocs
    deprecated: bool
    examples: Sequence[MessageExampleObject]
    bindings: ReferenceModel | MessageBindingsObject
    traits: Sequence[
        ReferenceModel | MessageTrait | Sequence[ReferenceModel | MessageTrait | Mapping[str, Any]]
    ]
