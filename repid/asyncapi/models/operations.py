from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required

if TYPE_CHECKING:
    from .common import ExternalDocs, OperationBindingsObject, ReferenceModel, Tag


class OperationReplyAddress(TypedDict, total=False):
    location: Required[str]
    description: str


class OperationReply(TypedDict, total=False):
    address: ReferenceModel | OperationReplyAddress
    channel: ReferenceModel
    messages: Sequence[ReferenceModel]


class OperationTrait(TypedDict, total=False):
    title: str
    summary: str
    description: str
    security: Sequence[ReferenceModel | Any]
    tags: Sequence[ReferenceModel | Tag]
    externalDocs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | OperationBindingsObject


class Operation(TypedDict, total=False):
    action: Required[Literal["send", "receive"]]
    channel: Required[ReferenceModel]
    messages: Sequence[ReferenceModel]
    reply: ReferenceModel | OperationReply
    traits: Sequence[ReferenceModel | OperationTrait]
    title: str
    summary: str
    description: str
    security: Sequence[ReferenceModel | Any]
    tags: Sequence[ReferenceModel | Tag]
    externalDocs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | OperationBindingsObject
