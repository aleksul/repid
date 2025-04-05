from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, TypedDict

from typing_extensions import Required

from .common import ExternalDocs, OperationBindingsObject, ReferenceModel, SecurityScheme, Tag


class OperationReplyAddress(TypedDict, total=False):
    location: Required[str]
    description: str


class OperationReply(TypedDict, total=False):
    address: ReferenceModel | OperationReplyAddress
    channel: ReferenceModel
    messages: Sequence[ReferenceModel]


Title = str
Summary = str
Description = str
SecurityRequirements = Sequence[ReferenceModel | SecurityScheme]
Security = SecurityRequirements
Tags = Sequence[ReferenceModel | Tag]
ExternalDocsModel = ReferenceModel | ExternalDocs


class OperationTrait(TypedDict, total=False):
    title: Title
    summary: Summary
    description: Description
    security: Security
    tags: Tags
    external_docs: ExternalDocsModel
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
    security: SecurityRequirements
    tags: Sequence[ReferenceModel | Tag]
    external_docs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | OperationBindingsObject
