from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypedDict

from .common import ChannelBindingsObject, ExternalDocs, MessageObject, ReferenceModel, Tag


class Parameter(TypedDict, total=False):
    description: str
    enum: Sequence[str]
    default: str
    examples: Sequence[str]
    location: str


Parameters = Mapping[str, ReferenceModel | Parameter] | None


ChannelMessages = Mapping[str, ReferenceModel | MessageObject] | None


class Channel(TypedDict, total=False):
    address: str | None
    messages: ChannelMessages
    parameters: Parameters
    title: str
    summary: str
    description: str
    servers: Sequence[ReferenceModel]
    tags: Sequence[ReferenceModel | Tag]
    external_docs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | ChannelBindingsObject
