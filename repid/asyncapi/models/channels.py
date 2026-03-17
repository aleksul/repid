from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from .common import ChannelBindingsObject, ExternalDocs, MessageObject, ReferenceModel, Tag


class ChannelParameter(TypedDict, total=False):
    description: str
    enum: Sequence[str]
    default: str
    examples: Sequence[str]
    location: str


class Channel(TypedDict, total=False):
    address: str | None
    parameters: Mapping[  # describes parameters included in the channel address
        str,
        ReferenceModel | ChannelParameter,
    ]
    messages: Mapping[str, ReferenceModel | MessageObject]
    title: str
    summary: str
    description: str
    servers: Sequence[ReferenceModel]
    tags: Sequence[ReferenceModel | Tag]
    externalDocs: ReferenceModel | ExternalDocs
    bindings: ReferenceModel | ChannelBindingsObject
