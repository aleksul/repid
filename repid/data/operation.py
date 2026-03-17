from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from repid.asyncapi.models.common.bindings import OperationBindingsObject

    from .channel import Channel
    from .external_docs import ExternalDocs
    from .message_schema import MessageSchema
    from .tag import Tag


@dataclass(frozen=True, kw_only=True, slots=True)
class SendOperation:
    channel: Channel
    messages: tuple[MessageSchema, ...] = ()
    title: str | None = None
    summary: str | None = None
    description: str | None = None
    security: tuple[Any, ...] = ()
    tags: tuple[Tag, ...] = ()
    external_docs: ExternalDocs | None = None
    bindings: OperationBindingsObject | None = None
