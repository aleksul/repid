from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from repid.asyncapi.models.common import MessageBindingsObject

    from .external_docs import ExternalDocs
    from .tag import Tag


JsonSchemaT = dict[str, Any]


@dataclass(frozen=True, kw_only=True, slots=True)
class CorrelationId:
    location: str
    description: str | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class MessageExample:
    name: str | None = None
    summary: str | None = None
    headers: dict[str, Any] | None = None
    payload: Any | None = None

    def __post_init__(self) -> None:
        if self.headers is None and self.payload is None:
            raise ValueError("Either headers or payload must be provided.")


@dataclass(frozen=True, kw_only=True, slots=True)
class MessageSchema:
    name: str
    title: str | None = None
    summary: str | None = None
    description: str | None = None
    payload: JsonSchemaT | None = None
    headers: JsonSchemaT | None = None
    correlation_id: CorrelationId | None = None
    content_type: str | None = None
    security: tuple[Any, ...] | None = None
    tags: tuple[Tag, ...] | None = None
    external_docs: ExternalDocs | None = None
    deprecated: bool = False
    examples: tuple[MessageExample, ...] | None = None
    bindings: MessageBindingsObject | None = None
