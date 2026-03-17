from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from repid.data import Channel, SendOperation

if TYPE_CHECKING:
    from repid.asyncapi.models.common import OperationBindingsObject
    from repid.data import ExternalDocs, MessageSchema, SendOperation, Tag


class MessageRegistry:
    def __init__(self) -> None:
        self._operations: dict[str, SendOperation] = {}

    def get_operation(self, operation_id: str) -> SendOperation | None:
        """Get a registered operation by ID."""
        return self._operations.get(operation_id)

    def register_operation(
        self,
        *,
        operation_id: str,
        channel: Channel | str,
        title: str | None = None,
        summary: str | None = None,
        description: str | None = None,
        messages: Sequence[MessageSchema] | None = None,
        security: Sequence[Any] | None = None,
        tags: Sequence[Tag] | None = None,
        external_docs: ExternalDocs | None = None,
        bindings: OperationBindingsObject | None = None,
    ) -> None:
        """Register a new send operation."""
        self._operations[operation_id] = SendOperation(
            channel=channel if isinstance(channel, Channel) else Channel(address=channel),
            title=title,
            summary=summary,
            description=description,
            messages=tuple(messages) if messages is not None else (),
            security=tuple(security) if security is not None else (),
            tags=tuple(tags) if tags is not None else (),
            external_docs=external_docs,
            bindings=bindings,
        )
